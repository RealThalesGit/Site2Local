"""Site2Local: main proxy route — orchestrates guards, fetch, recovery, fallbacks.

The old 896-line proxy() body was split by phase (match: s2l.py's single
def proxy() at line ~12499). This file keeps only the pipeline flow; each
phase lives in its own module inside Proxy/Routes/.
"""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from ... import Runtime as _s2l_state
from ..TunnelRoutes import tcp_ext, udp_ext, ws_ext  # noqa: F401 (compat re-export)
from .Guards import _guard_preflight, _offline_response
from .Fetch import (
    _decide_streaming,
    _fetch_with_retries,
    _push_redirect_history,
    _maybe_stream,
)
from .Recovery import (
    _consume_upstream_body,
    _waf_block_retry,
    _handle_304,
    _recover_empty_body,
    _final_blank_page_recovery,
)
from .Fallbacks import _cdn_fallback, _rsc_refetch, _bot_page_response


def _save_and_enqueue(target: str, ctx, method: str) -> None:
    """Queue the raw body for the disk cache and feed the discovery workers."""
    # Save raw body to disk BEFORE any hook/rewrite so the cache always holds
    # the real upstream response. Hooks re-run on every cache hit (see _cached_response),
    # so the browser always gets the hooked version even when served from cache.
    # Exception: don't save if SAVE_ERRORS=False and status >= 400.
    _should_save = (
        method == "GET"
        and not _s2l_state._is_wire_payload(ctx.resp_body)
        and ctx.resp_body
        and (ctx.resp_status < 400 or _s2l_state.SAVE_ERRORS)
    )
    if _should_save:
        # 4th element = device class of THIS request → the save worker maps
        # it to the HTML variant tag, so a phone's first visit caches
        # index__s2l_mobile.html and leaves the canonical desktop file
        # (and any user mods in it) untouched.
        _s2l_state.save_queue.put((target, ctx.resp_body, ctx.resp_ct, _s2l_state._effective_device()))
        # Mark this URL as visited so the crawler doesn't fetch it again.
        # The proxy already fetched it — re-fetching is wasteful and creates
        # duplicate log entries. enqueue() still runs for link extraction
        # but _crawl() will skip it immediately via the visited set.
        _norm_target = _s2l_state.normalize_url(target)
        with _s2l_state.visited_lock:
            _s2l_state.visited.add(_norm_target)
        _s2l_state.enqueue(target)
        # Extract links from served HTML so the discovery workers pre-cache
        # sub-pages and assets — this is what makes the FIRST page load work
        # fully without needing a reload to trigger crawling.
        # Only in bulk mode (CRAWL=True) — in procedural mode (CRAWL=False)
        # there are no discovery workers to process the enqueued links.
        if _s2l_state.CRAWL and _s2l_state.is_html(ctx.resp_body, ctx.resp_ct) and not _s2l_state.OFFLINE:
            # Use a bounded pool — one thread per page would exhaust the OS
            # thread limit on large sites with hundreds of pages.
            try:
                _s2l_state._link_pool.submit(_s2l_state._extract_links_async, ctx.resp_body, target)
            except Exception:
                pass


def _rewrite_resp_body(ctx) -> None:
    """HTML + CSS + JSON post-processing (CDN rewrites, absolute URLs, SW clear)."""
    _is_html = _s2l_state.is_html(ctx.resp_body, ctx.resp_ct)
    _is_css  = "text/css" in ctx.resp_ct
    _is_json = "json" in ctx.resp_ct.lower()
    if _is_html or _is_css:
        _body = ctx.resp_body
        if _s2l_state.PROXY_CDN:
            _body = _s2l_state._rewrite_ext_urls(_body, html_doc=_is_html)
        if _is_html:
            _body = _s2l_state.rewrite_abs_urls(_body)
            _body = _s2l_state._inject_sw_clear(_body)
        if _body is not ctx.resp_body:
            ctx.resp_body = _body
            ctx.resp_headers.pop("content-length", None)
            ctx.resp_headers.pop("Content-Length", None)
    elif _is_json and _s2l_state.PROXY_CDN:
        _body = _s2l_state._rewrite_json_urls(ctx.resp_body)
        if _body is not ctx.resp_body:
            ctx.resp_body = _body
            ctx.resp_headers.pop("content-length", None)
            ctx.resp_headers.pop("Content-Length", None)


def proxy(path: str) -> Response:
    method   = flask_request.method.upper()
    target   = _s2l_state._upstream_url(path)
    req_path = "/" + path.lstrip("/")
    _s2l_state.stats.inc("proxied")
    _has_range = bool(flask_request.headers.get("Range", ""))

    # ── Phase 1: entry guards (prefetch / SW / blocked / OPTIONS) ────────────
    r = _guard_preflight(method, target, req_path)
    if r is not None:
        return r

    # ── Phase 2: OFFLINE mode — disk cache only ──────────────────────────────
    if _s2l_state.OFFLINE:
        return _offline_response(method, target, req_path)

    # ── Phase 3: device-aware online cache lookup ────────────────────────────
    if method in _s2l_state._SAFE_METHODS:
        # DEVICE-AWARE CACHE LOOKUP (ONLINE): a phone/tablet is served its own
        # cached variant when one exists. When it does NOT exist yet, the
        # canonical (desktop) file is deliberately SKIPPED so the live fetch
        # below serves AND caches the device's own document — this is what
        # stops "phone access changes the PC's page" (the old single-file
        # cache let whichever device fetched first dictate the layout for
        # everyone). The canonical file itself is never written by a device
        # fetch, so the user's hand edits in it survive forever. If the
        # upstream then fails, the 502 handler falls back to the canonical
        # file instead of erroring out.
        _dev_tag = _s2l_state._device_variant(_s2l_state._effective_device())
        lp = _s2l_state.local_path(target, _dev_tag) if _dev_tag else _s2l_state.local_path(target)
        if not (_dev_tag and os.path.isfile(lp)):
            lp = _s2l_state.local_path(target)
            if _dev_tag and not _s2l_state.OFFLINE and os.path.isfile(lp):
                lp = None   # force the live fetch — the device must get ITS copy.
        # IMPORTANT: Range requests MUST always hit upstream.
        # Serving a full file from disk cache in response to "Range: bytes=X-Y"
        # causes the browser to receive a 200 instead of 206 Partial Content —
        # video players (HTML5 <video>) interpret this as a truncated
        # stream and loop / stall indefinitely.
        if lp is not None and os.path.isfile(lp) and not _has_range:
            # DEVICE-profile change (see Runtime._note_device_change): cached
            # HTML documents may still hold the layout fetched under the
            # PREVIOUS profile — e.g. a stale mobile homepage sitting in the
            # canonical slot, served to auto/desktop/macintosh alike. Skip the
            # cache ONCE per file so the live fetch below rewrites it with the
            # current profile's variant; later requests hit the refreshed
            # cache as usual. Assets are never skipped — desktop-class
            # profiles share them, so only documents vary.
            if (_s2l_state._DEVICE_CHANGED
                    and lp.lower().endswith((".html", ".htm"))
                    and lp not in _s2l_state._DEVICE_REFRESHED):
                _s2l_state._DEVICE_REFRESHED.add(lp)
            else:
                cached = _s2l_state._cached_response(lp, target, method, req_path)
                if cached is not None:
                    return cached

    # ── Phase 4: request hooks + upstream fetch with retry ladder ────────────
    raw_body = flask_request.get_data(cache=True) if method in _s2l_state._BODY_METHODS else b""
    ctx = _s2l_state._build_ctx(method, target, raw_body)

    if _s2l_state._REQ_HOOKS:
        _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._REQ_HOOKS, ctx))

    _do_stream = _decide_streaming(method, target, _has_range)
    upstream_r = _fetch_with_retries(method, target, ctx, _do_stream, req_path, _has_range)
    if isinstance(upstream_r, Response):
        return upstream_r

    _push_redirect_history(method, upstream_r)

    # ── Phase 5: streaming / SSE hand-off ────────────────────────────────────
    _streamed, upstream_r = _maybe_stream(upstream_r, method, target, _do_stream, ctx, req_path)
    if _streamed is not None:
        return _streamed

    # ── Phase 6: recovery — decompress, WAF retry, 304, empty bodies ─────────
    body, upstream_r = _consume_upstream_body(upstream_r, method, target, ctx, req_path)
    upstream_r, body = _waf_block_retry(upstream_r, body, method, target, ctx, req_path)

    r304 = _handle_304(upstream_r, body, method, target, req_path)
    if isinstance(r304, Response):
        return r304
    upstream_r, body = r304

    upstream_r, body = _recover_empty_body(upstream_r, body, method, target, ctx, req_path)

    # ── Phase 7: CDN-host fallback on 404 ────────────────────────────────────
    r = _cdn_fallback(upstream_r, method, target, req_path)
    if r is not None:
        return r

    # ── Phase 8: response assembly + RSC recovery ────────────────────────────
    _resp_ct_raw = upstream_r.headers.get("Content-Type", "")
    ctx.resp_status  = upstream_r.status_code
    ctx.resp_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(upstream_r), body,
                                    is_top_level_html="text/html" in _resp_ct_raw)
    # Browser freshness normalization (see _apply_freshness) — without this,
    # upstream lifetimes (immutable / max-age=1y) froze the BROWSER's copy
    # while the editable disk mirror went stale-invisible for months.
    _s2l_state._apply_freshness(ctx.resp_headers, _resp_ct_raw or ctx.resp_ct)
    ctx.resp_body    = body
    # Ensure WASM resources on the MAIN host are also served with the correct
    # Content-Type (same rationale as the CDN/ext paths — see _ensure_wasm_content_type).
    ctx.resp_ct      = _s2l_state._ensure_wasm_content_type(
        upstream_r.headers.get("Content-Type", "application/octet-stream"),
        urlparse(target).path or "")

    body = _rsc_refetch(ctx, method, target, body)

    # ── Phase 9: request log + reveal-hidden ─────────────────────────────────
    sc_color = (Fore.GREEN if upstream_r.status_code < 300
                else Fore.YELLOW if upstream_r.status_code < 400
                else Fore.RED)
    _is_api  = any(x in ctx.resp_ct for x in ("json", "xml", "event-stream"))
    _lbl     = "API" if _is_api else "HTML" if "text/html" in ctx.resp_ct else "   "
    _suppress = _s2l_state.is_subresource(ctx.resp_ct) and not _is_api
    _host_disp = _s2l_state._fmt_host(urlparse(target).netloc)
    if not _suppress:
        _waf_name = _s2l_state.get_last_platform().get("name", "Unknown")
        _s2l_state.log(f"{method} {sc_color}{upstream_r.status_code}{Style.RESET_ALL}"
            f" {_lbl} {_host_disp}{req_path} {_s2l_state._fmt_size(len(body))}"
            + (f" [{_waf_name}]" if _waf_name != "Unknown" else ""), "→")
    if upstream_r.status_code >= 400:
        _s2l_state.stats.inc("http_errors")
    if _s2l_state.SHOW_HIDDEN and _s2l_state.is_html(ctx.resp_body, ctx.resp_ct):
        ctx.resp_body = _s2l_state._reveal_hidden(ctx.resp_body)
        ctx.resp_headers.pop("content-length", None)
        _s2l_state.stats.inc("revealed")

    # ── Phase 10: bot-page refusal ───────────────────────────────────────────
    r = _bot_page_response(ctx, method, target, req_path)
    if r is not None:
        return r

    # ── Phase 11: cache save + HTML/CSS/JSON rewriting + hooks ───────────────
    _save_and_enqueue(target, ctx, method)
    _rewrite_resp_body(ctx)

    if _s2l_state._RESP_HOOKS:
        _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._RESP_HOOKS, ctx))
    _s2l_state._apply_gui_hooks(ctx)

    if not isinstance(ctx.resp_body, (bytes, bytearray)):
        ctx.resp_body = b""
    if ctx.resp_body and "content-length" not in {k.lower() for k in ctx.resp_headers}:
        ctx.resp_headers["Content-Length"] = str(len(ctx.resp_body))

    # ── Phase 12: final blank-page guard + delivery ──────────────────────────
    _final_blank_page_recovery(ctx, method, target, req_path)

    _s2l_state._gui_push(ctx)
    _s2l_state._maybe_capture(ctx)
    return _s2l_state._make_flask_resp(ctx, method)
