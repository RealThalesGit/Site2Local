"""Site2Local: disk cache serving, ranges and validators."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from ... import Runtime as _s2l_state
from ...Core.Cache.Memory import read_cached_file


def _serve_cached(lp: str) -> tuple[bytes, str] | None:
    """Load a cached file from disk and apply HTML/CSS/JSON rewrites as needed.

    Three file shapes arrive here:

    * ``.html`` paths (the long-standing layout for no-query URL caches
      and for HTML stored before the 09/10 rename) — treated as HTML.
    * No-extension paths (the beautified 9.2 ``__query__`` layout for
      "binary endpoint + query" URLs such as ``/v1/asset?id=N`` →
      ``host/__query__/v1/asset_id_7``, and the 09/10 layout for old caches)
      — the on-disk path gives no MIME hint, so the UNIFIED content
      resolver (``_resolve_mime_with_sniff``) decides: real HTML is served
      as ``text/html`` (full rewrite + runtime injection pipeline);
      CSS/JS/JSON/WASM/images get their true type (the Google Fonts
      ``css2`` stylesheet, the Fix 6R case); genuinely unknown binaries
      fall back to ``application/octet-stream`` — the JS consumer reads
      raw bytes and parses them itself, not the Content-Type.
    * Any other extension (``.js``, ``.css``, ``.json``, ``.wasm`` …) —
      MIME resolved by ``guess_mime()`` from the extension (with the
      content-based rescue for the lying-extension family), with
      URL-rewrite passes applied for CSS and JSON.
    """
    try:
        if getattr(_s2l_state, "ATOMIC_SERVE", False):
            data = read_cached_file(lp)
        else:
            with open(lp, "rb") as f:
                data = f.read()
    except OSError:
        return None

    looks_html = lp.endswith(".html") or lp.endswith("/index.html")
    has_ext = bool(os.path.splitext(os.path.basename(lp))[1])
    ct = None

    if not has_ext and not looks_html:
        # No-extension file (beautified/09/10 layouts) — unified content
        # resolver decides HTML vs typed vs octet-stream. This is the SAME
        # resolver the 304-probe and the 206 path use, so every response
        # shape for one file agrees on the type.
        ct = _s2l_state._resolve_mime_with_sniff(lp, data)
        if ct.startswith("text/html"):
            looks_html = True
        # Non-HTML files still need the shared CSS/JSON rewrite pipeline.

    if looks_html:
        if _s2l_state._is_wire_payload(data) or not _s2l_state._is_valid_html(data):
            # OFFLINE: never destroy irreplaceable cache — serve what we have
            # rather than permanently 404'ing the resource for the rest of the
            # session. In online mode, purging lets us re-fetch a clean copy.
            if _s2l_state.OFFLINE:
                _s2l_state.log(f"Stale cache kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                _s2l_state.log(f"Stale RSC cache — purging {os.path.relpath(lp)}", "WARN")
                _s2l_state._purge_cached_file(lp)
                return None
        # Purge bot/CAPTCHA pages that slipped into cache before this guard existed
        elif _s2l_state._is_bot_page(data):
            if _s2l_state.OFFLINE:
                _s2l_state.log(f"Bot page kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                _s2l_state.log(f"Bot page in cache — purging {os.path.relpath(lp)}", "WARN")
                _s2l_state._purge_cached_file(lp)
                return None
        if _s2l_state.PROXY_CDN:
            data = _s2l_state._rewrite_ext_urls(data, html_doc=True)
        data = _s2l_state.rewrite_abs_urls(data)   # strip absolute MAIN_HOST URLs → proxy-relative
        # Always inject the S2L JS runtime — it patches fetch/XHR/WebSocket/
        # Worker/innerHTML so JS-initiated requests go through the proxy instead
        # of hitting the real (unreachable in OFFLINE) host. Without it, HTML
        # pages load but SPAs can't fetch data/config → "nothing works."
        data = _s2l_state._inject_sw_clear(data)
        return data, "text/html; charset=utf-8"

    if ct is None:
        ct = _s2l_state._resolve_mime_with_sniff(lp, data)
    # Ensure WASM files served from disk cache also have the correct Content-Type
    # — critical for WebAssembly.instantiateStreaming() after a cache hit.
    ct = _s2l_state._ensure_wasm_content_type(ct, lp)
    # Rewrite external URLs inside cached CSS files so images/fonts load through the proxy
    if _s2l_state.PROXY_CDN and (lp.endswith(".css") or "css" in ct):
        data = _s2l_state._rewrite_ext_urls(data, html_doc=False)
    elif _s2l_state.PROXY_CDN and (lp.endswith((".json", ".webmanifest")) or "json" in ct):
        data = _s2l_state._rewrite_json_urls(data)
    return data, ct


def _cached_response(lp: str, target: str, method: str, req_path: str) -> Response | None:
    """Build a Flask Response from a cached file on disk."""
    # Conditional requests FIRST — a 304 skips the file read entirely, and the
    # validators come from the CURRENT disk state: a hand edit immediately
    # stops matching the old ETag (fresh 200 next request) and a deleted file
    # never 304s (the request falls through to refetch/404).
    #
    # For no-extension cache files (the 09/10 "asset/?id=N" layout), the
    # path gives no MIME hint. We sniff the real Content-Type so:
    #   (a) HTML/JSON no-ext files are NOT 304'd (the proxy must always
    #       re-serve them so the runtime/rewrite pipeline runs and the
    #       browser sees the right Content-Type — otherwise the browser
    #       caches an old octet-stream response with the wrong type and
    #       the JS consumer throws);
    #   (b) binary no-ext files (PNG/WASM/...) DO get a 304 with the
    #       sniffed MIME, so the browser can revalidate without re-downloading.
    # The probe uses the SAME unified content resolver (2048-byte window) as
    # the 200 and the 206 paths — a 304 that disagrees with the 200 about
    # the type would pin the wrong type for the life of the entry.
    _probe_ct = _s2l_state._resolve_mime_with_sniff(lp)
    _c304 = _s2l_state._conditional_304(lp, _probe_ct)
    if _c304 is not None:
        _s2l_state.stats.inc("cache_304")
        _s2l_state.log(f"{method} HIT 304 {_s2l_state._fmt_host(urlparse(target).netloc)}{_s2l_state._fmt_path_qs(req_path)}", "←")
        _s2l_state._gui_push_raw(method, req_path, 304, _probe_ct, b"",
                      origin=_s2l_state.MAIN_HOST, _skip_log=True)
        return _c304

    result = _s2l_state._serve_cached(lp)
    if result is None:
        return None

    data, ct = result
    target_host = urlparse(target).netloc
    if (_s2l_state.PROXY_CDN and target_host != _s2l_state.MAIN_HOST
            and ("text/html" in ct or "text/css" in ct)):
        data = _s2l_state.rewrite_doc_relative(data, target_host, html_doc="text/html" in ct)
    _s2l_state.log(f"{method} HIT {_s2l_state._fmt_host(urlparse(target).netloc)}{_s2l_state._fmt_path_qs(req_path)} {_s2l_state._fmt_size(len(data))}", "←")
    _is_static = not (ct.startswith("text/html") or "json" in ct)
    _val = _s2l_state._disk_validators(lp, ct) if _is_static else None
    _is_html_cached = _s2l_state._is_document_ct(ct)
    _hdrs: dict = {
        # HTML + JSON: never cache at browser level (multi-site on same origin,
        # and JSON API responses must always be fresh — caching them for 24h
        # freezes SPA data and causes stale-state render failures).
        # Static assets (JS/CSS/images/fonts): STORE but always REVALIDATE —
        # the old "public, max-age=86400" let the browser serve its own copy
        # for 24h without contacting S2L, so user edits/deletions of the
        # mirrored files were invisible (“game still loads after the assets
        # folder was deleted”). With no-cache + ETag/Last-Modified the check
        # is a fast 304 and disk edits go live on the next reload.
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
                         if not _is_static
                         else "no-cache",
        **({"ETag": _val[0], "Last-Modified": _val[1]} if _val else {}),
    }
    # Cached document and Worker responses need the same isolation headers as
    # live responses.
    _s2l_state._apply_isolation_headers(_hdrs, data, is_top_level_html=_is_html_cached,
                             is_worker_script=_s2l_state._is_worker_request())
    ctx = _s2l_state.HookContext(
        method       = method,
        url          = target,
        path         = req_path,
        query        = flask_request.query_string.decode("utf-8", "ignore"),
        req_headers  = _s2l_state.filter_fwd(dict(flask_request.headers)),
        req_body     = b"",
        resp_status  = 200,
        resp_headers = _hdrs,
        resp_body    = data,
        resp_ct      = ct,
    )

    if _s2l_state._REQ_HOOKS:
        _s2l_state._run_hooks(_s2l_state._REQ_HOOKS, ctx)
    if _s2l_state._RESP_HOOKS:
        _s2l_state._run_hooks(_s2l_state._RESP_HOOKS, ctx)
    _s2l_state._apply_gui_hooks(ctx)
    _s2l_state._gui_push(ctx)
    _s2l_state._maybe_capture(ctx)
    return _s2l_state._make_flask_resp(ctx, method)
