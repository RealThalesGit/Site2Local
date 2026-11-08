"""Site2Local: disk cache serving, ranges and validators."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from dataclasses import replace as _dc_replace
from flask import Response
from flask import redirect
from flask import request as flask_request
from urllib.parse import urlparse
import email.utils
import os
import re
import requests
import requests.adapters

from ... import Runtime as _s2l_state


def _serve_cached(lp: str) -> tuple[bytes, str] | None:
    """Load a cached file from disk and apply HTML/CSS/JSON rewrites as needed.

    Per the 09/10 spec, three file shapes arrive here:

    * ``.html`` paths (the long-standing layout for no-query URL caches
      and for HTML stored before the 09/10 rename) — treated as HTML.
    * No-extension paths (the 09/10 layout for "binary endpoint + query"
      URLs such as ``/v1/asset?id=N`` → ``host/v1/asset/id=N``) — the
      on-disk path gives no MIME hint, so we sniff the bytes: real HTML is
      served as ``text/html``; CSS/JS/JSON bodies are rescued by content
      too (correção 6 — an extensionless stylesheet must never ship as
      ``application/octet-stream``: with the universal nosniff hardening
      that is a refused stylesheet, ``NS_ERROR_CORRUPTED_CONTENT`` on
      Firefox); everything else is served verbatim as
      ``application/octet-stream``. This is the safest fallback for binary
      endpoints — the JS consumer reads the raw bytes and parses them
      according to its own logic, not the Content-Type.
    * Any other extension (``.js``, ``.css``, ``.json``, ``.wasm`` …) —
      MIME resolved by ``guess_mime()`` from the extension, with
      URL-rewrite passes applied for CSS and JSON.
    """
    try:
        with open(lp, "rb") as f:
            data = f.read()
    except OSError:
        return None

    looks_html = lp.endswith(".html") or lp.endswith("/index.html")
    has_ext = bool(os.path.splitext(os.path.basename(lp))[1])

    if not has_ext and not looks_html:
        # No-extension file (the "asset/?id=N" and "css2?family=…" layouts).
        # HTML: identified by content (spec). Correção 6: CSS/JS/JSON are
        # content-sniffed too — the extensionless css2 stylesheet used to
        # ship as application/octet-stream, and with nosniff that is a
        # browser-refused stylesheet: NS_ERROR_CORRUPTED_CONTENT on Firefox,
        # MIME refusal on Chrome — the classic "works on first visit (live,
        # upstream CT), breaks on every reload (cache hit)" pattern.
        if _s2l_state._is_valid_html(data) and not _s2l_state._is_wire_payload(data):
            looks_html = True
        else:
            ct = _s2l_state._sniff_nonhtml_type(data) or "application/octet-stream"
            # Same rewrite passes as the extensioned branches below, so a
            # sniffed stylesheet keeps its url(...) sub-resources on the
            # local CDN ports and a sniffed JSON keeps its URLs proxied.
            if _s2l_state.PROXY_CDN and "css" in ct:
                data = _s2l_state._rewrite_ext_urls(data, html_doc=False)
            elif _s2l_state.PROXY_CDN and "json" in ct:
                data = _s2l_state._rewrite_json_urls(data)
            return data, ct

    if looks_html:
        if _s2l_state._is_wire_payload(data) or not _s2l_state._is_valid_html(data):
            # Correção 6b — plain CSS/JS/JSON bodies cached at an index.html
            # path (no-query, no-extension endpoints such as /api/v1/listings):
            # serve them with their real content type instead of the
            # purge-and-refetch loop that re-downloaded the body from the
            # upstream on every request ("Stale RSC cache — purging" on each
            # reload). Wire payloads and true junk keep the old behavior.
            if not _s2l_state._is_wire_payload(data):
                _sniff_ct = _s2l_state._sniff_nonhtml_type(data)
                if _sniff_ct is not None:
                    if _s2l_state.PROXY_CDN and "css" in _sniff_ct:
                        data = _s2l_state._rewrite_ext_urls(data, html_doc=False)
                    elif _s2l_state.PROXY_CDN and "json" in _sniff_ct:
                        data = _s2l_state._rewrite_json_urls(data)
                    return data, _sniff_ct
            # OFFLINE: never destroy irreplaceable cache — serve what we have
            # rather than permanently 404'ing the resource for the rest of the
            # session. In online mode, purging lets us re-fetch a clean copy.
            if _s2l_state.OFFLINE:
                _s2l_state.log(f"Stale cache kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                _s2l_state.log(f"Stale RSC cache — purging {os.path.relpath(lp)}", "WARN")
                try:
                    os.remove(lp)
                except OSError:
                    pass
                return None
        # Purge bot/CAPTCHA pages that slipped into cache before this guard existed
        elif _s2l_state._is_bot_page(data):
            if _s2l_state.OFFLINE:
                _s2l_state.log(f"Bot page kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                _s2l_state.log(f"Bot page in cache — purging {os.path.relpath(lp)}", "WARN")
                try:
                    os.remove(lp)
                except OSError:
                    pass
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

    ct = _s2l_state.guess_mime(lp)
    # Ensure WASM files served from disk cache also have the correct Content-Type
    # — critical for WebAssembly.instantiateStreaming() after a cache hit.
    ct = _s2l_state._ensure_wasm_content_type(ct, lp)
    # Correção 5 — exotic extension lied about the content (npm/globe.gl →
    # video/gl): sniff the head and serve real JavaScript as text/javascript
    # so nosniff browsers still execute it. data is already fully in memory.
    ct = _s2l_state._js_mime_rescue(ct, lp, data[:512])
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
    # path gives no MIME hint. We do a cheap head-only read to sniff the
    # real Content-Type so:
    #   (a) HTML/JSON no-ext files are NOT 304'd (the proxy must always
    #       re-serve them so the runtime/rewrite pipeline runs and the
    #       browser sees the right Content-Type — otherwise the browser
    #       caches an old octet-stream response with the wrong type and
    #       the JS consumer throws);
    #   (b) binary no-ext files (PNG/WASM/...) DO get a 304 with the
    #       sniffed MIME, so the browser can revalidate without re-downloading.
    _has_ext = bool(os.path.splitext(os.path.basename(lp))[1])
    _looks_html_path = lp.endswith(".html") or lp.endswith("/index.html")
    if _has_ext or _looks_html_path:
        _probe_ct = _s2l_state._js_mime_rescue(_s2l_state.guess_mime(lp), lp)
    else:
        try:
            with open(lp, "rb") as _hf:
                _head = _hf.read(_s2l_state._SNIFF_HEAD)
        except OSError:
            return None
        _probe_ct = _s2l_state._js_mime_rescue(
            _s2l_state._resolve_mime_with_sniff(lp, _head), lp, _head)
    _c304 = _s2l_state._conditional_304(lp, _probe_ct)
    if _c304 is not None:
        _s2l_state.stats.inc("cache_304")
        _s2l_state.log(f"{method} HIT 304 {_s2l_state._fmt_host(urlparse(target).netloc)}{req_path}", "←")
        _s2l_state._gui_push_raw(method, req_path, 304, _probe_ct, b"",
                      origin=_s2l_state.MAIN_HOST, _skip_log=True)
        return _c304

    result = _s2l_state._serve_cached(lp)
    if result is None:
        return None

    data, ct = result
    _s2l_state.log(f"{method} HIT {_s2l_state._fmt_host(urlparse(target).netloc)}{req_path} {_s2l_state._fmt_size(len(data))}", "←")
    _is_static = not (ct.startswith("text/html") or "json" in ct)
    _val = _s2l_state._disk_validators(lp, ct) if _is_static else None
    _is_html_cached = ct.startswith("text/html")
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
