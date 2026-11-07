"""Site2Local: proxy entry guards and OFFLINE disk serving.

Extracted from the single proxy() body of the s2l.py monolith
(match: s2l.py lines ~12499-12641, the guard + OFFLINE branches).
"""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from ... import Runtime as _s2l_state


def _guard_preflight(method: str, target: str, req_path: str) -> Response | None:
    """Early short-circuits: speculative prefetch, no-op SW, blocked paths, OPTIONS."""
    purpose = flask_request.headers.get("Purpose", "").lower()
    _sec_dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
    # Only short-circuit SPECULATIVE prefetches on top-level document requests.
    # Returning 204 for ALL prefetch headers breaks <link rel="preload"> —
    # Chrome sends Purpose: prefetch for preloaded CSS/JS/fonts too, and the
    # page then tries to use resources that were never fetched.
    # SKIP this short-circuit in OFFLINE mode: a prefetched page that IS
    # cached on disk should be served, not 204'd (the browser may skip
    # navigating to it, showing a blank page even though content exists).
    if (not _s2l_state.OFFLINE
            and purpose in ("prefetch", "prerender")
            and _sec_dest in ("document", "", "frame", "iframe")):
        _s2l_state._gui_push_raw(method, req_path, 204, "text/plain", b"",
                      origin=_s2l_state.MAIN_HOST)
        return Response(status=204)  # No Content
    # NOTE: no Upgrade:websocket branch here — _S2LWSGIRequestHandler
    # intercepts every WS upgrade at the socket layer before this view ever
    # runs (see its docstring). A request reaching this function is
    # guaranteed to not be a WS upgrade.

    # Service worker — no-op SW
    if _s2l_state._is_sw_path(req_path):
        _s2l_state._gui_push_raw(method, req_path, 200, "application/javascript", _s2l_state._SW_NOOP,
                      origin=_s2l_state.MAIN_HOST)
        resp = Response(_s2l_state._SW_NOOP, content_type="application/javascript; charset=utf-8")
        resp.headers["Service-Worker-Allowed"] = "/"
        resp.headers["Cache-Control"] = "no-store"
        return resp

    if any(x in target for x in _s2l_state.BLOCK_PATHS):
        _s2l_state._gui_push_raw(method, req_path, 403, "text/plain", b"Blocked",
                      origin=_s2l_state.MAIN_HOST)
        return Response("Blocked", status=403)

    if method == "OPTIONS":
        _s2l_state._gui_push_raw("OPTIONS", req_path, 204, "text/plain", b"",
                      origin=_s2l_state.MAIN_HOST)
        r = Response(status=204)
        _req_origin = flask_request.headers.get("Origin", "")
        r.headers.update({
            "Allow":                        ", ".join(_s2l_state._ALL_METHODS),
            "Access-Control-Allow-Origin":  _req_origin if _req_origin else "*",
            "Access-Control-Allow-Credentials": "true" if _req_origin else None,
            "Access-Control-Allow-Methods": ", ".join(_s2l_state._ALL_METHODS),
            # Concrete list (never bare "*") so credentialed preflights pass.
            "Access-Control-Allow-Headers": _s2l_state._cors_allow_headers(),
            "Access-Control-Expose-Headers": "*",
            "Access-Control-Max-Age": "86400",
            "Vary": "Origin",
        })
        # Private Network Access: allow public-context fetches to localhost.
        r.headers.update(_s2l_state._pna_allow_header())
        # Remove None values (Credential header when no Origin)
        r.headers = {k: v for k, v in r.headers.items() if v is not None}
        return r

    return None


def _offline_response(method: str, target: str, req_path: str) -> Response:
    """OFFLINE mode: device-aware disk lookup with alternate-CDN-host fallback."""
    if method in _s2l_state._SAFE_METHODS:
        # DEVICE-AWARE LOOKUP (OFFLINE): try the device's cached variant
        # first, then fall back to the canonical file — offline there is
        # no upstream, so a desktop-shaped document still beats a 404.
        _dev_tag = _s2l_state._device_variant(_s2l_state._effective_device())
        lp = _s2l_state.local_path(target, _dev_tag) if _dev_tag else _s2l_state.local_path(target)
        if not os.path.isfile(lp):
            lp = _s2l_state.local_path(target)
        if os.path.isfile(lp):
            # Range request support in OFFLINE mode — critical for
            # video/audio playback. Without 206 Partial Content, HTML5
            # <video> players interpret a full 200 as a truncated stream
            # and loop/stall indefinitely.
            if flask_request.headers.get("Range", ""):
                _rng_resp = _s2l_state._serve_range_from_disk(lp, target, method, req_path)
                if _rng_resp is not None:
                    return _rng_resp
                # Range read failed — fall through to full-file serve below
            r = _s2l_state._cached_response(lp, target, method, req_path)
            if r is not None:
                return r
            # The variant file existed but failed validation (stale RSC /
            # bot page → purged). OFFLINE has no second chance at a
            # fetch — try the canonical desktop file before giving up.
            if _dev_tag:
                _canon_lp = _s2l_state.local_path(target)
                if _canon_lp != lp and os.path.isfile(_canon_lp):
                    r = _s2l_state._cached_response(_canon_lp, target, method, req_path)
                    if r is not None:
                        _s2l_state.log(f"Device variant invalid — canonical fallback "
                                f"{_s2l_state._fmt_host(urlparse(target).netloc)}{req_path}", "WARN")
                        return r
        # Fallback: try alternate CDN hosts that may have this path cached.
        # In OFFLINE mode the main-host 404 fallback below never runs, so
        # without this an asset cached on a CDN host but requested
        # against the main host returns 404.
        if _s2l_state.PROXY_CDN:
            req_path_cdn = urlparse(target).path
            req_qs = flask_request.query_string.decode("utf-8", "ignore")
            with _s2l_state._cdn_port_lock:
                _cdn_snap = dict(_s2l_state._cdn_host_port)
            for _cdn_host in _cdn_snap:
                _cdn_url = f"https://{_cdn_host}{req_path_cdn}"
                if req_qs:
                    _cdn_url += f"?{req_qs}"
                _cdn_lp = _s2l_state.local_path(_cdn_url)
                if os.path.isfile(_cdn_lp):
                    _cdn_r = _s2l_state._cached_response(_cdn_lp, _cdn_url, method, req_path)
                    if _cdn_r is not None:
                        _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} offline CDN fallback hit {_s2l_state._fmt_host(_cdn_host)}{req_path_cdn}", "CDN")
                        return _cdn_r
    elif method in _s2l_state._BODY_METHODS and os.path.isfile(_s2l_state.local_path(target)):
        # POST/PUT to a path that has a cached GET body — return 405 so
        # the client gets a deterministic status instead of misleading 404.
        return Response("Offline — method not supported, GET cached",
                        status=405, headers={"Allow": "GET, HEAD"})
    _s2l_state._gui_push_raw(method, req_path, 404, "text/plain", b"Offline",
                  origin=_s2l_state.MAIN_HOST)
    return Response("Offline — not cached", status=404)
