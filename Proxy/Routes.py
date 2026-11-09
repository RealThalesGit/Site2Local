"""Main proxy route plus WebSocket/TCP/UDP fallbacks."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from dataclasses import replace as _dc_replace
from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from .. import Runtime as _s2l_state
from .TunnelRoutes import tcp_ext, udp_ext, ws_ext  # noqa: F401 (compat re-export)


def proxy(path: str) -> Response:
    method   = flask_request.method.upper()
    target   = _s2l_state._upstream_url(path)
    req_path = "/" + path.lstrip("/")
    _s2l_state.stats.inc("proxied")
    _has_range = bool(flask_request.headers.get("Range", ""))
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

    if _s2l_state.OFFLINE:
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
                                f"{_s2l_state._fmt_host(urlparse(target).netloc)}{_s2l_state._fmt_path_qs(req_path)}", "WARN")
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
                            _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} offline CDN fallback hit {_s2l_state._fmt_host(_cdn_host)}{_s2l_state._fmt_path_qs(req_path_cdn, req_qs)}", "CDN")
                            return _cdn_r
        elif method in _s2l_state._BODY_METHODS and os.path.isfile(_s2l_state.local_path(target)):
            # POST/PUT to a path that has a cached GET body — return 405 so
            # the client gets a deterministic status instead of misleading 404.
            return Response("Offline — method not supported, GET cached",
                            status=405, headers={"Allow": "GET, HEAD"})
        _s2l_state._gui_push_raw(method, req_path, 404, "text/plain", b"Offline",
                      origin=_s2l_state.MAIN_HOST)
        return Response("Offline — not cached", status=404)

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
            cached = _s2l_state._cached_response(lp, target, method, req_path)
            if cached is not None:
                return cached

    raw_body = flask_request.get_data(cache=True) if method in _s2l_state._BODY_METHODS else b""
    ctx = _s2l_state._build_ctx(method, target, raw_body)

    if _s2l_state._REQ_HOOKS:
        _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._REQ_HOOKS, ctx))

    # Decide whether to use stream=True for the upstream request.
    # stream=True is needed for: Range requests (audio/video 206), URLs
    # whose extension suggests streamable content (video/audio/wasm/large bin),
    # AND Server-Sent Events endpoints (text/event-stream).
    #
    # SSE is enabled by default: the browser's EventSource() always sends
    # "Accept: text/event-stream" on the request — that's a perfect pre-flight
    # signal that we MUST use stream=True, otherwise curl_cffi refuses to
    # iterate ("stream mode is not enabled") and the SSE streamer emits the
    # "SSE upstream stream error ... stream mode is not enabled" warning on
    # every request. We also catch common SSE URL patterns for clients that
    # omit the Accept header (older polyfills, fetch()-based consumers).
    _req_ext = os.path.splitext(urlparse(target).path)[1].lower()
    _looks_streamable = _req_ext in (
        ".mp4", ".webm", ".ogg", ".ogv", ".mpeg", ".mp2t", ".m3u8",
        ".mp3", ".m4a", ".aac", ".wav", ".opus",
        ".wasm", ".zip", ".tar", ".gz", ".tgz", ".bz2",
    )
    # Pre-detect SSE so stream=True is used on the FIRST request, avoiding
    # the "stream mode is not enabled" warning + a re-fetch round-trip.
    _browser_accept = (flask_request.headers.get("Accept", "") or "").lower()
    _req_path_lower = (urlparse(target).path or "").lower()
    _url_looks_sse = (
        _req_path_lower.endswith((
            "/sse", "/events", "/event-stream", "/eventstream",
            "/stream", "/live", "/updates", "/subscribe",
            "/realtime", "/push", "/listen", "/feed",
        ))
        or any(_seg in _req_path_lower for _seg in (
            "/sse/", "/events/", "/stream/", "/live/",
            "/realtime/", "/updates/",
        ))
        or _req_ext in (".sse", ".event-stream", ".eventstream")
    )
    _looks_sse = ("text/event-stream" in _browser_accept) or _url_looks_sse
    _do_stream = (method in _s2l_state._SAFE_METHODS
                  and (_has_range or _looks_streamable or _looks_sse))

    try:
        upstream_r = _s2l_state._do_upstream(method, target, ctx, stream=_do_stream)
    except _s2l_state._CONN_ERRORS as exc:
        short = _s2l_state._short_exc(exc)
        _s2l_state.log(f"{method} {urlparse(target).path} — {short}", "WARN")
        _s2l_state.stats.inc("conn_errors")
        # Retry once with a fresh session on connection-level errors.
        # Try curl_cffi first (best CF bypass), then fall back to
        # cloudscraper (older but sometimes works when cffi's DNS/TLS fails
        # on specific networks), then plain requests as last resort.
        #
        # FIX: rotate the CLIENT session (per-IP, the one _do_upstream uses),
        # NOT _proxy_local.s (thread-local — _do_upstream ignores it, so the
        # old retry was a no-op that leaked sessions without rotating anything).
        try:
            _cid = _s2l_state._client_id()
        except Exception:
            _cid = None
        if _cid is not None:
            with _s2l_state._CLIENT_LOCK:
                _old_sess = _s2l_state._CLIENT_SESSIONS.get(_cid)
            if _old_sess is not None:
                try: _old_sess.close()
                except Exception: pass
        upstream_r = None
        last_exc = exc
        # Primary: fresh curl_cffi session
        for _attempt, _sess_factory in (
            (1, _s2l_state._make_session),
            (2, _s2l_state._make_cloudscraper_session),
            (3, _s2l_state._make_requests_session),
        ):
            try:
                _rsess = _sess_factory()
            except Exception:
                continue
            # Store the fresh session so _do_upstream picks it up
            if _cid is not None:
                with _s2l_state._CLIENT_LOCK:
                    _s2l_state._CLIENT_SESSIONS[_cid] = _rsess
            else:
                _s2l_state._proxy_local.s = _rsess   # fallback for background threads
            try:
                upstream_r = _s2l_state._do_upstream(method, target, ctx, stream=_do_stream)
                _s2l_state.log(f"Retry #{_attempt} succeeded: {urlparse(target).path}", "INFO")
                break
            except Exception as exc2:
                last_exc = exc2
                short2 = _s2l_state._short_exc(exc2)
                _s2l_state.log(f"Retry #{_attempt} failed: {urlparse(target).path} — {short2}", "WARN")
                try: _rsess.close()
                except Exception: pass
                continue
        if upstream_r is None:
            short2 = _s2l_state._short_exc(last_exc)
            _s2l_state.log(f"All retries failed: {urlparse(target).path} — {short2}", "ERROR")
            # GRACEFUL CACHE FALLBACK: upstream is unreachable right now —
            # serve the cached document instead of a bare 502 (same
            # philosophy as OFFLINE: irreplaceable cache beats an error
            # page). Canonical file only: it exists for every desktop fetch
            # and holds the user's mods; a missing device variant has
            # obviously never been fetched, so there is nothing newer to try.
            if method in _s2l_state._SAFE_METHODS and not _has_range:
                try:
                    _flp = _s2l_state.local_path(target)
                    if os.path.isfile(_flp):
                        _fc = _s2l_state._cached_response(_flp, target, method, req_path)
                        if _fc is not None:
                            _s2l_state.log(f"Upstream down — serving cached copy: {_s2l_state._fmt_path_qs(req_path)}", "WARN")
                            return _fc
                except Exception:
                    pass
            err_body = (f"Connection error: {short}\n"
                        f"Retry: {short2}\n\n"
                        f"This may be caused by:\n"
                        f"  • The server closed the connection prematurely\n"
                        f"  • A network interruption or timeout\n"
                        f"  • Cloudflare or WAF blocking the request\n"
                        f"  • DNS resolution failure (check internet connection)")
            return Response(err_body, status=502, content_type="text/plain; charset=utf-8")
    except Exception as exc:
        _s2l_state.log(f"{method} {urlparse(target).path} — {_s2l_state._short_exc(exc)}", "ERROR")
        _s2l_state.stats.inc("conn_errors")
        _r502 = Response(f"Upstream error: {_s2l_state._short_exc(exc)}", status=502)
        _s2l_state._apply_cors_headers(_r502.headers)
        _s2l_state._apply_security_headers(_r502.headers)
        return _r502
    if hasattr(upstream_r, 'history') and upstream_r.history:
        for hist_resp in upstream_r.history:
            hist_url = hist_resp.url
            hist_path = urlparse(hist_url).path or "/"
            try:
                hist_body = _s2l_state.decompress_body(
                    hist_resp.content,
                    hist_resp.headers.get("Content-Encoding", ""),
                )
            except Exception:
                hist_body = b""

            hist_ct = hist_resp.headers.get("Content-Type", "application/octet-stream")
            _hist_origin = _s2l_state._fmt_host(urlparse(hist_url).netloc)
            _s2l_state._gui_push_raw(
                method,
                hist_path,
                hist_resp.status_code,
                hist_ct,
                hist_body if hist_body else f"(redirect {hist_resp.status_code})".encode(),
                display_tag="[REDIR]",
                origin=_hist_origin,
            )

    # ── Streaming: hand off immediately for large/binary content ─────────────
    # Check the actual Content-Type now that we have the response headers.
    # If it should be streamed, do it NOW before consuming any body bytes.
    _real_ct = upstream_r.headers.get("Content-Type", "")
    _real_cl = int(upstream_r.headers.get("Content-Length", "0") or 0)
    # SSE: detect text/event-stream and use the dedicated SSE streamer
    # which re-buffers to event boundaries and injects keepalives.
    if _s2l_state.SSE_PROXY and _s2l_state._is_sse_response(_real_ct) and method in _s2l_state._SAFE_METHODS:
        # Safety net: if the upstream request was made with stream=False
        # (URL didn't match SSE heuristics but upstream still returned
        # text/event-stream), iter_content() would refuse to iterate and the
        # SSE streamer would emit "stream mode is not enabled" warnings.
        # Re-fetch once with stream=True so the SSE streamer has a real
        # stream to consume. Guarded by a thread-local flag to prevent
        # recursion if the re-fetch also returns SSE.
        if not _do_stream and not getattr(_s2l_state._proxy_local, "in_sse_refetch", False):
            try:
                _s2l_state._proxy_local.in_sse_refetch = True
                _sse_r = _s2l_state._do_upstream(method, target, ctx, stream=True)
                if (_sse_r is not None
                        and _sse_r.status_code == upstream_r.status_code):
                    _s2l_state.log(f"SSE re-fetch with stream=True OK: {req_path}", "INFO")
                    try: upstream_r.close()
                    except Exception: pass
                    upstream_r = _sse_r
                else:
                    if _sse_r is not None:
                        try: _sse_r.close()
                        except Exception: pass
                    _s2l_state.log(f"SSE re-fetch returned unexpected status "
                        f"(got {_sse_r.status_code if _sse_r is not None else 'None'}, "
                        f"expected {upstream_r.status_code}) on {req_path} — "
                        f"keeping non-streamed response", "WARN")
            except Exception as e:
                _s2l_state.log(f"SSE re-fetch failed on {req_path}: {_s2l_state._short_exc(e)}", "WARN")
            finally:
                _s2l_state._proxy_local.in_sse_refetch = False
        return _s2l_state._stream_sse_resp(upstream_r, method, target)
    # Streaming hand-off: if we fetched with stream=True AND the response is
    # streamable content (audio/video/large binary) OR a 206 Partial Content
    # (Range request), hand off to _stream_resp. CRITICAL: when stream=True
    # was used, we must NOT fall through to upstream_r.content for 206/range
    # responses — curl_cffi's HTTP/2 stream mode can return empty bytes for
    # partial content chunks, producing "0.0B" bodies that break audio/video
    # playback. _stream_resp reads the stream chunk-by-chunk and is reliable.
    _has_content_range = bool(upstream_r.headers.get("Content-Range", ""))
    if (_do_stream and method in _s2l_state._SAFE_METHODS
            and (_s2l_state._should_stream(_real_ct, _real_cl)
                 or upstream_r.status_code == 206
                 or _has_content_range)):
        return _s2l_state._stream_resp(upstream_r, method, target)

    # Decompress upstream body before we do anything with it.
    # With stream=True, upstream_r.content consumes the stream now.
    # If the body comes back empty despite Content-Length > 0, curl_cffi's
    # stream mode may have lost chunks — re-fetch with stream=False.
    enc  = upstream_r.headers.get("Content-Encoding", "")
    try:
        body = _s2l_state.decompress_body(upstream_r.content, enc)
    except Exception as e:
        _s2l_state.log(f"Body consume error on {req_path}: {_s2l_state._short_exc(e)}", "WARN")
        body = b""
    # Stream-mode empty-body recovery: if stream=True returned an empty body
    # but the response should have content, re-fetch with stream=False.
    # Guard against infinite recursion via a thread-local flag.
    if (not body and method in _s2l_state._SAFE_METHODS
            and upstream_r.status_code == 200
            and not getattr(_s2l_state._proxy_local, "in_refetch", False)):
        _cl_check = upstream_r.headers.get("Content-Length", "")
        _ct_check = upstream_r.headers.get("Content-Type", "")
        if (_cl_check and int(_cl_check or 0) > 0) or "text/html" in _ct_check:
            _s2l_state.log(f"Stream-mode empty body on {req_path} — re-fetching with stream=False", "WARN")
            try:
                _s2l_state._proxy_local.in_refetch = True
                _rf_r = _s2l_state._do_upstream(method, target, ctx, stream=False)
                _rf_body = _s2l_state.decompress_body(_rf_r.content, _rf_r.headers.get("Content-Encoding", ""))
                if _rf_body:
                    body = _rf_body
                    upstream_r = _rf_r
                    _s2l_state.log(f"Stream re-fetch OK: {_s2l_state._fmt_size(len(body))}", "INFO")
            except Exception as e:
                _s2l_state.log(f"Stream re-fetch failed: {_s2l_state._short_exc(e)}", "WARN")
            finally:
                _s2l_state._proxy_local.in_refetch = False

    # ── WAF block detection + retry ─────────────────────────────────────────
    # Detects classic 403/503 block pages and plain-text "blocked" bodies.
    # On detection, rotates the CLIENT session (not the proxy session —
    # _do_upstream uses _get_client_session, so rotating _proxy_local.s is
    # a no-op) and retries once, preserving cf_clearance cookies.
    # IMPORTANT: skip the block check entirely for API paths and 429 responses.
    # API endpoints return small JSON bodies that can false-positive, and 429
    # is a legitimate rate-limit signal — treating it as a block causes a retry
    # cascade that doubles the request rate and pushes the API into a sustained
    # 429 loop, starving the SPA of data.
    _api_exempt = any(req_path.startswith(p) for p in _s2l_state._BOT_EXEMPT_PREFIXES)
    _looks_blocked = (
        not _api_exempt
        and upstream_r.status_code != 429
        and method in _s2l_state._SAFE_METHODS
        and (_s2l_state._is_cf_block(body, upstream_r.status_code, dict(upstream_r.headers))
             or _s2l_state._is_raw_block_text(body, upstream_r.status_code))
    )
    if _looks_blocked:
        _s2l_state.log(f"WAF block detected on {req_path} — rotating client session and retrying", "WARN")
        try:
            # Rotate the CLIENT session (the one _do_upstream actually uses),
            # preserving cf_clearance cookies from the old session.
            _old_client_sess = _s2l_state._get_client_session()
            _fresh = _s2l_state._make_session()
            try:
                for _c in _old_client_sess.cookies:
                    if _c.name in ("cf_clearance", "cf_chl_2", "cf_chl_rc_z"):
                        _fresh.cookies.set(_c.name, _c.value, domain=_c.domain, path=_c.path)
            except Exception:
                pass
            _browser_ua_r = flask_request.headers.get("User-Agent", "")
            if _browser_ua_r:
                _fresh.headers["User-Agent"] = _s2l_state._sanitize_ua(_browser_ua_r)
            with _s2l_state._CLIENT_LOCK:
                _cid = _s2l_state._client_id()
                _s2l_state._CLIENT_SESSIONS[_cid] = _fresh
            # FIX: close the OLD session before it's orphaned. Without this,
            # its connection pool (up to POOL_MAXSIZE=64 conns) and file
            # descriptors leak — repeated WAF blocks accumulate leaked sessions.
            try:
                _old_client_sess.close()
            except Exception:
                pass
            upstream_r2 = _s2l_state._do_upstream(method, target, ctx)
            body2 = _s2l_state.decompress_body(upstream_r2.content,
                                    upstream_r2.headers.get("Content-Encoding", ""))
            if not (_s2l_state._is_cf_block(body2, upstream_r2.status_code, dict(upstream_r2.headers))
                    or _s2l_state._is_raw_block_text(body2, upstream_r2.status_code)):
                _s2l_state.log(f"Block retry succeeded: {req_path} [{upstream_r2.status_code}]", "INFO")
                upstream_r, body = upstream_r2, body2
            else:
                _s2l_state.log(f"Retry also returned a block page for {req_path} — serving as-is", "WARN")
        except Exception as e:
            _s2l_state.log(f"Block retry error {req_path}: {_s2l_state._short_exc(e)}", "ERROR")
    if upstream_r.status_code == 304:
        # Upstream returned 304 despite us not sending If-None-Match.
        # Serve from local cache if available, otherwise force a clean fetch.
        # Device-aware: a phone whose variant isn't cached yet skips the
        # canonical file (online) so the forced re-fetch below populates the
        # device's own variant instead of re-serving the desktop document.
        _dev_tag = _s2l_state._device_variant(_s2l_state._effective_device())
        lp = _s2l_state.local_path(target, _dev_tag) if _dev_tag else _s2l_state.local_path(target)
        if not (_dev_tag and os.path.isfile(lp)):
            lp = _s2l_state.local_path(target)
            if _dev_tag and not _s2l_state.OFFLINE and os.path.isfile(lp):
                lp = None
        if lp and os.path.isfile(lp):
            cached = _s2l_state._cached_response(lp, target, method, req_path)
            if cached is not None:
                return cached
        _s2l_state.log(f"304 without local cache — re-fetching {req_path}", "WARN")
        try:
            forced_r = _s2l_state._get_proxy_session().get(
                target,
                headers={"Host": _s2l_state.MAIN_HOST, "Cache-Control": "no-cache", "Pragma": "no-cache"},
                cookies=flask_request.cookies,
                timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ),
                allow_redirects=True,
                verify=False,
            )
            body       = _s2l_state.decompress_body(forced_r.content, forced_r.headers.get("Content-Encoding", ""))
            upstream_r = forced_r
            _s2l_state.log(f"Re-fetch OK: {len(body)} bytes [{upstream_r.status_code}]", "INFO")
        except Exception as e:
            _s2l_state.log(f"Re-fetch failed: {_s2l_state._short_exc(e)}", "ERROR")
            return Response("304 with no cached copy", status=502)
    # ── Empty body recovery ───────────────────────────────────────────────────
    # Cases that can produce a real 200 with an empty body:
    #
    #   A) Decompression failed: Content-Length > 0 but body is 0 after decompress.
    #      curl_cffi usually auto-decompresses, but some encodings (zstd, malformed
    #      brotli) slip through. Fix: re-fetch with Accept-Encoding: identity.
    #
    #   B) API endpoints using chunked Transfer-Encoding.
    #      No Content-Length header, yet body should be JSON. curl_cffi occasionally
    #      returns empty content for HTTP/2 multiplexed responses on the first try.
    #      Fix: retry via _do_upstream with a fresh session & stripped Accept-Encoding.
    #
    #   C) HTML page that came back empty (Cloudflare challenge page stripped etc.)
    #      Already handled below.
    #
    # Legitimate empty-body responses we must NOT re-fetch:
    #   • 204 No Content, 304 Not Modified (already handled above)
    #   • POST/DELETE/PATCH endpoints that intentionally return empty 200/201
    #   • Responses where Content-Length is explicitly 0
    _ct_now     = upstream_r.headers.get("Content-Type", "")
    _cl_raw     = upstream_r.headers.get("Content-Length", None)
    _cl_header  = int(_cl_raw or 0)
    _cl_explicit_zero = (_cl_raw is not None and _cl_header == 0)
    _is_html_ct = "text/html" in _ct_now or not _ct_now.strip()
    _is_json_ct = "application/json" in _ct_now or "application/javascript" in _ct_now

    # Case A: Content-Length said non-zero but body came out empty → decompression bust.
    # Only re-fetch for 2xx (excluding 204): 4xx/5xx responses legitimately
    # have small or empty bodies, and curl_cffi sometimes returns the body
    # empty for error responses — re-fetching them causes cascading 429s on
    # rate-limited APIs (every 404 triggers a retry that itself gets limited).
    _decomp_failed = (
        len(body) == 0
        and not _cl_explicit_zero
        and _cl_header > 0
        and 200 <= upstream_r.status_code < 300
        and upstream_r.status_code != 204
    )

    # Case B: JSON API endpoint, empty body, chunked (no Content-Length), status 200
    # Only for GET/HEAD — POST/DELETE intentionally return empty 200.
    # Triggering on 4xx/5xx too would be wrong: rate-limited APIs return 429
    # with an empty JSON body when rate-limited, and 404 with an empty body
    # — re-fetching those just burns the rate-limit budget faster. Now we
    # explicitly require 200 (or 206 for partial content) AND Content-Type
    # starts with application/json.
    _empty_api = (
        method in _s2l_state._SAFE_METHODS
        and upstream_r.status_code in (200, 206)
        and len(body) == 0
        and not _cl_explicit_zero
        and _is_json_ct
    )

    # Case C: HTML page with empty body — re-fetch when the server PROMISED
    # bytes (Content-Length > 0) OR when there's no Content-Length at all
    # (chunked transfer) and the body came out empty. Some SPA routes
    # intentionally return 200 with empty body, but for a top-level HTML
    # document that's almost always a bug (CF challenge, decompression failure,
    # curl_cffi stream issue) — re-fetch with identity encoding to recover.
    _empty_html = (
        method in _s2l_state._SAFE_METHODS
        and upstream_r.status_code == 200
        and len(body) == 0
        and _is_html_ct
        and not _cl_explicit_zero
        and not any(req_path.startswith(p) for p in _s2l_state._BOT_EXEMPT_PREFIXES)
    )

    if _decomp_failed or _empty_api or _empty_html:
        if _decomp_failed:
            reason = "decompression failure"
        elif _empty_api:
            reason = "empty API body"
        else:
            reason = "empty HTML body"
        _s2l_state.log(f"Empty body ({reason}) on {req_path} — re-fetching", "WARN")
        try:
            # Re-use _do_upstream so the same session, cookies, and auth headers
            # are forwarded. We modify ctx to force identity encoding.
            retry_fwd      = dict(ctx.req_headers)
            # Force no compression on retry — defeats decompression failures
            retry_fwd["Accept-Encoding"] = "identity"
            # Preserve Authorization header explicitly (Bearer tokens)
            orig_auth = flask_request.headers.get("Authorization", "")
            if orig_auth and "authorization" not in {k.lower() for k in retry_fwd}:
                retry_fwd["Authorization"] = orig_auth
            retry_ctx = _dc_replace(ctx, req_headers=retry_fwd)
            refetch_r = _s2l_state._do_upstream(method, target, retry_ctx)
            body_r = refetch_r.content   # identity encoding → no decompress needed
            if not body_r:
                body_r = _s2l_state.decompress_body(
                    refetch_r.content,
                    refetch_r.headers.get("Content-Encoding", ""),
                )
            if len(body_r) > 0:
                body       = body_r
                upstream_r = refetch_r
                _s2l_state.log(f"Re-fetch OK: {_s2l_state._fmt_size(len(body))}", "INFO")
            else:
                _s2l_state.log(f"Re-fetch also returned empty body for {req_path}", "WARN")
        except Exception as e:
            _s2l_state.log(f"Re-fetch failed: {_s2l_state._short_exc(e)}", "WARN")
    # If main host 404 and we have CDN hosts, try them
    if upstream_r.status_code == 404 and _s2l_state.PROXY_CDN and method in _s2l_state._SAFE_METHODS:
        req_path_cdn = urlparse(target).path
        req_qs   = flask_request.query_string.decode("utf-8", "ignore")
        with _s2l_state._cdn_port_lock:
            snap = dict(_s2l_state._cdn_host_port)
        for cdn_host in snap:
            cdn_url = f"https://{cdn_host}{req_path_cdn}"
            if req_qs:
                cdn_url += f"?{req_qs}"
            cdn_lp = _s2l_state.local_path(cdn_url)
            if os.path.isfile(cdn_lp):
                try:
                    # Read directly instead of send_file: send_file's mimetype
                    # guessing can disagree with our sidecar-resolved type, and it
                    # doesn't let us set CORP/Cache-Control in one place. A direct
                    # read also avoids a TOCTOU crash if the file is deleted by the
                    # save worker between the isfile() check above and the open.
                    with open(cdn_lp, "rb") as _cdnf:
                        cdn_data = _cdnf.read()
                except OSError as e:
                    _s2l_state.log(f"CDN disk hit unreadable: {cdn_lp} — {_s2l_state._short_exc(e)}", "WARN")
                else:
                    _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} CDN disk hit {_s2l_state._fmt_host(cdn_host)}{_s2l_state._fmt_path_qs(req_path_cdn, req_qs)}", "CDN")
                    # For no-extension cache files (the 09/10 "asset/?id=N"
                    # layout), guess_mime() can't tell a PNG from a JSON
                    # from a WASM — sniff the leading bytes so the browser
                    # gets the right Content-Type and the JS consumer can
                    # pick a parser. A generic binary MIME type on an
                    # extensionless response can make clients reject valid data.
                    _cdn_ct = _s2l_state._resolve_mime_with_sniff(cdn_lp, cdn_data)
                    _cdn_ct = _s2l_state._ensure_wasm_content_type(_cdn_ct, cdn_lp)
                    _c304 = _s2l_state._conditional_304(cdn_lp, _cdn_ct)
                    if _c304 is not None:
                        _s2l_state.stats.inc("cache_304")
                        return _c304
                    resp = Response(cdn_data, content_type=_cdn_ct)
                    _s2l_state._apply_cors_headers(resp.headers)
                    _s2l_state._apply_isolation_headers(resp.headers,
                                             is_worker_script=_s2l_state._is_worker_request())
                    _s2l_state._apply_security_headers(resp.headers)
                    if _cdn_ct.startswith("text/html") or "json" in _cdn_ct:
                        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                    else:
                        resp.headers["Cache-Control"] = "no-cache"
                        _val = _s2l_state._disk_validators(cdn_lp)
                        if _val:
                            resp.headers["ETag"] = _val[0]
                            resp.headers["Last-Modified"] = _val[1]
                    return resp
            try:
                # Use TIMEOUT_STREAM_READ (120s) — CDN fallback may download large
                # WASM/asset bundles that would timeout at TIMEOUT_READ=12s.
                cdn_r    = _s2l_state._get_proxy_session().get(cdn_url,
                                                     timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_STREAM_READ),
                                                     verify=False)
                cdn_body = _s2l_state.decompress_body(cdn_r.content, cdn_r.headers.get("Content-Encoding", ""))
                ct       = _s2l_state._ensure_wasm_content_type(
                    cdn_r.headers.get("Content-Type", "application/octet-stream"),
                    req_path_cdn)
                if cdn_r.status_code < 400:
                    _s2l_state.save_queue.put((cdn_url, cdn_body, ct, _s2l_state._effective_device()))
                    _s2l_state.stats.inc("cdn_fetched")
                    _s2l_state.log(f"CDN fallback {_s2l_state._fmt_host(cdn_host)}{_s2l_state._fmt_path_qs(req_path_cdn, req_qs)} {cdn_r.status_code}", "CDN")
                    out = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(cdn_r))
                    _s2l_state._apply_freshness(out, ct)
                    # CORS + CORP are already set correctly by filter_resp() —
                    # do NOT re-set them here (see _cdn_serve for rationale).
                    return Response(cdn_body, status=cdn_r.status_code, headers=out, content_type=ct)
            except Exception as exc:
                _s2l_state.log(f"CDN fetch failed: {cdn_url} — {_s2l_state._short_exc(exc)}", "WARN")
    _resp_ct_raw = upstream_r.headers.get("Content-Type", "")
    ctx.resp_status  = upstream_r.status_code
    ctx.resp_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(upstream_r), body,
                                    is_top_level_html=_s2l_state._is_document_ct(_resp_ct_raw))
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
    # Detect RSC wire format — re-fetch with stronger HTML signal
    if _s2l_state._is_wire_payload(body) and method == "GET":
        _s2l_state.log(f"RSC detected on {urlparse(target).path} — re-fetching as full HTML", "WARN")
        try:
            refetch_sess = _s2l_state._get_proxy_session()
            refetch_hdrs = {
                "Host":    _s2l_state.MAIN_HOST,
                "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control":   "no-cache",
                "Pragma":          "no-cache",
                "Upgrade-Insecure-Requests": "1",
            }
            refetch_r = refetch_sess.get(
                target,
                headers=refetch_hdrs,
                cookies=flask_request.cookies,
                timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ),
                allow_redirects=True,
                verify=False,
            )
            refetch_body = _s2l_state.decompress_body(refetch_r.content, refetch_r.headers.get("Content-Encoding", ""))
            if not _s2l_state._is_wire_payload(refetch_body) and _s2l_state._is_valid_html(refetch_body):
                body = refetch_body
                ctx.resp_body = body
                ctx.resp_ct   = refetch_r.headers.get("Content-Type", "text/html")
                ctx.resp_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(refetch_r), body, is_top_level_html=True)
                _s2l_state.log(f"RSC re-fetch succeeded: {urlparse(target).path}", "INFO")
            else:
                _s2l_state.log("RSC re-fetch also returned RSC payload — serving as-is", "WARN")
        except Exception as exc:
            _s2l_state.log(f"RSC re-fetch error: {_s2l_state._short_exc(exc)}", "WARN")
    sc_color = (Fore.GREEN if upstream_r.status_code < 300
                else Fore.YELLOW if upstream_r.status_code < 400
                else Fore.RED)
    _is_api  = any(x in ctx.resp_ct for x in ("json", "xml", "event-stream"))
    _lbl     = "API" if _is_api else "HTML" if "text/html" in ctx.resp_ct else "   "
    _host_disp = _s2l_state._fmt_host(urlparse(target).netloc)
    # Log every upstream response, including JS/CSS/fonts/images on cache misses.
    _waf_name = _s2l_state.get_last_platform().get("name", "Unknown")
    _s2l_state.log(f"{method} {sc_color}{upstream_r.status_code}{Style.RESET_ALL}"
        f" {_lbl} {_host_disp}{_s2l_state._fmt_path_qs(req_path)} {_s2l_state._fmt_size(len(body))}"
        + (f" [{_waf_name}]" if _waf_name != "Unknown" else ""), "→")
    if upstream_r.status_code >= 400:
        _s2l_state.stats.inc("http_errors")
    if _s2l_state.SHOW_HIDDEN and _s2l_state.is_html(ctx.resp_body, ctx.resp_ct):
        ctx.resp_body = _s2l_state._reveal_hidden(ctx.resp_body)
        ctx.resp_headers.pop("content-length", None)
        _s2l_state.stats.inc("revealed")
    # ── Bot page detection ────────────────────────────────────────────────────
    # Must happen on raw body BEFORE _inject_sw_clear adds our script (which
    # pushes the page content past the 8192-byte scan window).
    # Also catches Google Sorry (HTTP 200) which is missed by the CF-block check.
    if _s2l_state._is_bot_page(ctx.resp_body, ctx.resp_status, req_path):
        _s2l_state.log(f"Bot page on {req_path} [{ctx.resp_status}] — returning 503", "WARN")
        err_body = (
            b"<html><head><title>S2L: Blocked</title>"
            b"<style>body{font:16px sans-serif;padding:2em;background:#111;color:#eee}"
            b"h1{color:#f66}pre{background:#1a1a1a;padding:1em;border-radius:4px;"
            b"overflow:auto;font-size:13px;color:#aaa}</style></head><body>"
            b"<h1>Upstream returned a bot-detection page</h1>"
            b"<p>The target server blocked S2L's request (CAPTCHA / IP rate-limit).</p>"
            b"<p>This response has <strong>not</strong> been cached.</p>"
            b"<pre>Path:   " + req_path.encode() + b"\n"
            b"Status: " + str(ctx.resp_status).encode() + b"</pre>"
            b"</body></html>"
        )
        err_ctx = _s2l_state.HookContext(
            method=method, url=target, path=req_path,
            query=flask_request.query_string.decode("utf-8", "ignore"),
            req_headers=ctx.req_headers, req_body=ctx.req_body,
            resp_status=503, resp_headers={},
            resp_body=err_body, resp_ct="text/html; charset=utf-8",
        )
        _s2l_state._gui_push(err_ctx)
        return _s2l_state._make_flask_resp(err_ctx, method)

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
            if _norm_target not in _s2l_state.visited:
                _s2l_state.visited.add(_norm_target)
                _s2l_state._visited_order.append(_norm_target)
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

    # HTML + CSS + JSON post-processing
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

    if _s2l_state._RESP_HOOKS:
        _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._RESP_HOOKS, ctx))
    _s2l_state._apply_gui_hooks(ctx)

    if not isinstance(ctx.resp_body, (bytes, bytearray)):
        ctx.resp_body = b""
    if ctx.resp_body and "content-length" not in {k.lower() for k in ctx.resp_headers}:
        ctx.resp_headers["Content-Length"] = str(len(ctx.resp_body))

    # Final blank-page guard. If after ALL recovery the body is still
    # empty AND it's a top-level HTML document (status 200, text/html, GET),
    # try ONE last re-fetch with stream=False + identity encoding (the stream
    # mode of curl_cffi can sometimes lose chunks). If that also fails, show
    # a diagnostic page instead of a blank white screen.
    if (method in _s2l_state._SAFE_METHODS
            and ctx.resp_status == 200
            and len(ctx.resp_body) == 0
            and "text/html" in (ctx.resp_ct or "")
            and not flask_request.headers.get("Range")):
        # Last-resort re-fetch with stream=False + identity encoding
        try:
            _last_fwd = dict(ctx.req_headers)
            _last_fwd["Accept-Encoding"] = "identity"
            _last_ctx = _dc_replace(ctx, req_headers=_last_fwd)
            _last_r = _s2l_state._do_upstream(method, target, _last_ctx, stream=False)
            _last_body = _last_r.content
            if _last_body and len(_last_body) > 0:
                ctx.resp_body = _last_body
                ctx.resp_ct = _last_r.headers.get("Content-Type", "text/html")
                ctx.resp_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(_last_r), _last_body,
                                                is_top_level_html=True)
                ctx.resp_headers.pop("content-length", None)
                ctx.resp_headers.pop("Content-Length", None)
                _s2l_state.log(f"Last-resort re-fetch OK for {req_path}: {_s2l_state._fmt_size(len(_last_body))}", "INFO")
            else:
                _s2l_state.log(f"Last-resort re-fetch also returned empty body for {req_path}", "WARN")
        except Exception as e:
            _s2l_state.log(f"Last-resort re-fetch error: {_s2l_state._short_exc(e)}", "WARN")
    if (method in _s2l_state._SAFE_METHODS
            and ctx.resp_status == 200
            and len(ctx.resp_body) == 0
            and "text/html" in (ctx.resp_ct or "")
            and not flask_request.headers.get("Range")):
        ctx.resp_ct = "text/html; charset=utf-8"
        ctx.resp_body = (
            b"<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
            b"<title>S2L \xe2\x80\x94 Empty Response</title>"
            b"<style>body{font-family:system-ui,sans-serif;max-width:600px;"
            b"margin:50px auto;padding:20px;color:#333}"
            b"h1{color:#e74c3c}code{background:#f4f4f4;padding:2px 6px;"
            b"border-radius:3px}</style></head><body>"
            b"<h1>Empty response from upstream</h1>"
            b"<p>The server returned HTTP 200 but with an empty body. This "
            b"usually means:</p>"
            b"<ul>"
            b"<li>The page is behind a CAPTCHA / bot challenge that S2L couldn't bypass</li>"
            b"<li>The upstream server detected the proxy and returned an empty page</li>"
            b"<li>A compression/decompression issue occurred</li>"
            b"</ul>"
            b"<p>Try:</p>"
            b"<ul>"
            b"<li>Refreshing the page (S2L auto-rotates sessions on failures)</li>"
            b"<li>Checking the S2L console for CF block warnings</li>"
            b"<li>Installing <code>curl_cffi</code> for better Cloudflare bypass</li>"
            b"</ul>"
            b"<p><small>Path: <code>" + req_path.encode("utf-8", "replace") + b"</code></small></p>"
            b"</body></html>"
        )
        ctx.resp_headers.pop("content-length", None)
        ctx.resp_headers.pop("Content-Length", None)

    _s2l_state._gui_push(ctx)
    _s2l_state._maybe_capture(ctx)
    return _s2l_state._make_flask_resp(ctx, method)
