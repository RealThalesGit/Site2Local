"""Site2Local: upstream fetch, session-rotation retry ladder and stream hand-off.

Extracted from the single proxy() body of the s2l.py monolith
(match: s2l.py lines ~12676-12876).
"""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from ... import Runtime as _s2l_state


def _decide_streaming(method: str, target: str, _has_range: bool) -> bool:
    """Decide whether to use stream=True for the upstream request.

    stream=True is needed for: Range requests (audio/video 206), URLs
    whose extension suggests streamable content (video/audio/wasm/large bin),
    AND Server-Sent Events endpoints (text/event-stream).

    SSE is enabled by default: the browser's EventSource() always sends
    "Accept: text/event-stream" on the request — that's a perfect pre-flight
    signal that we MUST use stream=True, otherwise curl_cffi refuses to
    iterate ("stream mode is not enabled") and the SSE streamer emits the
    "SSE upstream stream error ... stream mode is not enabled" warning on
    every request. We also catch common SSE URL patterns for clients that
    omit the Accept header (older polyfills, fetch()-based consumers).
    """
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
    return (method in _s2l_state._SAFE_METHODS
            and (_has_range or _looks_streamable or _looks_sse))


def _fetch_with_retries(method: str, target: str, ctx, _do_stream: bool,
                        req_path: str, _has_range: bool):
    """Fetch upstream, rotating the client session on connection errors.

    Returns the upstream response object, or a Flask Response (502) when
    every retry tier failed — the caller short-circuits on isinstance().
    """
    try:
        return _s2l_state._do_upstream(method, target, ctx, stream=_do_stream)
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
                            _s2l_state.log(f"Upstream down — serving cached copy: {req_path}", "WARN")
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


def _push_redirect_history(method: str, upstream_r) -> None:
    """Feed the GUI traffic log with every intermediate redirect response."""
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


def _maybe_stream(upstream_r, method: str, target: str, _do_stream: bool, ctx,
                  req_path: str):
    """SSE / large-body streaming hand-off.

    Returns (streamed_response, upstream_r): the response is None when the
    request must continue through the buffered pipeline; upstream_r may have
    been replaced by the SSE re-fetch (stream=True).
    """
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
        return _s2l_state._stream_sse_resp(upstream_r, method, target), upstream_r
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
        return _s2l_state._stream_resp(upstream_r, method, target), upstream_r

    return None, upstream_r
