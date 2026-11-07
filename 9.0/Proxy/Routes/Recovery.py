"""Site2Local: response recovery — WAF retry, 304, empty bodies, blank pages.

Extracted from the single proxy() body of the s2l.py monolith
(match: s2l.py lines ~12876-13101 and ~13328-13389).
"""
from __future__ import annotations

from dataclasses import replace as _dc_replace
from flask import Response
from flask import request as flask_request
import os

from ... import Runtime as _s2l_state


def _consume_upstream_body(upstream_r, method: str, target: str, ctx, req_path: str):
    """Decompress the upstream body, recovering from stream-mode empty bodies.

    Returns (body, upstream_r) — upstream_r may be replaced by the
    stream=False re-fetch.
    """
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
    return body, upstream_r


def _waf_block_retry(upstream_r, body: bytes, method: str, target: str, ctx,
                     req_path: str):
    """Detect classic WAF block pages, rotate the client session, retry once.

    Returns (upstream_r, body) — replaced when the retry succeeded.
    """
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
    return upstream_r, body


def _handle_304(upstream_r, body: bytes, method: str, target: str, req_path: str):
    """304 from upstream without validators: serve cache or force a clean fetch.

    Returns (upstream_r, body), or a Flask Response when unrecoverable.
    """
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
    return upstream_r, body


def _recover_empty_body(upstream_r, body: bytes, method: str, target: str, ctx,
                        req_path: str):
    """Re-fetch with identity encoding on decompression/API/HTML empty bodies.

    Returns (upstream_r, body).
    """
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
    return upstream_r, body


def _final_blank_page_recovery(ctx, method: str, target: str, req_path: str) -> None:
    """Last-resort re-fetch + diagnostic page for empty top-level HTML. Mutates ctx."""
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
