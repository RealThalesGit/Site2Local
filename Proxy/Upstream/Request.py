"""Site2Local: upstream request execution and response building."""
from __future__ import annotations

from flask import request as flask_request
from urllib.parse import urlparse
import re
import requests
import requests.adapters

from ... import Runtime as _s2l_state


def _upstream_url(path: str) -> str:
    origin = f"{urlparse(_s2l_state.SITE_URL).scheme}://{_s2l_state.MAIN_HOST}"
    base   = f"{origin}/{path.lstrip('/')}"
    qs     = flask_request.query_string.decode("utf-8", "ignore")
    return f"{base}?{qs}" if qs else base


def _build_ctx(method: str, target: str, req_body: bytes) -> _s2l_state.HookContext:
    parsed = urlparse(target)
    return _s2l_state.HookContext(
        method      = method,
        url         = target,
        path        = parsed.path or "/",
        query       = flask_request.query_string.decode("utf-8", "ignore"),
        req_headers = _s2l_state.filter_fwd(dict(flask_request.headers)),
        req_body    = req_body,
    )


def _is_sw_path(path: str) -> bool:
    """Detect service worker paths. Must be precise — a too-broad match
    (e.g. any path containing "service-worker") clobbers unrelated helper
    modules, web workers, and API endpoints with the no-op SW that nukes
    all browser caches. Only match actual SW entry-point filenames."""
    if path in _s2l_state._SW_PATHS:
        return True
    # Match /service-worker.js, /sw.js, /serviceworker.js at any path depth,
    # plus /something-sw.js. Do NOT match -worker.js (that catches web workers
    # and audio worklets, not service workers) or bare substring matches.
    _sw_re = getattr(_s2l_state._is_sw_path, "_re", None)
    if _sw_re is None:
        _sw_re = re.compile(r"(?:^|/)(?:service[-_]?worker|sw|serviceworker)\.js(?:[?#]|$)", re.IGNORECASE)
        _s2l_state._is_sw_path._re = _sw_re
    return bool(_sw_re.search(path))


def _do_upstream(method: str, target: str, ctx: _s2l_state.HookContext,
                  stream: bool = False) -> requests.Response:
    """Execute upstream request and return response object with content consumed."""
    sess = _s2l_state._get_client_session()   # cookies isolated by signed browser identity

    fwd = dict(ctx.req_headers)
    fwd["Host"] = _s2l_state.MAIN_HOST

    # Rewrite Origin/Referer so upstream sees its real origin, not localhost:PORT
    origin_base = f"{urlparse(_s2l_state.SITE_URL).scheme}://{_s2l_state.MAIN_HOST}"
    _s2l_state.rewrite_origin(fwd, origin_base)
    # Carry CSRF tokens from the real browser request → upstream
    _s2l_state.inject_csrf_headers(fwd)

    browser_ua = flask_request.headers.get("User-Agent", "")
    if _s2l_state.DEVICE == "auto":
        if browser_ua:
            fwd["User-Agent"] = _s2l_state._sanitize_ua(browser_ua)
        else:
            device = _s2l_state._effective_device()
            ua = _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES.get(device, _s2l_state.UA_PROFILES["macintosh"]))
            fwd["User-Agent"] = ua
    else:
        ua = _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES.get(_s2l_state.DEVICE, _s2l_state.UA_PROFILES["macintosh"]))
        fwd["User-Agent"] = ua
    # NOTE: do NOT mutate sess.headers here — the session is shared across
    # concurrent requests from the same browser. Mutating it races with
    # other threads using the same session (RuntimeError: dict changed size
    # during iteration inside requests' header merging). The per-request
    # `fwd` headers already override session-level headers via kwargs["headers"].

    for hint_hdr in ("sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
                     "sec-ch-ua-arch", "sec-ch-ua-bitness", "sec-ch-ua-model",
                     "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
                     "sec-fetch-user"):
        if hint_hdr not in {k.lower() for k in fwd}:
            val = flask_request.headers.get(hint_hdr, "")
            if val:
                fwd[hint_hdr] = val

    orig_accept  = fwd.get("Accept", "")
    orig_purpose = flask_request.headers.get("Purpose", "")
    is_api_req = (
        "application/json" in orig_accept
        or "text/plain" in orig_accept
        or orig_purpose == "prefetch"
        or method in _s2l_state._BODY_METHODS
    )
    # Only inject a browser-like Accept when the original is missing or bare wildcard.
    # If the browser specified a typed Accept (text/css, image/*, etc.) preserve it —
    # asset bundle servers use it to pick format.
    # CRITICAL: do NOT rewrite Accept for script/style/image/font/worker requests.
    # Browsers send Accept: */* for <script src>, <link rel=stylesheet>, dynamic
    # import(), etc. Rewriting it to text/html makes content-negotiating servers
    # return HTML instead of the JS chunk → the chunk fails to parse → SPA halts
    # partway through boot (shell visible, main content blank).
    _sec_dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
    _is_asset = _sec_dest in ("script", "style", "image", "font", "video",
                              "audio", "track", "worker", "manifest", "fetch")
    _bare_accept = not orig_accept or orig_accept.strip() in ("*/*", "")
    if not is_api_req and _bare_accept and not _is_asset:
        fwd["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

    # Accept-Encoding strategy:
    # curl_cffi impersonate mode uses libcurl under the hood. libcurl with
    # CURLOPT_ACCEPT_ENCODING set will auto-decompress AND strip Content-Encoding
    # from response headers (so decompress_body sees no encoding to act on).
    # Without it set, the raw compressed bytes are returned unchanged.
    #
    # Our decompress_body handles both cases via magic-byte detection, but the
    # safest approach is to request "identity" (no compression) so we always
    # receive plain bytes — no ambiguity, no double-decompress risk.
    #
    # Exception: curl_cffi's impersonate mode may override Accept-Encoding
    # internally to match Chrome's fingerprint. We use the session header
    # override to ensure identity wins.
    fwd["Accept-Encoding"] = "identity"
    # NOTE: do NOT mutate sess.headers["Accept-Encoding"] — the session is
    # shared across concurrent requests, and mutating it races with other
    # threads. The per-request `fwd` header overrides session-level headers.

    # Use a longer read timeout for streaming requests (video/audio/large
    # downloads) — the default TIMEOUT_READ=12s is too short for slow CDNs
    # and causes playback to stall after ~30-40s.
    _read_timeout = _s2l_state.TIMEOUT_STREAM_READ if stream else _s2l_state.TIMEOUT_READ
    kwargs: dict = {
        "headers":         fwd,
        "timeout":         (_s2l_state.TIMEOUT_CONN, _read_timeout),
        "allow_redirects": True,
        "verify":          _s2l_state.TLS_VERIFY,
        "stream":          stream,
    }

    fwd.pop("Cookie",  None)
    fwd.pop("cookie",  None)
    merged_cookies = _s2l_state._flatten_cookiejar(sess.cookies, prefer_host=urlparse(target).hostname or "")
    merged_cookies.update(_s2l_state._browser_cookies())
    merged_cookies.pop("__s2l_browser", None)
    if merged_cookies:
        kwargs["cookies"] = merged_cookies

    if method in _s2l_state._BODY_METHODS and ctx.req_body:
        ct = flask_request.content_type or ""
        if "multipart/form-data" in ct:
            # Forward the raw body verbatim with the original Content-Type header
            # (which contains the boundary string). Do NOT let requests rebuild the
            # multipart — it would generate a new boundary and corrupt the payload.
            kwargs["data"] = ctx.req_body
            fwd["Content-Type"]   = ct
            fwd["Content-Length"] = str(len(ctx.req_body))
        elif "application/x-www-form-urlencoded" in ct:
            kwargs["data"] = ctx.req_body
            fwd["Content-Type"] = ct
        else:
            kwargs["data"] = ctx.req_body
            existing_ct = {k.lower() for k in fwd}
            if "content-type" not in existing_ct and ct:
                fwd["Content-Type"] = ct
    response = sess.request(method, target, **kwargs)
    if not stream:
        try:
            _ = response.content   # force-consume so connection is reusable
        except Exception:
            pass

    # ── HTTP/2 empty body bug workaround ─────────────────────────────────────
    # curl_cffi occasionally returns empty content on first request for HTTP/2
    # multiplexed responses (chunked API endpoints using HTTP/2).
    # Status 200/201 with 0 bytes and no explicit Content-Length: 0 → retry once.
    #
    # CRITICAL: reuse the SAME session (with cookies + Authorization) — do NOT
    # create a fresh unauthenticated session, which returns 401 and we would
    # mistakenly serve that 401 body instead of the real 200 data.
    _resp_cl_raw = response.headers.get("Content-Length", None) if not stream else None
    _resp_cl_explicit_zero = (_resp_cl_raw is not None and int(_resp_cl_raw or 0) == 0)
    if (not stream
            and not _resp_cl_explicit_zero
            and response.status_code in (200, 201)
            and response.content == b""
            and method in _s2l_state._SAFE_METHODS):
        try:
            # A successful empty POST may already have changed upstream state.
            # Only retry safe methods to avoid duplicating that operation.
            # Retry with the SAME authenticated session + identity encoding
            retry_fwd = dict(fwd)
            retry_fwd["Accept-Encoding"] = "identity"
            retry_kwargs = dict(kwargs)
            retry_kwargs["headers"] = retry_fwd
            retry_response = sess.request(method, target, **retry_kwargs)
            try:
                _ = retry_response.content
            except Exception:
                pass
            # Only promote the retry if it kept the same status code —
            # never replace a 200 with a 401/403 auth-error body.
            if retry_response.content and retry_response.status_code == response.status_code:
                response = retry_response
        except Exception:
            pass   # silently fall through — proxy layer will handle empty body

    # Cloudflare block detected → rotate to a fresh session and retry once.
    # IMPORTANT: when stream=True, DO NOT access response.content here —
    # accessing it would consume the entire stream before iter_content() is called,
    # causing an empty response. Classic 403/503 blocks are always HTML, never
    # binary, but Managed Challenge can also return 200 with a JS challenge body
    # (the q=78/jschl_vc family) — _is_cf_block checks for both via headers+body.
    if not stream and _s2l_state._is_cf_block(
            response.content[:8192] if response.content else b"",
            response.status_code,
            dict(response.headers)):
        _s2l_state.log(f"CF block on {urlparse(target).netloc} — rotating session and retrying", "WARN")
        # FIX: rotate the CLIENT session (the one that made this request),
        # NOT _proxy_local.s (thread-local, used only by background crawl
        # threads — rotating it here is a no-op that leaks the new session).
        # Mirrors the WAF-retry pattern in the proxy() route.
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
            _fresh = _s2l_state._make_session()
            _fresh.headers["User-Agent"] = fwd.get("User-Agent",
                _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES["macintosh"]))
            with _s2l_state._CLIENT_LOCK:
                _s2l_state._CLIENT_SESSIONS[_cid] = _fresh
            sess = _fresh
        else:
            # Background thread (no flask context) — fall back to _proxy_local.s
            _old_sess = getattr(_s2l_state._proxy_local, "s", None)
            if _old_sess is not None:
                try: _old_sess.close()
                except Exception: pass
            _s2l_state._proxy_local.s = _s2l_state._make_session()
            sess = _s2l_state._proxy_local.s
            sess.headers["User-Agent"] = fwd.get("User-Agent",
                _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES["macintosh"]))
        response = sess.request(method, target, **kwargs)
        if not stream:
            try:
                _ = response.content
            except Exception:
                pass

    return response
