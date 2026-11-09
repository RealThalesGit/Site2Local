"""Cross-origin /__s2l_ext__/ CDN asset route."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import os

from .. import Runtime as _s2l_state
from ..HTTP.Streaming.Response import _buffer_rewritable_response


def ext_asset(extpath: str) -> Response:
    slash = extpath.find("/")
    if slash == -1:
        return Response("Bad ext path", status=400)
    ext_host = extpath[:slash]
    ext_path = extpath[slash:]
    method   = flask_request.method.upper()
    qs       = flask_request.query_string.decode("utf-8", "ignore")

    # Recover a still-percent-encoded object path when possible — see
    # _raw_path_after() for why this matters (object-key %2F paths, etc.)
    _raw_rest = _s2l_state._raw_path_after(_s2l_state._EXT_PREFIX + "/")
    if _raw_rest is not None:
        _raw_slash = _raw_rest.find("/")
        if _raw_slash != -1:
            ext_path = _raw_rest[_raw_slash:]

    real_url = f"https://{ext_host}{ext_path}"
    if qs:
        real_url += f"?{qs}"

    if method == "OPTIONS":
        _aco, _acc = _s2l_state._cors_origin_pair()
        r = Response(status=204)
        r.headers.update({
            "Access-Control-Allow-Origin":  _aco,
            "Access-Control-Allow-Credentials": _acc,
            "Access-Control-Allow-Methods": ", ".join(_s2l_state._ALL_METHODS),
            # Concrete list (never bare "*") so credentialed preflights pass.
            "Access-Control-Allow-Headers": _s2l_state._cors_allow_headers(),
            "Access-Control-Expose-Headers": "*",
            "Access-Control-Max-Age": "86400",
            "Vary": "Origin",
        })
        # Private Network Access: allow public-context fetches to localhost.
        r.headers.update(_s2l_state._pna_allow_header())
        r.headers = {k: v for k, v in r.headers.items() if v is not None}
        return r

    lp = _s2l_state.local_path(real_url)
    if method in _s2l_state._SAFE_METHODS and (_s2l_state.CACHE_CDN or _s2l_state.OFFLINE) and os.path.isfile(lp):
        # Range requests on a cache hit MUST be answered with 206 Partial
        # Content — a full 200 makes HTML5 <video>/<audio> treat the stream
        # as truncated and loop/stall (same rule as the main proxy's cache
        # path; shared seam _serve_range_from_disk).
        if flask_request.headers.get("Range", ""):
            _rng = _s2l_state._serve_range_from_disk(lp, real_url, method, ext_path)
            if _rng is not None:
                return _rng
            # unparseable range / read failure — fall through to full serve
        _probe_ct_e = _s2l_state._resolve_mime_with_sniff(lp)
        _c304 = _s2l_state._conditional_304(lp, _probe_ct_e)
        if _c304 is not None:
            _s2l_state.stats.inc("cache_304")
            _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} [EXT] {method} HIT 304 {_s2l_state._fmt_host(ext_host)}{_s2l_state._fmt_path_qs(ext_path, qs)}", "CDN")
            _s2l_state._gui_push_raw(method, ext_path, 304, _probe_ct_e, b"",
                          display_tag="[CDN] [←] [EXT]", origin=ext_host, _skip_log=True)
            return _c304
        result = _s2l_state._serve_cached(lp)
        if result is not None:
            data, ct = result
            # Fix 17 — the root-relative pass on cache HITs: _serve_cached
            # already applied the absolute-URL passes, but root-relative
            # URLs need THIS route's host to resolve — without this they'd
            # land on the main-site route (404 JSON + nosniff → the browser
            # drops the resource with a MIME mismatch).
            _ctb_e = ct.split(";")[0].strip().lower()
            if _s2l_state.PROXY_CDN and ("text/html" in _ctb_e or "text/css" in _ctb_e):
                data = _s2l_state.rewrite_doc_relative(
                    data, ext_host, html_doc=("text/html" in _ctb_e))
            _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} [EXT] {method} HIT {_s2l_state._fmt_host(ext_host)}{_s2l_state._fmt_path_qs(ext_path, qs)} {_s2l_state._fmt_size(len(data))}", "CDN")
            _s2l_state._gui_push_raw("GET", ext_path, 200, ct, data,
                          display_tag="[CDN] [←] [EXT]", origin=ext_host, _skip_log=True)
            resp = Response(data, content_type=ct)
            if ct.startswith("text/html") or "json" in ct:
                resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            else:
                resp.headers["Cache-Control"] = "no-cache"
                _val = _s2l_state._disk_validators(lp, ct)
                if _val:
                    resp.headers["ETag"] = _val[0]
                    resp.headers["Last-Modified"] = _val[1]
            _s2l_state._apply_cors_headers(resp.headers)
            _s2l_state._apply_isolation_headers(resp.headers,
                                     is_worker_script=_s2l_state._is_worker_request())
            _s2l_state._apply_security_headers(resp.headers)
            return resp
    if _s2l_state.OFFLINE:
        _s2l_state.log(f"EXT {method} 404 {_s2l_state._fmt_host(ext_host)}{_s2l_state._fmt_path_qs(ext_path, qs)} (not cached)", "CDN")
        return Response("Offline — not cached", status=404)
    try:
        req_body = flask_request.get_data(cache=False) if method in _s2l_state._BODY_METHODS else b""
        ctx = _s2l_state.HookContext(
            method=method, url=real_url, path=ext_path, query=qs,
            req_headers=_s2l_state.filter_fwd(dict(flask_request.headers)),
            req_body=req_body,
        )
        _real_origin = _s2l_state._real_origin_for(
            ctx.req_headers.get("Origin") or ctx.req_headers.get("Referer"))
        for _k in list(ctx.req_headers.keys()):
            _kl = _k.lower()
            if _kl == "origin": ctx.req_headers[_k] = _real_origin
            elif _kl == "referer": ctx.req_headers[_k] = _real_origin + "/"
        ctx.req_headers["Host"] = ext_host

        if _s2l_state._REQ_HOOKS:
            _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._REQ_HOOKS, ctx))

        # NOTE: do NOT force Accept-Encoding: identity here — see the matching
        # comment in _cdn_serve. Our decompress_body() / the streaming
        # decompressor handle every encoding correctly.

        # Decide streaming BEFORE building kwargs so we can use the longer
        # TIMEOUT_STREAM_READ for streamed (large/binary) responses. See the
        # matching comment in _cdn_serve for why extensionless large binary
        # paths need the path-level heuristic.
        _has_range = bool(flask_request.headers.get("Range", ""))
        _guess_ct_e = _s2l_state.guess_mime(ext_path)
        _path_lower_e = ext_path.lower()
        _looks_large_binary_e = (
            "wasm" in _path_lower_e
            or "/builds/" in _path_lower_e
            or "/objects/" in _path_lower_e
            or "/assets/" in _path_lower_e
        )
        _do_stream_ext = method in _s2l_state._SAFE_METHODS and (
            _has_range or _s2l_state._should_stream(_guess_ct_e) or _looks_large_binary_e)
        _read_timeout_e = _s2l_state.TIMEOUT_STREAM_READ if _do_stream_ext else _s2l_state.TIMEOUT_READ

        kw: dict = {
            "headers":         ctx.req_headers,
            "timeout":         (_s2l_state.TIMEOUT_CONN, _read_timeout_e),
            "allow_redirects": True,
            "verify":          False,
        }
        if method in _s2l_state._BODY_METHODS:
            kw["data"] = ctx.req_body
        kw["stream"] = _do_stream_ext
        r    = _s2l_state._get_client_session().request(method, real_url, **kw)
        # Documents need the normal rewrite/hook pipeline even when path
        # heuristics selected streaming. Consume iter_content, never r.content.
        body = _buffer_rewritable_response(r) if _do_stream_ext else None
        if _do_stream_ext and body is None:
            return _s2l_state._stream_resp(r, method, real_url)
        if body is None:
            body = _s2l_state.decompress_body(r.content, r.headers.get("Content-Encoding", ""))
        ct   = _s2l_state._ensure_wasm_content_type(
            r.headers.get("Content-Type", "application/octet-stream"), ext_path)
        # Only save real 200 responses with content — 304 has empty body and saving
        # it would corrupt any existing cached file.
        if r.status_code == 200 and body and method in _s2l_state._SAFE_METHODS and _s2l_state.CACHE_CDN:
            _s2l_state.save_queue.put((real_url, body, ct, _s2l_state._effective_device()))
            _s2l_state._register_cdn_host(ext_host)
            _s2l_state.stats.inc("cdn_fetched")
        elif r.status_code < 400 and method in _s2l_state._SAFE_METHODS:
            # CACHE_CDN=False: still register host so URL rewriting works next time
            _s2l_state._register_cdn_host(ext_host)
        out_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(r), body,
                                    is_top_level_html=_s2l_state._is_document_ct(ct))
        # CORS + CORP are already set correctly by filter_resp() — including
        # the concrete Access-Control-Expose-Headers list for credentialed
        # requests. Do NOT re-set them here (see _cdn_serve for rationale).
        if method in _s2l_state._SAFE_METHODS:
            _s2l_state._apply_freshness(out_headers, ct)
        # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
        # through the proxy (critical for cross-origin iframes served via /__s2l_ext__/).
        _ct_base_e = ct.split(";")[0].strip().lower()
        if r.status_code < 400 and _s2l_state.PROXY_CDN:
            if "text/html" in _ct_base_e:
                body = _s2l_state._rewrite_ext_urls(body, html_doc=True)
                body = _s2l_state.rewrite_abs_urls(body)
                body = _s2l_state._inject_sw_clear(body)
                # Fix 17 — root-relative URLs resolve against THIS document's
                # host, not the proxy origin (Discord widget /assets/* case).
                body = _s2l_state.rewrite_doc_relative(body, ext_host, html_doc=True)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)
            elif "text/css" in _ct_base_e:
                body = _s2l_state._rewrite_ext_urls(body, html_doc=False)
                body = _s2l_state.rewrite_doc_relative(body, ext_host, html_doc=False)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)
            elif "json" in _ct_base_e:
                body = _s2l_state._rewrite_json_urls(body)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)

        ctx.resp_status, ctx.resp_headers, ctx.resp_body, ctx.resp_ct = (
            r.status_code, out_headers, body, ct)
        if _s2l_state._RESP_HOOKS:
            _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._RESP_HOOKS, ctx))
        _s2l_state._apply_gui_hooks(ctx)

        _s2l_state.log(f"[EXT] {method} {ctx.resp_status} {_s2l_state._fmt_host(ext_host)}{_s2l_state._fmt_path_qs(ext_path, qs)} {_s2l_state._fmt_size(len(ctx.resp_body))}", "CDN")
        _s2l_state._gui_push_raw(method, ext_path, ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                      display_tag="[EXT]", origin=_s2l_state._fmt_host(ext_host),
                      _skip_log=True)   # caller already logged
        if _s2l_state.CAPTURE and _s2l_state.CAPTURE_CDN:
            _s2l_state._maybe_capture(ctx)
        return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
    except _s2l_state._CONN_ERRORS as exc:
        return Response(f"CDN unreachable: {_s2l_state._short_exc(exc)}", status=502)
    except Exception as exc:
        return Response(f"CDN error: {_s2l_state._short_exc(exc)}", status=502)


