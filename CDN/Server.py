"""Per-CDN-host Werkzeug mini-server (MULTIPORT)."""
from __future__ import annotations

from flask import Flask
from flask import Response
from flask import request as flask_request
from werkzeug.serving import make_server
import logging
import os
import threading

from .. import Runtime as _s2l_state


def _start_cdn_server(cdn_host: str, port: int) -> None:
    cdn_app = Flask(f"cdn_{cdn_host}", static_folder=None)
    # OOM FIX: bound request bodies on the CDN mini-servers too.
    cdn_app.config["MAX_CONTENT_LENGTH"] = _s2l_state.MAX_BODY_BYTES

    @cdn_app.after_request
    def _cdn_security_headers(response: Response) -> Response:
        return _s2l_state._finalize_security_headers(response)

    @cdn_app.route("/", defaults={"p": ""}, methods=_s2l_state._ALL_METHODS)
    @cdn_app.route("/<path:p>",             methods=_s2l_state._ALL_METHODS)
    def _cdn_serve(p: str):
        method  = flask_request.method.upper()
        _raw_rest = _s2l_state._raw_path_after("/")
        if _raw_rest is not None:
            p = _raw_rest
        # v8.8 — defensive: if a request reaches this CDN mini-server with
        # the /__s2l_ext__/<host>/ prefix still in the path, strip it.
        # This happens when a stale (cached) HTML response carries a URL
        # that was rewritten by an older _proxy_target() that incorrectly
        # used the current Host header (so it pointed to localhost:8085
        # instead of localhost:8080). The CDN mini-server would then
        # construct cdn_url = "https://{cdn_host}/__s2l_ext__/{cdn_host}/..."
        # which 400s at upstream. Stripping the prefix recovers gracefully.
        # NOTE: Werkzeug's <path:p> route converter strips the leading "/",
        # so we compare against the prefix without it.
        _ext_prefix_rel = _s2l_state._EXT_PREFIX.lstrip("/")
        if p.startswith(_ext_prefix_rel + "/"):
            _after = p[len(_ext_prefix_rel) + 1:]
            _slash = _after.find("/")
            if _slash != -1:
                p = _after[_slash:]
            else:
                p = "/"
        cdn_url = f"https://{cdn_host}/{p.lstrip('/')}"
        qs      = flask_request.query_string.decode("utf-8", "ignore")
        if qs:
            cdn_url += f"?{qs}"

        # NOTE: no Upgrade:websocket branch here — _S2LWSGIRequestHandler
        # intercepts every WS upgrade at the socket layer before this view
        # ever runs (see its docstring). A request reaching this function
        # is guaranteed to not be a WS upgrade.

        # OPTIONS — fast CORS preflight
        if method == "OPTIONS":
            _req_origin = flask_request.headers.get("Origin", "")
            r = Response(status=204)
            r.headers.update({
                "Access-Control-Allow-Origin":  _req_origin if _req_origin else "*",
                "Access-Control-Allow-Credentials": "true" if _req_origin else None,
                "Access-Control-Allow-Methods": ", ".join(_s2l_state._ALL_METHODS),
                "Access-Control-Allow-Headers": _s2l_state._cors_allow_headers(),
                "Access-Control-Expose-Headers": "*",
                "Access-Control-Max-Age": "86400",
                "Vary": "Origin",
            })
            # Private Network Access: allow public-context fetches to localhost.
            r.headers.update(_s2l_state._pna_allow_header())
            r.headers = {k: v for k, v in r.headers.items() if v is not None}
            return r

        # Cache-hit only for GET/HEAD when CACHE_CDN is enabled.
        # In OFFLINE mode, always check cache regardless of CACHE_CDN —
        # the flag governs WRITES, not reads; a previously-cached asset
        # should still be served when offline.
        lp = _s2l_state.local_path(cdn_url)
        if method in _s2l_state._SAFE_METHODS and (_s2l_state.CACHE_CDN or _s2l_state.OFFLINE) and os.path.isfile(lp):
            # Range requests on a cache hit MUST get 206 Partial Content —
            # a full 200 stalls/loops HTML5 <video>/<audio> (shared seam
            # _serve_range_from_disk, same rule as the main proxy).
            if flask_request.headers.get("Range", ""):
                _rng_c = _s2l_state._serve_range_from_disk(lp, cdn_url, method, "/" + p.lstrip("/"))
                if _rng_c is not None:
                    return _rng_c
            _c304 = _s2l_state._conditional_304(lp, _s2l_state._resolve_mime_with_sniff(lp))
            if _c304 is not None:
                _s2l_state.stats.inc("cache_304")
                _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} {method} HIT 304 {_s2l_state._fmt_host(cdn_host)}/{_s2l_state._fmt_path_qs(p, qs)}", "CDN")
                return _c304
            result = _s2l_state._serve_cached(lp)
            if result is not None:
                data, ct = result
                # Fix 17 — the root-relative pass on cache HITs (same rule as
                # ext_asset): root-relative URLs need THIS mini-server's host
                # to resolve, or they land on the main-site route.
                _ctb_c = ct.split(";")[0].strip().lower()
                if _s2l_state.PROXY_CDN and ("text/html" in _ctb_c or "text/css" in _ctb_c):
                    data = _s2l_state.rewrite_doc_relative(
                        data, cdn_host, html_doc=("text/html" in _ctb_c))
                _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} {method} HIT {_s2l_state._fmt_host(cdn_host)}/{_s2l_state._fmt_path_qs(p, qs)} {_s2l_state._fmt_size(len(data))}", "CDN")
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
            _s2l_state.log(f"CDN {method} 404 {_s2l_state._fmt_host(cdn_host)}/{_s2l_state._fmt_path_qs(p, qs)} (not cached)", "CDN")
            return Response("Offline", status=404)

        # Forward request to real CDN (all methods, including POST/PUT for uploads)
        try:
            req_body = flask_request.get_data(cache=False) if method in _s2l_state._BODY_METHODS else b""
            ctx = _s2l_state.HookContext(
                method=method, url=cdn_url, path="/" + p.lstrip("/"), query=qs,
                req_headers=_s2l_state.filter_fwd(dict(flask_request.headers)),
                req_body=req_body,
            )
            _real_origin = _s2l_state._real_origin_for(
                ctx.req_headers.get("Origin") or ctx.req_headers.get("Referer"))
            for _k in list(ctx.req_headers.keys()):
                _kl = _k.lower()
                if _kl == "origin": ctx.req_headers[_k] = _real_origin
                elif _kl == "referer": ctx.req_headers[_k] = _real_origin + "/"
            ctx.req_headers["Host"] = cdn_host

            if _s2l_state._REQ_HOOKS:
                _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._REQ_HOOKS, ctx))

            # NOTE: we deliberately do NOT force Accept-Encoding: identity here
            # (unlike _do_upstream). Our decompress_body() + the streaming
            # decompressor in _stream_resp handle every encoding (gzip/deflate/
            # br/zstd) correctly, so letting the upstream negotiate compression
            # is safe and avoids the round-trip through identity decoding.

            # Decide streaming BEFORE building kwargs so we can use the longer
            # TIMEOUT_STREAM_READ for streamed (large/binary) CDN responses.
            # The CDN path used to hardcode TIMEOUT_READ=12s for ALL requests,
            # which truncated large WASM/asset downloads (>5MB) on slow CDNs
            # and produced "Failed to fetch" errors in the browser.
            _has_range_c = bool(flask_request.headers.get("Range", ""))
            _guess_ct_c = _s2l_state.guess_mime("/" + p)
            # Also force-stream paths that look like large binary assets even
            # when they have no file extension (e.g. /objects/builds/.../2026-05-10).
            # guess_mime returns application/octet-stream for extensionless paths,
            # and _should_stream returns False for octet-stream when Content-Length
            # is unknown (cl=0) — so a 14.4MB WASM bundle with no extension would
            # be fully buffered in memory with a 12s timeout. The path-level check
            # below catches common binary-resource path patterns so these
            # responses always stream.
            _path_lower = "/" + p.lower()
            _looks_large_binary = (
                "wasm" in _path_lower
                or "/builds/" in _path_lower
                or "/objects/" in _path_lower
                or "/assets/" in _path_lower
            )
            _do_stream_c = method in _s2l_state._SAFE_METHODS and (
                _has_range_c or _s2l_state._should_stream(_guess_ct_c) or _looks_large_binary)
            _read_timeout_c = _s2l_state.TIMEOUT_STREAM_READ if _do_stream_c else _s2l_state.TIMEOUT_READ

            kw: dict = {
                "headers":         ctx.req_headers,
                "timeout":         (_s2l_state.TIMEOUT_CONN, _read_timeout_c),
                "allow_redirects": True,
                "verify":          False,
            }
            if method in _s2l_state._BODY_METHODS:
                kw["data"] = ctx.req_body
            kw["stream"] = _do_stream_c
            r    = _s2l_state._get_client_session().request(method, cdn_url, **kw)
            # CRITICAL: when stream=True was used, ALWAYS hand off to _stream_resp.
            # Falling through to r.content loses chunks on curl_cffi HTTP/2 for
            # large binary files (.tar bundles, .wasm, video, etc), producing
            # "0.0B" bodies even though the server returned 200 with real content.
            if _do_stream_c:
                return _s2l_state._stream_resp(r, method, cdn_url)
            body = _s2l_state.decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            ct   = _s2l_state._ensure_wasm_content_type(
                r.headers.get("Content-Type", "application/octet-stream"),
                "/" + p.lstrip("/"))
            # Empty-body recovery: curl_cffi's HTTP/2 mode sometimes returns
            # empty content for small images/avatars on CDN hosts. Without
            # this, the browser gets a 200 with 0 bytes — a "phantom" image
            # that loads but displays nothing. Re-fetch with stream=False and
            # identity encoding to recover the real bytes.
            if (not body and r.status_code == 200 and method in _s2l_state._SAFE_METHODS
                    and not getattr(_s2l_state._proxy_local, "in_cdn_refetch", False)):
                _cl_cdn = r.headers.get("Content-Length", "0") or "0"
                try:
                    _cl_cdn_int = int(_cl_cdn)
                except ValueError:
                    _cl_cdn_int = 0
                if _cl_cdn_int > 0:
                    _s2l_state.log(f"CDN empty body on {p} — re-fetching with identity encoding", "WARN")
                    try:
                        _s2l_state._proxy_local.in_cdn_refetch = True
                        _retry_hdrs = dict(ctx.req_headers)
                        _retry_hdrs["Accept-Encoding"] = "identity"
                        _retry_r = _s2l_state._get_client_session().request(
                            method, cdn_url, headers=_retry_hdrs,
                            timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ),
                            allow_redirects=True, verify=False, stream=False)
                        _retry_body = _retry_r.content
                        if _retry_body:
                            body = _retry_body
                            r = _retry_r
                            ct = r.headers.get("Content-Type", ct)
                            _s2l_state.log(f"CDN re-fetch OK: {_s2l_state._fmt_size(len(body))}", "INFO")
                    except Exception as _re:
                        _s2l_state.log(f"CDN re-fetch failed: {_s2l_state._short_exc(_re)}", "WARN")
                    finally:
                        _s2l_state._proxy_local.in_cdn_refetch = False
            # Only save on real 200 responses with content — 304 has an empty body and
            # saving it would corrupt/truncate a previously-good cached file.
            if r.status_code == 200 and body and method in _s2l_state._SAFE_METHODS and _s2l_state.CACHE_CDN:
                _s2l_state.save_queue.put((cdn_url, body, ct, _s2l_state._effective_device()))
                _s2l_state.stats.inc("cdn_fetched")
            out_headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(r), body,
                                        is_top_level_html=_s2l_state._is_document_ct(ct))
            if method in _s2l_state._SAFE_METHODS:
                _s2l_state._apply_freshness(out_headers, ct)
            # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
            # through the proxy (critical for cross-origin iframes served from CDN hosts).
            _ct_base_c = ct.split(";")[0].strip().lower()
            if r.status_code < 400 and _s2l_state.PROXY_CDN:
                if "text/html" in _ct_base_c:
                    body = _s2l_state._rewrite_ext_urls(body, html_doc=True)
                    body = _s2l_state.rewrite_abs_urls(body)
                    body = _s2l_state._inject_sw_clear(body)
                    # Fix 17 — root-relative URLs resolve against THIS
                    # document's host (Discord widget /assets/* case).
                    body = _s2l_state.rewrite_doc_relative(body, cdn_host, html_doc=True)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)
                elif "text/css" in _ct_base_c:
                    body = _s2l_state._rewrite_ext_urls(body, html_doc=False)
                    body = _s2l_state.rewrite_doc_relative(body, cdn_host, html_doc=False)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)
                elif "json" in _ct_base_c:
                    body = _s2l_state._rewrite_json_urls(body)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)

            ctx.resp_status, ctx.resp_headers, ctx.resp_body, ctx.resp_ct = (
                r.status_code, out_headers, body, ct)
            if _s2l_state._RESP_HOOKS:
                _s2l_state.stats.inc("hooks_run", _s2l_state._run_hooks(_s2l_state._RESP_HOOKS, ctx))
            _s2l_state._apply_gui_hooks(ctx)
            if _s2l_state.CAPTURE and _s2l_state.CAPTURE_CDN:
                _s2l_state._maybe_capture(ctx)

            _s2l_state.log(f"{method} {ctx.resp_status} {_s2l_state._fmt_host(cdn_host)}/{_s2l_state._fmt_path_qs(p, qs)} {_s2l_state._fmt_size(len(ctx.resp_body))} :{port}", "CDN")
            _s2l_state._gui_push_raw(method, f"/{p}", ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                          display_tag=f"[CDN:{port}]", origin=_s2l_state._fmt_host(cdn_host),
                          _skip_log=True)
            # Health tracking for the web viewer (per-host dot + RTT + requests).
            _s2l_state.record_cdn_health(cdn_host, ctx.resp_status, int(r.elapsed.total_seconds()*1000))
            return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
        except Exception as exc:
            _s2l_state.log(f"CDN error: {cdn_host}/{p} — {_s2l_state._short_exc(exc)}", "WARN")
            _s2l_state.record_cdn_health(cdn_host, 502, 0, is_error=True)
            return Response(str(exc), status=502)

    def _run():
        _wz = logging.getLogger("werkzeug")
        _wz.setLevel(logging.ERROR)
        try:
            server = make_server(_s2l_state.HOST, port, cdn_app, threaded=True,
                                 request_handler=_s2l_state._S2LWSGIRequestHandler)
            server.__class__ = _s2l_state._PooledWSGIServer   # swap to use pool
            # _S2LWSGIRequestHandler intercepts WS upgrades at the raw-socket
            # layer for ALL server instances (see class docstring). It needs
            # to know THIS server's upstream host for its "not a special
            # /__s2l_*__/ path" fallback — without this it defaults to
            # SITE_URL (the main host) for every CDN port too, silently
            # misrouting any WS connection opened directly against a CDN
            # mini-server to the wrong upstream.
            server.s2l_target_base = f"https://{cdn_host}"
        except Exception as exc:
            _s2l_state.log(f"CDN {cdn_host}:{port} bind failed — {exc}", "ERROR")
            with _s2l_state._cdn_port_lock:
                _s2l_state._cdn_host_port.pop(cdn_host, None)   # clear sentinel
            return
        # Port is now bound — register so JS injector can start routing here
        with _s2l_state._cdn_port_lock:
            _s2l_state._cdn_host_port[cdn_host] = port
        _s2l_state.log(f"{cdn_host} → :{port}", "CDN")
        try:
            server.serve_forever()
        except Exception as exc:
            _s2l_state.log(f"CDN {cdn_host}:{port} crashed — {exc}", "ERROR")
        finally:
            # If this mini-server dies after a successful bind, drop the stale
            # mapping so the next request for this host re-registers and spins
            # up a fresh instance instead of permanently hitting
            # ERR_CONNECTION_REFUSED on a port nothing is listening on anymore.
            with _s2l_state._cdn_port_lock:
                if _s2l_state._cdn_host_port.get(cdn_host) == port:
                    _s2l_state._cdn_host_port.pop(cdn_host, None)

    threading.Thread(target=_run, daemon=True, name=f"cdn-{cdn_host}").start()


