"""Site2Local: CDN 404 fallback, RSC re-fetch and bot-page detection.

Extracted from the single proxy() body of the s2l.py monolith
(match: s2l.py lines ~13101-13262).
"""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os

from ... import Runtime as _s2l_state


def _cdn_fallback(upstream_r, method: str, target: str, req_path: str) -> Response | None:
    """Main host 404: try alternate CDN hosts (disk cache first, then live)."""
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
                    _s2l_state.log(f"{_s2l_state._CACHE_HIT_MARKER} CDN disk hit {_s2l_state._fmt_host(cdn_host)}{req_path_cdn}", "CDN")
                    # For no-extension cache files (the 09/10 "asset/?id=N"
                    # layout), guess_mime() can't tell a PNG from a JSON
                    # from a WASM — sniff the leading bytes so the browser
                    # gets the right Content-Type and the JS consumer can
                    # pick a parser. A generic binary MIME type on an
                    # extensionless response can make clients reject valid data.
                    _cdn_ct = _s2l_state._resolve_mime_with_sniff(cdn_lp, cdn_data)
                    _cdn_ct = _s2l_state._ensure_wasm_content_type(_cdn_ct, cdn_lp)
                    # Correção 5 — exotic-extension JS (npm/globe.gl → video/gl)
                    # must stay executable on this fallback path too.
                    _cdn_ct = _s2l_state._js_mime_rescue(_cdn_ct, cdn_lp, cdn_data[:512])
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
                        _val = _s2l_state._disk_validators(cdn_lp, _cdn_ct)
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
                # Correção 5 — JS content rescue on the live fallback fetch.
                ct = _s2l_state._js_mime_rescue(ct, req_path_cdn, cdn_body[:512] if cdn_body else None)
                if cdn_r.status_code < 400:
                    _s2l_state.save_queue.put((cdn_url, cdn_body, ct, _s2l_state._effective_device()))
                    _s2l_state.stats.inc("cdn_fetched")
                    _s2l_state.log(f"CDN fallback {_s2l_state._fmt_host(cdn_host)}{req_path_cdn} {cdn_r.status_code}", "CDN")
                    out = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(cdn_r))
                    _s2l_state._apply_freshness(out, ct)
                    # CORS + CORP are already set correctly by filter_resp() —
                    # do NOT re-set them here (see _cdn_serve for rationale).
                    return Response(cdn_body, status=cdn_r.status_code, headers=out, content_type=ct)
            except Exception as exc:
                _s2l_state.log(f"CDN fetch failed: {cdn_url} — {_s2l_state._short_exc(exc)}", "WARN")
    return None


def _rsc_refetch(ctx, method: str, target: str, body: bytes) -> bytes:
    """Detect RSC wire format and re-fetch with a stronger HTML signal.

    Mutates ctx (resp_body/resp_ct/resp_headers) on success; always returns
    the body to use from now on.
    """
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
    return body


def _bot_page_response(ctx, method: str, target: str, req_path: str) -> Response | None:
    """Bot/CAPTCHA page detection: refuse to serve (and to cache) the block."""
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
    return None
