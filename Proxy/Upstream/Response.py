"""Site2Local: upstream request execution and response building."""
from __future__ import annotations

from flask import Response

from ... import Runtime as _s2l_state


def _make_flask_resp(ctx: _s2l_state.HookContext, method: str) -> Response:
    """Build Flask Response from HookContext with body validation."""
    # Keep the representation for HEAD metadata; omit only the wire body.
    body = ctx.resp_body
    if body is None:
        body = b""
        _s2l_state.log(f"Body was None in _make_flask_resp: {method} {ctx.path}", "ERROR")

    # Ensure body is bytes
    if not isinstance(body, bytes):
        try:
            body = str(body).encode('utf-8')
        except Exception:
            body = b"(conversion error)"
        _s2l_state.log(f"Body converted to bytes: {method} {ctx.path}", "WARN")

    # Strip content-type from headers dict — we pass it explicitly via content_type
    # to avoid Flask receiving two conflicting Content-Type values
    clean_headers = {k: v for k, v in ctx.resp_headers.items()
                     if k.lower() != "content-type"}

    resp = Response(b"" if method == "HEAD" else body, status=ctx.resp_status,
                    headers=clean_headers, content_type=ctx.resp_ct)
    if method == "HEAD":
        upstream_length = next((v for k, v in clean_headers.items()
                                if k.lower() == "content-length"), None)
        resp.headers["Content-Length"] = str(upstream_length if upstream_length is not None else len(body))
    # Keep CORS and CORP headers consistent across all response paths.
    _s2l_state._apply_cors_headers(resp.headers)
    _is_html_resp = _s2l_state._is_document_ct(ctx.resp_ct)
    # Apply isolation headers to document and Worker responses.
    _s2l_state._apply_isolation_headers(resp.headers, body, is_top_level_html=_is_html_resp,
                             is_worker_script=_s2l_state._is_worker_request())
    _s2l_state._apply_security_headers(resp.headers)

    is_html_resp = _is_html_resp
    is_hooked    = bool(ctx.resp_headers.get("_hooked"))

    # HOOK RELIABILITY: when GUI is active, all responses get no-store so the
    # browser never caches — hooks fire on every request, not just the first.
    if _s2l_state.HOOK_GUI or is_html_resp or is_hooked:
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"]        = "no-cache"
        resp.headers["Expires"]       = "0"
        resp.headers.pop("ETag",          None)
        resp.headers.pop("Last-Modified", None)

    return resp
