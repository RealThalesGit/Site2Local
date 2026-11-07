"""Site2Local: CDN viewer endpoint — serves the live dashboard page."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request

from ... import Runtime as _s2l_state
from .Page import _CDN_VIEWER_PAGE


def _s2l_viewer_endpoint() -> Response:
    """Self-contained live CDN viewer page (refreshes every 50 ms)."""
    if not _s2l_state.WEB_CDNVIEWER:
        return _s2l_state._viewer_disabled_response()
    if not _s2l_state._viewer_ip_allowed():
        return _s2l_state._viewer_ip_denied_response()
    if flask_request.method == "OPTIONS":
        r = Response(status=204)
        r.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
        return r
    resp = Response(_CDN_VIEWER_PAGE, content_type="text/html; charset=utf-8")
    resp.headers["Cache-Control"] = "no-store"
    resp.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
    return resp
