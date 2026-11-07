"""POST endpoint that zeroes live counters for a benchmark window."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import ipaddress
import json
import time

from .. import Runtime as _s2l_state


def _s2l_reset_stats_endpoint() -> Response:
    if not _s2l_state.WEB_CDNVIEWER:
        return _s2l_state._viewer_disabled_response()
    if not _s2l_state._viewer_ip_allowed():
        return _s2l_state._viewer_ip_denied_response()
    if flask_request.method == "OPTIONS":
        r = Response(status=204)
        r.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
        return r
    _s2l_state.stats.reset()
    _s2l_state.log("Stats reset via /__s2l_reset_stats__", "INFO")
    resp = Response(json.dumps({"ok": True, "ts": time.strftime("%H:%M:%S")}),
                    content_type="application/json")
    resp.headers["Cache-Control"] = "no-store"
    resp.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
    return resp


