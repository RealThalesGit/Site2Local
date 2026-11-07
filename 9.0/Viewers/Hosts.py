"""Live CDN host→port map polled by the JS injector."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import ipaddress
import json
import time

from .. import Runtime as _s2l_state


def _s2l_hp_endpoint() -> Response:
    """Live CDN host→port map for JS injector polling.
    The injector fetches PX+'/.__s2l_hp' every 2s so CDN hosts registered
    AFTER the initial page load are picked up without a refresh.
    """
    with _s2l_state._cdn_port_lock:
        hp = {h: p for h, p in _s2l_state._cdn_host_port.items() if p > 0}
    resp = Response(json.dumps(hp), content_type="application/json")
    resp.headers["Cache-Control"]               = "no-store"
    _s2l_state._apply_cors_headers(resp.headers)
    _s2l_state._apply_security_headers(resp.headers)
    return resp


