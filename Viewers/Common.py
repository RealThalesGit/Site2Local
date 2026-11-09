"""Shared viewer access control and CORS helpers."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import ipaddress

from .. import Runtime as _s2l_state


def _viewer_cors_headers(origin: str) -> dict:
    """CORS headers shared by all /__s2l_*__ viewer/dashboard routes."""
    h = {
        "Access-Control-Allow-Methods": ", ".join(_s2l_state._ALL_METHODS),
        "Access-Control-Allow-Headers": _s2l_state._cors_allow_headers(),
        "Access-Control-Max-Age": "86400",
        "Vary": "Origin",
    }
    if origin:
        h["Access-Control-Allow-Origin"] = origin
        h["Access-Control-Allow-Credentials"] = "true"
        # Concrete expose list (not "*") so credentialed dashboard embeds can
        # read Authorization / Content-Encoding / etc. from the viewer's JSON
        # responses — the wildcard is spec-forbidden for credentialed responses
        # and silently strips those headers, breaking cross-origin dashboards.
        h["Access-Control-Expose-Headers"] = _s2l_state._CORS_CONCRETE_EXPOSE
    else:
        h["Access-Control-Allow-Origin"] = "*"
        h["Access-Control-Expose-Headers"] = "*"
    # Private Network Access: allow public-context fetches to localhost so an
    # embedding dashboard served from a real https origin can still poll the
    # viewer's data endpoint without a CORS policy violation.
    h.update(_s2l_state._pna_allow_header())
    return h


def _viewer_disabled_response() -> Response:
    """404 returned by any /__s2l_*__ viewer route when its master flag
    (WEB_CDNVIEWER) is off. Served for BOTH GET and
    OPTIONS so a disabled viewer has no discoverable CORS surface either.
    """
    r = Response("not found", status=404, content_type="text/plain; charset=utf-8")
    r.headers["Cache-Control"] = "no-store"
    return r


def _viewer_ip_denied_response() -> Response:
    """403 for clients outside the optional viewer IP allowlist."""
    r = Response("forbidden", status=403, content_type="text/plain; charset=utf-8")
    r.headers["Cache-Control"] = "no-store"
    return r


def _viewer_ip_allowed() -> bool:
    """Check the direct peer IP against the optional exact-match allowlist.

    Forwarded headers are intentionally ignored: unless a trusted proxy is
    explicitly configured, clients can spoof them.
    """
    # Check before producing data or mutating counters, not just in CORS
    # response headers: cross-origin simple POSTs do not need preflight.
    origin = flask_request.headers.get("Origin", "")
    if origin and (origin == "null" or (
            origin != flask_request.host_url.rstrip("/")
            and origin not in _s2l_state.WEB_CDNVIEWER_ALLOWED_ORIGINS)):
        return False
    if not _s2l_state.WEB_CDNVIEWER_IP_WHITELIST:
        return True
    try:
        client_ip = ipaddress.ip_address(flask_request.remote_addr or "")
    except ValueError:
        return False
    for allowed in _s2l_state.WEB_CDNVIEWER_IP_WHITELIST:
        try:
            if client_ip == ipaddress.ip_address(str(allowed).strip()):
                return True
        except ValueError:
            _s2l_state.log(f"Ignoring invalid WEB_CDNVIEWER_IP_WHITELIST entry: {allowed!r}", "WARN")
    return False


