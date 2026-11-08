"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations

from flask import request as flask_request

from ... import Runtime as _s2l_state


def _raw_path_after(marker: str) -> str | None:
    """Return the RAW (still percent-encoded) request path following `marker`.

    Werkzeug's <path:...> route converter can't help here: by the time a WSGI
    request reaches Flask, the server has already percent-decoded PATH_INFO per
    PEP 3333, so an intentionally-encoded slash (%2F) inside a single opaque path
    segment is irreversibly indistinguishable from a real path separator.

    Object-key storage services are the textbook case: an object key that legitimately
    contains a slash (e.g. "listings/foo.jpg") is transmitted as ONE percent-
    encoded segment ("listings%2Ffoo.jpg") specifically so it is NOT split into
    two path segments. Once PATH_INFO decodes that %2F, we've already lost the
    distinction — re-emitting it as a literal "/" makes the real API see two
    segments instead of one opaque key, and it rejects the request outright.

    REQUEST_URI / RAW_URI are de-facto (non-PEP-3333) extensions that Werkzeug's
    dev server and most production WSGI servers populate with the untouched,
    still-encoded request target straight off the wire — use that instead
    whenever it's available; PATH_INFO remains the only option otherwise.
    """
    raw = (flask_request.environ.get("REQUEST_URI")
           or flask_request.environ.get("RAW_URI"))
    if not raw:
        return None
    raw_path = raw.split("?", 1)[0]
    idx = raw_path.find(marker)
    if idx == -1:
        return None
    return raw_path[idx + len(marker):]


def _request_host_header() -> str | None:
    """Validated Host header (netloc, incl. port) the CURRENT client used.

    Returns None outside a request context (background workers) or when the
    header contains unexpected characters — callers then fall back to the
    historical localhost default.
    """
    try:
        host = flask_request.host or ""
    except (RuntimeError, AttributeError, TypeError):
        return None
    if not host:
        return None
    if not (_s2l_state._NETLOC_RE.match(host) or _s2l_state._NETLOC_IPV6_RE.match(host)):
        return None
    return host


def _request_hostname() -> str:
    """Hostname part (no port, IPv6 kept bracketed) of the client's Host."""
    h = _s2l_state._request_host_header()
    if not h:
        return "localhost"
    if h.startswith("["):
        return h.split("]")[0] + "]"
    return h.split(":", 1)[0]


def _proxy_base() -> str:
    """Absolute proxy base URL as seen by the CURRENT client."""
    h = _s2l_state._request_host_header()
    return f"http://{h}" if h else f"http://localhost:{_s2l_state.PORT}"
