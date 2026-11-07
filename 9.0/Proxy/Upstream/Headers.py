"""Site2Local: upstream request execution and response building."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from dataclasses import replace as _dc_replace
from flask import Response
from flask import redirect
from flask import request as flask_request
from urllib.parse import urlparse
import email.utils
import os
import re
import requests
import requests.adapters

from ... import Runtime as _s2l_state


def _main_security_headers(response: Response) -> Response:
    return _s2l_state._finalize_security_headers(response)


def _normalize_request_origin():
    # When HTTPS-Only mode is active, or when a site JS reads
    # window.location.protocol and rebuilds asset URLs as "https://localhost:PORT/..."
    # the browser connects to our HTTP-only Flask server over TLS, which makes
    # it immediately close with ERR_SSL_PROTOCOL_ERROR.  Detect this via the
    # X-Forwarded-Proto or the Flask request scheme — if the *client* believes
    # it's on HTTPS but we're running plain HTTP, issue a 307 to the http://
    # equivalent so the browser retries correctly without TLS.
    #
    # Flask/Werkzeug behind a plain TCP socket always reports scheme="http",
    # so the reliable signal is the X-Forwarded-Proto header — if the Upgrade
    # header is present (websocket), skip both checks below entirely; a
    # redirect response doesn't make sense mid-handshake.
    if flask_request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    fwd_proto = flask_request.headers.get("X-Forwarded-Proto", "").lower()
    if fwd_proto == "https":
        # Strip the https in favour of http; preserve full path + query
        new_url = flask_request.url.replace("https://", "http://", 1)
        return redirect(new_url, code=307)

    # Browsers treat 127.0.0.1 and localhost as different origins. Redirecting
    # between them cross-origin drops Authorization headers and host-only cookies,
    # which breaks SPA auth on the first API call. Only redirect top-level
    # document navigations (not API/XHR/fetch requests), and use 302 (not 307)
    # so browsers don't preserve method/body on a cross-origin hop.
    host = flask_request.host
    if host == "127.0.0.1" or host.startswith("127.0.0.1:"):
        _dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
        if _dest in ("document", "") and flask_request.method == "GET":
            new_host = "localhost" + host[len("127.0.0.1"):]
            new_url = flask_request.url.replace(host, new_host, 1)
            return redirect(new_url, code=302)
    return None


def _cors_allow_headers() -> str:
    """Return a concrete Access-Control-Allow-Headers value for a preflight.

    Echoes the browser's Access-Control-Request-Headers (merged with a safe
    baseline) when present, so credentialed preflights get a real list instead
    of the spec-forbidden "*". Falls back to "*" only for anonymous requests
    where the wildcard is legal and shortest.
    """
    req_hdrs = ""
    try:
        req_hdrs = (flask_request.headers.get("Access-Control-Request-Headers", "")
                    or "").strip()
    except RuntimeError:
        pass  # outside a request context
    if not req_hdrs:
        return "*"
    # Merge (case-insensitive dedup) the requested tokens with the baseline.
    wanted = {t.strip().lower() for t in req_hdrs.split(",") if t.strip()}
    base   = {t.strip().lower() for t in _s2l_state._CORS_BASE_ALLOW_HEADERS.split(",") if t.strip()}
    merged = [t for t in (req_hdrs.split(",") + _s2l_state._CORS_BASE_ALLOW_HEADERS.split(","))
              if t.strip() and t.strip().lower() in (wanted | base)]
    # Dedupe preserving first-seen order + original casing.
    seen, out = set(), []
    for t in merged:
        tl = t.strip().lower()
        if tl and tl not in seen:
            seen.add(tl)
            out.append(t.strip())
    return ", ".join(out) or "*"
