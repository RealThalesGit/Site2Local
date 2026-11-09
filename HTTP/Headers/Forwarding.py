"""Site2Local: outgoing request header filtering and rewriting."""
from __future__ import annotations

from flask import request as flask_request
from urllib.parse import urlparse

from ... import Runtime as _s2l_state


def filter_fwd(headers: dict) -> dict:
    skip = _s2l_state._HOP_BY_HOP | _s2l_state._STRIP_FWD_EXTRA
    # RFC 7230 §6.1: the client's own Connection field names extra
    # hop-by-hop headers for this message — honor them dynamically
    # instead of hoping every name is in the static list.
    skip |= _s2l_state._connection_tokens(
        _s2l_state._ci_get_header(headers, "Connection", ""))
    return _s2l_state._strip_browser_cookie(
        {k: v for k, v in headers.items() if k.lower() not in skip})


def inject_csrf_headers(fwd: dict) -> None:
    """Forward CSRF tokens from browser → upstream so POST/PUT requests don't 403."""
    for key in flask_request.headers.keys():
        kl = key.lower()
        if kl in ("x-csrf-token", "x-csrftoken", "x-xsrf-token",
                  "x-requested-with", "x-request-id"):
            fwd[key] = flask_request.headers[key]


def rewrite_origin(fwd: dict, origin_base: str) -> None:
    """Rewrite Origin/Referer from proxy localhost address → real target origin.

    Without this, CORS preflight checks see Origin: http://localhost:8080 and
    reject it.  Referer is rewritten similarly so hotlink-protection passes.

    Additionally strips any header value that still contains localhost /
    127.0.0.1 / 0.0.0.0 / the proxy port — some sites read these to detect
    proxy/MITM usage and trigger CAPTCHAs ("verify you are human"). We make
    a final sweep AFTER the standard rewrite to catch anything that slipped
    through (e.g. a Referer built from window.location that the injector missed).
    """
    proxy_markers = (
        f"localhost:{_s2l_state.PORT}", f"127.0.0.1:{_s2l_state.PORT}", f"0.0.0.0:{_s2l_state.PORT}",
        "localhost:8080", "127.0.0.1:8080",  # common defaults
        "://localhost", "://127.0.0.1", "://0.0.0.0",
    )
    for key in list(fwd.keys()):
        kl = key.lower()
        if kl == "origin":
            fwd[key] = origin_base
        elif kl == "referer":
            try:
                p = urlparse(fwd[key])
                fwd[key] = origin_base + p.path + (f"?{p.query}" if p.query else "")
            except Exception:
                fwd[key] = origin_base + "/"
        elif kl in ("x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
                     "x-real-ip", "via", "forwarded"):
            # Strip proxy-revealing headers entirely — their presence is
            # itself a fingerprint that some bot-detection systems key on.
            del fwd[key]
        # Final sweep — replace any remaining localhost reference in any
        # header value. This catches edge cases like a custom Referer built
        # from JS that read window.location before the injector patched it.
        val = fwd.get(key)
        if isinstance(val, str):
            for marker in proxy_markers:
                if marker in val:
                    fwd[key] = val.replace(marker, origin_base)
                    val = fwd[key]
                    break
