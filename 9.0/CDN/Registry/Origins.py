"""Site2Local: CDN origin registry."""
from __future__ import annotations

from flask import Flask
from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
from werkzeug.serving import make_server
import logging
import os
import threading

from ... import Runtime as _s2l_state


def _real_origin_for(incoming: str | None) -> str:
    """Translate the Origin/Referer header the BROWSER actually sent for an
    outgoing CDN/API/WS request into the origin a real, unproxied browser
    would have sent to that upstream.

    The browser's own header reflects whatever local address is currently
    serving the calling document — http://localhost:PORT on the S2L host
    machine, http://<LAN-IP>:PORT on phones/other devices, or
    http://<local-alias>:<cdn-port> when the call originates from inside a
    CDN sub-iframe served on its own MULTIPORT port. Blindly forwarding THAT
    local value (or, just as wrong, blindly substituting the call's OWN
    target host — self-referencing) doesn't match what a real browser would
    send, and a surprising number of first-party APIs use Origin/Referer to
    decide what to return: an unrecognized value commonly gets a stripped /
    empty fallback response instead of real data, which is exactly the kind
    of thing that can leave a page's JS waiting forever on data that never
    arrives in the shape it expects.
    """
    if incoming:
        try:
            p = urlparse(incoming)
            host, port = p.hostname or "", p.port
            _req_host = _s2l_state._request_host_header()
            _req_hostname = urlparse(f"http://{_req_host}").hostname if _req_host else None
            # "Local" = the localhost aliases OR whatever address the current
            # client actually used to reach us (LAN IP / hostname for remote
            # devices — only localhost used to be recognized, which made every
            # Origin/Referer check fail on phones and other LAN clients).
            _is_local = (
                host in _s2l_state._LOCAL_HOSTS
                or (_req_host is not None and (p.netloc == _req_host or host == _req_hostname))
            )
            if _is_local:
                if port and port != _s2l_state.PORT:
                    with _s2l_state._cdn_port_lock:
                        for h, pt in _s2l_state._cdn_host_port.items():
                            if pt == port:
                                return f"https://{h}"
                    return f"https://{_s2l_state.MAIN_HOST}"
                return f"https://{_s2l_state.MAIN_HOST}"
            if host in (_s2l_state.MAIN_HOST, "www." + _s2l_state.MAIN_HOST):
                return f"https://{_s2l_state.MAIN_HOST}"
            # Port-only match: the origin came from one of our CDN mini-server
            # ports via a local alias we don't recognize (e.g. a phone hitting
            # http://<LAN-IP>:8087). The ports belong to us, so the mapping is
            # safe — translate to that CDN's real domain.
            if port:
                with _s2l_state._cdn_port_lock:
                    for h, pt in _s2l_state._cdn_host_port.items():
                        if pt == port:
                            return f"https://{h}"
            with _s2l_state._cdn_port_lock:
                if host in _s2l_state._cdn_host_port:
                    return f"https://{host}"
        except Exception:
            pass
    # No usable incoming header, or it didn't match anything we recognize —
    # MAIN_HOST is correct for the overwhelming majority of first-party API
    # calls, which are made directly from the main page's own JS.
    return f"https://{_s2l_state.MAIN_HOST}"


def _proxy_target(host: str, tail: str) -> str | None:
    """Resolve an external host+path into the correct local-proxy URL.

    Shared by the HTML/CSS rewriter and the JSON/webmanifest rewriter so both
    reach a real destination (MAIN_HOST, a registered CDN's dedicated
    MULTIPORT port, or the catch-all /__s2l_ext__/ route) through identical
    routing logic — one place to get this right instead of two copies that
    can quietly drift apart.

    Absolute https://MAIN_HOST/... baked into server-rendered content (e.g. an
    <iframe src="https://some-cdn/..."> deep inside a nested iframe) must
    be rewritten so the browser re-enters our proxy instead of connecting to
    the real internet host directly (→ "connection refused"). A bare relative
    path only works when the CURRENT document is itself served from the main
    proxy port — but this same rewriter also runs on CDN documents served via
    /__s2l_ext__/ or (MULTIPORT) a dedicated CDN port, where a relative path
    resolves against THAT origin instead, landing nowhere near MAIN_HOST. Use
    an absolute URL (base from the request's Host header — correct for the
    machine running S2L AND for phones/other LAN devices) so it's correct
    from any origin.
    """
    # IDEMPOTENCY GUARD — never wrap an already-proxied URL. Rewriting runs on
    # multiple regex passes (HTML attributes, inline <script> bodies, CSS); the
    # attribute pass legitimately matches JS property assignments inside inline
    # scripts (j.src = 'https://...'), so a once-rewritten URL can reach this
    # function again. Without this guard the second pass wrapped the proxy's
    # own address one more level deep: an analytics snippet became
    # /__s2l_ext__/localhost:PORT/__s2l_ext__/cdn.example.com/... (a
    # 404). None → every caller keeps the current text unchanged.
    if tail.startswith(_s2l_state._EXT_PREFIX):
        return None
    if host.split(":", 1)[0].lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
        return None
    _mh = {_s2l_state.MAIN_HOST, "www." + _s2l_state.MAIN_HOST}
    if _s2l_state.MAIN_HOST.startswith("www."):
        _mh.add(_s2l_state.MAIN_HOST[4:])
    # Strip the port from the captured host before matching against MAIN_HOST.
    # The regexes feeding this function capture host:port (e.g. "example.com:443"),
    # which would fail the `host in _mh` check and incorrectly route MAIN_HOST
    # assets through /__s2l_ext__/ — losing session cookies and auth.
    host_no_port = host.split(":", 1)[0]
    if host_no_port in _mh:
        return f"{_s2l_state._proxy_base()}{tail}"
    if any(bad in host for bad in _s2l_state.CDN_BLOCK):
        return None
    with _s2l_state._cdn_port_lock:
        port = _s2l_state._cdn_host_port.get(host, 0)
        if port == 0:
            port = _s2l_state._cdn_host_port.get(host_no_port, 0)
    if port > 0 and _s2l_state.MULTIPORT:
        return f"http://{_s2l_state._request_hostname()}:{port}{tail}"
    # Use the main port: /__s2l_ext__/<host>/<path> is routed by the main
    # Flask app, while each CDN mini-server only proxies its own host.
    # Using the current request's port can send rewritten cross-CDN URLs
    # to a mini-server that cannot route them.
    return f"http://{_s2l_state._request_hostname()}:{_s2l_state.PORT}{_s2l_state._EXT_PREFIX}/{host}{tail}"
