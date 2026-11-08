"""Site2Local: upstream WebSocket transports."""
from __future__ import annotations


from ... import Runtime as _s2l_state


def _get_session_for_ws():
    """Get a session for the WS connection.

    Reuses the client session (which has cf_clearance cookies and
    the same TLS fingerprint as the main page). This is critical for
    CF-protected WS endpoints — a fresh session gets 403'd.

    Returns (session, should_close):
      - should_close=False: shared session, do NOT close after use
      - should_close=True:  freshly created, safe to close
    """
    # First try the client session (per-browser, has cookies)
    try:
        sess = _s2l_state._get_client_session()
        if sess is not None and hasattr(sess, "ws_connect"):
            return sess, False
    except Exception:
        pass
    # Fall back to the proxy session
    try:
        sess = _s2l_state._get_proxy_session()
        if sess is not None and hasattr(sess, "ws_connect"):
            return sess, False
    except Exception:
        pass
    # Last resort: create a fresh session
    return _s2l_state._make_session(), True


def _ws_connect_upstream(ws_url: str, extra_headers: dict):
    """
    Establish the WS connection to the real upstream server. Returns an
    object exposing .recv_frame() -> (fin, rsv1, opcode, payload) | None,
    .send_frame(opcode, payload) -> bool, and .close() — either a
    _CffiUpstreamWS (real browser TLS fingerprint, tried first whenever
    curl_cffi is available) or a _RawSocketUpstreamWS (fallback).

    Raises on total failure — caller must NOT have told the browser
    anything yet.
    """
    if _s2l_state._CURL_CFFI_OK:
        try:
            return _s2l_state._CffiUpstreamWS(ws_url, extra_headers)
        except Exception:
            # _CffiUpstreamWS already logs real curl_cffi failures at WARN.
            # The IPv6-literal skip is expected (and silent): it falls through
            # to the raw-socket path below, which handles IPv6 fine.
            pass
    return _s2l_state._RawSocketUpstreamWS(ws_url, extra_headers)
