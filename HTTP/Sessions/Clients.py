"""Site2Local: upstream HTTP session management."""
from __future__ import annotations

from flask import request as flask_request
from flask import g
import hmac
from http.cookies import SimpleCookie
import re
import secrets
import time

from ... import Runtime as _s2l_state

_BROWSER_COOKIE = "__s2l_browser"
_BROWSER_SECRET = secrets.token_bytes(32)


def _signed_browser_token():
    token = secrets.token_hex(32)
    return token + "." + hmac.digest(_BROWSER_SECRET, token.encode(), "sha256").hex()


def _validated_browser_id(value):
    token, separator, signature = (value or "").partition(".")
    if (separator and re.fullmatch(r"[0-9a-f]{64}", token)
            and re.fullmatch(r"[0-9a-f]{64}", signature)):
        expected = hmac.digest(_BROWSER_SECRET, token.encode(), "sha256").hex()
        if hmac.compare_digest(signature, expected):
            return token
    return None


def _browser_cookies():
    return {key: value for key, value in flask_request.cookies.items() if key != _BROWSER_COOKIE}


def _strip_browser_cookie(headers):
    result = dict(headers)
    for key, value in list(result.items()):
        if key.lower() == "cookie":
            jar = SimpleCookie()
            jar.load(value)
            jar.pop(_BROWSER_COOKIE, None)
            del result[key]
            if jar:
                result[key] = "; ".join(item.OutputString() for item in jar.values())
    return result


def _set_browser_cookie(response):
    # Upstream responses must not replace the proxy's browser identity.
    cookies = response.headers.getlist("Set-Cookie")
    if cookies:
        response.headers.remove("Set-Cookie")
        for cookie in cookies:
            if cookie.split("=", 1)[0].strip() != _BROWSER_COOKIE:
                response.headers.add("Set-Cookie", cookie)
    value = getattr(g, "_s2l_new_browser_token", None)
    if value:
        response.set_cookie(_BROWSER_COOKIE, value, httponly=True,
                            secure=flask_request.is_secure, samesite="Lax", path="/")
    return response


def _ensure_browser_session():
    _client_id()


def _get_proxy_session():
    if not hasattr(_s2l_state._proxy_local, "s"):
        _s2l_state._proxy_local.s = _s2l_state._make_session()
    return _s2l_state._proxy_local.s


def _client_id(browser_token=None) -> str:
    """Opaque signed browser identity; never derived from an IP or header."""
    if browser_token is not None:
        return _validated_browser_id(browser_token) or secrets.token_hex(32)
    try:
        value = _validated_browser_id(flask_request.cookies.get(_BROWSER_COOKIE))
        if value:
            return value
        if not getattr(g, "_s2l_new_browser_token", None):
            g._s2l_new_browser_token = _signed_browser_token()
        return _validated_browser_id(g._s2l_new_browser_token)
    except RuntimeError:
        return secrets.token_hex(32)


def _get_client_session(browser_token=None):
    """Return an upstream cookie jar isolated to one signed browser identity."""
    cid  = _s2l_state._client_id(browser_token) if browser_token is not None else _s2l_state._client_id()
    now  = time.time()
    with _s2l_state._CLIENT_LOCK:
        # Evict stale sessions to avoid memory growth. Close them properly —
        # each session holds a connection pool (up to POOL_MAXSIZE=64 conns)
        # and leaking them accumulates file descriptors over time.
        stale = [k for k, t in _s2l_state._CLIENT_LAST.items() if now - t > _s2l_state._CLIENT_TTL]
        if cid not in _s2l_state._CLIENT_SESSIONS:
            overflow = max(0, len(_s2l_state._CLIENT_SESSIONS) - len(stale) - 511)
            candidates = sorted((t, k) for k, t in _s2l_state._CLIENT_LAST.items() if k not in stale)
            stale.extend(k for _, k in candidates[:overflow])
        retired = [_s2l_state._CLIENT_SESSIONS.pop(k, None) for k in stale]
        for key in stale:
            _s2l_state._CLIENT_LAST.pop(key, None)
        if cid not in _s2l_state._CLIENT_SESSIONS:
            _s2l_state._CLIENT_SESSIONS[cid] = _s2l_state._make_session()
        _s2l_state._CLIENT_LAST[cid] = now
        session = _s2l_state._CLIENT_SESSIONS[cid]
        authenticated = bool(session.cookies)
    # Capture authentication before another request can evict this session
    # or an upstream response can clear its cookies. Never reset within a request.
    if authenticated:
        try:
            g._s2l_private_request = True
        except RuntimeError:
            pass
    for old_session in retired:
        if old_session is not None:
            try: old_session.close()
            except Exception: pass
    return session


def _short_exc(exc: Exception) -> str:
    msg = str(exc)
    if not msg.strip():
        return type(exc).__name__
    for pat in (r"HTTPSConnectionPool\(host='([^']+)'",
                r"nodename nor servname provided",
                r"SSL: (.+)", r"Caused by (.+)"):
        m = re.search(pat, msg, re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    first = msg.splitlines()[0]
    return first[:120] + ("…" if len(first) > 120 else "")
