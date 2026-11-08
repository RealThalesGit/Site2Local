"""Site2Local: upstream HTTP session management."""
from __future__ import annotations

from flask import request as flask_request
import hashlib
import re
import time

from ... import Runtime as _s2l_state


def _get_proxy_session():
    if not hasattr(_s2l_state._proxy_local, "s"):
        _s2l_state._proxy_local.s = _s2l_state._make_session()
    return _s2l_state._proxy_local.s


def _client_id() -> str:
    """Stable per-browser identifier derived from source IP."""
    try:
        ip  = flask_request.remote_addr or "unknown"
        xff = flask_request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        return hashlib.sha1((xff or ip).encode()).hexdigest()[:16]
    except RuntimeError:
        return "default"


def _get_client_session():
    """Return a session bound to the calling browser's IP, not the thread."""
    cid  = _s2l_state._client_id()
    now  = time.time()
    with _s2l_state._CLIENT_LOCK:
        # Evict stale sessions to avoid memory growth. Close them properly —
        # each session holds a connection pool (up to POOL_MAXSIZE=64 conns)
        # and leaking them accumulates file descriptors over time.
        stale = [k for k, t in _s2l_state._CLIENT_LAST.items() if now - t > _s2l_state._CLIENT_TTL]
    for k in stale:
        old_sess = None
        with _s2l_state._CLIENT_LOCK:
            old_sess = _s2l_state._CLIENT_SESSIONS.pop(k, None)
            _s2l_state._CLIENT_LAST.pop(k, None)
        if old_sess is not None:
            try: old_sess.close()
            except Exception: pass
    with _s2l_state._CLIENT_LOCK:
        if cid not in _s2l_state._CLIENT_SESSIONS:
            _s2l_state._CLIENT_SESSIONS[cid] = _s2l_state._make_session()
        _s2l_state._CLIENT_LAST[cid] = now
        return _s2l_state._CLIENT_SESSIONS[cid]


def _short_exc(exc: Exception) -> str:
    msg = str(exc)
    for pat in (r"HTTPSConnectionPool\(host='([^']+)'",
                r"nodename nor servname provided",
                r"SSL: (.+)", r"Caused by (.+)"):
        m = re.search(pat, msg, re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    first = msg.splitlines()[0]
    return first[:120] + ("…" if len(first) > 120 else "")
