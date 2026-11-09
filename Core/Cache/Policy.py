"""Keep authenticated and explicitly private responses out of shared mirrors."""
from flask import request, has_request_context, g
from ... import Runtime as _s2l_state


def _header(headers, name):
    return ", ".join(str(value) for key, value in (headers or {}).items()
                     if key.lower() == name.lower())


def _shared_cache_request_allowed(headers=None):
    credentials = bool(_header(headers, "Authorization") or _header(headers, "Cookie"))
    if credentials and has_request_context():
        g._s2l_private_request = True
    if credentials:
        return False
    if not has_request_context():
        return True
    if getattr(g, "_s2l_private_request", False):
        return False
    if request.headers.get("Authorization"):
        g._s2l_private_request = True
        return False
    if any(key != "__s2l_browser" for key in request.cookies):
        g._s2l_private_request = True
        return False
    # A Secure upstream cookie can live only in the server-side cookie jar.
    client_id = getattr(_s2l_state, "_client_id", lambda: None)()
    lock = getattr(_s2l_state, "_CLIENT_LOCK", None)
    if lock is not None:
        with lock:
            session = _s2l_state._CLIENT_SESSIONS.get(client_id)
            if session is not None and bool(session.cookies):
                g._s2l_private_request = True
                return False
    return True


def _queue_cache_response(url, body, content_type, device="", *,
                          response_headers=None, request_headers=None, authenticated=False):
    directives = {part.strip().split("=", 1)[0].lower()
                  for part in _header(response_headers, "Cache-Control").split(",")}
    vary = {part.strip().lower() for part in _header(response_headers, "Vary").split(",")}
    if (authenticated or not _shared_cache_request_allowed(request_headers)
            or directives & {"private", "no-store"}
            or vary & {"*", "cookie", "authorization"}
            or _header(response_headers, "Set-Cookie")):
        return False
    _s2l_state.save_queue.put((url, body, content_type, device))
    return True
