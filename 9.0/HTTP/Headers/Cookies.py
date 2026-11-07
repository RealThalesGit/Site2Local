"""Site2Local: cookie header handling."""
from __future__ import annotations

from ... import Runtime as _s2l_state


def _all_set_cookies(response) -> list:
    """Return every Set-Cookie value on `response`, uncorrupted.

    dict(response.headers) / response.headers.items() silently comma-join
    repeated header names into a single string — which is actively wrong
    for Set-Cookie specifically: a cookie's own Expires=<day>, <date>
    attribute already contains a comma, so a comma-joined multi-cookie
    value is ambiguous to split back apart. A login response that sets 3
    real cookies (session token, CSRF token, a challenge/verification
    token — hCaptcha and friends all set their own) arrives at the browser
    as one mangled value; the browser then drops some of them or mis-splits
    it, which shows up as exactly what it looks like: a verification widget
    stuck in a loop because its own token cookie never made it through,
    "duplicate cookie" console warnings on refresh, and login/session/
    WebSocket-auth state that depends on any of the dropped cookies quietly
    breaking. curl_cffi's Headers exposes .get_list(); urllib3/requests'
    raw header dict exposes .getlist(). Try both; only fall back to the
    single (lossy) value if neither multi-value accessor is available.
    """
    h = getattr(response, "headers", None)
    for meth_name in ("get_list", "getlist"):
        meth = getattr(h, meth_name, None)
        if callable(meth):
            try:
                vals = [v for v in meth("Set-Cookie") if v]
                if vals:
                    return vals
            except Exception:
                pass
    raw_headers = getattr(getattr(response, "raw", None), "headers", None)
    for meth_name in ("getlist", "get_all"):
        meth = getattr(raw_headers, meth_name, None)
        if callable(meth):
            try:
                vals = list(meth("Set-Cookie"))
                if vals:
                    return vals
            except Exception:
                pass
    try:
        single = response.headers.get("Set-Cookie")
    except Exception:
        single = None
    return [single] if single else []


def _resp_headers_dict(response) -> dict:
    """dict(response.headers), with Set-Cookie repaired to a real list
    instead of the comma-mangled single string dict() would silently give.
    Use this everywhere a fetched response's headers feed into filter_resp
    (which already knows how to handle a list of cookies correctly) —
    plain dict(response.headers) must never be used for that purpose."""
    h = dict(response.headers)
    # dict(response.headers) can hand back "set-cookie" in whatever casing
    # that particular header-object implementation uses (curl_cffi's Headers
    # always lower-cases keys) — Python dict keys are case-sensitive even
    # though HTTP header names aren't, so just adding h["Set-Cookie"] = [...]
    # below would create a SECOND key sitting right next to the original
    # mangled one instead of replacing it, and Werkzeug would then send BOTH:
    # the one mangled comma-joined header AND the correctly-split ones,
    # duplicating every single cookie on the wire. Remove any casing of the
    # key first so there is exactly one Set-Cookie entry to work with.
    for k in list(h.keys()):
        if k.lower() == "set-cookie":
            del h[k]
    cookies = _s2l_state._all_set_cookies(response)
    if len(cookies) > 1:
        h["Set-Cookie"] = cookies
    elif len(cookies) == 1:
        h["Set-Cookie"] = cookies[0]
    return h


def _flatten_cookiejar(cookies_obj, prefer_host: str = "") -> dict:
    """Flatten a requests-style cookie jar into a plain name→value dict for
    use as the `cookies=` kwarg on an outgoing request.

    dict(session.cookies) / session.cookies.get(name) look completely safe
    but are landmines here: requests' RequestsCookieJar (used by cloudscraper
    and plain requests.Session) raises CookieConflictError, and curl_cffi's
    Cookies raises CookieConflict, the instant the jar holds two cookies
    with the same name under two different domains. Sessions here are
    per-client and live for the whole run (see _get_client_session), so
    this isn't an edge case — a mirrored page whose CDN/analytics/embeds
    each set their own "session", "_ga", "__cf_bm", etc. hits it on the
    second or third distinct host, and it used to take the whole request
    down with it (or, where it was wrapped in a bare except, silently
    produce a wrong response instead).

    We walk the jar's raw entries ourselves instead of the dict-like
    accessors, so a name collision just picks a value instead of raising:
    prefer whichever cookie's domain actually matches `prefer_host` (the
    upstream host we're about to call), otherwise keep the last one seen.
    """
    if not cookies_obj:
        return {}
    if isinstance(cookies_obj, dict):
        return dict(cookies_obj)
    host = (prefer_host or "").lower()
    chosen_domain: dict[str, str] = {}
    out: dict[str, str] = {}
    try:
        jar = getattr(cookies_obj, "jar", cookies_obj)  # curl_cffi wraps a .jar; requests IS the jar
        for cookie in jar:
            name = getattr(cookie, "name", None)
            if not name:
                continue
            domain = (getattr(cookie, "domain", "") or "").lstrip(".").lower()
            if name in out and host:
                prev = chosen_domain.get(name, "")
                if prev and host.endswith(prev) and not (domain and host.endswith(domain)):
                    continue   # keep the previous, more domain-specific match
            out[name] = getattr(cookie, "value", "") or ""
            chosen_domain[name] = domain
        return out
    except Exception as e:
        _s2l_state.log(f"Cookie jar flatten failed — falling back to get_dict(): {_s2l_state._short_exc(e)}", "DEBUG")
        for attr in ("get_dict", "items"):
            fn = getattr(cookies_obj, attr, None)
            if not fn:
                continue
            try:
                return dict(fn())
            except Exception:
                continue
        return {}
