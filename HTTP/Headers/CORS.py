"""Site2Local: CORS / CORP response headers."""
from __future__ import annotations

from flask import request as flask_request

from ... import Runtime as _s2l_state


def _cors_origin_pair() -> tuple[str, str | None]:
    """Return (Access-Control-Allow-Origin value, Access-Control-Allow-Credentials value or None).

    A browser fetch() using credentials:"include" is rejected outright if
    Access-Control-Allow-Origin is the literal wildcard "*" — the spec requires
    the exact origin to be echoed back whenever the request carries credentials
    (cookies, HTTP auth, or client certs). We can't know in advance whether a
    given request used credentials:"include", so the safe universal rule is:
    whenever the browser sent an Origin header, echo it back verbatim paired
    with Access-Control-Allow-Credentials: true. Only fall back to "*" when
    there's no Origin header to echo (non-CORS requests, direct navigation).
    This is the same rule filter_resp() already applies — every OTHER place
    in this file that sets Access-Control-Allow-Origin should call this
    instead of hardcoding "*", or it silently undoes filter_resp()'s work.
    """
    try:
        origin = flask_request.headers.get("Origin", "") if flask_request else ""
    except RuntimeError:
        origin = ""
    if origin:
        return origin, "true"
    return "*", None


def _pna_allow_header() -> dict:
    """Return {"Access-Control-Allow-Private-Network": "true"} when the
    browser's preflight requested Private Network Access, else {}.

    Chrome's Private Network Access (PNA) spec (WICG): a public-context page
    making a fetch() to localhost / a private IP sends a CORS preflight with
    `Access-Control-Request-Private-Network: true`. The preflight response
    MUST include `Access-Control-Allow-Private-Network: true` or the request
    is blocked with an opaque "CORS error" / "policy violation" — the most
    common PNA failure mode for a localhost proxy. Since S2L listens on
    localhost, ANY public-context fetch() triggers PNA; without this header
    the user sees a flood of CORS errors in the devtools console.

    We only echo the header when the request asked for it (spec-compliant).
    Sending it unconditionally is harmless in practice but produces console
    warnings on some browsers, so we gate it on the request header.
    """
    try:
        if flask_request and flask_request.headers.get(
                "Access-Control-Request-Private-Network", "").lower() == "true":
            return {"Access-Control-Allow-Private-Network": "true"}
    except (RuntimeError, AttributeError):
        pass
    return {}


def _apply_corp_header(out) -> None:
    """Allow proxied resources to be embedded by isolated cross-origin pages."""
    _s2l_state._ci_remove_header(out, "Cross-Origin-Resource-Policy")
    out["Cross-Origin-Resource-Policy"] = "cross-origin"


def _apply_cors_headers(out: dict) -> None:
    """Set CORS + CORP headers on a response dict OR werkzeug Headers object,
    correctly handling the credentialed-request case. Use this in cache-hit /
    manually-built response paths INSTEAD of inlining `Access-Control-Allow-
    Origin` / `Expose-Headers` / `Cross-Origin-Resource-Policy` — it
    guarantees:

      • Access-Control-Allow-Origin echoes the request Origin (or "*" when
        there's no Origin, e.g. same-origin / direct navigation).
      • Access-Control-Allow-Credentials: true is paired with the echoed
        origin so credentialed fetches (credentials:"include") aren't rejected.
      • Access-Control-Expose-Headers is the CONCRETE list (not "*") when
        credentialed — the wildcard does NOT expose Authorization /
        Content-Encoding / WWW-Authenticate for credentialed responses per
        the CORS spec, which is the most common "policy violation" seen on
        cross-origin CDN-port fetches that read those headers.
      • Vary: Origin is merged (case-insensitively) so caches don't pin one
        origin's response for another — and no duplicate Vary entries are
        left behind when the existing key had different casing.
      • Cross-Origin-Resource-Policy: cross-origin so the resource is visible
        inside cross-origin iframes / COEP contexts.

    filter_resp() already does all of the above via this helper; this is also
    for paths that build a Response without going through filter_resp() (cache
    hits, 304s, 206s, error fallbacks).

    NOTE: `out` may be a plain dict (from filter_resp / out_headers) or a
    werkzeug Headers object (resp.headers). The _ci_* helpers above handle
    the case-sensitivity differences between the two.
    """
    _aco, _acc = _s2l_state._cors_origin_pair()
    out["Access-Control-Allow-Origin"] = _aco
    if _acc:
        out["Access-Control-Allow-Credentials"] = _acc
        # Case-insensitive Vary merge: dict.get is case-sensitive, so use the
        # _ci_get_header / _ci_remove_header helpers to find and remove any
        # existing Vary (regardless of casing) before setting a single
        # canonical "Vary" entry. Without this, a dict carrying "vary"
        # (lowercase) would end up with BOTH "vary: ..." and "Vary: ..., Origin".
        _old_vary = _s2l_state._ci_get_header(out, "Vary", "")
        _s2l_state._ci_remove_header(out, "Vary")
        out["Vary"] = (_old_vary + ", Origin").lstrip(", ")
        out["Access-Control-Expose-Headers"] = _s2l_state._CORS_CONCRETE_EXPOSE
    else:
        out["Access-Control-Expose-Headers"] = "*"
    _s2l_state._apply_corp_header(out)
