"""Site2Local: disk cache serving, ranges and validators."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import email.utils
import os
import zlib

from ... import Runtime as _s2l_state


def _disk_validators(lp: str, ct: str = "") -> tuple[str, str] | None:
    """(ETag, Last-Modified) derived from the file's CURRENT on-disk state.

    mtime_ns + size — a hand edit (user "mods") or a re-save always moves at
    least one of them, so the validator changes the moment the file changes.
    Deliberately NOT a body hash: revalidation happens for every asset on
    every reload, and stat() is ~free while hashing every WASM/video on each
    request is not.

    `ct` folds a crc32 of the Content-Type BASE into the ETag when the
    caller knows the type it is about to serve. This is the MIME auto-heal:
    when the content-based resolver learns to type a file it used to call
    octet-stream (or a Fix changes its answer), the ETag changes with it, so
    every browser holding the old validators gets a fresh 200 with the new
    Content-Type instead of a 304 that would have pinned the old type for
    the life of the entry.
    """
    try:
        st = os.stat(lp)
    except OSError:
        return None
    etag = f'"s2l:{zlib.crc32(_s2l_state._ct_base(ct or "").encode()) & 0xFFFFFFFF:x}:{st.st_size}:{st.st_mtime_ns}"'
    return etag, email.utils.formatdate(st.st_mtime, usegmt=True)


def _conditional_304(lp: str, ct: str) -> Response | None:
    """Answer If-None-Match / If-Modified-Since from the CURRENT disk state.

    The mirror is editable by design ("mods"): the browser must revalidate
    assets instead of trusting its copy, and the server must judge against
    what is on disk RIGHT NOW — a modified file yields a new ETag (200 with
    the new bytes on the next request), a deleted file yields no 304 at all
    (the request falls through to the refetch/404 path). HTML/JSON are served
    no-store, so browsers never send conditionals for them — skipped here to
    keep the semantics consistent.
    """
    if ct.startswith("text/html") or "json" in ct:
        return None
    val = _s2l_state._disk_validators(lp, ct)
    if val is None:
        return None
    etag, last_mod = val
    try:
        inm = flask_request.headers.get("If-None-Match", "")
    except RuntimeError:
        return None
    hit = False
    if inm.strip():
        if inm.strip() == "*":
            hit = True
        else:
            for tok in inm.split(","):
                tok = tok.strip()
                # W/"..." (weak) prefixes compare equal for our purposes
                if tok == etag or (tok.startswith("W/") and tok[2:] == etag):
                    hit = True
                    break
    else:
        try:
            ims = flask_request.headers.get("If-Modified-Since", "")
        except RuntimeError:
            return None
        if ims.strip():
            try:
                st = os.stat(lp)
                hit = int(st.st_mtime) <= int(
                    email.utils.parsedate_to_datetime(ims).timestamp())
            except Exception:
                hit = False
    if not hit:
        return None
    resp = Response(status=304)
    resp.headers["ETag"] = etag
    resp.headers["Last-Modified"] = last_mod
    # A 304 updates the stored response's freshness — keep revalidating.
    resp.headers["Cache-Control"] = "no-cache"
    _s2l_state._apply_cors_headers(resp.headers)
    _s2l_state._apply_isolation_headers(resp.headers,
                             is_worker_script=_s2l_state._is_worker_request())
    _s2l_state._apply_security_headers(resp.headers)
    return resp


def _apply_freshness(out: dict, ct: str) -> None:
    """Normalize browser-side freshness so the EDITABLE disk mirror stays the
    source of truth.

    Upstream lifetimes (max-age=31536000, immutable, Expires:+1y) used to
    pass through on live responses: the browser then froze its copy for
    months and never re-contacted S2L — hand edits and deletions on disk
    were invisible (“removed the assets folder and the game still loaded”).
    HTML/JSON → no-store (always re-downloaded, SPA state must stay live);
    everything else → no-cache (store, but revalidate before every use —
    validators turn that check into a fast 304).

    Case-safe: upstream header dicts may carry 'Cache-Control' in any casing
    (curl_cffi lower-cases, requests does not) — only one spelling survives.
    """
    _lc = (ct or "").lower()
    _is_doc = "text/html" in _lc or "json" in _lc
    _cc_key = None
    for k in out:
        if k.lower() == "cache-control":
            _cc_key = k
            break
    _cur = out.get(_cc_key, "") if _cc_key else ""
    if _is_doc or not any(t in _cur for t in ("no-cache", "no-store", "max-age=0")):
        if _cc_key is not None:
            del out[_cc_key]
        out["Cache-Control"] = ("no-store, no-cache, must-revalidate, max-age=0"
                                if _is_doc else "no-cache")
    # Expires predates Cache-Control but some browsers still peek at it when
    # revalidating; a future-Expires must not resurrect old bodies.
    for k in [k for k in out if k.lower() == "expires"]:
        del out[k]
