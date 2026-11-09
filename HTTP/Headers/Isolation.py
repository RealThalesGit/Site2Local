"""Site2Local: cross-origin isolation headers (COOP/COEP)."""
from __future__ import annotations

from flask import request as flask_request
import re
import threading

from ... import Runtime as _s2l_state


#: Content-Types that identify a DOCUMENT (not a fragment/subresource).
#: Callers use this instead of hand-rolling `"text/html" in ct` so XHTML
#: documents get the same isolation treatment as HTML ones.
_DOC_CT_MARKERS = ("text/html", "application/xhtml+xml")
_auto_isolation_lock = threading.Lock()
_auto_isolation_required = False


def _is_document_ct(ct: str) -> bool:
    """True when this Content-Type marks a navigable HTML/XHTML document."""
    lc = (ct or "").lower()
    return any(m in lc for m in _DOC_CT_MARKERS)


def _needs_isolation(body: bytes) -> bool:
    """Heuristic: does this HTML look like it needs Cross-Origin isolation?

    Applications that use SharedArrayBuffer / WASM threads need COOP+COEP on
    the top-level document
    or the browser silently refuses to hand them a working SharedArrayBuffer
    — the app initializes just far enough to paint its background/canvas and
    then hangs forever waiting on a threading primitive that was never
    granted (or throws "The WebAssembly.Memory object cannot be serialized").
    A real, unproxied deployment of such a site sends these headers itself
    (from its own server config); scan for the tell-tale APIs so we can
    reproduce that even when the origin's own header choice didn't survive
    being fetched through us, and even when the threading logic itself lives
    in an external script (the HTML only carries a hint sentence — e.g.
    aya.nodium.lol's "This page needs cross-origin isolation for
    multithreaded WebAssembly").
    """
    if not body:
        return False
    head = body[:65536].lower()
    has_sab = b"sharedarraybuffer" in head
    has_atomics = b"atomics.wait" in head
    has_worker = b"new worker(" in head and b"postmessage" in head
    # Some clients explicitly fail startup when cross-origin isolation is off.
    # Covers both runtime checks (crossOriginIsolated) and the human-facing
    # hint sentences apps embed ("needs cross-origin isolation…",
    # "…isolation is required…").
    has_xorigin_check = (
        b"crossoriginisolated" in head
        or b"cross-origin isolation is required" in head
        or b"cross origin isolation is required" in head
        or b"needs cross-origin isolation" in head
        or b"multithreaded webassembly" in head
        or (b"coop" in head and b"coep" in head
            and b"cross-origin isolation" in head)
    )
    # A .wasm reference alone does not imply that isolation is required.
    has_wasm = b".wasm" in head
    return (has_sab or has_atomics or has_xorigin_check
            or (has_worker and (has_wasm or has_sab)))


def _is_worker_request() -> bool:
    """Detect if the current request is fetching a Worker script.

    Browsers tag Worker script fetches with `Sec-Fetch-Dest: worker` (classic
    dedicated worker), `sharedworker` (SharedWorker) or `serviceworker`.
    Module workers also use `Sec-Fetch-Dest: worker`. Worklet module loads
    (`audio-worklet`, `paint-worklet`, `layout-worklet`) follow the same
    contract — an AudioWorklet that receives a WebAssembly.Memory via
    postMessage needs the SAME COEP treatment, so they are included here.

    This is the most reliable "on-demand" signal: when this header is
    present, we know the page is using Workers/Worklets and the script
    response needs COEP for the context to inherit `crossOriginIsolated
    === true` from its parent.

    Returns False if no Flask request context is active (background threads,
    tests) so callers can safely default to "don't apply".
    """
    try:
        if not flask_request:
            return False
        _dest = (flask_request.headers.get("Sec-Fetch-Dest", "") or "").lower()
        return _dest in ("worker", "sharedworker", "serviceworker",
                         "audio-worklet", "audioworklet",
                         "paint-worklet", "layout-worklet")
    except (RuntimeError, AttributeError):
        return False


def _is_document_request() -> bool:
    """Detect a browser navigation that must establish (or join) isolation.

    `document` covers top-level navigations. `iframe`/`frame`/`fencedframe`
    cover embedded document loads: a Worker or an iframe created by an
    isolated page needs COEP on ITS response to inherit crossOriginIsolated,
    and an app running inside an iframe (common for embedded players) breaks
    with the exact same WebAssembly.Memory DOMException when the iframe
    response is missing the pair — so iframes are treated as documents here.
    COOP on an iframe response is simply ignored by browsers, sending it is
    harmless.
    """
    try:
        if not flask_request:
            return False
        _dest = (flask_request.headers.get("Sec-Fetch-Dest", "") or "").lower()
        if _dest in ("document", "iframe", "frame", "fencedframe"):
            return True
        _mode = (flask_request.headers.get("Sec-Fetch-Mode", "") or "").lower()
        _accept = (flask_request.headers.get("Accept", "") or "").lower()
        return _mode == "navigate" and "text/html" in _accept
    except (RuntimeError, AttributeError):
        return False


# Firefox shipped COEP `credentialless` in 119 (Nov 2023). Below that (and on
# Safari, which never shipped it) we keep `require-corp` — the widely
# supported value. credentialless preserves cross-origin isolation while
# allowing anonymous no-CORS third-party assets that cannot be made to send
# CORP, which cures the NS_ERROR_DOM_CORP_FAILED console flood on sites that
# load some resources directly from their real CDNs.
_FIREFOX_VER_RE = re.compile(r"Firefox/(\d+)")
_CREDENTIALLESS_MIN_FIREFOX = 119


def _coep_value() -> str:
    """Use credentialless on Chromium and modern Firefox; require-corp else."""
    try:
        ua = (flask_request.headers.get("User-Agent", "") or "")
    except (RuntimeError, AttributeError):
        ua = ""
    if "CriOS/" not in ua and re.search(r"(?:Chrome|Chromium|Edg|OPR)/", ua, re.I):
        return "credentialless"
    m = _FIREFOX_VER_RE.search(ua)
    if m:
        try:
            if int(m.group(1)) >= _CREDENTIALLESS_MIN_FIREFOX:
                return "credentialless"
        except ValueError:
            pass
    return "require-corp"


def _apply_isolation_headers(out, body: bytes = b"",
                             is_top_level_html: bool = False,
                             is_worker_script: bool = False) -> None:
    """Apply automatic or explicit COOP/COEP policy consistently.

    Two paths trigger isolation:

      1. Top-level HTML document navigations (including iframe document
         loads — see _is_document_request). Runtime isolation checks are
         often in a separately loaded script and absent from cached HTML.

      2. Worker/Worklet script responses — detected via the
         `is_worker_script` parameter, set by callers that inspect
         Sec-Fetch-Dest. Whenever the browser fetches a Worker script, we
         know the page is using Workers and the script needs COEP for the
         Worker to inherit crossOriginIsolated from its parent. Sites that
         don't use Workers pay zero overhead.

    Chromium and Firefox ≥119 use COEP `credentialless`: it preserves
    cross-origin isolation while allowing anonymous cross-origin no-CORS
    assets that cannot be made to send CORP (for example, third-party SDK
    scripts). Other browsers retain the broadly supported `require-corp`
    policy.

    Idempotent — responses that need isolation receive the complete,
    compatible pair even if the origin supplied only one header or supplied
    a value that does not enable cross-origin isolation.

    Centralized here so the SAME behavior applies on every response path
    (online filter_resp, cache-hit _cached_response, _make_flask_resp
    safety net, _stream_resp, CDN mini-server, ext_asset route).
    """
    # Isolation changes which third-party frames a page may embed. Enforcing
    # it on every mirrored site breaks ordinary game portals, even when all
    # proxied assets have CORP. Default to compatibility; threaded WASM sites
    # can explicitly opt in for the entire proxy (main page, CDN and workers).
    global _auto_isolation_required
    mode = getattr(_s2l_state, "CROSS_ORIGIN_ISOLATION", "auto")
    if mode is not True and mode is not False and mode != "auto":
        raise ValueError('CROSS_ORIGIN_ISOLATION must be True, False, or "auto"')
    enabled = mode is True
    if mode == "auto":
        is_document = bool(is_top_level_html or _s2l_state._is_document_request())
        upstream_coep = _s2l_state._ci_get_header(out, "Cross-Origin-Embedder-Policy").split(";", 1)[0].strip().lower()
        evidence = is_document and (
            upstream_coep in ("require-corp", "credentialless")
            or bool(body and _needs_isolation(body)))
        try:
            destination = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
        except RuntimeError:
            destination = ""
        with _auto_isolation_lock:
            # Only a top-level document can establish isolation for its
            # descendants. A late iframe/worker cannot change its parent.
            if evidence and destination not in ("iframe", "frame", "fencedframe"):
                _auto_isolation_required = True
            enabled = _auto_isolation_required or evidence
    if not enabled:
        _s2l_state._ci_remove_header(out, "Cross-Origin-Opener-Policy")
        _s2l_state._ci_remove_header(out, "Cross-Origin-Embedder-Policy")
        return
    # Navigation metadata identifies the destination even when a redirect,
    # error, streamed response or revalidation lacks an HTML Content-Type.
    # Conversely HTML must keep the same policy when Fetch Metadata is absent.
    # Otherwise an isolated parent can reject a nested document on reload.
    _html_needs_isolation = bool(
        is_top_level_html or _s2l_state._is_document_request())
    # Worker responses need COEP so the Worker can inherit
    # crossOriginIsolated from its parent.
    _should_apply = _html_needs_isolation or is_worker_script
    if not _should_apply:
        return
    if _html_needs_isolation:
        _s2l_state._ci_remove_header(out, "Cross-Origin-Opener-Policy")
        out["Cross-Origin-Opener-Policy"] = "same-origin"
    if _should_apply:
        _s2l_state._ci_remove_header(out, "Cross-Origin-Embedder-Policy")
        out["Cross-Origin-Embedder-Policy"] = _s2l_state._coep_value()
