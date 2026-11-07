"""Site2Local: cross-origin isolation headers (COOP/COEP)."""
from __future__ import annotations

from flask import request as flask_request
import re

from ... import Runtime as _s2l_state


def _needs_isolation(body: bytes) -> bool:
    """Heuristic: does this HTML look like it needs Cross-Origin isolation?

    Applications that use SharedArrayBuffer / WASM threads need COOP+COEP on
    the top-level document
    or the browser silently refuses to hand them a working SharedArrayBuffer
    — the app initializes just far enough to paint its background/canvas and
    then hangs forever waiting on a threading primitive that was never
    granted.
    A real, unproxied deployment of such a site sends these headers itself
    (from its own server config); scan for the tell-tale APIs so we can
    reproduce that even when the origin's own header choice didn't survive
    being fetched through us.
    """
    if not body:
        return False
    head = body[:65536].lower()
    has_sab = b"sharedarraybuffer" in head
    has_atomics = b"atomics.wait" in head
    has_worker = b"new worker(" in head and b"postmessage" in head
    # Some clients explicitly fail startup when cross-origin isolation is off.
    has_xorigin_check = (
        b"crossoriginisolated" in head
        or b"cross-origin isolation is required" in head
        or b"cross origin isolation is required" in head
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
    dedicated worker) or `Sec-Fetch-Dest: sharedworker` (SharedWorker).
    Module workers also use `Sec-Fetch-Dest: worker`. This is the most
    reliable "on-demand" signal: when this header is present, we know the
    page is using Workers and the script response needs COEP for the
    Worker to inherit `crossOriginIsolated === true` from the parent.

    Returns False if no Flask request context is active (background threads,
    tests) so callers can safely default to "don't apply".
    """
    try:
        if not flask_request:
            return False
        _dest = (flask_request.headers.get("Sec-Fetch-Dest", "") or "").lower()
        return _dest in ("worker", "sharedworker", "serviceworker")
    except (RuntimeError, AttributeError):
        return False


def _is_document_request() -> bool:
    """Detect a top-level browser navigation that must establish isolation."""
    try:
        if not flask_request:
            return False
        _dest = (flask_request.headers.get("Sec-Fetch-Dest", "") or "").lower()
        if _dest == "document":
            return True
        _mode = (flask_request.headers.get("Sec-Fetch-Mode", "") or "").lower()
        _accept = (flask_request.headers.get("Accept", "") or "").lower()
        return _mode == "navigate" and "text/html" in _accept
    except (RuntimeError, AttributeError):
        return False


def _coep_credentialless_supported() -> bool:
    """True when the browser supports COEP: credentialless (Chromium).

    Firefox and Safari have NOT shipped `credentialless` (MDN, dez/2025) —
    for them COEP only has the `require-corp` flavor, which hard-blocks
    every cross-origin no-CORS subresource whose response lacks a CORP
    header. The proxy rewrites and proxies the vast majority of URLs, but
    a runtime-constructed URL can still slip past every patch (template
    literals like `https://cdn/icons/${name}.svg`, CSS custom properties
    with url(), imgs inserted via innerHTML before the MutationObserver
    microtask fires). On Chromium those escapees still load under
    `credentialless`; on Firefox/Safari they die with
    NS_ERROR_DOM_CORP_FAILED. Callers use this to decide whether the
    blanket document-navigation isolation is safe.

    Detection mirrors _coep_value(): CriOS is excluded because iOS
    Chrome is WebKit underneath — no credentialless there either.
    """
    try:
        ua = (flask_request.headers.get("User-Agent", "") or "")
    except (RuntimeError, AttributeError):
        ua = ""
    return ("CriOS/" not in ua
            and bool(re.search(r"(?:Chrome|Chromium|Edg|OPR)/", ua, re.I)))


def _coep_value() -> str:
    """Use credentialless on Chromium so anonymous third-party assets can load."""
    if _coep_credentialless_supported():
        return "credentialless"
    return "require-corp"


def _apply_isolation_headers(out, body: bytes = b"",
                             is_top_level_html: bool = False,
                             is_worker_script: bool = False) -> None:
    """Inject COOP/COEP on document navigations and Worker responses.

    Two paths trigger isolation:

      1. Top-level HTML document navigations. Runtime isolation checks are
         often in a separately loaded script and absent from cached HTML.

      2. Worker script responses — detected via the `is_worker_script`
         parameter, set by callers that inspect Sec-Fetch-Dest: worker.
         Whenever the browser fetches a Worker script, we know the page
         is using Workers and the script needs COEP for the Worker to
         inherit crossOriginIsolated from the parent. Sites that don't
         use Workers pay zero overhead.

    Chromium uses COEP `credentialless`: it preserves cross-origin isolation
    while allowing anonymous cross-origin no-CORS assets that cannot be made
    to send CORP (for example, third-party SDK scripts). Other browsers retain
    the broadly supported `require-corp` policy.

    Idempotent — responses that need isolation receive the complete,
    compatible pair even if the origin supplied only one header or supplied
    a value that does not enable cross-origin isolation.

    Centralized here so the SAME behavior applies on every response path
    (online filter_resp, cache-hit _cached_response, _make_flask_resp
    safety net, _stream_resp, CDN mini-server, ext_asset route).
    """
    # Document navigations get the pair — but ONLY on Chromium: browsers
    # without COEP credentialless (Firefox/Safari) hard-block every
    # cross-origin no-CORS subresource that escapes the proxy while the
    # document is require-corp (NS_ERROR_DOM_CORP_FAILED in Firefox), and
    # no rewriter can guarantee 100% of runtime-constructed URLs pass
    # through us (template literals, CSS custom properties with url(),
    # imgs inserted via innerHTML before the observer microtask fires).
    # There, isolation is granted only when the page's own body asks for
    # it — the same posture as the real site (most send no COEP at all).
    # Body inspection remains a fallback for HTML responses in clients
    # without Fetch Metadata headers.
    _blanket_ok = _coep_credentialless_supported()
    _html_needs_isolation = bool(
        is_top_level_html
        and ((_s2l_state._is_document_request() and _blanket_ok)
             or (body and _s2l_state._needs_isolation(body))))
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
