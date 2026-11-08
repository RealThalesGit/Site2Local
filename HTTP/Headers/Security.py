"""Site2Local: baseline response security headers."""
from __future__ import annotations

from ... import Runtime as _s2l_state


def _apply_security_headers(out, body: bytes = b"",
                            is_top_level_html: bool = False) -> None:
    """Add safe HTTP security headers in addition to COOP/COEP.

    These complement the cross-origin isolation headers (COOP/COEP/CORP) that
    filter_resp() already sets. X-Content-Type-Options is enforced as
    ``nosniff``; Referrer-Policy is added when absent. Both are safe for the
    proxy and harden responses without blocking proxy functionality:

      • X-Content-Type-Options: nosniff — prevents MIME-sniffing attacks where
        a browser interprets a text/plain response as HTML/JS. Universal best
        practice; never breaks a legitimate use case.
      • Referrer-Policy: strict-origin-when-cross-origin — the modern browser
        default. Sends the full URL for same-origin requests, origin-only for
        cross-origin (HTTPS→HTTPS), and no Referer for HTTPS→HTTP. Safe to set
        explicitly so sites that rely on a specific referrer behavior get a
        predictable one even when their own header was stripped.
      • Cross-Origin-Opener-Policy / Cross-Origin-Embedder-Policy — applied
        only when the document/worker needs cross-origin isolation.

    Headers deliberately NOT added here (they would break proxy functionality):
      • Content-Security-Policy — would block our injected service-worker
        clearer and URL-rewriting inline scripts.
      • X-Frame-Options — would prevent the proxy from being iframed by
        dashboards / the S2L viewer.
      • Strict-Transport-Security — pointless on localhost HTTP and would
        pin the browser to a TLS cert we don't serve.

    NOTE: `out` may be a plain dict (from filter_resp / out_headers) or a
    werkzeug Headers object (resp.headers). _ci_header_keys() handles the
    iteration difference (Headers.__iter__ yields tuples in some versions).
    """
    _s2l_state._ci_remove_header(out, "X-Content-Type-Options")
    out["X-Content-Type-Options"] = "nosniff"
    _existing = _s2l_state._ci_header_keys(out)
    if "referrer-policy" not in _existing:
        out["Referrer-Policy"] = "strict-origin-when-cross-origin"
