"""Site2Local: response header filtering and finalization."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import re

from ... import Runtime as _s2l_state


def filter_resp(headers: dict, body: bytes = b"", is_top_level_html: bool = False) -> dict:
    # Strip security headers that block our local proxy, plus hop-by-hop headers.
    skip = _s2l_state._HOP_BY_HOP | {
        "content-encoding",
        "content-security-policy", "content-security-policy-report-only",
        "x-frame-options", "strict-transport-security", "x-content-type-options",
        "cross-origin-resource-policy", "permissions-policy",
        "nel", "report-to", "reporting-endpoints",
        # Additional headers that block rendering or reveal proxy
        "x-permitted-cross-domain-policies",  # Adobe Flash/PDF cross-domain
        "x-download-options",                 # IE download behavior
        "x-dns-prefetch-control",            # DNS prefetch control
        "expect-ct",                          # Certificate Transparency
        # Strip upstream CORS — we set our own wildcard below
        "access-control-allow-origin",
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-expose-headers",
        "access-control-max-age",
    }
    out = {k: v for k, v in headers.items() if k.lower() not in skip}
    # CORS + CORP — centralized in _apply_cors_headers() so every response path
    # (filter_resp here, cache-hit helpers, 304/206/error fallbacks) gets the
    # identical concrete Access-Control-Expose-Headers list for credentialed
    # requests. With MULTIPORT, CDN assets are served on different ports — a
    # fetch() from the main page to a CDN port is cross-origin and needs
    # Access-Control-Allow-Credentials if the SPA sends credentials:"include".
    # Wildcard "*" blocks credentialed requests, so _apply_cors_headers only
    # uses it when no Origin header is present.
    _s2l_state._apply_cors_headers(out)
    _s2l_state._apply_isolation_headers(out, body, is_top_level_html,
                             is_worker_script=_s2l_state._is_worker_request())
    _s2l_state._apply_security_headers(out, body, is_top_level_html)
    if "Set-Cookie" in out:
        # Determine if the request came in over HTTPS or from localhost.
        # Secure cookies require HTTPS except on localhost (per browser specs).
        # For non-localhost HTTP access, forcing Secure silently drops all cookies.
        _req_scheme = flask_request.scheme if flask_request else "http"
        _req_host = flask_request.host if flask_request else "localhost"
        _is_localhost = (_req_scheme == "http"
                         and (_req_host.startswith("localhost")
                              or _req_host.startswith("127.0.0.1")
                              or _req_host.startswith("::1")))
        _can_secure = (_req_scheme == "https") or _is_localhost
        def _rewrite_cookie(c: str) -> str:
            # Set SameSite=None + Secure so cookies work cross-origin (CDN
            # sub-iframe on a different port needs this). Without SameSite=None,
            # browsers block cookies on cross-origin requests.
            c = re.sub(r";\s*SameSite=[^;]+", "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Secure\b",        "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Partitioned\b", "", c, flags=re.IGNORECASE)
            c = c.rstrip("; ").rstrip()
            if _can_secure:
                c += "; SameSite=None; Secure"
            else:
                # Non-localhost HTTP: SameSite=Lax works without Secure.
                # SameSite=None would be rejected without Secure.
                c += "; SameSite=Lax"
            # Strip Domain= entirely rather than rewriting it — a Domain that
            # doesn't match the host the browser is actually on gets the WHOLE
            # cookie silently rejected. Dropping it makes the cookie host-only,
            # which is always valid for whatever host is currently in the
            # address bar.
            c = re.sub(r";\s*Domain=[^;]+", "", c, flags=re.IGNORECASE)
            return c
        cookie_val = out["Set-Cookie"]
        if isinstance(cookie_val, list):
            out["Set-Cookie"] = [_rewrite_cookie(c) for c in cookie_val]
        else:
            out["Set-Cookie"] = _rewrite_cookie(cookie_val)
    return out


def _finalize_security_headers(response: Response) -> Response:
    """Apply baseline security and cross-origin resource headers to responses."""
    _s2l_state._apply_security_headers(response.headers)
    # COEP: require-corp blocks cross-origin no-CORS resources unless their
    # response explicitly opts in. The proxy owns every response on both the
    # main and CDN Flask apps, so set CORP here as a final universal safeguard,
    # including errors and routes that don't pass through filter_resp().
    _s2l_state._apply_corp_header(response.headers)
    _is_html = _s2l_state._is_document_ct(response.mimetype)
    _body = b""
    if _is_html and not response.is_streamed:
        _body = response.get_data()
    _s2l_state._apply_isolation_headers(
        response.headers, _body,
        is_top_level_html=_is_html,
        is_worker_script=_s2l_state._is_worker_request(),
    )
    return response
