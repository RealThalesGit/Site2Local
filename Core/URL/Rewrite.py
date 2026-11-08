"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations

from urllib.parse import urldefrag
from urllib.parse import urlparse
import re

from ... import Runtime as _s2l_state


def rewrite_abs_urls(html: bytes) -> bytes:
    """Rewrite absolute MAIN_HOST URLs inside HTML *attribute values* → proxy-relative.

    Only targets attribute-value contexts (href=, src=, action=, data-href=, etc.)
    so that inline JSON/JS (hydration payloads like __NEXT_DATA__, etc.) is NOT modified.
    Touching bare JSON strings breaks SPA frameworks because their JS uses the
    full URLs for API calls and dynamic manifests.

    Before: href="https://example.com/watch?v=abc"
    After:  href="/watch?v=abc"

    JSON (untouched): "url":"https://example.com/watch?v=abc"
    """
    if not html or not _s2l_state.MAIN_HOST:
        return html
    # Match only when preceded by an HTML attribute value opener (=" or =')
    # This excludes bare JSON strings and JS string literals outside attributes.
    # The trailing negative lookahead requires the host to actually END there
    # (next char is /, ", ', :, ?, # or nothing) — without it, a bare prefix
    # match would also strip a longer domain sharing the same prefix, or any other domain
    # that merely starts with the same characters as MAIN_HOST.
    #
    # The replacement is an ABSOLUTE URL (base derived from the request's Host
    # header — see _proxy_base) rather than a bare relative path: this function
    # runs standalone whenever PROXY_CDN is False (no _rewrite_ext_urls pass
    # first), and also on CDN documents served from a different origin
    # (dedicated MULTIPORT port, /__s2l_ext__/...) — a relative path only
    # resolves correctly back to MAIN_HOST when the current document already
    # happens to be served from the main proxy port.
    host_b = re.escape(_s2l_state.MAIN_HOST.encode())
    proxy_root = _s2l_state._proxy_base().encode()
    pattern = (rb'((?<![A-Za-z0-9_$.])(?:href|src|action|data-src|data-href|poster|srcset|content)\s*=\s*["\'])'
               rb'https?://' + host_b + rb'(?![A-Za-z0-9\-.])')
    html = re.sub(pattern, rb'\1' + proxy_root, html, flags=re.IGNORECASE)

    # <meta http-equiv="refresh" content="0;url=https://MAIN_HOST/path"> — the
    # URL is never the first thing after the quote ("N;url=..."), so the
    # pattern above never matches it. Same risk class as an unrewritten
    # location.href: the browser performs this navigation on its own.
    def _meta_refresh_rep(m: re.Match) -> bytes:
        tag = m.group(0)
        if not re.search(rb'http-equiv\s*=\s*["\']refresh["\']', tag, re.IGNORECASE):
            return tag
        return re.sub(
            rb'(content\s*=\s*["\'][^"\']*?url\s*=\s*)https?://' + host_b + rb'(?![A-Za-z0-9\-.])',
            rb'\1' + proxy_root, tag, flags=re.IGNORECASE)
    html = re.sub(rb'<meta\b[^>]*>', _meta_refresh_rep, html, flags=re.IGNORECASE)

    return html


def strip_fragment(u: str) -> str:
    return urldefrag(u)[0]


def normalize_url(u: str) -> str:
    u = _s2l_state.strip_fragment(u).strip()
    return "https:" + u if u.startswith("//") else u


def build_base_url(raw: str) -> str | None:
    """Probe https:// then http:// to find which scheme `raw` actually serves.

    "Probe" here is the verb form — testing each scheme in turn. The log
    message "Probing {url}..." reflects this.

    In OFFLINE mode, skip the probe entirely — there's no upstream to test.
    Default to https:// (the common case) and return immediately without any
    network call.

    verify=False is mandatory here: curl_cffi's Session defaults to real
    certificate verification, and targets with a self-signed cert, a hostname
    mismatch, or a CA bundle libcurl can't find (common on Termux) would fail
    silently. The session itself is built with verify=False too — this is
    belt-and-suspenders.
    """
    if _s2l_state.OFFLINE:
        # No upstream — don't probe. Default to https:// (the common case),
        # which is what the probe would have picked anyway.
        _offline_url = f"https://{raw}"
        _s2l_state.log(f"OFFLINE: skipping upstream probe for {raw} → {_offline_url}", "INFO")
        return _offline_url

    s = _s2l_state._make_session()
    try:
        for scheme in ("https://", "http://"):
            try:
                r = s.get(scheme + raw, timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ), verify=False)
                if r.status_code < 500:
                    try:
                        platform = _s2l_state.detect_platform(r.url)
                    except Exception as exc:
                        platform = "Detection failed"
                        _s2l_state.log(f"WAFW00F scan failed: {_s2l_state._short_exc(exc)}", "ERROR")
                    _s2l_state.set_last_platform(platform)
                    if platform == "Unavailable":
                        # Optional feature absent, not a failure — same WARN
                        # level every other optional dep uses (brotli,
                        # zstandard, ai-cloudscraper, tqdm). ERROR made this
                        # look like a crash in the console.
                        _s2l_state.log("WAFW00F not installed (optional — pip install wafw00f); "
                            "platform detection disabled.", "WARN")
                    _s2l_state.log(f"Resolved {raw} → {r.url}  [{platform}]  IP: {_s2l_state.resolve_ip(urlparse(r.url).netloc)}", "INFO")
                    return r.url
            except Exception as e:
                _s2l_state.log(f"Probing {scheme+raw}: {_s2l_state._short_exc(e)}", "WARN")
    finally:
        try: s.close()
        except Exception: pass
    return None
