"""Site2Local: HTML visibility, service-worker and URL rewriting."""
from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import re

from ... import Runtime as _s2l_state


def _inject_sw_clear(html_bytes: bytes) -> bytes:
    if not html_bytes:
        return html_bytes
    with _s2l_state._s2l_js_lock:
        script = _s2l_state._build_s2l_injector()

    # ── Strip any stale __s2l__ injector from disk-cached files ──────────────
    # Without this, a cached page that had the old injector baked in gets a NEW
    # injector prepended; both run; the OLD (possibly with buggy rw()) executes
    # LAST and overwrites window.__s2l_rw / fetch / setAttribute / src-setters.
    # The regex matches id="__s2l__" anywhere in the <script> open tag (not just
    # as the first attribute), so <script type="..." id="__s2l__"> is also caught.
    html_bytes = re.sub(
        rb'<script\b[^>]*\bid=["\']__s2l__["\'][^>]*>.*?</script\s*>',
        b'', html_bytes,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Insert the injector AFTER <meta charset> / <meta http-equiv content-type>
    # if present in the first 2KB of <head>. Per HTML spec, the charset
    # declaration must appear within the first 1024 bytes — inserting a ~5KB
    # script before it pushes the charset past the sniff window, causing
    # browsers to fall back to a default charset (Latin-1/Windows-1252 in some
    # WebViews). This mojibake's non-ASCII chars in inline JS → syntax error
    # → SPA init halts → blank page.
    m_meta = re.search(
        rb'<meta[^>]+(?:charset|http-equiv\s*=\s*["\']?content-type)["\']?[^>]*>',
        html_bytes, re.IGNORECASE)
    if m_meta and m_meta.start() < 2048:
        idx = m_meta.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

    lower = html_bytes.lower()
    m = re.search(rb"<head[^>]*>", html_bytes, re.IGNORECASE)
    if m:
        idx = m.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

    m = re.search(rb"<html[^>]*>", html_bytes, re.IGNORECASE)
    if m:
        idx = m.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

    for marker in (b"</head>", b"</body>"):
        idx = lower.rfind(marker)
        if idx != -1:
            return html_bytes[:idx] + script + html_bytes[idx:]
    return script + html_bytes
