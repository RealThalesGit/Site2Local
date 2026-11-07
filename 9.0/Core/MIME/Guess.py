"""Site2Local: MIME type detection and asset classification."""
from __future__ import annotations

import mimetypes
import os

from ... import Runtime as _s2l_state


def guess_mime(path: str) -> str:
    """Guess a MIME type from a path extension.

    Uses the stdlib mimetypes module (dynamic — reads /etc/mime.types at
    import time). Falls back to application/octet-stream when the extension
    is unknown or absent, which is the safest default for binary-safe
    proxying. No static type list is maintained on our side.
    """
    if not path:
        return "application/octet-stream"
    # Strip query string / fragment if present (can happen when guess_mime
    # is called on a URL path instead of a local file path).
    path = path.split("?", 1)[0].split("#", 1)[0]
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"


def _resolve_mime_with_sniff(lp: str, data: bytes | None = None) -> str:
    """Resolve a MIME type for a cached file, sniffing HTML for no-ext files.

    Per the 09/10 spec: for files WITHOUT an extension (the new
    ``asset/?id=N`` layout), identify HTML by content and serve as
    ``text/html``; everything else is ``application/octet-stream``.
    Files WITH an extension use ``guess_mime(lp)`` as before.

    Only the first 512 bytes are read when ``data`` is None — the HTML
    check (``_is_valid_html``) only looks at the first 512 bytes, so
    reading more would waste memory when many large binary assets are
    processed concurrently.
    """
    has_ext = bool(os.path.splitext(os.path.basename(lp))[1])
    looks_html = lp.endswith(".html") or lp.endswith("/index.html")
    if has_ext or looks_html:
        return _s2l_state.guess_mime(lp)
    # Extensionless file — sniff HTML only (spec: "rest → octet-stream").
    if data is None:
        try:
            with open(lp, "rb") as f:
                data = f.read(512)   # head only — never read the full file here
        except OSError:
            return "application/octet-stream"
    if _s2l_state._is_valid_html(data) and not _s2l_state._is_wire_payload(data):
        return "text/html; charset=utf-8"
    return "application/octet-stream"


# ── JavaScript content rescue (correção 5) ───────────────────────────────────
# Types that already ARE executable JavaScript — never re-sniffed.
_JS_EXECUTABLE = ("text/javascript", "application/javascript",
                  "application/x-javascript", "text/jscript")

# Types the rescue must NEVER touch. The browser consumes them as-is and a
# JS-looking head would be a false positive: CSS banners `/*! … */` are
# byte-level near-identical to JS license banners (gsap ships both kinds),
# HTML starts with `<`, JSON with `{`/`[`/`"`, SVG is text-but-image.
# application/json also keeps _serve_cached's JSON-rewrite branch active.
_NEVER_JS_RESCUE = ("text/html", "text/css", "text/xml", "application/xml",
                    "application/json", "image/svg+xml", "application/wasm")

# High-precision "this is JavaScript" token prefixes. Trailing spaces where a
# bare word could prefix English prose ("various", "constant", "classical").
_JS_SIGNATURES = (
    "'use strict'", '"use strict"', "!function", "!(function", "(function",
    "(()=>", "!(()=>", "(async", "async function", "function ", "function*",
    "class ", "var ", "let ", "const ", "import ", "import(", "export ",
    "export{", "define(", "require(", "module.exports", "Object.defineProperty",
    "window.", "self.", "globalThis", "void ", "typeof ", ";(function",
    ";!function", "(typeof", "new Function", "#!",
)

# Bounded dedupe for rescue log lines (one WARN per path, cap 256).
_js_rescue_logged: set[str] = set()


def _looks_like_js(head: bytes) -> bool:
    """High-precision check that a byte head is JavaScript source.

    Strips up to three leading comment blocks (`//` line / `/* … */` banner,
    the universal minified-library prelude) and then matches a conservative
    token-prefix list. Comment-only heads that never terminate within the
    window return False (can't confirm). CSS/HTML/JSON never match: `.foo{`,
    `@media`, `<!doctype`, `{`, `[` are not in the signature list — which is
    exactly why CSS files (whose `/*!` banners look like JS banners) are safe.
    """
    try:
        s = head.decode("utf-8", "ignore").lstrip("\ufeff ﻿ \t\r\n")
    except Exception:
        return False
    for _ in range(3):
        if not s:
            return False
        if s.startswith("#!"):
            return True          # hashbang — valid at the start of JS modules
        if s.startswith("/*"):
            end = s.find("*/", 2)
            if end == -1:
                return False     # unterminated banner inside the head — give up
            s = s[end + 2:].lstrip(" \t\r\n")
        elif s.startswith("//"):
            nl = s.find("\n")
            if nl == -1:
                return False
            s = s[nl + 1:].lstrip(" \t\r\n")
        else:
            break
    if not s:
        return False
    return any(s.startswith(sig) for sig in _JS_SIGNATURES)


def _js_mime_rescue(ct: str, lp: str, head: bytes | None = None) -> str:
    """Serve executable JavaScript even when the path/extension lies about it.

    Field bug (mirror jeskojets.com, globe section rendered as a plain black
    circle): the page loads ``<script src="https://cdn.jsdelivr.net/npm/globe.gl">``
    — the package NAME looks like a filename with extension ``.gl``, and the
    stdlib maps ``.gl`` → ``video/gl`` (the 1993 Graphics Language animation
    type, still in the IANA registry). The disk cache stores only the body
    (the upstream's ``application/javascript`` is not persisted), so every
    cache-hit serve re-derives the Content-Type from the extension and ships
    ``video/gl``. jsdelivr — like the proxy itself — sends
    ``X-Content-Type-Options: nosniff``, and Chromium REFUSES to execute a
    classic script whose MIME is not a JavaScript type: window.Globe never
    exists, the site's ``if (typeof Globe !== 'function') return;`` guard
    bails, and the styled-but-empty container shows through. First visit
    (live fetch, upstream CT) worked; every later visit (cache hit) broke —
    the classic "worked once, then never again" pattern.

    The rescue: when the derived type is NOT a type the browser can consume
    as this content (and not one of the never-touch text types where a
    JS-looking head would be a false positive), sniff the leading bytes —
    content that is really JavaScript is served as ``text/javascript``.
    Identical bytes, correct label. Real binaries (PNG/MP4/WASM/…) never
    match the token signatures and keep their extension-derived type.

    Companion: ``_disk_validators()`` folds this Content-Type into the ETag,
    so a browser that already cached a poisoned entry (e.g. video/gl) gets a
    fresh 200 instead of being pinned to it by matching 304s.
    """
    ct_base = (ct or "").split(";", 1)[0].strip().lower()
    if not ct_base:
        ct_base = "application/octet-stream"
    if (ct_base in _JS_EXECUTABLE
            or ct_base in _NEVER_JS_RESCUE
            or ct_base.endswith("+json") or ct_base.endswith("+xml")):
        return ct
    if head is None:
        try:
            with open(lp, "rb") as f:
                head = f.read(512)
        except OSError:
            return ct
    if not head or not _looks_like_js(head):
        return ct
    if len(_js_rescue_logged) < 256 and lp not in _js_rescue_logged:
        _js_rescue_logged.add(lp)
        try:
            _s2l_state.log(f"MIME rescue: {os.path.basename(lp)!r} is JavaScript "
                           f"but its path suggested {ct_base!r} → text/javascript", "WARN")
        except Exception:
            pass
    return "text/javascript; charset=utf-8"
