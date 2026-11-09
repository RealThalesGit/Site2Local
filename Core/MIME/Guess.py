"""Site2Local: MIME type detection and asset classification."""
from __future__ import annotations

import mimetypes
import os
import re
from functools import lru_cache

from ... import Runtime as _s2l_state
from ..Cache.Memory import read_cached_file


def guess_mime(path: str) -> str:
    """Guess a MIME type from a path extension.

    Uses the stdlib mimetypes module (dynamic — reads /etc/mime.types at
    import time). Falls-back to application/octet-stream when the extension
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


# ── Content-based MIME rescue (Fix 6, restored + unified) ────────────────────
#
# The cache re-derives the Content-Type on every request (the CT that came
# from upstream is not persisted), so a file whose NAME lies about its type
# would be mis-served forever. This resolver sniffs the leading bytes and
# only ever RESCUES — it never downgrades a type that the extension/caller
# already established with confidence.
#
# Design rules (learned the hard way — see the [{"id":… regression):
#   * false positives are THE enemy: a JSON array must never be sniffed as
#     CSS, so the CSS discriminator only fires on strong evidence
#     (magic > parse > at-rule > selector, in that order of strength);
#   * only the types nosniff consumers actually reject are rescue targets:
#     script / style / JSON / wasm / images / fonts — text/plain and friends
#     stay out of scope;
#   * one sniff window for every caller (2048 bytes) so the 200, the 304
#     probe and the 206 of the SAME file can never disagree.

#: Single sniff window used by every caller (magic + heuristics fit well
#: under 2 KB; a license banner longer than the old 512-byte window used to
#: defeat the JS rescue).
_SNIFF_HEAD = 2048

#: Magic-byte signatures checked at offset 0. Highest evidence strength.
_MAGIC_PREFIXES: tuple[tuple[bytes, str], ...] = (
    (b"\x00asm",              "application/wasm"),
    (b"\x89PNG\r\n\x1a\n",    "image/png"),
    (b"\xff\xd8\xff",         "image/jpeg"),
    (b"GIF87a",               "image/gif"),
    (b"GIF89a",               "image/gif"),
    (b"RIFF",                 ""),   # resolved via sub-magic (WEBP)
    (b"wOF2",                 "font/woff2"),
    (b"wOFF",                 "font/woff"),
    (b"OTTO",                 "font/otf"),
    (b"\x00\x01\x00\x00",     "font/ttf"),
    (b"BM",                   ""),   # too weak alone — skipped below
)

#: RIFF sub-magic (bytes 8..12) — only WEBP matters in practice.
_RIFF_SUBMAGIC = {b"WEBP": "image/webp"}

#: mp4/iso-bmff: bytes 4..8 == b"ftyp".
_FTYP_OFF = (4, 8)

#: Strong JavaScript evidence. Counted with word-ish boundaries; a banner
#: comment, a license header or a single stray token never reaches the
#: threshold. These are the tokens every bundler/minifier emits.
_JS_EXECUTABLE = (
    b"function", b"return", b"var ", b"let ", b"const ", b"=>",
    b"import", b"export", b"typeof", b"new ", b"this.", b"class ",
    b"async ", b"await ", b"try", b"catch", b"throw", b"window",
    b"document", b"self.", b"Math.", b"Object", b"Array",
)
_JS_MIN_HITS = 4

#: Extensions whose guess_mime() answer is trusted only when the content
#: does not contradict it. Anything else keeps the extension's answer.
_RESCUE_TARGET_EXTS = frozenset({
    ".js", ".mjs", ".cjs", ".jsx",          # Fix 5 family: lying extension
    ".css",
    ".json", ".map", ".webmanifest",
    "",                                      # extensionless (09/10 layout)
})

#: Content-Type bases whose extension-derived answer is ALWAYS trusted
#: (text formats, executables and binary formats with unambiguous
#: extensions). An extension that maps to anything OUTSIDE this set (e.g.
#: ``.gl`` → ``video/gl``, an obscure IANA type that shadowed the
#: globe.gl JS library and blacked out an entire 3D site) is treated as
#: "obscure" and the CONTENT gets a chance to rescue the real type.
_TRUSTED_CT_BASES = frozenset({
    "application/javascript", "application/ecmascript",
    "application/json", "application/manifest+json",
    "application/xhtml+xml", "application/wasm",
    "application/pdf", "application/zip", "application/gzip",
    "application/x-tar", "application/x-bzip2", "application/rtf",
    "application/font-woff", "application/vnd.ms-fontobject",
    "image/png", "image/jpeg", "image/gif", "image/webp",
    "image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon",
    "image/avif", "image/bmp", "image/tiff",
    "audio/mpeg", "audio/mp4", "audio/ogg", "audio/wav", "audio/webm",
    "audio/aac", "audio/flac", "audio/opus",
    "video/mp4", "video/webm", "video/ogg", "video/quicktime",
    "video/x-matroska", "video/x-msvideo", "video/mpeg",
    "font/woff", "font/woff2", "font/ttf", "font/otf",
})

#: CSS @-rules — near-zero false-positive rate (JSON never starts a line
#: with these, JS never has `foo:` followed by `;` in this shape).
_CSS_AT_RULES = (
    b"@media", b"@import", b"@font-face", b"@keyframes", b"@supports",
    b"@charset", b"@-webkit-", b"@-moz-", b"@layer", b"@container",
    b"@property", b"@scope",
)

#: CSS property regex (ASCII, bytes): `foo:` at line start or after `;{`.
#: Deliberately EXCLUDES `(`, `=`, `;`, `[` so a JSON/JS line like
#: `"a":1,` or `x=(y)` can never match (the field regression precedent).
_CSS_PROP_RE = re.compile(
    rb"(?:^|[\n\r;{])[ \t]*[a-zA-Z-]{2,24}[ \t]*:[ \t]*[^ \t\n\r;{}]")


def _sniff_magic(head: bytes) -> str:
    """Strongest-evidence check: magic bytes at offset 0."""
    for sig, mime in _MAGIC_PREFIXES:
        if mime and head.startswith(sig):
            return mime
    if head.startswith(b"RIFF") and head[8:12] in _RIFF_SUBMAGIC:
        return _RIFF_SUBMAGIC[head[8:12]]
    if head[_FTYP_OFF[0]:_FTYP_OFF[1]] == b"ftyp":
        # iso-bmff: brand at 8..12 — mp4 brands start with 'isom'/'mp4'/'avc'…
        brand = head[8:12]
        if brand[:2] in (b"is", b"mp", b"av", b"da", b"mo", b"MS", b"ma"):
            return "video/mp4"
    return ""


def _sniff_svg(head: bytes) -> bool:
    """SVG: `<svg` at start, or an `<?xml` prologue followed by `<svg`."""
    h = head.lstrip()
    if h.startswith(b"<svg") or h.startswith(b"<!DOCTYPE svg"):
        return True
    if h.startswith(b"<?xml"):
        return b"<svg" in h[:512]
    return False


def _sniff_js(head: bytes) -> bool:
    """Executable-JavaScript check with a hit-count threshold.

    A >4-token consensus is required: minified JS hits double digits within
    the first 2 KB, while JSON payloads, license banners and HTML text
    stay far below. `crossOriginIsolated`-style API prose in HTML comments
    never reaches the threshold either.
    """
    # Small webpack/CSS-module chunks often contain only a registration and
    # an object of class names: they never reach the token-count threshold.
    # Their CSS-like strings/properties must not turn executable JS into CSS.
    # Keep quoted literals intact while removing comments, so // inside a
    # URL does not eat the rest of the line. Ignore truncated comments too.
    quoted = rb'''"(?:\\.|[^"\\])*(?:"|$)|'(?:\\.|[^'\\])*(?:'|$)|`(?:\\.|[^`\\])*(?:`|$)'''
    uncommented = re.sub(
        quoted + rb'|(?P<comment>/\*.*?(?:\*/|$)|//[^\r\n]*)',
        lambda m: b' ' if m.group('comment') is not None else m.group(0),
        head, flags=re.DOTALL)
    prefix = uncommented.lstrip()
    if re.match(rb'''["']use (?:strict|asm)["']\s*;''', prefix):
        return True
    if re.match(
            rb'(?:\(\s*)?(?:this|self|window|globalThis)\s*'
            rb'(?:\.\s*webpackChunk[\w$]*|\[\s*["\']webpackChunk[^"\']*["\']\s*\])\s*=',
            prefix):
        return True
    # CSS comments and content strings can describe JavaScript without
    # being executable code. Only count evidence outside those literals.
    code = re.sub(quoted, b' ', uncommented, flags=re.DOTALL)
    hits = 0
    for tok in _JS_EXECUTABLE:
        if tok in code:
            hits += 1
            if hits >= _JS_MIN_HITS:
                return True
    return False


def _sniff_css(head: bytes) -> bool:
    """Stylesheet check — @-rules first (strong), then property shapes."""
    # CSS inside a JavaScript/JSON string is data, not a stylesheet. Ignore
    # quoted strings and comments before looking for rules/declarations.
    # The sniff window may end inside a long string/comment.
    head = re.sub(
        rb'''/\*.*?(?:\*/|$)|"(?:\\.|[^"\\])*(?:"|$)|'(?:\\.|[^'\\])*(?:'|$)|`(?:\\.|[^`\\])*(?:`|$)''',
        b' ', head, flags=re.DOTALL)
    for rule in _CSS_AT_RULES:
        if rule in head:
            return True
    # A run of property declarations is convincing; a single one is not.
    return len(_CSS_PROP_RE.findall(head)) >= 3


def _json_prefix_ok(t: str) -> bool:
    """Is `t` valid JSON, or a CLEAN PREFIX of a JSON document?

    A cached JSON payload larger than the 2048-byte sniff window gets
    truncated mid-document; the honest test is then: cut at the last
    structural character, strip a dangling comma, close every bracket that
    is still open (string/escape aware), and see if json.loads accepts the
    completed document. Only structural characters count — a truncation in
    the middle of a string or an unbalanced bracket refuses (returns
    False), keeping the false-positive rate at zero.
    """
    import json

    def _complete(s: str) -> bool:
        s = s.rstrip(",")
        if not s:
            return False
        stack: list[str] = []
        in_str = False
        esc = False
        for ch in s:
            if esc:
                esc = False
                continue
            if ch == "\\":
                if in_str:
                    esc = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch in "[{":
                stack.append("]" if ch == "[" else "}")
            elif ch in "]}":
                if not stack or stack[-1] != ch:
                    return False
                stack.pop()
        if in_str or esc:
            return False
        try:
            json.loads(s + "".join(reversed(stack)))
            return True
        except ValueError:
            return False

    # 1. Direct parse (with short trailing-garbage retries).
    for cut in range(len(t), max(len(t) - 40, 0), -1):
        try:
            json.loads(t[:cut])
            return True
        except ValueError:
            continue
    # 2. Close the truncated document as-is (number/keyword cut).
    if _complete(t):
        return True
    # 3. Cut at the last structural character, then close.
    last = max(t.rfind(","), t.rfind("}"), t.rfind("]"))
    if last >= 4:
        return _complete(t[:last + 1])
    return False


def _sniff_json(head: bytes) -> bool:
    """JSON check: valid prefix that json.loads accepts (or a clean slice)."""
    try:
        text = head.decode("utf-8", "strict")
    except (UnicodeDecodeError, ValueError):
        return False
    t = text.lstrip()
    if not t or t[0] not in "[{":
        return False
    return _json_prefix_ok(t)


def _read_head(lp: str, size: int) -> bytes:
    try:
        if getattr(_s2l_state, "ATOMIC_SERVE", False):
            return read_cached_file(lp, size)
        with open(lp, "rb") as f:
            return f.read(size)
    except OSError:
        return b""


def _resolve_mime_with_sniff(lp: str, data: bytes | None = None,
                             live_ct: str = "") -> str:
    """Resolve MIME, memoizing sniff results when ATOMIC_SERVE is enabled."""
    if getattr(_s2l_state, "ATOMIC_SERVE", False):
        head = _read_head(lp, _SNIFF_HEAD) if data is None else data[:_SNIFF_HEAD]
        return _memoized_mime(lp, head, live_ct)
    return _resolve_mime_uncached(lp, data, live_ct)


@lru_cache(maxsize=4096)
def _memoized_mime(lp: str, head: bytes, live_ct: str) -> str:
    # Actual sniff bytes in the key keep edited files' MIME results current.
    return _resolve_mime_uncached(lp, head, live_ct)


def _resolve_mime_uncached(lp: str, data: bytes | None = None,
                           live_ct: str = "") -> str:
    """Resolve the MIME type of a cached file from name + content + any CT
    the live pipeline still knows about.

    Order of authority (strongest first):

      1. magic bytes at offset 0 (wasm/PNG/JPEG/GIF/WEBP/woff2/woff/otf/
         ttf/mp4) — content can never lie about its container;
      2. SVG prologue;
      3. HTML doctype/html tags in the first bytes;
      4. extension's guess_mime() — trusted UNLESS the extension belongs to
         the rescue-target family (``.js``/``.css``/``.json`` — the lying
         extension case) and content checks contradict it with confidence;
      5. JSON parse / JS token consensus / CSS @-rule+property checks
         (only for extensionless files);
      6. application/octet-stream.

    `data` may be a prefix of the file (≥ _SNIFF_HEAD bytes when the caller
    has them; a shorter slice still works, just with less evidence). When
    None, the head is read from disk. `live_ct` is the upstream's own
    Content-Type when available — used only as a tie-breaker between two
    plausible content types, never to downgrade a magic-byte finding.

    Never-downgrade guarantee: a caller-provided live_ct or extension type
    is only ever REPLACED by a stronger content finding, and octet-stream
    (the "unknown" answer) is always replaceable.
    """
    ext = os.path.splitext(os.path.basename(lp))[1].lower()
    looks_html_path = lp.endswith(".html") or lp.endswith("/index.html")

    if data is None:
        data = _read_head(lp, _SNIFF_HEAD)
    head = data[:_SNIFF_HEAD] if data else b""

    # 1. magic bytes — the strongest possible evidence.
    magic = _sniff_magic(head) if head else ""
    if magic:
        return magic

    # 2. SVG prologue.
    if head and _sniff_svg(head):
        return "image/svg+xml"

    # 3. HTML by content.
    if head and _s2l_state._is_valid_html(head) \
            and not _s2l_state._is_wire_payload(head):
        return "text/html; charset=utf-8"

    # 4. Extension-derived type (the normal, trusted case).
    ext_ct = ""
    if ext or looks_html_path:
        ext_ct = _s2l_state.guess_mime(lp)
        if looks_html_path:
            # .html / /index.html — the layout promises HTML unconditionally.
            return ext_ct
        base_ct = _s2l_state._ct_base(ext_ct)
        _trusted = (base_ct.startswith("text/")
                    or base_ct in _TRUSTED_CT_BASES)
        if ext not in _RESCUE_TARGET_EXTS and _trusted:
            # Trusted extension (text/executable/unambiguous binary) —
            # its answer stands unless magic bytes already said otherwise.
            return ext_ct
        # else: rescue-target family (.js/.css/.json lying) OR an OBSCURE
        # extension (.gl → video/gl, .dyn → application/octet-stream …) —
        # fall through and let the content speak.

    # From here on we are in rescue territory: extensionless files, or an
    # extension from the lying family (.js/.css/.json) that content may
    # contradict.
    if head:
        if _sniff_json(head):
            return "application/json"
        if _sniff_js(head):
            return "application/javascript; charset=utf-8"
        if _sniff_css(head):
            return "text/css; charset=utf-8"

    # Nothing contradicted the extension — keep its answer (or the live
    # pipeline's own CT when the extension had nothing to say).
    if ext_ct and ext_ct != "application/octet-stream":
        return ext_ct
    if live_ct:
        return live_ct
    return "application/octet-stream"
