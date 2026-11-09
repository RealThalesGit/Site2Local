"""Site2Local: cache path naming helpers."""
from __future__ import annotations

import hashlib
import os
import re

from ... import Runtime as _s2l_state


def _safe_seg(name: str) -> str:
    # Sanitize unsafe chars BEFORE length check. Without this, characters like
    # <, >, !, spaces, newlines pass through and contaminate file/directory
    # names — a URL path containing HTML (e.g. from a malformed API response)
    # would produce cache paths like "vue_app_<!DOCTYPE html>..." which breaks
    # on Windows and creates unusable cache entries on Linux.
    name = re.sub(r"[^\w.\-]", "_", name)
    # Dot-only components have filesystem semantics rather than file names.
    # Windows also normalizes longer runs of dots; never pass them to join().
    if name and not name.strip("."):
        name = "_s2l_dots_" + str(len(name))
    if len(name) <= _s2l_state.MAX_FNAME:
        return name
    base, ext = os.path.splitext(name)
    h = hashlib.sha1(name.encode()).hexdigest()[:12]
    limit = _s2l_state.MAX_FNAME
    if limit <= len(h):
        return h[:limit]
    # Extensions can themselves be huge. Keep ordinary MIME-bearing suffixes,
    # but never let a suffix defeat the final component-length limit.
    if len(ext) > min(32, limit - len(h) - 2):
        ext = ""
    room = limit - len(h) - 1 - len(ext)
    return f"{base[:min(32, room)]}_{h}{ext}"


#: Characters a beautified query segment keeps. Everything else (spaces,
#: ``:``/``;``/``/``/``?``/``#``, quotes, non-ASCII …) becomes ``_``.
_BEAUTY_SAFE_RE = re.compile(r"[^A-Za-z0-9@+.,~=\-]")
_BEAUTY_SEG_MAX = 64          # per-segment cap before hashing
_BEAUTY_TOTAL_MAX = 160       # whole-name cap before hashing (base joins later)


def _beauty_seg(s: str) -> str:
    """Sanitize one key/value fragment for the beautified name."""
    seg = _BEAUTY_SAFE_RE.sub("_", s).strip("_")
    if not seg:
        return ""
    if len(seg) > _BEAUTY_SEG_MAX:
        seg = seg[:_BEAUTY_SEG_MAX - 7] + "_" + hashlib.sha1(
            s.encode("utf-8", "surrogatepass")).hexdigest()[:6]
    return seg


def _beautify_query_name(qs: str) -> str:
    """Human-readable name for a query string (the "remove the junk" pass).

    Query naming:

      1 param   → ``[K]_[V]``                  ``?v=26b3…``   → ``v_26b3…``
      2+ params → ``[K1]_[V1]_N_[K2]_[V2]``    (``N`` stands for "&")

    so the final file name reads ``Aya.App_v_26b389885145fb927a677cca.js``, and
    ``css2?family=Montserrat:wght@400&display=swap`` becomes
    ``css2_family_Montserrat_wght@400_N_display_swap``.

    Values are percent-DECODED first (readability), which makes the mapping
    lossy — ``?a=1&b=2`` vs ``?a=1_b=2`` share a name. That is fine because
    ``local_path()`` guards collisions with the ``<file>.q`` sidecar (the
    owner's raw query) and falls back to the collision-free legacy layout
    for the losing URL. Flags (``?debug``) and empty values (``?k=``) use
    the key itself so the name is never empty; a value without a key
    (``?=x``) uses the value alone.
    """
    from urllib.parse import parse_qsl
    try:
        pairs = parse_qsl(qs, keep_blank_values=True, errors="replace")
    except Exception:
        pairs = []
    if not pairs:
        # Completely unparseable query (e.g. binary garbage) — keep the old
        # reversible encoding so nothing is lost.
        return _encode_query_for_filename(qs, bare=True).lstrip("=")

    if len(pairs) == 1:
        k, v = pairs[0]
        sk, sv = _beauty_seg(k), _beauty_seg(v)
        # Include both the parameter name and value to reduce collisions.
        if sk and sv:
            return f"{sk}_{sv}"
        # Flag (?debug) or empty value (?k=) → the key alone; a value with
        # no key (?=x) → the value alone; never empty.
        return sk or sv or "_"

    parts: list[str] = []
    for k, v in pairs:
        sk = _beauty_seg(k)
        sv = _beauty_seg(v)
        parts.append(f"{sk}_{sv}".rstrip("_") if sv else (sk or "_"))
    name = "_N_".join(parts)

    if len(name) > _BEAUTY_TOTAL_MAX:
        head = re.sub(r"_h?[0-9a-f]{0,6}$", "", name[:120]).rstrip("_")
        name = head + "_h" + hashlib.sha1(
            qs.encode("utf-8", "surrogatepass")).hexdigest()[:6]
    return name or "_"


def _cap_query_fname(name: str) -> str:
    """Post-join filesystem cap for a ``__query__`` filename (9.2).

    Each part is capped on its own (``_safe_seg``: 180; the beautified
    query: 160; ``_encode_query_for_filename``: MAX_FNAME-24), but the
    JOINED name can still cross ``_FS_NAME_MAX``: a no-extension pretty
    name is ``last_seg(≤180) + '_' + beauty(≤160)`` = up to 341 chars,
    and the extension collision fallback is ``capped_base + '_=' +
    encoding(≤156)``. An overlong name fails the cache write with
    ENAMETOOLONG — the asset then re-fetches on every request and never
    caches offline.

    When the join overflows, keep a 32-char head plus a sha1 tag of the
    FULL intended name: deterministic for the same URL, distinct for
    different URLs, and still guarded by the ``.q`` sidecar like any
    other beautified name. Names within the limit pass through
    unchanged (bit-identical to 9.1), so this cap never moves a file
    that 9.1 managed to write.
    """
    if len(name) <= _s2l_state._FS_NAME_MAX:
        return name
    head, ext = os.path.splitext(name)
    return (f"{head[:32]}_"
            f"{hashlib.sha1(name.encode('utf-8', 'surrogatepass')).hexdigest()[:12]}{ext}")


def _encode_query_for_filename(qs: str, bare: bool) -> str:
    """Raw query string → filesystem-safe name, readable and collision-free.

    This is the LEGACY (pre-9.1) __query__ layout, still used as the
    COLLISION FALLBACK: when two different queries beautify to the same
    pretty name, the sidecar ``<file>.q`` detects it and the losing URL
    keeps this reversible encoding instead.

    The query is stored as the server saw it (no decoding, no re-ordering):

    * ``[A-Za-z0-9_-=&,+~@]`` are kept as-is, so ``id=7`` and ``a=1&b=2``
      stay legible.
    * Existing ``%XX`` escapes are kept as-is (``q=a%20b`` stays ``q=a%20b``).
    * Everything else — ``/ \\ ? : * " < > |``, spaces, quotes, ``$ ! ;``,
      non-ASCII, control chars, a stray ``%`` — becomes ``%XX`` (UTF-8 bytes).
      Unlike the 09/10 scheme (which collapsed all of these to ``_``) two
      different queries no longer land on the same file.
    * ``bare=True`` (no extension in the URL path: the file IS the query,
      e.g. ``__query__/v1/asset/id=7``): dots are written ``%2E``. Otherwise
      ``?file=app.js`` would produce a file called ``file=app.js`` that
      ``_serve_cached`` mistakes for a real ``.js`` (served as JavaScript and
      URL-rewritten), and a query that is just ``..`` would resolve to the
      parent folder. With dots encoded a bare file never has an extension.
    * ``bare=True`` and no ``=`` in the result (``?debug``): a leading ``=``
      is added (``=debug``). URL path segments can never contain ``=``
      (``_safe_seg`` replaces it), so a name with ``=`` can never collide
      with a sub-folder (``/a?c`` vs ``/a/c?x=1``).
    * Longer than ``MAX_FNAME - 24``: the first 80 chars (never cutting an
      escape in half) + ``_`` + sha1 of the ORIGINAL query.

    Known, accepted corner: a malformed stray ``%`` (→ ``%25``) shares a name
    with a literal ``%25``; and ``?debug`` shares ``=debug`` with ``?=debug``.
    """
    out: list[str] = []
    i, n = 0, len(qs)
    while i < n:
        ch = qs[i]
        if ch == "%" and _s2l_state._QS_PCT_RE.fullmatch(qs[i:i + 3]):
            out.append(qs[i:i + 3])
            i += 3
            continue
        if ch == ".":
            out.append("%2E" if bare else ".")
        elif _s2l_state._QS_SAFE_RE.fullmatch(ch):
            out.append(ch)
        else:
            out.append("".join(f"%{b:02X}" for b in ch.encode("utf-8", "surrogatepass")))
        i += 1
    name = "".join(out)
    if len(name) > _s2l_state.MAX_FNAME - 24:
        head = re.sub(r"%[0-9A-Fa-f]?$", "", name[:80])
        name = head + "_" + hashlib.sha1(qs.encode("utf-8", "surrogatepass")).hexdigest()[:12]
    if bare and "=" not in name:
        name = "=" + name
    return name
