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
    if len(name) <= _s2l_state.MAX_FNAME:
        return name
    base, ext = os.path.splitext(name)
    h = hashlib.sha1(name.encode()).hexdigest()[:12]
    return f"{base[:32]}_{h}{ext}"


def _sanitize_query_for_filename(qs: str) -> str:
    """LEGACY (09/10) query → filename mapping. LOSSY — do not use for new files.

    Kept only so ``_legacy_cache_paths()`` can locate caches written by the
    09/10 layout (``b_=<query>.js`` / ``v1/asset/id=N`` straight in the host
    folder) and migrate them into ``__query__/``. Everything outside
    ``[\\w.\\-=]`` collapses to ``_`` here, so different queries could share
    one name (``a=1&b=2`` and ``a=1_b=2``). New files use
    ``_encode_query_for_filename()`` instead.
    """
    safe = re.sub(r"[^\w.\-=]", "_", qs)
    if len(safe) > _s2l_state.MAX_FNAME - 24:
        safe = safe[:80] + "_" + hashlib.sha1(qs.encode()).hexdigest()[:12]
    return safe


def _encode_query_for_filename(qs: str, bare: bool) -> str:
    """Raw query string → filesystem-safe name, readable and collision-free.

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
