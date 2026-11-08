"""Site2Local: URL → on-disk cache path mapping."""
from __future__ import annotations

from urllib.parse import urlparse
import hashlib
import os

from ... import Runtime as _s2l_state


def _query_sidecar_path(lp: str) -> str:
    """Path of the `<file>.q` sidecar that records a beautified cache
    file's owning query string (raw, as the server sent it)."""
    return lp + ".q"


def _query_sidecar_owner(lp: str) -> str | None:
    """Read the owning query recorded next to a beautified cache file.

    Returns None when there is no sidecar (cache written before 9.1, or a
    plain non-query file) — callers then treat the file as unowned.
    """
    try:
        with open(_query_sidecar_path(lp), "r", encoding="utf-8", errors="replace") as f:
            owner = f.readline().strip("\r\n")
        return owner or None
    except OSError:
        return None


def _write_query_sidecar(lp: str, qs: str) -> None:
    """Record the owning query next to a beautified cache file.

    Best-effort by design: a failed sidecar write must never fail the cache
    write itself. Without the sidecar the collision guard degrades to the
    pre-9.1 behavior (first writer wins), it never makes things worse.
    """
    if not qs:
        return
    try:
        with open(_query_sidecar_path(lp), "w", encoding="utf-8") as f:
            f.write(qs + "\n")
    except OSError:
        pass


def _purge_cached_file(lp: str) -> None:
    """Remove a cache file AND its query sidecar (when present).

    Used by every purge site (stale RSC, bot pages, startup sweep) so that
    deleting the body never leaves an orphan `<file>.q` behind — an orphan
    sidecar would keep "reserving" the pretty name for a query whose file
    no longer exists.
    """
    try:
        os.remove(lp)
    except OSError:
        pass
    if lp.endswith(".q"):
        return
    try:
        os.remove(_query_sidecar_path(lp))
    except OSError:
        pass


def local_path(u: str, device: str = "") -> str:
    """Map a URL → its on-disk cache path.

    Naming rules (the 9.1 layout):

    * **No query string** — the host folder mirrors the site's own paths:
        ``/a/b.js``      → ``host/a/b.js``
        ``/a/`` or ``/a`` → ``host/a/index.html``

    * **Any query string** — the file goes under the host's reserved
      ``__query__/`` folder, so query variants never mix with real files.
      The pretty name follows the user's sketch (CORRECTED in 9.2 — the
      parameter NAME always joins the value, for 1 and 2+ params alike):

      - path has an extension → the last segment's base, the beautified
        query and the real extension:
            ``/a/b.js?v=1.2``   → ``host/__query__/a/b_v_1.2.js``
        (1 param → ``[base]_[k]_[v]``)
      - path has no extension (binary/data endpoints such as
        ``/v1/asset?id=N``) → the last path segment becomes the file's
        base, in its parent folder:
            ``/v1/asset?id=N``  → ``host/__query__/v1/asset_id_N``
            ``/css2?family=Montserrat:wght@400&display=swap``
                                → ``host/__query__/css2_family_Montserrat_wght@400_N_display_swap``
        (2+ params → ``[k1]_[v1]_N_[k2]_[v2]``, ``N`` stands for "&")

      Beautifying is lossy (``?v=a%20b`` and ``?v=a_b`` share
      ``base_v_a_b``), so every write records the owning RAW query in a
      ``<file>.q`` sidecar. When ``local_path()`` finds the pretty name
      already owned by a DIFFERENT query, this URL falls back to the
      collision-free legacy name (``base_=<encoded query>`` / bare
      ``<encoded query>``) — deterministic across restarts because the
      sidecar lives on disk.

      Caches written by OLDER layouts (pre-9.1 / 9.1a) are NOT migrated
      anymore — a request for such a URL is a normal MISS. The startup
      sweep (``_sweep_legacy_cache()``) deletes those unreachable files.

    `device` is a VARIANT TAG from _device_variant() ("", "mobile",
    "tablet"). Non-empty tags suffix the filename (index.html →
    index__s2l_mobile.html, bundle.css → bundle__s2l_mobile.css)" so each
    device family caches its own copy of EVERY file type — UA-sniffing
    sites serve different HTML, CSS/JS bundles, JSON payloads, manifest
    lists and even images (WebP vs JPEG) per device. The canonical
    (unsuffixed) file stays the desktop-family copy AND the home of the
    user's hand edits ("mods" — the host has full filesystem access),
    which device fetches can never overwrite.
    """
    p = urlparse(_s2l_state.normalize_url(u))
    path = p.path or "/"
    # ``splitext`` returns ('', '') for '/', ('/a/b', '') for '/a/b', and
    # ('/a/b', '.js') for '/a/b.js'. A trailing '/' means "directory" → no
    # extension.
    has_ext = bool(os.path.splitext(path)[1]) and not path.endswith("/")

    if p.query:
        path_clean = path.rstrip("/")
        seg_all = [_s2l_state._safe_seg(x) for x in path_clean.split("/") if x]
        dir_parts = seg_all[:-1]
        last_seg = seg_all[-1] if seg_all else "index"
        beauty = _s2l_state._beautify_query_name(p.query)

        def _apply_device(fname: str) -> str:
            if not device:
                return fname
            b, e = os.path.splitext(fname)
            return f"{b}__s2l_{device}{e}"

        if has_ext:
            base, ext = os.path.splitext(last_seg)
            # Keep the whole name under the filesystem limit (long base +
            # long query used to overflow 255 bytes and fail on write).
            room = _s2l_state._FS_NAME_MAX - 2 - len(ext) - 12 - len(beauty)   # 12 = device suffix
            if len(base) > room:
                base = base[:max(8, room - 13)] + "_" + hashlib.sha1(base.encode()).hexdigest()[:12]
            fname = f"{base}_{beauty}{ext}"
            # Collision-free fallback name (pre-9.1 layout). It joins the
            # SAME capped base with the longer reversible encoding, so it
            # gets its own post-join cap (9.2) — the join could still pass
            # _FS_NAME_MAX when the query encodes long.
            legacy_fname = _s2l_state._cap_query_fname(
                f"{base}_={_s2l_state._encode_query_for_filename(p.query, bare=False)}{ext}")
        else:
            # last_seg (≤180 via _safe_seg) + '_' + beauty (≤160) can join
            # to 341 chars — cap it so the write never fails (9.2).
            fname = _s2l_state._cap_query_fname(f"{last_seg}_{beauty}")
            # Bare fallback: the query encoded as the file name, inside the
            # path-named folder (the pre-9.1 bare layout; capped inside the
            # encoder itself).
            legacy_fname = _s2l_state._encode_query_for_filename(p.query, bare=True)

        parts = [_s2l_state._QUERY_DIR, *dir_parts, _apply_device(fname)]
        new_lp = os.path.join(_s2l_state.SRC_FOLDER, _host_dir_of(p),
                              *parts)

        # ── Collision guard ─────────────────────────────────────────────
        # The pretty name is lossy; the sidecar records who owns it. When
        # this URL is not the owner, use the collision-free legacy name so
        # two different queries can never share (or silently overwrite)
        # one cache file. Only checked when the pretty file exists — the
        # hot miss path stays a pure name computation.
        if os.path.isfile(new_lp):
            owner = _query_sidecar_owner(new_lp)
            if owner is not None and owner != p.query:
                if has_ext:
                    lf = _apply_device(legacy_fname)
                    parts = [_s2l_state._QUERY_DIR, *dir_parts, lf]
                else:
                    lf = _apply_device(legacy_fname)
                    # pre-9.1 bare layout: path folders + bare query file
                    parts = [_s2l_state._QUERY_DIR, *seg_all, lf]
                new_lp = os.path.join(_s2l_state.SRC_FOLDER, _host_dir_of(p),
                                      *parts)
    else:
        # ── no query → mirrors the site's own paths ──
        if path.endswith("/") or not os.path.splitext(path)[1]:
            path = path.rstrip("/") + "/index.html"
        parts = [_s2l_state._safe_seg(x) for x in path.split("/") if x]
        if device and parts:
            base, ext = os.path.splitext(parts[-1])
            parts[-1] = f"{base}__s2l_{device}{ext}"
        new_lp = os.path.join(_s2l_state.SRC_FOLDER, _host_dir_of(p), *parts)

    return new_lp


def _host_dir_of(p) -> str:
    """Host directory for a parsed URL (strip credentials/port, www prefix).

    Stripping the port avoids ":" in directory names (invalid on Windows).
    """
    host_dir = p.netloc.split("@")[-1].split(":")[0]
    host_dir = host_dir.removeprefix("www.") if host_dir.startswith("www.") else host_dir
    return host_dir
