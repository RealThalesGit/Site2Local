"""Site2Local: URL → on-disk cache path mapping."""
from __future__ import annotations

from urllib.parse import urlparse
import hashlib
import os

from ... import Runtime as _s2l_state


def local_path(u: str, device: str = "") -> str:
    """Map a URL → its on-disk cache path.

    Naming rules (the 10/10 layout):

    * **No query string** — the host folder mirrors the site's own paths:
        ``/a/b.js``      → ``host/a/b.js``
        ``/a/`` or ``/a`` → ``host/a/index.html``

    * **Any query string** — the file goes under the host's reserved
      ``__query__/`` folder, so query variants never mix with real files:

      - path has an extension → the query sits between ``_=`` and the real
        extension (MIME is preserved, name stays readable):
            ``/a/b.js?v=1.2`` → ``host/__query__/a/b_=v=1.2.js``
      - path has no extension (binary/data endpoints such as
        ``/v1/asset?id=N``) → the path becomes a folder and the query is the
        file name, no extension:
            ``/v1/asset?id=N`` → ``host/__query__/v1/asset/id=N``
        ``_serve_cached`` sniffs the content to decide HTML vs
        ``application/octet-stream`` because the name carries no extension.

      Query text is stored as sent (not decoded or re-ordered) and encoded
      by ``_encode_query_for_filename()`` — readable, reversible in
      practice, no collisions from lossy ``_`` replacement.

    Caches from the three older layouts (``_q<hash>``, ``<seg>_=<query>.html``
    and the 09/10 files that sat directly in the host folder) are
    auto-migrated into ``__query__/`` on first request via
    ``_migrate_legacy_cache()``.

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
        dir_parts = [_s2l_state._safe_seg(x) for x in path_clean.split("/") if x]
        if has_ext:
            # ── extension + query → __query__/<dirs>/base_=<query>.ext ──
            base, ext = os.path.splitext(dir_parts.pop())
            qname = _s2l_state._encode_query_for_filename(p.query, bare=False)
            # Keep the whole name under the filesystem limit (long base +
            # long query used to overflow 255 bytes and fail on write).
            room = _s2l_state._FS_NAME_MAX - 2 - len(ext) - 12 - len(qname)   # 12 = device suffix
            if len(base) > room:
                base = base[:max(8, room - 13)] + "_" + hashlib.sha1(base.encode()).hexdigest()[:12]
            parts = [_s2l_state._QUERY_DIR, *dir_parts, f"{base}_={qname}{ext}"]
        else:
            # ── no extension + query → __query__/<path folders>/<query> ──
            # e.g. /v1/asset?id=N → host/__query__/v1/asset/id=N
            qname = _s2l_state._encode_query_for_filename(p.query, bare=True)
            parts = [_s2l_state._QUERY_DIR, *dir_parts, qname]
    else:
        # ── no query → mirrors the site's own paths ──
        if path.endswith("/") or not os.path.splitext(path)[1]:
            path = path.rstrip("/") + "/index.html"
        parts = [_s2l_state._safe_seg(x) for x in path.split("/") if x]

    # Device-variant suffix — applied AFTER the query handling, to EVERY
    # file type. The __s2l_ prefix avoids colliding with real files. For
    # no-extension files (the bare query case) splitext returns ('', '') so
    # the tag is appended with no extension — the result is still
    # extensionless and _serve_cached's content sniff still kicks in.
    if device and parts:
        base, ext = os.path.splitext(parts[-1])
        parts[-1] = f"{base}__s2l_{device}{ext}"

    # Strip the port and only the "www." PREFIX (not all occurrences) from netloc.
    # Stripping the port avoids ":" in directory names (invalid on Windows).
    host_dir = p.netloc.split("@")[-1].split(":")[0]   # strip user:pass@ and :port
    host_dir = host_dir.removeprefix("www.") if host_dir.startswith("www.") else host_dir
    new_lp = os.path.join(_s2l_state.SRC_FOLDER, host_dir, *parts)

    # Auto-migrate legacy caches on first request. Idempotent and
    # fast-path-gated — when the new file already exists this is a single
    # os.path.isfile() call.
    _s2l_state._migrate_legacy_cache(u, new_lp, device)

    return new_lp
