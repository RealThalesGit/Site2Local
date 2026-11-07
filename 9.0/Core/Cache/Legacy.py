"""Site2Local: legacy cache path discovery."""
from __future__ import annotations

from urllib.parse import urlparse
import hashlib
import os

from ... import Runtime as _s2l_state


def _legacy_cache_paths(u: str, device: str = "") -> list[str]:
    """Compute LEGACY on-disk cache paths for a URL — used only for migration.

    Three prior naming schemes are recognised so existing caches keep working
    after the move into ``__query__/``. Listed newest first, so the most
    recent layout wins when several exist:

    0. The 09/10 layout (query files straight in the host folder, lossy name):
       * with extension + query → ``a/b_=<query>.js``
       * no extension + query   → ``a/asset/id=N``

    1. ``_q<sha1[:10]>`` suffix:
       * with extension + query  → ``a/b_q<hash>.js``
       * no extension + query    → ``a/index_q<hash>.html``
         (the old code unconditionally appended ``/index.html`` before
         hashing, so the segment became ``index_q<hash>.html`` inside the
         path-named folder).

    2. ``<last_seg>_=<query>.html`` (only ever produced for no-extension +
       query URLs — e.g. ``v1/asset?id=N`` → ``v1/asset_=id=N.html``).

    The optional ``device`` tag is applied to the legacy filename exactly
    as ``local_path()`` would have done at the time, so a device-variant
    legacy cache migrates to the device-variant new path.
    """
    p = urlparse(_s2l_state.normalize_url(u))
    path = p.path or "/"
    host_dir = p.netloc.split("@")[-1].split(":")[0]
    host_dir = host_dir.removeprefix("www.") if host_dir.startswith("www.") else host_dir
    if not p.query:
        return []

    qs_hash = hashlib.sha1(p.query.encode()).hexdigest()[:10]
    qs_safe = _s2l_state._sanitize_query_for_filename(p.query)
    has_ext = bool(os.path.splitext(path)[1]) and not path.endswith("/")
    legacy: list[str] = []

    path_clean = path.rstrip("/")
    raw_parts = [_s2l_state._safe_seg(x) for x in path_clean.split("/") if x]

    # ── Layout 0: the 09/10 layout ──
    parts0 = list(raw_parts)
    if has_ext:
        base0, ext0 = os.path.splitext(parts0[-1])
        parts0[-1] = f"{base0}_={qs_safe}{ext0}"
    else:
        parts0.append(qs_safe)
    if device and parts0:
        b0, e0 = os.path.splitext(parts0[-1])
        parts0[-1] = f"{b0}__s2l_{device}{e0}"
    legacy.append(os.path.join(_s2l_state.SRC_FOLDER, host_dir, *parts0))

    if has_ext:
        # ── Layout 1 only (layout 2 never existed for extension+query) ──
        parts = list(raw_parts)
        last = parts[-1]
        base, ext = os.path.splitext(last)
        parts[-1] = f"{base}_q{qs_hash}{ext}"
        if device and parts:
            b, e = os.path.splitext(parts[-1])
            parts[-1] = f"{b}__s2l_{device}{e}"
        legacy.append(os.path.join(_s2l_state.SRC_FOLDER, host_dir, *parts))
        return legacy

    # ── No-extension + query: two older layouts to try ──
    last_seg = raw_parts[-1] if raw_parts else "index"

    # Layout 1: <path>/index_q<hash>.html
    idx_parts = raw_parts + ["index.html"]
    b, e = os.path.splitext(idx_parts[-1])
    idx_parts[-1] = f"{b}_q{qs_hash}{e}"
    if device and idx_parts:
        b2, e2 = os.path.splitext(idx_parts[-1])
        idx_parts[-1] = f"{b2}__s2l_{device}{e2}"
    legacy.append(os.path.join(_s2l_state.SRC_FOLDER, host_dir, *idx_parts))

    # Layout 2: <path_dir>/<last_seg>_=<query>.html
    legacy2_name = f"{last_seg}_={qs_safe}.html"
    if device:
        legacy2_name = f"{last_seg}_={qs_safe}__s2l_{device}.html"
    legacy2_dir_parts = raw_parts[:-1]  # parent directory of last_seg
    if legacy2_dir_parts:
        legacy.append(os.path.join(_s2l_state.SRC_FOLDER, host_dir, *legacy2_dir_parts, legacy2_name))
    else:
        legacy.append(os.path.join(_s2l_state.SRC_FOLDER, host_dir, legacy2_name))

    return legacy
