"""Site2Local: legacy cache migration."""
from __future__ import annotations

import os
import shutil

from ... import Runtime as _s2l_state


def _migrate_legacy_cache(u: str, new_lp: str, device: str = "") -> str:
    """Auto-migrate an old cache file to the new naming on first request.

    Called from ``local_path()`` as a side effect. Three tiers:

    1. **TIER 1 (hottest)**: ``new_lp`` is in the in-memory
       ``_migration_checked`` set → return immediately. Zero stat calls.
       This is the steady state after the first request for each URL.

    2. **TIER 2**: ``new_lp`` is on disk (file exists) → add to the set
       and return. One stat call.

    3. **TIER 3 (cold)**: neither — check legacy paths and atomically
       rename the first one found. Uses ``os.rename`` (lock-free, atomic
       on same filesystem). If two threads race, one wins and the other
       gets ``OSError`` (caught). No global lock needed — this is the
       key fix for the lock-contention bug that serialised all 150+
       concurrent asset requests under a single threading.Lock.
    """
    # TIER 1: in-memory cache (amortised zero-cost after first request).
    if new_lp in _s2l_state._migration_checked:
        return new_lp

    # TIER 2: file already on disk.
    try:
        if os.path.isfile(new_lp):
            with _s2l_state._migration_checked_lock:
                _s2l_state._migration_checked.add(new_lp)
                if len(_s2l_state._migration_checked) > _s2l_state._MIGRATION_CHECKED_MAX:
                    _s2l_state._migration_checked.clear()
                    _s2l_state._migration_checked.add(new_lp)
            return new_lp
    except OSError:
        with _s2l_state._migration_checked_lock:
            _s2l_state._migration_checked.add(new_lp)
        return new_lp

    # TIER 3: check legacy paths and try atomic rename (no lock).
    for legacy in _s2l_state._legacy_cache_paths(u, device):
        if not os.path.isfile(legacy):
            continue
        try:
            os.makedirs(os.path.dirname(new_lp) or ".", exist_ok=True)
            # os.rename is atomic on same filesystem — if two threads race,
            # one succeeds and the other gets OSError (caught below).
            os.rename(legacy, new_lp)
            _s2l_state.log(f"Cache migrated: {os.path.relpath(legacy)} → {os.path.relpath(new_lp)}", "INFO")
            with _s2l_state._migration_checked_lock:
                _s2l_state._migration_checked.add(new_lp)
            return new_lp
        except OSError:
            # Another thread may have just migrated (FileExistsError), or
            # cross-filesystem (EXDEV — fall back to shutil.move), or the
            # legacy file was deleted between our isfile check and rename.
            try:
                if os.path.isfile(new_lp):
                    # Another thread won the race — we're done.
                    with _s2l_state._migration_checked_lock:
                        _s2l_state._migration_checked.add(new_lp)
                    return new_lp
                # Cross-filesystem fallback (rare — SRC_FOLDER is one tree).
                shutil.move(legacy, new_lp)
                _s2l_state.log(f"Cache migrated (shutil): {os.path.relpath(legacy)} → {os.path.relpath(new_lp)}", "INFO")
                with _s2l_state._migration_checked_lock:
                    _s2l_state._migration_checked.add(new_lp)
                return new_lp
            except OSError:
                continue

    # No legacy file found — caller will fetch and write at new_lp.
    with _s2l_state._migration_checked_lock:
        _s2l_state._migration_checked.add(new_lp)
    return new_lp
