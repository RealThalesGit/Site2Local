"""Site2Local: startup sweep of unservable legacy cache files.

9.0's contract (the "new generation" cut): the lazy migration bridges that
used to rename pre-9.1 / 9.1a caches into the current beautified layout are
GONE — a request for a URL whose only on-disk copy sits in an old layout is
a normal MISS (online re-fetches into the current name; offline 404s). The
old-layout files themselves are unreachable garbage, so this sweep deletes
them unconditionally at startup.

SAFETY INVARIANT — a file is deleted only when NO URL can resolve to it:
for every plausible interpretation of the on-disk name the sweeper rebuilds
the candidate URL(s) and asks ``local_path()``; the file is unreachable iff
none of them map back to it. Since serving happens exclusively through
``local_path()``, an unreachable file can never be served — deleting it
cannot change any response. Every heuristic below fails in the "keep"
direction:

* Host-tree files whose name contains ``=`` (the 09/10 query layouts
  ``b_=<query>.js`` / ``v1/asset/id=N`` written straight into the host
  folder): URL path segments are sanitized to ``[\\w.\\-]`` and query URLs
  ALWAYS live under ``__query__/``, so an ``=`` in a host-tree file name
  is unreachable by construction. (The pre-9.1 ``_q<hash>`` family is NOT
  provably unreachable — a real no-query URL can legitimately contain such
  a segment — so those files stay.)
* ``__query__`` files WITH a ``.q`` sidecar: every ``_``-split of the stem
  (with and without a device suffix) is tried as ``<base>`` carrying the
  sidecar's raw query, under both the with-extension and the bare
  (no-extension) interpretations — a beautified value may itself contain
  dots, so ``splitext`` alone cannot tell them apart.
* ``__query__`` files whose name contains ``=`` WITHOUT a sidecar (the
  pre-9.1 reversible encodings): decoded as the raw query, in both the
  ``base_=<enc><ext>`` and the bare ``<enc>`` shapes.
* A pretty file WITHOUT a sidecar and WITHOUT ``=`` is unprovable (its
  query is unknowable) — kept.
* An encoder output whose tail matches the head+sha1 CAP signature is not
  reversible — kept.
* Orphan ``.q`` sidecars (base file gone) — always deleted.

Documented corners (accepted, both self-heal by re-fetching online): a
hand-made file mimicking a pretty name that contains ``=`` with no
sidecar, and hosts whose directory name re-triggers the ``www.`` strip
(``www.www.*``). Every layout any 9.x release ever WROTE is exempt from
both: 9.0+ writes sidecars with every query cache, and pre-9.1 wrote the
reversible ``_=`` / bare encodings.
"""
from __future__ import annotations

import os
import re

from ... import Runtime as _s2l_state

# Device-variant suffix appended by local_path() (_apply_device).
_DEVICE_RE = re.compile(r"__s2l_([A-Za-z0-9_]+)$")
# Tail of a capped name (head + "_" + 12 hex): NOT reversible.
_CAPPED_RE = re.compile(r"_[0-9a-f]{12}$")


def _query_file_is_live(host_dir: str, reldir: str, fname: str) -> bool:
    """Can ANY URL still resolve to this ``__query__`` file via local_path()?"""
    src = _s2l_state.SRC_FOLDER
    fp = os.path.normpath(os.path.join(src, host_dir,
                                       _s2l_state._QUERY_DIR, reldir, fname))
    dirs = [d for d in reldir.split(os.sep) if d and d != "."]
    stem, ext = os.path.splitext(fname)
    side_q = _s2l_state._query_sidecar_owner(fp)

    def _mk(last: str, q: str) -> str:
        path = "/".join([*dirs, last]) if last else "/".join(dirs)
        if not path.startswith("/"):
            path = "/" + path
        return f"https://{host_dir}{path}?{q}"

    def _try(url: str, dev: str) -> bool:
        try:
            return os.path.normpath(_s2l_state.local_path(url, dev)) == fp
        except Exception:
            return False

    # Interpretation triples: (stem, ext, device). splitext yields the
    # with-extension reading; the full filename as a bare stem covers the
    # no-extension reading (beautified values may contain dots); a device
    # suffix is tried both ways because a query VALUE could legitimately
    # end in "__s2l_<word>".
    triples: list[tuple[str, str, str]] = [(stem, ext, "")]
    m = _DEVICE_RE.search(stem)
    if m:
        triples.append((stem[:m.start()], ext, m.group(1)))
    if ext:
        triples.append((fname, "", ""))
        if m:
            triples.append((stem[:m.start()] + ext, "", m.group(1)))

    for st, ex, dev in triples:
        # 1) Sidecar interpretations — every '_' split as the URL base.
        if side_q:
            for i in range(1, len(st)):
                if st[i] == "_":
                    if _try(_mk(st[:i] + ex, side_q), dev):
                        return True
        # 2) Raw-encoding interpretations (pre-9.1 / collision fallback).
        if "=" in st:
            idx = st.find("_=")
            if idx > 0:
                enc = st[idx + 2:]
                if not enc:
                    pass
                elif _CAPPED_RE.search(enc):
                    # Capped encoder output (head + "_" + sha1[:12]) is
                    # irreversible — the true query is unknowable, so the
                    # file is UNPROVABLE, not dead. Keep it (inert).
                    return True
                elif _try(_mk(st[:idx] + ex, enc), dev):
                    return True
            if not ex and st:
                if _CAPPED_RE.search(st):
                    return True   # capped bare encoding — unprovable, keep
                if _try(_mk("", st), dev):
                    return True

    # Last resort: a capped PRETTY name (``_cap_query_fname`` writes
    # head[:32] + "_" + sha1[:12] when the base+query join overflows the
    # filesystem limit) hashes the FULL pre-cap join — irreversible, so a
    # matching file is UNPROVABLE, not dead. Keep it. (A long-base URL
    # whose cap ate the base/query split point lands here: the sidecar
    # splits cannot reconstruct a base that is no longer in the name.)
    for st, _ex, _dev in triples:
        if _CAPPED_RE.search(st):
            return True
    return False


def _prune_empty_dirs(root: str) -> int:
    """Bottom-up removal of empty directories under ``root`` (never root)."""
    removed = 0
    for dirpath, _dirnames, _files in os.walk(root, topdown=False):
        if dirpath == root:
            continue
        try:
            os.rmdir(dirpath)   # fails loudly on non-empty → skip
            removed += 1
        except OSError:
            pass
    return removed


def _sweep_legacy_cache() -> tuple[int, int]:
    """Delete every cache file no URL can resolve to. Returns (n, bytes).

    Two passes over ``SRC_FOLDER``:

    * HOST TREE (outside ``__query__/``): ``=``-named files are the 09/10
      query layouts — unreachable by construction (see module docstring).
    * ``__query__/`` trees: liveness via ``_query_file_is_live()``; orphan
      ``.q`` sidecars go unconditionally.

    Empty directories left behind are pruned bottom-up. Every removal is
    logged (one INFO line, mirroring the old "Cache migrated" lines); a
    summary line closes the sweep. Concurrency-safe by construction: the
    sweep only deletes files that no request can resolve, and files being
    written right now are reachable through their own URL by definition.
    """
    src = _s2l_state.SRC_FOLDER
    if not os.path.isdir(src):
        return (0, 0)
    removed = 0
    freed = 0

    def _rm(fp: str, note: str = "") -> None:
        nonlocal removed, freed
        try:
            freed += os.path.getsize(fp)
        except OSError:
            pass
        _s2l_state._purge_cached_file(fp)   # body + companion .q sidecar
        removed += 1
        _s2l_state.log(f"Swept legacy cache: {os.path.relpath(fp)}{note}", "INFO")

    # ── Pass 1: host tree (skip __query__/ — pass 2 owns those) ──────────
    for root, dirnames, files in os.walk(src):
        if _s2l_state._QUERY_DIR in dirnames:
            dirnames.remove(_s2l_state._QUERY_DIR)
        for fn in files:
            if "=" in fn:
                _rm(os.path.join(root, fn))

    # ── Pass 2: every host's __query__/ tree ─────────────────────────────
    for host in sorted(os.listdir(src)):
        qdir = os.path.join(src, host, _s2l_state._QUERY_DIR)
        if not os.path.isdir(qdir):
            continue
        for root, _dirnames, files in os.walk(qdir):
            reldir = os.path.relpath(root, qdir)
            for fn in files:
                fp = os.path.join(root, fn)
                if fn.endswith(".q"):
                    if os.path.isfile(fp[:-2]):
                        continue   # base still there — its own check handles it
                    # os.walk captured the listing BEFORE we started deleting
                    # in this directory, so a sidecar whose base was just
                    # swept is still in `files` — only count a removal that
                    # actually happens (the second os.remove fails silently).
                    try:
                        _sz = os.path.getsize(fp)
                    except OSError:
                        _sz = 0
                    try:
                        os.remove(fp)
                    except OSError:
                        continue   # phantom (stale listing) — nothing removed
                    freed += _sz
                    removed += 1
                    _s2l_state.log(f"Swept orphan sidecar: {os.path.relpath(fp)}", "INFO")
                    continue
                if _query_file_is_live(host, reldir, fn):
                    continue
                _rm(fp)

    dirs_gone = _prune_empty_dirs(src)

    if removed:
        _s2l_state.log(f"Legacy cache sweep: removed {removed} unreachable "
                       f"file(s), freed {_s2l_state._fmt_size(freed)}, "
                       f"pruned {dirs_gone} empty dir(s)", "INFO")
    return (removed, freed)
