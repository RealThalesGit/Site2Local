"""Site2Local: hidden-path scanner."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from urllib.parse import urlparse
import json
import os
import sys
import threading
import time

from ... import Runtime as _s2l_state


def _load_wordlist(path: str) -> list[str]:
    """Read one wordlist file → list of path strings (de-duplicated, order kept)."""
    out: list[str] = []
    seen: set[str] = set()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                p = line.lstrip("/")
                if p and p not in seen:
                    seen.add(p)
                    out.append(p)
    except OSError as e:
        _s2l_state.log(f"Wordlist read error: {path} — {_s2l_state._short_exc(e)}", "WARN")
    return out


def _resolve_wordlists() -> list[tuple[str, list[str]]]:
    """Resolve SCAN_PATHS mode → ordered list of (display_name, paths).

    Returns [] when SCAN_PATHS is falsy or no wordlists are found.
    """
    if not _s2l_state.SCAN_PATHS or not isinstance(_s2l_state.SCAN_PATHS, str):
        return []
    mode = _s2l_state.SCAN_PATHS.strip()
    if not mode:
        return []

    files: list[str] = []   # absolute/relative file paths

    if mode == "all":
        # Recursive walk of WORDLISTS_DIR
        if os.path.isdir(_s2l_state.WORDLISTS_DIR):
            for root, _dirs, fns in sorted(os.walk(_s2l_state.WORDLISTS_DIR)):
                for fn in sorted(fns):
                    fp = os.path.join(root, fn)
                    if os.path.isfile(fp):
                        files.append(fp)
    elif mode == "all-in-dir":
        # Top-level only
        if os.path.isdir(_s2l_state.WORDLISTS_DIR):
            for fn in sorted(os.listdir(_s2l_state.WORDLISTS_DIR)):
                fp = os.path.join(_s2l_state.WORDLISTS_DIR, fn)
                if os.path.isfile(fp):
                    files.append(fp)
    else:
        # Specific file: relative to WORDLISTS_DIR
        fp = os.path.join(_s2l_state.WORDLISTS_DIR, mode)
        if os.path.isfile(fp):
            files.append(fp)
        else:
            # Also allow an absolute / cwd-relative path as a convenience
            if os.path.isfile(mode):
                files.append(mode)
            else:
                _s2l_state.log(f"SCAN_PATHS wordlist not found: {mode} (looked in {_s2l_state.WORDLISTS_DIR}/)", "ERROR")

    result: list[tuple[str, list[str]]] = []
    for fp in files:
        paths = _s2l_state._load_wordlist(fp)
        if paths:
            disp = os.path.relpath(fp, _s2l_state.WORDLISTS_DIR) if fp.startswith(_s2l_state.WORDLISTS_DIR) else fp
            result.append((disp, paths))
    return result
