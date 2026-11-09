"""Isolated, portable workspaces for the historical regression batteries."""
import atexit
import os
from pathlib import Path
import re
import shutil
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
_workspaces = []


def legacy_workspace(name):
    workspace = TemporaryDirectory(prefix=f"s2l_{name}_")
    _workspaces.append(workspace)
    atexit.register(_cleanup)  # Before TemporaryDirectory's weakref finalizer.
    return workspace.name


def prepare_legacy(work):
    destination = Path(work) / "Site2Local"
    destination.mkdir()
    for source in ROOT.iterdir():
        if source.is_file() and source.suffix in (".py", ".txt", ".md", ".MD"):
            shutil.copy2(source, destination / source.name)
        elif source.is_dir() and (source / "__init__.py").exists():
            shutil.copytree(source, destination / source.name,
                            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    # Deterministic values, independent of the developer's personal settings.
    values = dict(SITE='aya.nodium.lol', HOST='127.0.0.1', PORT=8080,
                  OFFLINE=True, CRAWL=False, HOOK_GUI=False, MULTIPORT=True,
                  PROXY_CDN=True, CACHE_CDN=True, ATOMIC_SERVE=False,
                  CAPTURE=False, CAPTURE_WS=False, SCAN_PATHS=False,
                  CROSS_ORIGIN_ISOLATION=True, WEB_CDNVIEWER=True,
                  WEB_CDNVIEWER_IP_WHITELIST=[], WEB_CDNVIEWER_ALLOWED_ORIGINS=[],
                  SCAN_LIMIT='256KB', WS_MAX_MSG_BYTES='8MB', MAX_BODY_BYTES='256MB',
                  MAX_DECOMPRESSED_BYTES='512MB', STREAM_CACHE_MAX_BYTES='256MB')
    config = destination / "Config.py"
    text = config.read_text(encoding="utf-8")
    import json
    for key, value in values.items():
        literal = json.dumps(value) if isinstance(value, str) else repr(value)
        text = re.sub(rf"^{key}\s*=.*$", lambda match: f"{key} = {literal}", text, flags=re.M)
    config.write_text(text, encoding="utf-8")
    return destination


def load_legacy_runtime(name):
    work = legacy_workspace(name)
    package = prepare_legacy(work)
    os.chdir(work)
    sys.path.insert(0, work)
    from Site2Local import Runtime
    return package, Runtime


def _cleanup():
    os.chdir(ROOT)
    for workspace in reversed(_workspaces):
        workspace.cleanup()
