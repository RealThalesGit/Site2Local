"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations

from flask import request as flask_request
from urllib.parse import urldefrag
from urllib.parse import urlparse
import re
import socket
import time

from ... import Runtime as _s2l_state


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}GB"


def _fmt_host(host: str) -> str:
    """Normalize a hostname for display: strip www. prefix and port.
    Returns 'host' (no scheme, no port) for consistent log lines.
    Examples: 'www.example.com:443' → 'example.com'
              'cdn.example.com' → 'cdn.example.com'
    """
    if not host:
        return ""
    # Strip user:pass@ if present
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    # Strip port
    if ":" in host:
        host = host.split(":", 1)[0]
    # Strip www. prefix
    if host.startswith("www."):
        host = host[4:]
    return host


def _fmt_path_qs(path: str, qs: str | None = None) -> str:
    """Access-log display path: append '?<query>' when the request had one.

    Query-variant cache files live in ``__query__/`` but the access log
    shows only the URL path, so N hits on the same path with different
    queries are indistinguishable — the field complaint was six lines like
    ``GET HIT aya.nodium.lol/asset/ 25.3KB / 8.9KB / 6.1KB ...`` where every
    line was a DIFFERENT ``?`` variant. Appending the raw query string makes
    each line self-explanatory (``GET HIT host/asset/?id=1 8.9KB``); lines
    without a query stay byte-identical to the old format.

    ``qs`` defaults to the LIVE request's query string (Flask), so plain
    view-code call sites need no extra plumbing. Pass a known string when
    the caller already decoded it (CDN/EXT/STREAM sites), or ``""`` to
    suppress. Outside a request context (background threads) the path is
    returned unchanged — the helper never raises.
    """
    if qs is None:
        try:
            qs = flask_request.query_string.decode("utf-8", "ignore")
        except RuntimeError:      # working outside a request context
            return path
    return f"{path}?{qs}" if qs else path


def log_req(method: str, status: int, host: str, path: str, size: int,
            tag: str = "", query: str = "") -> None:
    """Structured one-line request log + GUI traffic-log entry.

    ``query`` (raw query string, no leading '?') is appended to the DISPLAYED
    path only — the GUI traffic entry keeps the bare path.
    """
    prefix = f"[{tag}] " if tag else ""
    _h = _s2l_state._fmt_host(host)
    _s2l_state.log(f"{prefix}{method} {status} {_h}{_s2l_state._fmt_path_qs(path, query)} {_s2l_state._fmt_size(size)}", "→")
    _s2l_state._gui_push_raw(method, path, status, "", b"",
                  display_tag=f"[{tag.upper()}]" if tag else "",
                  origin=_s2l_state._fmt_host(host),
                  _skip_log=True)
