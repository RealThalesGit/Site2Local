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


def log_req(method: str, status: int, host: str, path: str, size: int, tag: str = "") -> None:
    """Structured one-line request log + GUI traffic-log entry."""
    prefix = f"[{tag}] " if tag else ""
    _h = _s2l_state._fmt_host(host)
    _s2l_state.log(f"{prefix}{method} {status} {_h}{path} {_s2l_state._fmt_size(size)}", "→")
    _s2l_state._gui_push_raw(method, path, status, "", b"",
                  display_tag=f"[{tag.upper()}]" if tag else "",
                  origin=_s2l_state._fmt_host(host),
                  _skip_log=True)
