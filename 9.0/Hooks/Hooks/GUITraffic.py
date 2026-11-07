"""Site2Local: hook context, registry, GUI bridge and editor store."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from urllib.parse import urlparse
import base64
import json
import os
import queue
import re
import tempfile
import time

from ... import Runtime as _s2l_state


def _gui_push(ctx: _s2l_state.HookContext) -> None:
    """Push a proxied request/response to the GUI traffic log with body."""
    if not _s2l_state.HOOK_GUI:
        return

    body = ctx.resp_body if ctx.resp_body is not None else b""

    # Show a readable placeholder in the Body Editor for truly empty responses.
    # Distinguish between legitimately-empty statuses (204/304/3xx) and
    # suspicious empties (200 with no body) so the user can tell at a glance
    # whether the emptiness is expected or a bug.
    display_body = body
    if len(body) == 0:
        sc = ctx.resp_status
        ct = (ctx.resp_ct or "").lower()
        if sc == 204:
            display_body = "(204 No Content - response has no body by definition)".encode()
        elif sc == 304:
            display_body = "(304 Not Modified - served from browser cache, no body)".encode()
        elif sc == 206:
            display_body = "(206 Partial Content - empty body, range request may have produced no bytes)".encode()
        elif sc in (301, 302, 303, 307, 308):
            display_body = "(redirect - no body, follow Location header)".encode()
        elif "json" in ct:
            display_body = b"(empty JSON response -- upstream returned 200 with no body)"
        elif "html" in ct:
            display_body = b"(empty HTML response -- upstream returned 200 with no body)"
        else:
            display_body = b"(empty response)"

    try:
        # Extract origin host from ctx.url so the hook editor can auto-populate
        # the "Origin URL" field and hooks can match by domain.
        _origin = ""
        try:
            _origin = urlparse(ctx.url).netloc or ""
        except Exception:
            pass
        _s2l_state._gui_log_queue.put_nowait({
            "ts":       time.strftime("%H:%M:%S"),
            "method":   ctx.method,
            "path":     ctx.path,
            "query":    ctx.query,
            "status":   ctx.resp_status,
            "ct":       ctx.resp_ct.split(";")[0].strip() if ctx.resp_ct else "",
            "web_type": "None",            # main proxy requests have no CDN tag — show "None"
            "size":     len(body),         # real size (0 = correct)
            "body":     display_body,      # what the editor shows
            "origin":   _origin,
        })
    except queue.Full:
        # Throttle: only log the first drop in a 5-second window, otherwise
        # a busy site floods the console with hundreds of identical WARNs.
        now = time.time()
        if now - _s2l_state._gui_drop_last_log[0] > 5.0:
            _s2l_state.log(f"GUI queue full — dropping entries ({ctx.method} {ctx.path} …)", "WARN")
            _s2l_state._gui_drop_last_log[0] = now


def _gui_push_raw(method: str, path: str, status: int, ct: str, body: bytes,
                  display_tag: str = "", origin: str = "",
                  _skip_log: bool = False) -> None:
    """Lightweight push for CDN mini-server / ext_asset routes.

    Also logs a matching line to the terminal so the GUI traffic log and the
    terminal log stay in sync. Callers that already log to terminal themselves
    (e.g. log_req) set _skip_log=True to avoid double-logging.

    display_tag is shown as a prefix in the Content-Type column (e.g. "[cdn:8081]")
    so the path column always contains the bare path that hook patterns match against.
    The hook editor's pattern field tests against ctx.path (always the bare path),
    so the tag must NOT be merged into `path` or hook patterns would never match.

    origin is the upstream host this request was served from (e.g. "cdn.example.com").
    It's stored in the traffic log entry so the hook editor can auto-populate the
    "Origin URL" field, and so hooks can match by domain (not just path).
    """
    # Sync: log to terminal even when HOOK_GUI is off, so the terminal always
    # shows the same set of entries the GUI would show.
    if body is None:
        body = b""
    elif not isinstance(body, bytes):
        try:
            body = str(body).encode("utf-8")
        except Exception:
            body = b""

    # Build a meaningful display body for the GUI editor.
    if body:
        display_body = body
    elif status == 204:
        display_body = "(204 No Content - response has no body by definition)".encode()
    elif status == 304:
        display_body = "(304 Not Modified - served from browser cache, no body)".encode()
    elif status == 206:
        display_body = "(206 Partial Content - empty body, range request may have produced no bytes)".encode()
    elif status in (301, 302, 303, 307, 308):
        display_body = "(redirect - no body, follow Location header)".encode()
    else:
        display_body = b"(empty response)"
    ct_clean = ct.split(";")[0].strip() if ct else ""
    web_type = display_tag or "None"

    # Sync: emit a terminal line for this entry (unless the caller already did).
    if not _skip_log:
        _tag = display_tag.strip("[]") if display_tag else ""
        _prefix = f"[{_tag}] " if _tag else ""
        _host = origin or ""
        _log_level = "CDN" if _tag and ("cdn" in _tag.lower() or "ext" in _tag.lower()) else "→"
        _s2l_state.log(f"{_prefix}{method} {status} {_host}{path} {_s2l_state._fmt_size(len(body))}", _log_level)

    if not _s2l_state.HOOK_GUI:
        return

    try:
        _s2l_state._gui_log_queue.put_nowait({
            "ts":       time.strftime("%H:%M:%S"),
            "method":   method,
            "path":     path,
            "query":    "",
            "status":   status,
            "ct":       ct_clean,
            "web_type": web_type,
            "size":     len(body),
            "body":     display_body,
            "origin":   origin,
        })
    except queue.Full:
        pass
