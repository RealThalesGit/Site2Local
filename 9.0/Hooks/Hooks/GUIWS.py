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


def _gui_ws_push(direction: str, opcode: int, payload: bytes,
                 ws_url: str, tunnel_id: int) -> None:
    """Push the original WS message to the GUI traffic log (before replacement)."""
    if not _s2l_state.HOOK_GUI:
        return
    try:
        parsed = urlparse(ws_url)
        _s2l_state._gui_ws_log_queue.put_nowait({
            "ts":        time.strftime("%H:%M:%S"),
            "direction": direction,        # "in" (browser→srv) | "out" (srv→browser)
            "opcode":    opcode,
            "op_name":   _s2l_state._WS_OP_NAMES.get(opcode, f"?{opcode}"),
            "ws_url":    ws_url,
            "origin":    parsed.netloc or "",
            "path":      parsed.path or "/",
            "size":      len(payload),
            "payload":   payload,
            "tunnel_id": tunnel_id,
        })
    except queue.Full:
        now = time.time()
        if now - _s2l_state._gui_ws_drop_last_log[0] > 5.0:
            _s2l_state.log(f"WebSocket GUI queue full — dropping messages ({direction} {ws_url})", "WARN")
            _s2l_state._gui_ws_drop_last_log[0] = now


def _apply_gui_ws_hooks(direction: str, opcode: int, payload: bytes,
                        ws_url: str, tunnel_id: int) -> bytes:
    """Apply the first matching enabled GUI WS hook. Returns the (possibly
    replaced) payload. Thread-safe.

    Filters: direction, opcode, origin_url, path pattern. First match wins.

    INSTANCED HOOKS:
      Each hook carries a `match_mode` that decides whether the hook should
      fire for THIS specific payload. Without this check, a hook created from
      one captured message would override EVERY subsequent message on the
      same path — the user's actual intent ("replace this one response") was
      being violated, polluting unrelated messages on the same WebSocket.

      match_mode == "any"      → fire on every (direction, opcode, origin, path) match
      match_mode == "exact"    → fire only when payload == match_payload (byte-for-byte)
      match_mode == "regex"    → fire only when payload (UTF-8 decoded) matches match_payload regex
      match_mode == "contains" → fire only when match_payload is a substring of payload
    """
    if not _s2l_state.HOOK_GUI:
        return payload
    with _s2l_state._gui_ws_hooks_lock:
        hooks = list(_s2l_state._gui_ws_hooks)
    _path = urlparse(ws_url).path or "/"
    _host = urlparse(ws_url).netloc.lower().split(":", 1)[0]
    if _host.startswith("www."):
        _host = _host[4:]
    for h in hooks:
        if not h["enabled"]:
            continue
        hd = (h.get("direction") or "*").strip().lower()
        if hd not in ("*", "any", "both", "") and hd != direction:
            continue
        try:
            ho = int(h.get("opcode", 0) or 0)
        except (ValueError, TypeError):
            ho = 0
        if ho and ho != opcode:
            continue
        ho_url = (h.get("origin_url") or "*").strip().lower()
        if ho_url not in ("", "*", "any"):
            if ho_url in ("target", "main"):
                if _host != _s2l_state.MAIN_HOST.lower():
                    continue
            elif not (_host == ho_url or _host.endswith("." + ho_url)):
                continue
        try:
            if not re.search(h["pattern"], _path, re.IGNORECASE):
                continue
        except re.error:
            continue
        # ── Instanced match check: does THIS payload match the hook's
        # captured target? If not, skip — don't blanket-override every
        # message on the same path.
        mode = (h.get("match_mode") or "any").strip().lower()
        if mode not in ("any", "", "exact", "regex", "contains"):
            mode = "any"
        if mode != "any":
            mp = h.get("match_payload") or b""
            if not mp:
                # A payload-specific hook without its persisted target must
                # never degrade into a wildcard replacement.
                continue
            elif mode == "exact":
                if payload != mp:
                    continue
            elif mode == "contains":
                if mp not in payload:
                    continue
            elif mode == "regex":
                try:
                    pat = re.compile(mp.decode("utf-8", "ignore"), re.DOTALL)
                    if not pat.search(payload.decode("utf-8", "ignore")):
                        continue
                except (re.error, UnicodeDecodeError):
                    continue
        new_payload = h["body_bytes"]
        _s2l_state.log(f"WS hook '{h['name']}' matched {direction} {ws_url} "
            f"op={_s2l_state._WS_OP_NAMES.get(opcode, opcode)} mode={mode} "
            f"{_s2l_state._fmt_size(len(new_payload))}", "HOOK")
        return new_payload
    return payload
