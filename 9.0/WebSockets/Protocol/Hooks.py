"""Site2Local: RFC 6455 frames and permessage-deflate."""
from __future__ import annotations

from typing import Callable
from urllib.parse import urlparse
import base64
import gc
import hashlib
import os
import socket
import ssl
import struct
import threading
import time
import zlib

from ... import Runtime as _s2l_state


def on_ws_message(fn: Callable) -> Callable:
    """Register a hook called for every WS message passing through any tunnel."""
    _s2l_state._WS_MSG_HOOKS.append(fn)
    _s2l_state.log(f"WS msg hook → {fn.__name__}()", "HOOK")
    return fn


def _run_ws_msg_hooks(direction: str, opcode: int, payload: bytes, tunnel_id: int) -> None:
    if not _s2l_state._WS_MSG_HOOKS:
        return
    for fn in _s2l_state._WS_MSG_HOOKS:
        try:
            fn(direction, opcode, payload, tunnel_id)
        except Exception as exc:
            _s2l_state.log(f"WS msg hook {fn.__name__} raised: {exc}", "ERROR")
