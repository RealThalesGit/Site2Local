"""Site2Local: WebSocket capture records and writer worker."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from urllib.parse import urlparse
import base64
import hashlib
import json
import os
import queue
import re
import time

from ... import Runtime as _s2l_state


@dataclass
class _WSCaptureRecord:
    direction: str            # "in" (browser→srv) | "out" (srv→browser)
    opcode:    int            # 1=text, 2=binary, 8=close, 9=ping, 10=pong
    op_name:   str
    ws_url:    str
    tunnel_id: int
    payload:   bytes
    ts:        str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S")
                          + f".{int((time.time() % 1) * 1000):03d}")


def _ws_capture_dir(ws_url: str, tunnel_id: int) -> str:
    parsed = urlparse(ws_url)
    host = re.sub(r"[^\w.\-]", "_", parsed.netloc or "tunnel") or "tunnel"
    return os.path.join(_s2l_state.DATA_FOLDER, "captures_ws", host, f"t{tunnel_id}")
