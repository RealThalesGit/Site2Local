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


def _ws_deflate_init(tunnel_id: int) -> None:
    """Initialize permessage-deflate contexts for one tunnel direction."""
    if not _s2l_state.WS_DEFLATE:
        return
    with _s2l_state._ws_ctx_lock:
        if tunnel_id not in _s2l_state._ws_deflate_ctx:
            _s2l_state._ws_deflate_ctx[tunnel_id] = zlib.compressobj(
                zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED,
                -_s2l_state._WS_PMDE_WINDOW_BITS, _s2l_state._WS_PMDE_MEM_LEVEL,
                zlib.Z_DEFAULT_STRATEGY,
            )
        if tunnel_id not in _s2l_state._ws_inflate_ctx:
            # -zlib.MAX_WBITS = raw deflate (no zlib header)
            _s2l_state._ws_inflate_ctx[tunnel_id] = zlib.decompressobj(-_s2l_state._WS_PMDE_WINDOW_BITS)


def _ws_deflate_msg(tunnel_id: int, data: bytes) -> bytes:
    """Compress one WS message per RFC 7692: deflate, append 0x00 0x00 0xFF 0xFF."""
    c = _s2l_state._ws_deflate_ctx.get(tunnel_id)
    if c is None:
        return data
    out = c.compress(data) + c.flush(zlib.Z_SYNC_FLUSH)
    # RFC 7692 §7.2.1: strip the trailing 0x00 0x00 0xFF 0xFF
    if out.endswith(b"\x00\x00\xff\xff"):
        out = out[:-4]
    return out


def _ws_inflate_msg(tunnel_id: int, data: bytes) -> bytes:
    """Decompress one permessage-deflate message."""
    d = _s2l_state._ws_inflate_ctx.get(tunnel_id)
    if d is None:
        return data
    # RFC 7692 §7.2.2: append the trailing 4 bytes before decompressing
    try:
        return d.decompress(data + b"\x00\x00\xff\xff") + d.flush()
    except Exception:
        # If decompression fails (context desync, missing tail), return raw
        # data so the message isn't silently dropped — the app layer will
        # likely fail to parse it, which is more debuggable than a hang.
        return data


def _ws_deflate_cleanup(tunnel_id: int) -> None:
    """Release deflate contexts for a closed tunnel."""
    with _s2l_state._ws_ctx_lock:
        _s2l_state._ws_deflate_ctx.pop(tunnel_id, None)
        _s2l_state._ws_inflate_ctx.pop(tunnel_id, None)
