"""Site2Local: RFC 6455 frames and permessage-deflate."""
from __future__ import annotations

import base64
import hashlib
import os
import socket
import struct

from ... import Runtime as _s2l_state


def _ws_unmask(payload: bytes, mask_key: bytes) -> bytes:
    """XOR payload with a 4-byte mask key. Used by both client→server (mask)
    and server→client (unmask) directions — XOR is its own inverse.

    Fast path: use int.from_bytes + struct for 8-byte chunks when payload is
    large enough that the per-byte Python loop would dominate. Falls back to
    the simple loop for tiny payloads.
    """
    if not payload:
        return b""
    if len(payload) < 64:
        return bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
    # Build a 4-byte → 8-byte mask by repeating, then XOR via int ops.
    # This is ~10-50× faster than the per-byte loop on large frames.
    mk4 = mask_key
    mk8 = mk4 + mk4
    out = bytearray(len(payload))
    n8 = len(payload) & ~7    # largest multiple of 8 ≤ len
    mk_int = int.from_bytes(mk8, "little")
    # Process 8 bytes at a time
    for i in range(0, n8, 8):
        chunk = int.from_bytes(payload[i:i+8], "little")
        out[i:i+8] = (chunk ^ mk_int).to_bytes(8, "little")
    # Tail (0-7 bytes)
    for i in range(n8, len(payload)):
        out[i] = payload[i] ^ mk4[i & 3]
    return bytes(out)


def _ws_handshake_accept(key: str) -> str:
    """Compute the Sec-WebSocket-Accept value for the handshake response."""
    magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    sha1  = hashlib.sha1((key + magic).encode()).digest()
    return base64.b64encode(sha1).decode()


def _recv_exact(sock, n: int) -> bytes | None:
    """Read exactly n bytes, or return None if the peer closes first.

    No socket timeout is set: recv() blocks until either data arrives or the
    peer closes (returns b''). This means a WS tunnel stays open for the full
    lifetime of the browser tab — the browser itself closes the socket when
    the user navigates away, and that closure unblocks recv() so the pump can
    shut down cleanly. Removing the timeout fixes the "WS dies after 30s of
    idle" bug: many WS apps have long quiet periods (push notifications,
    chat, realtime dashboards) and the old 30s socket timeout was killing
    them silently.
    """
    buf = bytearray()
    try:
        sock.settimeout(None)   # block forever — let the peer drive teardown
    except Exception:
        pass
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except (socket.timeout, TimeoutError):
            return None   # only fires if caller set its own timeout earlier
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


def _ws_read_frame(sock) -> tuple[bool, bool, int, bytes] | None:
    """Read a single WebSocket frame.

    Returns (fin, rsv1, opcode, payload) — the RSV1 bit is required
    to correctly implement permessage-deflate (RFC 7692 §6.1): only frames
    with RSV1 set on the FIRST frame of a message are compressed. The previous
    3-tuple discarded RSV1, forcing the pump to guess whether a frame was
    compressed based solely on the negotiated use_deflate flag — which broke
    for any frame the peer chose not to compress, and made it impossible to
    distinguish permessage-deflate from an application's unrelated zlib-stream scheme.

    Returns None on EOF, timeout, or protocol error.
    """
    try:
        h = _s2l_state._recv_exact(sock, 2)
        if h is None:
            return None
        b0, b1  = h
        fin     = bool(b0 & 0x80)
        rsv1    = bool(b0 & 0x40)   # permessage-deflate marker (RFC 7692 §6.1)
        opcode  = b0 & 0x0F
        masked  = bool(b1 & 0x80)
        pay_len = b1 & 0x7F
        if pay_len == 126:
            l2 = _s2l_state._recv_exact(sock, 2)
            if l2 is None:
                return None
            pay_len = struct.unpack("!H", l2)[0]
        elif pay_len == 127:
            l8 = _s2l_state._recv_exact(sock, 8)
            if l8 is None:
                return None
            pay_len = struct.unpack("!Q", l8)[0]
        mask_key = b""
        if masked:
            mask_key = _s2l_state._recv_exact(sock, 4)
            if mask_key is None:
                return None
        # Cap payload size to prevent a malicious or buggy peer from
        # exhausting memory with a multi-GB frame. 8 MiB is well above any
        # legitimate WS message (typical gateway payloads are < 100 KB;
        # even the largest real-time APIs cap at ~1 MB). Anything bigger is
        # almost certainly an attack or a desync.
        if pay_len > _s2l_state.WS_MAX_MSG_BYTES:
            _s2l_state.log(f"WS frame too large ({pay_len} bytes, max {_s2l_state.WS_MAX_MSG_BYTES}) — dropping",
                "WARN")
            return None
        # Use bytearray + extend for better memory performance on large frames
        # (bytearray.extend avoids the quadratic copy of bytes concatenation).
        _payload = bytearray()
        _remaining = pay_len
        while _remaining > 0:
            chunk = sock.recv(min(65536, _remaining))
            if not chunk:
                return None
            _payload.extend(chunk)
            _remaining -= len(chunk)
        payload = bytes(_payload)
        if masked:
            # Fast C-level unmask instead of per-byte Python loop — the
            # original `bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))`
            # path was the #1 CPU hot spot on busy WS tunnels (active
            # gateways can push hundreds of small frames per second).
            mk = mask_key
            payload = _s2l_state._ws_unmask(payload, mk)
        return fin, rsv1, opcode, payload
    except Exception:
        return None


def _ws_make_frame(opcode: int, payload: bytes, mask: bool = False,
                   fin: bool = True, rsv1: bool = False) -> bytes:
    """Build a WebSocket frame (server → client: no mask; client → server: masked).

    `fin=False` is required to correctly relay one piece of a fragmented
    message — leaving this hardcoded to True (as if every frame were always
    a complete, standalone message) silently mis-tags every continuation
    frame in a fragmented message as if it were the final one.

    `rsv1=True` sets the RSV1 bit (0x40) on the first byte. This is
    MANDATORY for permessage-deflate (RFC 7692 §6.1): the decompressor on the
    receiving side will not attempt to inflate a message whose first frame
    lacks RSV1. The previous version had no way to set RSV1, so every
    "compressed" frame it emitted was indistinguishable from a plain frame —
    the receiver either parsed raw deflate bytes as JSON (garbage) or, on
    an application using its own zlib-stream, applied its decompressor to the
    mangled payload and crashed with "zlib error, -3, incorrect header check".
    """
    l = len(payload)
    h = bytearray()
    b0 = (0x80 if fin else 0x00) | (0x40 if rsv1 else 0x00) | opcode
    h.append(b0)   # FIN + RSV1 + opcode
    if l < 126:
        h.append((0x80 if mask else 0) | l)
    elif l < 65536:
        h.append((0x80 if mask else 0) | 126)
        h.extend(struct.pack("!H", l))
    else:
        h.append((0x80 if mask else 0) | 127)
        h.extend(struct.pack("!Q", l))
    if mask:
        mk = os.urandom(4)
        h.extend(mk)
        payload = _s2l_state._ws_unmask(payload, mk)   # mask == unmask XOR semantics
    return bytes(h) + payload
