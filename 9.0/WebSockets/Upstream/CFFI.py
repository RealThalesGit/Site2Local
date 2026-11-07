"""Site2Local: upstream WebSocket transports."""
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


class _CffiUpstreamWS:
    """Upstream WS connection via curl_cffi's own WebSocket client.

    This is what actually fixes gateway-style endpoints behind anti-bot WAFs:
    the opening TLS handshake goes through the SAME browser-impersonation
    engine _make_session() uses for every plain HTTP request in this file
    (real JA3/JA4, not a hand-tuned approximation), because ws_connect()
    reuses the session's already-impersonated curl handle.

    Trade-off: curl_cffi reassembles fragmented messages internally and
    only hands back / accepts complete ones — it can't stream individual
    wire frames the way the raw-socket path does. That's invisible to any
    real WS consumer (onmessage always sees the whole reassembled message
    either way); it only costs a bit of extra latency/memory on unusually
    large messages.
    """

    def __init__(self, ws_url: str, extra_headers: dict):
        # curl_cffi has a known bug with IPv6 address literals in ws_connect:
        # "TLS connect error: error:00000000:invalid library" — the internal
        # OpenSSL layer chokes on IPv6 SNI. Skip curl_cffi ONLY for IPv6
        # literals and let _ws_connect_upstream fall through to the raw-socket
        # path. IPv4 literals work fine with curl_cffi, so we still benefit
        # from browser TLS fingerprinting for IPv4 hosts.
        _parsed_ws = urlparse(ws_url)
        _ws_host = _parsed_ws.hostname or ""
        if _s2l_state._is_ipv6_literal(_ws_host):
            raise RuntimeError(f"curl_cffi skipped for IPv6 literal {_ws_host} (known IPv6 TLS bug)")
        hdrs = {k: v for k, v in extra_headers.items()
                if k.lower() not in ("host", "upgrade", "connection",
                                      "sec-websocket-key", "sec-websocket-version",
                                      "sec-websocket-extensions", "accept-encoding")}
        # Reuse the CLIENT session (which has cf_clearance cookies +
        # the same TLS fingerprint as the main page) instead of creating a
        # fresh one. This is what fixes CF-protected WS endpoints
        # — a fresh session has no cookies and gets 403'd by Cloudflare.
        sess, should_close = _s2l_state._get_session_for_ws()
        if not hasattr(sess, "ws_connect"):
            if should_close:
                try: sess.close()
                except Exception: pass
            raise RuntimeError("active session backend has no WS support")
        try:
            # timeout=15 caps ONLY the connect+handshake phase. Empirically
            # proven (WS probe, curl_cffi 0.16.3): CURLOPT_TIMEOUT_MS is NOT
            # enforced on post-handshake ws_recv() — neither with constant
            # traffic nor on a fully idle connection — so this does NOT kill
            # long-lived tunnels. It just fails fast when a host is
            # unreachable. Do NOT raise it "to fix dropped connections";
            # drops come from elsewhere (e.g. the keepalive thread).
            self._ws = sess.ws_connect(ws_url, headers=hdrs, timeout=15)
        except Exception as exc:
            # A rejected/failed handshake (403 from a WAF, refused upgrade,
            # TLS failure, ...) must not leak the session + its underlying
            # curl handle. Only close if we created it (shared client sessions
            # must NOT be closed here — they're used by other requests).
            if should_close:
                try: sess.close()
                except Exception: pass
                gc.collect()
            # Log at WARN so the user can see WHY the WS failed.
            _s2l_state.log(f"curl_cffi ws_connect failed: {exc}", "WARN")
            raise
        self._sess = sess
        self._should_close = should_close
        # curl_cffi's WebSocket object is NOT thread-safe for concurrent
        # sends — two send() calls from the keepalive and relay threads
        # corrupt internal buffers, producing garbled data that desyncs
        # the browser's application-level decompressor (e.g. zlib-stream:
        # "invalid stored block lengths"). _send_lock serializes send_frame
        # calls across threads; recv_frame is intentionally lock-free to
        # avoid deadlock on the blocking recv() call (see recv_frame
        # docstring for the rationale).
        self._send_lock = threading.Lock()

    def recv_frame(self):
        # Returns (fin, rsv1, opcode, payload) to match _ws_read_frame signature.
        #
        # IMPORTANT: do NOT wrap this in a lock. recv() is a blocking call that
        # can wait indefinitely for the next upstream frame. If a lock were held
        # here, send_frame() (called from the relay thread forwarding browser
        # messages like heartbeats) would block waiting for the lock, causing a
        # deadlock: upstream is quiet → recv blocks → send can't fire → browser
        # heartbeat never reaches upstream → upstream stays quiet → deadlock.
        #
        # The curl_cffi WebSocket tolerates concurrent send()/recv() from
        # different threads in practice (this proxy has always done so). The
        # zlib-stream corruption we saw earlier was caused by a THIRD thread —
        # the keepalive thread — calling send(PING) while recv() was blocked.
        # That thread is now disabled by default (WS_PING_INTERVAL=0), so
        # there's no concurrent send/recv race to protect against.
        #
        # PONG handling: curl_cffi has no CurlWsFlag.PONG. When upstream pongs
        # our PING, libcurl auto-consumes it. recv() returns (b"", 0) for a
        # pure-PONG wakeup — we map that to opcode 10 so the v2 pump (if
        # active) can update last_pong.
        try:
            data, flags = self._ws.recv()
        except Exception:
            return None
        if flags & _s2l_state._CurlWsFlag.CLOSE:
            return (True, False, 8, data)
        if flags & _s2l_state._CurlWsFlag.PING:
            return (True, False, 9, data)
        # No flags + no data → treat as auto-consumed PONG (libcurl eat it)
        if flags == 0 and not data:
            return (True, False, 10, b"")
        # NOTE: do NOT treat CurlWsFlag.OFFSET (value 32) as a PONG marker.
        # OFFSET is libcurl's flag for fragmented-message offset metadata,
        # not a PONG indicator. Treating it as PONG caused real data frames
        # carrying OFFSET to be silently dropped by the v2 pump's PONG handler.
        opcode = 1 if (flags & _s2l_state._CurlWsFlag.TEXT) else 2
        return (True, False, opcode, data)

    def send_frame(self, opcode: int, payload: bytes) -> bool:
        # Serialize sends via _send_lock. recv() stays lock-free to avoid
        # deadlock (it can block indefinitely waiting for upstream frames).
        # The keepalive thread (WS_PING_INTERVAL > 0) and the relay thread
        # both call this — without the lock, concurrent curl_cffi .send()
        # calls can corrupt internal buffers.
        if opcode == 10:
            return True   # PONG — libcurl auto-responds to upstream PINGs
        with self._send_lock:
            if opcode == 9:
                # PING — send a real control frame
                try:
                    self._ws.send(payload or b"", _s2l_state._CurlWsFlag.PING)
                    return True
                except Exception:
                    return False
            flag = {1: _s2l_state._CurlWsFlag.TEXT, 8: _s2l_state._CurlWsFlag.CLOSE}.get(opcode, _s2l_state._CurlWsFlag.BINARY)
            try:
                self._ws.send(payload or b"", flag)
                return True
            except Exception:
                return False

    def close(self):
        try: self._ws.close()
        except Exception: pass
        # Only close the session if we created it — shared client sessions
        # are used by other in-flight HTTP requests and must not be closed.
        if getattr(self, "_should_close", False):
            try: self._sess.close()
            except Exception: pass
