"""Site2Local: upstream WebSocket transports."""
from __future__ import annotations

from urllib.parse import urlparse
import base64
import os
import socket
import ssl
import struct
import threading

from ... import Runtime as _s2l_state


class _RawSocketUpstreamWS:
    """Upstream WS connection over a bare Python `ssl` socket.

    WS endpoints behind an anti-bot WAF (CF-protected gateways, similar
    Cloudflare-fronted realtime APIs) fail the handshake every single time
    on this path: those edges fingerprint the *opening TLS ClientHello*
    (JA3/JA4), and Python's `ssl` module produces one that's trivially
    distinguishable from a real browser's no matter what cipher list or
    ALPN gets bolted onto it — that's a property of OpenSSL's fixed
    extension set/ordering, not something fixable from Python's ssl API.
    Kept as the fallback for targets that don't need spoofing, or for when
    curl_cffi is unavailable.
    """

    def __init__(self, ws_url: str, extra_headers: dict):
        parsed  = urlparse(ws_url)
        use_ssl = ws_url.startswith("wss://")
        host    = parsed.hostname    # strips [] from IPv6 literals
        port    = parsed.port or (443 if use_ssl else 80)
        path_qs = parsed.path or "/"
        if parsed.query:
            path_qs += "?" + parsed.query
        # For the Host header, reconstruct the netloc with brackets for IPv6.
        # parsed.hostname strips brackets, but HTTP Host: must include them.
        if ":" in (host or "") and not host.startswith("["):
            host_hdr = f"[{host}]"
        else:
            host_hdr = host or ""
        if port and not ((use_ssl and port == 443) or (not use_ssl and port == 80)):
            host_hdr = f"{host_hdr}:{port}"
        # SNI: don't send an IP literal as server_hostname — many servers/WAFs
        # reject SNI that looks like an IP (RFC 6066 says SNI is for DNS names).
        # Pass None for IP literals so OpenSSL sends an empty SNI extension.
        sni_hostname = host if host and not _s2l_state._is_ip_literal(host) else None

        raw = socket.create_connection((host, port), timeout=15)
        # After the TCP connect succeeds, clear the timeout so the WS
        # handshake AND all subsequent frame reads block until either side
        # closes the socket. The 15s connect timeout above is intentional
        # (we want to fail fast on unreachable hosts), but no socket-level
        # timeout should apply once we're connected.
        raw.settimeout(None)
        srv = raw
        try:
            if use_ssl:
                try:
                    # Best-effort nudge toward a Chrome-like ClientHello (ALPN +
                    # cipher order) — meaningfully less fingerprintable than the
                    # bare interpreter default, but not a real JA3 match. See
                    # _CffiUpstreamWS below for the actual fix.
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False
                    ctx.verify_mode    = ssl.CERT_NONE
                    ctx.set_alpn_protocols(["http/1.1"])
                    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
                    try:
                        ctx.set_ciphers(
                            "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
                            "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                            "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305"
                        )
                    except ssl.SSLError:
                        pass
                except Exception:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode    = ssl.CERT_NONE
                # Set a longer timeout for the TLS handshake — IPv6 connections
                # to remote servers can take longer than the default, and a timeout
                # here produces "The handshake operation timed out" which the
                # user saw in the log.
                raw.settimeout(30)
                srv = ctx.wrap_socket(raw, server_hostname=sni_hostname)
                # Back to NO timeout for the WS handshake AND for the subsequent
                # frame reads — the tunnel should outlive idle periods (chat,
                # push, dashboards). The handshake itself is fast; once it's
                # done, recv blocks until either side closes the socket.
                raw.settimeout(None)

            # RFC 6455 §4.1: Sec-WebSocket-Key is 16 random bytes, base64-encoded
            # DIRECTLY — no hashing. SHA1 only enters the picture on the SERVER
            # side, to compute Sec-WebSocket-Accept from this key (see
            # _ws_handshake_accept). Hashing it here first produced a 20-byte
            # value (28 base64 chars instead of the required 24), which lenient
            # servers ignore but strict ones reject outright with a 400 before
            # ever reaching the application — silently sabotaging every upstream
            # WS connection this function ever made, independent of TLS/anti-bot
            # concerns.
            key = base64.b64encode(os.urandom(16)).decode()
            hdrs = [
                f"GET {path_qs} HTTP/1.1",
                f"Host: {host_hdr}",
                "Upgrade: websocket",
                "Connection: Upgrade",
                f"Sec-WebSocket-Key: {key}",
                "Sec-WebSocket-Version: 13",
            ]
            for k, v in extra_headers.items():
                kl = k.lower()
                if kl not in ("host", "upgrade", "connection",
                              "sec-websocket-key", "sec-websocket-version",
                              "sec-websocket-extensions", "accept-encoding"):
                    hdrs.append(f"{k}: {v}")
            srv.sendall(("\r\n".join(hdrs) + "\r\n\r\n").encode())

            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = srv.recv(4096)
                if not chunk:
                    raise ConnectionError("Server closed during WS handshake")
                resp += chunk
                if len(resp) > 65536:
                    raise ConnectionError("WS handshake response too large")
            if b"101" not in resp.split(b"\r\n")[0]:
                raise ConnectionError(f"WS handshake rejected: {resp[:200]!r}")
            # Preserve any bytes the server sent right after the 101 handshake
            # (pipelined first WS frame). Without this, realtime upstreams that
            # fire a frame immediately after upgrading lose those leading bytes
            # and the tunnel desyncs permanently.
            _hdr_end = resp.index(b"\r\n\r\n") + 4
            _leftover = resp[_hdr_end:]
        except Exception:
            # Any failure past this point — TLS handshake, send, a dropped
            # connection mid-handshake, or an outright rejection — must not
            # leak the socket. This runs on every reconnect attempt a
            # backing-off client makes against a target that's actively
            # blocking it, so a leak here isn't a one-off: it's one held
            # file descriptor per retry, forever, for as long as the target
            # keeps saying no.
            try: srv.close()
            except Exception: pass
            raise
        self._sock = srv
        self._pushback = bytearray(_leftover)   # pre-frame bytes from handshake read
        # Dedicated send-lock: socket.sendall is NOT thread-safe, and the
        # keepalive thread (when WS_PING_INTERVAL > 0) can call send_frame()
        # concurrently with the relay thread. recv stays lock-free to avoid
        # deadlocking on a blocking recv.
        self._send_lock = threading.Lock()
        # No recv timeout: the upstream WS socket blocks forever waiting for
        # the next frame, just like the browser side. The tunnel only tears
        # down when either side closes the socket. The previous 15s timeout
        # killed idle WS connections (chat, push, dashboards) after 15s of
        # silence, which broke every realtime app.
        try:
            self._sock.settimeout(None)
        except Exception:
            pass

    def _recv_exact_pushback(self, n: int) -> bytes | None:
        """Read exactly n bytes, draining the pushback buffer first, then the
        socket. Returns None on EOF/timeout (same semantics as _recv_exact)."""
        buf = bytearray()
        # Drain pushback first
        if self._pushback:
            take = min(n, len(self._pushback))
            buf.extend(self._pushback[:take])
            del self._pushback[:take]
        # Read the rest from the socket (no timeout — see __init__)
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except (socket.timeout, TimeoutError, OSError):
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def recv_frame(self):
        # _ws_read_frame now returns (fin, rsv1, opcode, payload). We never
        # negotiate permessage-deflate with the upstream on this path (the
        # Sec-WebSocket-Extensions header is stripped from the outbound
        # handshake), so rsv1 will always be False here in practice — but we
        # pass it through verbatim so the v2 pump can make its own decision.
        #
        # Inline frame reading that drains self._pushback first: we can't
        # delegate to _ws_read_frame(self._sock) because that would bypass
        # the pushback buffer and lose pipelined bytes from the handshake.
        try:
            h = self._recv_exact_pushback(2)
            if h is None:
                return None
            b0, b1  = h
            fin     = bool(b0 & 0x80)
            rsv1    = bool(b0 & 0x40)
            opcode  = b0 & 0x0F
            masked  = bool(b1 & 0x80)
            pay_len = b1 & 0x7F
            if pay_len == 126:
                l2 = self._recv_exact_pushback(2)
                if l2 is None: return None
                pay_len = struct.unpack("!H", l2)[0]
            elif pay_len == 127:
                l8 = self._recv_exact_pushback(8)
                if l8 is None: return None
                pay_len = struct.unpack("!Q", l8)[0]
            mask_key = b""
            if masked:
                mask_key = self._recv_exact_pushback(4)
                if mask_key is None: return None
            if pay_len > _s2l_state.WS_MAX_MSG_BYTES:
                _s2l_state.log(f"WS upstream frame too large ({pay_len} bytes, max {_s2l_state.WS_MAX_MSG_BYTES}) — dropping", "WARN")
                return None
            payload = self._recv_exact_pushback(pay_len)
            if payload is None:
                return None
            if masked:
                payload = _s2l_state._ws_unmask(payload, mask_key)
            return fin, rsv1, opcode, payload
        except Exception:
            return None

    def send_frame(self, opcode: int, payload: bytes) -> bool:
        # Serialize sends to prevent frame interleaving when the keepalive
        # thread and the relay thread send concurrently. recv stays lock-free.
        with self._send_lock:
            try:
                self._sock.sendall(_s2l_state._ws_make_frame(opcode, payload, mask=True))
                return True
            except Exception:
                return False

    def close(self):
        try: self._sock.close()
        except Exception: pass
