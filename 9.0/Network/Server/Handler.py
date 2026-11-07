"""Site2Local: WSGI server and request handler."""
from __future__ import annotations

from urllib.parse import urlparse
from werkzeug.serving import ThreadedWSGIServer
from werkzeug.serving import WSGIRequestHandler
import socket
import time

from ... import Runtime as _s2l_state


class _S2LWSGIRequestHandler(WSGIRequestHandler):
    """WSGIRequestHandler with WebSocket upgrade interception.

    Werkzeug 3.x returns 400 Bad Request when the browser sends
    `Connection: Upgrade` — this happens BEFORE the Flask view function is
    called. We override handle_one_request() to detect WS upgrades and
    handle them DIRECTLY, completely bypassing Werkzeug's WSGI pipeline.
    No Flask context needed — we have everything in self.

    THIS IS THE ONLY WS HANDLING PATH. Every server instance in this file
    (main app + every CDN mini-server) is started with
    request_handler=_S2LWSGIRequestHandler, so a request with an Upgrade:
    websocket header NEVER reaches Flask routing — it's fully handled here
    or in _handle_tunnel_direct(). Don't add a second WS code path in a
    Flask view; it will silently never run. If you need a different
    upstream host for the "no /__s2l_*__/ prefix" case (e.g. a new kind of
    mini-server), set `<server>.s2l_target_base` right after make_server(),
    the way _start_cdn_server() does — don't hardcode SITE_URL further down.
    """
    def make_environ(self):
        environ = super().make_environ()
        environ["werkzeug.socket"] = self.connection
        return environ

    def handle_one_request(self):
        """Override to intercept WebSocket upgrades BEFORE WSGI processing."""
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                return
        except Exception:
            return

        # ── WS upgrade detection — BEFORE WSGI ──────────────────────────
        # Werkzeug 3.x 400s on Connection: Upgrade before reaching Flask
        upgrade_hdr = self.headers.get("Upgrade", "").lower().strip()
        if upgrade_hdr == "websocket":
            self._handle_ws_direct()
            return

        # Normal request — use Werkzeug's WSGI path (do_* → run_wsgi)
        mname = 'do_' + self.command
        if not hasattr(self, mname):
            self.send_error(501, f"Unsupported method ({self.command!r})")
            return
        method = getattr(self, mname)
        method()
        self.wfile.flush()

    def _handle_ws_direct(self):
        """Handle WS upgrade directly — NO Flask context needed.

        We have everything we need:
          - self.connection  = raw client socket
          - self.headers     = request headers
          - self.path        = URL path + query string
          - self.rfile       = read stream (for any remaining body data)
        """
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"
            query = parsed.query

            # ── Route matching ────────────────────────────────────────────
            ws_prefix_ext = "/__s2l_ws_ext__/"
            ws_prefix_ext_plain = "/__s2l_ws_ext_plain__/"
            ws_prefix_tcp = "/__s2l_tcp__/"
            ws_prefix_udp = "/__s2l_udp__/"

            if path.startswith(ws_prefix_tcp):
                if not _s2l_state.TCP_TUNNEL:
                    self.connection.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\nTCP tunneling disabled (set TCP_TUNNEL=True)")
                    return
                self._handle_tunnel_direct(path[len(ws_prefix_tcp):], use_udp=False)
                return
            if path.startswith(ws_prefix_udp):
                if not _s2l_state.UDP_TUNNEL:
                    self.connection.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\nUDP tunneling disabled (set UDP_TUNNEL=True)")
                    return
                self._handle_tunnel_direct(path[len(ws_prefix_udp):], use_udp=True)
                return

            # ── Build WS target URL ──────────────────────────────────────
            if path.startswith(ws_prefix_ext) or path.startswith(ws_prefix_ext_plain):
                _plain = path.startswith(ws_prefix_ext_plain)
                wspath = path[len(ws_prefix_ext_plain if _plain else ws_prefix_ext):]
                ws_host = wspath.split("/", 1)[0]
                ws_path_q = wspath[len(ws_host):] or "/"
                if query:
                    ws_path_q += f"?{query}"
                # plain ws:// targets (original page used ws://, not wss://)
                # must be dialed without TLS, keeping their explicit port.
                target = f"{'http' if _plain else 'https'}://{ws_host}{ws_path_q}"
                req_display = f"{ws_prefix_ext_plain if _plain else ws_prefix_ext}{wspath}"
            else:
                # Main host WS, or a CDN mini-server's own host if this
                # request came in on one of those ports (see s2l_target_base,
                # set right after make_server() in _start_cdn_server).
                base = getattr(self.server, "s2l_target_base", None) or _s2l_state.SITE_URL
                target = f"{base.rstrip('/')}{path}"
                if query:
                    target += f"?{query}"
                req_display = path

            # ── Build forward headers ────────────────────────────────
            fwd_hdrs = {}
            for k, v in self.headers.items():
                fwd_hdrs[k] = v

            client_sock = self.connection

            # OFFLINE guard: no upstream to dial. WebSockets can't be served
            # from disk, so fail fast with a distinct 503 instead of hanging
            # for a connection timeout then returning a misleading 502.
            if _s2l_state.OFFLINE:
                try:
                    _offline_body = b"Offline \xe2\x80\x94 no WS upstream"
                    client_sock.sendall(
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_offline_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _offline_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # Parse target for WS URL
            target_parsed = urlparse(target)
            ws_scheme = "wss" if target_parsed.scheme in ("https", "wss") else "ws"
            ws_path = target_parsed.path or "/"
            ws_qs = target_parsed.query
            ws_url = f"{ws_scheme}://{target_parsed.netloc}{ws_path}"
            if ws_qs:
                ws_url += f"?{ws_qs}"

            # WS handshake values
            ws_key = self.headers.get("Sec-WebSocket-Key", "")
            accept = _s2l_state._ws_handshake_accept(ws_key)
            # RFC 6455 §4.2.2: the server must select ONE subprotocol. Echoing
            # the entire comma-separated list back is invalid and strict browsers
            # reject the handshake. Pick the first requested subprotocol.
            proto_req = self.headers.get("Sec-WebSocket-Protocol", "")
            proto_pick = proto_req.split(",")[0].strip() if proto_req else ""
            proto_hdrs = f"Sec-WebSocket-Protocol: {proto_pick}\r\n" if proto_pick else ""

            # Build extra headers for upstream
            extra_fwd = {}
            for k, v in fwd_hdrs.items():
                kl = k.lower()
                if kl in ("host", "upgrade", "connection",
                           "sec-websocket-key", "sec-websocket-version",
                           "sec-websocket-extensions", "accept-encoding"):
                    continue
                extra_fwd[k] = v
            # Origin = MAIN_HOST (what a real browser sends)
            _incoming_origin = next((v for k, v in fwd_hdrs.items() if k.lower() == "origin"), None)
            extra_fwd["Origin"] = _s2l_state._real_origin_for(_incoming_origin)
            if proto_req:
                extra_fwd["Sec-WebSocket-Protocol"] = proto_req

            # ── Connect upstream FIRST ────────────────────────────────────
            _s2l_state.log(f"WS-direct: {req_display} → {ws_url}", "WS")
            try:
                srv = _s2l_state._ws_connect_upstream(ws_url, extra_fwd)
            except Exception as exc:
                _s2l_state.log(f"WS upstream failed: {ws_url} — {_s2l_state._short_exc(exc)}", "WARN")
                try:
                    err_msg = str(exc)[:200].encode("utf-8", "replace")
                    _502_body = b"WS upstream: " + err_msg
                    client_sock.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_502_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _502_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # ── Send 101 to browser ──────────────────────────────────────
            # Decide permessage-deflate BEFORE building the handshake: if we're
            # going to run the pump in deflate mode, the browser MUST see
            # Sec-WebSocket-Extensions: permessage-deflate in THIS (the only)
            # 101 response, or it'll keep sending/expecting plain frames while
            # we deflate/inflate on our end — corrupting every frame. There is
            # exactly one handshake response; a second one isn't valid HTTP.
            #
            # zlib-stream guard. Some endpoints (notably real-time
            # gateways via ?compress=zlib-stream) layer their OWN zlib
            # compression at the application level — it is NOT permessage-
            # deflate (RFC 7692), the frames do NOT set RSV1, and the bytes
            # are zlib-wrapped (2-byte 0x78 0x9C header) rather than raw
            # deflate. If we accept permessage-deflate from the browser on
            # such a tunnel, the v2 pump will try to inflate already-
            # decompressed/plain bytes, fail, re-compress them as raw deflate,
            # and ship them to the browser without RSV1 — the browser's own
            # zlib-stream decompressor then chokes on the double-mangled
            # payload with "zlib error, -3, incorrect header check" and the
            # gateway dies on a reconnect loop. Force permessage-deflate OFF
            # whenever the upstream URL advertises a non-RFC-7692 compression
            # scheme of its own.
            _ext_hdr = self.headers.get("Sec-WebSocket-Extensions", "")
            _upstream_has_own_compression = (
                "compress=zlib-stream" in ws_qs
                or "compress=" in ws_qs        # catch any future variant
            )
            if _upstream_has_own_compression and _s2l_state.WS_DEFLATE:
                _s2l_state.log(f"WS: upstream advertises its own compression "
                    f"({ws_qs!r}) — disabling permessage-deflate to avoid "
                    f"conflict (zlib-stream != RFC 7692)", "INFO")
            _browser_wants_deflate = (
                "permessage-deflate" in _ext_hdr
                and _s2l_state.WS_DEFLATE
                and not _upstream_has_own_compression
            )
            deflate_hdr = "Sec-WebSocket-Extensions: permessage-deflate\r\n" if _browser_wants_deflate else ""
            handshake = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                f"{proto_hdrs}"
                f"{deflate_hdr}"
                "\r\n"
            )
            try:
                client_sock.sendall(handshake.encode())
            except Exception as exc:
                _s2l_state.log(f"WS handshake send failed: {exc}", "WARN")
                try: srv.close()
                except Exception: pass
                self.close_connection = True
                return

            # ── Pump frames ──────────────────────────────────────────────
            # WS_AUTO_RECONNECT: if the upstream WS drops, try to re-establish
            # the connection transparently so the browser doesn't see a broken
            # tunnel. The pump keeps the browser socket (and deflate contexts)
            # alive on an upstream-only drop; this loop probes it and re-dials
            # up to 3 times. A browser-side end (CLOSE / socket error) never
            # triggers a reconnect.
            #
            # We always use _pump_ws_frames_v2 — it's a strict superset of the
            # old v1: same frame relay, plus optional keepalive/deflate/log/
            # hook/gui integration. When all of those are disabled, v2 behaves
            # exactly like v1 did, so there's no reason to keep two code
            # paths. The old v1 _pump_ws_frames has been removed to keep the
            # WS pipeline in one place.
            _ws_attempts = 0
            _ws_max_reconnects = 3 if _s2l_state.WS_AUTO_RECONNECT else 0
            _ws_tunnel = None
            while True:
                _ws_tunnel = _s2l_state._pump_ws_frames_v2(
                    client_sock, srv, ws_url,
                    use_deflate=_browser_wants_deflate, tunnel=_ws_tunnel)
                if _ws_max_reconnects <= 0 or _ws_tunnel.client_gone:
                    break
                # Upstream-only drop. Check the browser side is still open —
                # a closed client_sock means the browser navigated away or
                # closed the tab; no point reconnecting. (The pump no longer
                # closes it in this path, so the probe is meaningful.)
                # FIX: sendall(b"") is a no-op on most platforms (returns 0
                # immediately without calling send(2)), so it can't detect a
                # closed browser socket. Use a non-blocking MSG_PEEK recv
                # instead — returns b"" if the browser closed, raises on error.
                try:
                    client_sock.setblocking(False)
                    _probe = client_sock.recv(1, socket.MSG_PEEK)
                    client_sock.setblocking(True)
                    if not _probe:
                        break   # browser closed the connection
                except (BlockingIOError, InterruptedError):
                    # No data ready → socket is still open → restore blocking
                    client_sock.setblocking(True)
                except Exception:
                    break   # probe failed → socket is dead
                if _ws_attempts >= _ws_max_reconnects:
                    _s2l_state.log(f"WS upstream dropped — reconnect limit reached "
                        f"({_ws_max_reconnects}): {ws_url}", "WS")
                    break
                _ws_attempts += 1
                _s2l_state.log(f"WS upstream dropped — reconnecting (attempt {_ws_attempts}/{_ws_max_reconnects})", "WS")
                time.sleep(0.5)   # brief backoff before re-dialing the upstream
                try:
                    srv = _s2l_state._ws_connect_upstream(ws_url, extra_fwd)
                except Exception as exc:
                    _s2l_state.log(f"WS reconnect failed: {exc}", "WARN")
                    break
            # Give-up path after a reconnectable drop: the pump left the
            # browser socket open — close it and release deflate contexts now.
            # Idempotent when the pump already closed everything.
            if _ws_tunnel is not None:
                _ws_tunnel.close()

            self.close_connection = True

        except Exception as exc:
            _s2l_state.log(f"WS-direct error: {exc}", "ERROR")
            self.close_connection = True

    def _handle_tunnel_direct(self, wspath, use_udp=False):
        """Handle TCP/UDP tunnel directly."""
        try:
            # OFFLINE guard: no upstream to dial. Like the WS handler, fail
            # fast with 503 instead of hanging on a connection timeout.
            if _s2l_state.OFFLINE:
                try:
                    _offline_body = b"Offline \xe2\x80\x94 no tunnel upstream"
                    self.connection.sendall(
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_offline_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _offline_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            host_port = wspath.split("/", 1)[0].split("?", 1)[0]
            if ":" not in host_port:
                self.connection.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return
            host, _, port_s = host_port.rpartition(":")
            port = int(port_s)
            if not (1 <= port <= 65535):
                self.connection.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return

            client_sock = self.connection
            proto = "UDP" if use_udp else "TCP"
            label = f"{proto} {host}:{port}"
            _s2l_state.log(f"Tunnel opened: {label}", "TUNNEL")

            # Connect upstream FIRST, then send 101. Sending 101 before
            # connecting leaves the browser thinking the tunnel is established
            # when it isn't — if create_connection fails, the browser hangs.
            try:
                if use_udp:
                    # Detect IPv6 literal to pick the right socket family.
                    # Hardcoding AF_INET breaks /__s2l_udp__/[::1]:53 etc.
                    _fam = socket.AF_INET6 if ":" in host else socket.AF_INET
                    upstream_sock = socket.socket(_fam, socket.SOCK_DGRAM)
                    upstream_sock.settimeout(_s2l_state.TUNNEL_TIMEOUT)
                    upstream_sock.connect((host, port))
                else:
                    upstream_sock = socket.create_connection((host, port), timeout=_s2l_state.TIMEOUT_CONN)
                    upstream_sock.settimeout(_s2l_state.TUNNEL_TIMEOUT)
            except Exception as exc:
                err_msg = str(exc)[:200].encode("utf-8", "replace")
                try:
                    _502_body = b"Tunnel upstream: " + err_msg
                    self.connection.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_502_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _502_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # Upstream connected — NOW tell the browser the tunnel is open.
            ws_key = self.headers.get("Sec-WebSocket-Key", "")
            accept = _s2l_state._ws_handshake_accept(ws_key)
            handshake = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "\r\n"
            )
            client_sock.sendall(handshake.encode())

            _s2l_state._pump_tcp_over_ws(client_sock, upstream_sock, label, use_udp=use_udp)
            self.close_connection = True
        except Exception as exc:
            _s2l_state.log(f"Tunnel error: {exc}", "ERROR")
            self.close_connection = True
