"""Site2Local: tunnels."""
from __future__ import annotations

import socket
import threading
import time

from .. import Runtime as _s2l_state


def _pump_tcp_over_ws(client_sock, upstream_sock, label: str,
                      use_udp: bool = False) -> None:
    """Bridge a WebSocket (browser) to a raw TCP/UDP socket (upstream).

    Browser→upstream: parse WS frames, write payload to the socket.
    Upstream→browser: read from the socket, wrap in WS binary frames.

    use_udp: when True, the upstream socket is a SOCK_DGRAM. UDP is
    connectionless — recv() returns b"" for a zero-length datagram, NOT
    on disconnect. Only break on recv() errors or timeout for UDP; break
    on empty data (peer closed) for TCP.
    """
    stop = threading.Event()
    tunnel_id = id(client_sock)
    # Shared last-activity timestamp — makes TUNNEL_TIMEOUT (idle teardown)
    # actually enforceable: without it the 5.0s poll timeout below just loops
    # forever on an idle tunnel and the config value is dead code.
    last_activity = [time.time()]
    with _s2l_state._active_tunnels_lock:
        _s2l_state._active_tunnels[tunnel_id] = {"label": label, "started": time.time(),
                                       "bytes_in": 0, "bytes_out": 0}

    def _ws_to_tcp():
        """Read WS frames from browser, write payload to upstream socket."""
        try:
            while not stop.is_set():
                frame = _s2l_state._ws_read_frame(client_sock)
                if frame is None:
                    break
                _fin, _rsv1, opcode, payload = frame   # _fin/_rsv1 ignored (raw tunnel, no deflate)
                if opcode == _s2l_state._WS_OPCODE_CLOSE:
                    break
                if opcode in (_s2l_state._WS_OPCODE_TEXT, _s2l_state._WS_OPCODE_BINARY, _s2l_state._WS_OPCODE_CONTINUATION):
                    if payload:
                        upstream_sock.sendall(payload)
                        last_activity[0] = time.time()
                        with _s2l_state._active_tunnels_lock:
                            if tunnel_id in _s2l_state._active_tunnels:
                                _s2l_state._active_tunnels[tunnel_id]["bytes_in"] += len(payload)
                elif opcode == _s2l_state._WS_OPCODE_PING:
                    # RFC 6455 §5.5.2: server MUST respond to PING with a PONG.
                    # Browsers send keepalive PINGs on long-lived tunnels —
                    # without a PONG response they tear down the connection.
                    try:
                        client_sock.sendall(_s2l_state._ws_make_frame(_s2l_state._WS_OPCODE_PONG, payload))
                    except Exception:
                        break
                # PONG (opcode 10): unsolicited from browser, ignore.
        except Exception:
            pass
        finally:
            stop.set()

    def _tcp_to_ws():
        """Read from upstream socket, wrap in WS binary frames, send to browser."""
        try:
            # Use a finite timeout so the loop can periodically check `stop`.
            # settimeout(None) blocks forever and the thread never exits when
            # the browser disconnects without an explicit close (NAT timeout,
            # mobile sleep) — especially unreliable for UDP sockets.
            upstream_sock.settimeout(5.0)
            while not stop.is_set():
                try:
                    data = upstream_sock.recv(65536)
                except (socket.timeout, TimeoutError):
                    # Idle poll tick — enforce TUNNEL_TIMEOUT: no traffic in
                    # EITHER direction for that long → tear the tunnel down
                    # (frees the upstream socket + both relay threads).
                    if _s2l_state.TUNNEL_TIMEOUT > 0 and time.time() - last_activity[0] > _s2l_state.TUNNEL_TIMEOUT:
                        _s2l_state.log(f"{label}: idle > {_s2l_state.TUNNEL_TIMEOUT}s — closing tunnel", "INFO")
                        break
                    continue   # check stop, then loop
                except OSError:
                    break
                # TCP: empty recv means peer closed the connection → stop.
                # UDP: connectionless, empty datagram (b"") is valid — skip it
                # but keep the tunnel alive. Only break on error/timeout above.
                if not data and not use_udp:
                    break
                if data:
                    last_activity[0] = time.time()
                    # Send as binary WS frame (server→client, unmasked)
                    try:
                        client_sock.sendall(_s2l_state._ws_make_frame(_s2l_state._WS_OPCODE_BINARY, data))
                    except Exception:
                        break
                    with _s2l_state._active_tunnels_lock:
                        if tunnel_id in _s2l_state._active_tunnels:
                            _s2l_state._active_tunnels[tunnel_id]["bytes_out"] += len(data)
        except Exception:
            pass
        finally:
            stop.set()

    t1 = threading.Thread(target=_ws_to_tcp, daemon=True, name=f"tcp-w2t-{tunnel_id}")
    t2 = threading.Thread(target=_tcp_to_ws, daemon=True, name=f"tcp-t2w-{tunnel_id}")
    t1.start(); t2.start()
    stop.wait()

    _s2l_state.log(f"Tunnel closed: {label}", "INFO")
    try: upstream_sock.close()
    except Exception: pass
    try: client_sock.close()
    except Exception: pass
    # Join threads with timeout so they don't accumulate when the blocking
    # recv/send doesn't unblock immediately on socket close.
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    with _s2l_state._active_tunnels_lock:
        _s2l_state._active_tunnels.pop(tunnel_id, None)
