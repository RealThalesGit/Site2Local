"""Browser↔upstream WebSocket tunnel pump."""
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

from .. import Runtime as _s2l_state


class _WSTunnel:
    """Stateful wrapper around a browser↔upstream WS tunnel.

    Tracks per-tunnel deflate contexts, ping/pong keepalive, reconnect
    attempts, and message accounting. Used by _pump_ws_frames_v2 below.
    """
    def __init__(self, client_sock, srv, ws_url: str, use_deflate: bool):
        self.client_sock = client_sock
        self.srv         = srv
        self.ws_url      = ws_url
        self.use_deflate = use_deflate
        self.tunnel_id   = id(self)
        self.stop        = threading.Event()
        self.last_pong   = time.time()
        self.msgs_in     = 0
        self.msgs_out    = 0
        self.bytes_in    = 0
        self.bytes_out   = 0
        # True once the BROWSER leg has ended (closed socket, CLOSE frame, or
        # a send failure). When False, an upstream-only drop is eligible for
        # transparent reconnect (WS_AUTO_RECONNECT) — the browser socket is
        # kept open so the client never sees the upstream blip.
        self.client_gone = False
        # The single browser→upstream reader thread. Lives on the tunnel (not
        # the pump call frame) so reconnect rounds can verify it's still alive
        # and restart it if it gave up during a long upstream outage.
        self.c2s_thread = None
        # Bookkeeping for the deflate direction flag — RFC 7692 says the
        # RSV1 bit on the FIRST frame of a message indicates compression.
        # Continuation frames never carry it.
        self._client_deflate_seen = False
        self._srv_deflate_seen    = False

    def close(self, keep_client: bool = False):
        """Close both sides and release deflate contexts.

        keep_client=True leaves the BROWSER socket (and the permessage-deflate
        contexts — the browser's compressor state is per-connection) alive:
        an upstream-only drop with reconnect pending reuses them in the next
        pump round. Closing them would make the transparent reconnect probe
        hit a dead fd and would corrupt every subsequent compressed frame.
        """
        self.stop.set()
        if not keep_client:
            try: self.client_sock.close()
            except Exception: pass
            _s2l_state._ws_deflate_cleanup(self.tunnel_id)
        try: self.srv.close()
        except Exception: pass


def _ws_keepalive_loop(tunnel: "_WSTunnel") -> None:
    """Send periodic pings to the upstream server. If pong doesn't arrive
    within WS_PONG_TIMEOUT seconds, declare the tunnel dead and tear it down.

    Tracks the timestamp of the LAST PING SENT separately from last_pong.
    After sending a ping, wait WS_PONG_TIMEOUT seconds for a pong. If
    last_pong hasn't advanced past the ping-send time by then, declare the
    tunnel dead. This correctly handles both cases:
      - Upstream is alive → pong arrives within milliseconds → last_pong
        advances → check passes → next ping cycle.
      - Upstream is dead → no pong ever → last_pong stays at its last
        value → check fires after the window → tunnel torn down.

    Guard: this function must NEVER be called with WS_PING_INTERVAL <= 0.
    The caller (_pump_ws_frames_v2) already guards against that, but we
    double-check here to prevent a tight CPU spin: Event.wait(0) returns
    immediately (False), which would turn the while-loop into a 100% CPU
    busy loop. Bail out safely instead.
    """
    if _s2l_state.WS_PING_INTERVAL <= 0:
        return   # keepalive disabled — should never be called, but guard anyway
    _pong_window = max(5.0, min(float(_s2l_state.WS_PONG_TIMEOUT), float(_s2l_state.WS_PING_INTERVAL)))
    _last_ping_sent = 0.0   # timestamp of the most recent ping we sent
    while not tunnel.stop.wait(_s2l_state.WS_PING_INTERVAL):
        if tunnel.stop.is_set():
            return
        try:
            # Send a WS PING frame to upstream — the upstream responds with PONG.
            ok = tunnel.srv.send_frame(_s2l_state._WS_OPCODE_PING, os.urandom(4))
            if not ok:
                _s2l_state.log(f"WS keepalive: upstream send failed: {tunnel.ws_url}", "DEBUG")
                tunnel.stop.set()
                return
            _last_ping_sent = time.time()
            # Wait for the pong window to elapse. If the tunnel is closed
            # externally during this wait, exit cleanly.
            if tunnel.stop.wait(_pong_window):
                return   # tunnel was closed while we were waiting
            if tunnel.stop.is_set():
                return
            # If no pong arrived since we sent the ping (i.e. last_pong is
            # still older than _last_ping_sent), declare the tunnel dead.
            # The 0.5s grace absorbs thread-scheduling jitter on busy systems.
            if tunnel.last_pong < _last_ping_sent - 0.5:
                _s2l_state.log(f"WS keepalive: pong timeout: {tunnel.ws_url}", "WARN")
                tunnel.stop.set()
                return
        except Exception as e:
            _s2l_state.log(f"WS keepalive error: {_s2l_state._short_exc(e)}", "DEBUG")
            tunnel.stop.set()
            return


def _pump_ws_frames_v2(client_sock, srv, ws_url: str, use_deflate: bool = False,
                       tunnel: "_WSTunnel | None" = None) -> "_WSTunnel":
    """v2 frame pump: bidirectional relay with ping/pong keepalive, deflate,
    message hooks, and auto-reconnect.

    `tunnel` may be passed to RE-USE an existing _WSTunnel after a transparent
    upstream reconnect (WS_AUTO_RECONNECT): the same browser socket, counters
    and permessage-deflate contexts continue — the browser's deflate context
    is per-CONNECTION, so resetting it mid-session would corrupt every
    subsequent frame. Returns the tunnel so the reconnect loop can reuse it.

    Block-until-done semantics: returns when either leg ends (or keepalive
    declares the upstream dead). Does NOT close the sockets when the drop was
    upstream-only and WS_AUTO_RECONNECT is enabled — the caller decides.
    """
    new_tunnel = tunnel is None
    if new_tunnel:
        tunnel = _s2l_state._WSTunnel(client_sock, srv, ws_url, use_deflate)
        if use_deflate:
            _s2l_state._ws_deflate_init(tunnel.tunnel_id)
        _s2l_state.log(f"WS tunnel opened → {ws_url}  (deflate={'on' if use_deflate else 'off'}, "
            f"ping={_s2l_state.WS_PING_INTERVAL}s)", "INFO")
    else:
        # Reconnect round: swap in the fresh upstream, revive the pump.
        tunnel.srv = srv
        tunnel.last_pong = time.time()
        tunnel.stop.clear()
        _s2l_state.log(f"WS tunnel resumed → {ws_url}", "INFO")

    # Start keepalive thread (fresh round each reconnect — old one exited when
    # the upstream died; joining it here is unnecessary, it's daemon + short).
    keepalive_t = None
    if _s2l_state.WS_PING_INTERVAL > 0:
        keepalive_t = threading.Thread(
            target=_s2l_state._ws_keepalive_loop, args=(tunnel,),
            daemon=True, name=f"ws-keepalive-{tunnel.tunnel_id}",
        )
        keepalive_t.start()

    def _fwd_client_to_srv():
        """Browser → upstream server.

        FIXED (zlib/permessage-deflate): uses the actual RSV1 bit from the
        browser's frame to decide whether to inflate, instead of blindly
        assuming every frame is compressed when use_deflate=True. Per RFC 7692
        §6.1, RSV1 is only set on the FIRST frame of a message; continuation
        frames never carry it. We forward the decompressed message to the
        upstream WITHOUT RSV1 (we never negotiated permessage-deflate with
        the upstream, so it must receive plain frames).
        """
        frag_opcode = None
        frag_buf = bytearray()
        frag_compressed = False
        while not tunnel.stop.is_set():
            frame = _s2l_state._ws_read_frame(client_sock)
            if frame is None:
                tunnel.client_gone = True   # browser closed / socket error
                break
            fin, rsv1, opcode, payload = frame
            if _s2l_state.WS_LOG_FRAMES:
                _op_name = _s2l_state._WS_OP_NAMES.get(opcode, f"?{opcode}")
                _s2l_state.log(f"WS→ {ws_url} fin={fin} rsv1={int(rsv1)} op={_op_name} len={len(payload)}", "DEBUG")
            if opcode == _s2l_state._WS_OPCODE_CLOSE:
                # RFC 6455 §5.5.1: echo the CLOSE back so the browser's close
                # handshake completes cleanly, then forward it upstream.
                tunnel.client_gone = True
                try:
                    client_sock.sendall(_s2l_state._ws_make_frame(_s2l_state._WS_OPCODE_CLOSE, payload))
                except Exception:
                    pass
                tunnel.srv.send_frame(_s2l_state._WS_OPCODE_CLOSE, payload)
                break
            if opcode in (_s2l_state._WS_OPCODE_CONTINUATION, _s2l_state._WS_OPCODE_TEXT, _s2l_state._WS_OPCODE_BINARY):
                # RFC 7692 §6.1: RSV1 on the FIRST frame of a message marks
                # the entire message as compressed. Continuation frames never
                # carry RSV1 — the compressed/not-compressed decision is made
                # once, at the first frame, and sticks for the whole message.
                if opcode != _s2l_state._WS_OPCODE_CONTINUATION:
                    frag_opcode = opcode
                    frag_buf = bytearray(payload)
                    frag_compressed = use_deflate and rsv1
                else:
                    frag_buf += payload
                # Cap reassembly — WS_MAX_MSG_BYTES is enforced per-frame in
                # _ws_read_frame, but an endless fin=0 continuation stream
                # would otherwise grow frag_buf without bound (memory DoS).
                if len(frag_buf) > _s2l_state.WS_MAX_MSG_BYTES:
                    _s2l_state.log(f"WS→ reassembled message exceeds {_s2l_state._fmt_size(_s2l_state.WS_MAX_MSG_BYTES)} "
                        f"— dropping tunnel {ws_url}", "WARN")
                    tunnel.client_gone = True
                    break
                if not fin:
                    continue
                msg = bytes(frag_buf)
                if frag_compressed:
                    msg = _s2l_state._ws_inflate_msg(tunnel.tunnel_id, msg)
                _s2l_state._run_ws_msg_hooks("in", frag_opcode or opcode, msg, tunnel.tunnel_id)
                # Capture the original frame for the GUI before applying any
                # replacement, so the When panel can retain the exact source.
                # Wrapped in try/except so a hook error NEVER kills the tunnel —
                # the site stays functional even if the hook has a bad pattern
                # or the GUI queue is full.
                original_msg = msg
                try:
                    msg = _s2l_state._apply_gui_ws_hooks("in", frag_opcode or opcode, msg,
                                              ws_url, tunnel.tunnel_id)
                except Exception as _hk:
                    _s2l_state.log(f"WS hook error (in): {_s2l_state._short_exc(_hk)}", "WARN")
                try:
                    _s2l_state._gui_ws_push("in", frag_opcode or opcode, original_msg,
                                 ws_url, tunnel.tunnel_id)
                except Exception as _push_err:
                    _s2l_state.log(f"WS GUI capture error (in): {_s2l_state._short_exc(_push_err)}", "WARN")
                _s2l_state._maybe_capture_ws("in", frag_opcode or opcode, msg, ws_url, tunnel.tunnel_id)
                tunnel.msgs_in  += 1
                tunnel.bytes_in += len(msg)
                # Forward to upstream uncompressed (no RSV1) — we did not
                # negotiate permessage-deflate with the upstream, so it would
                # choke on a frame with RSV1 set.
                ok = tunnel.srv.send_frame(frag_opcode or opcode, msg)
                frag_opcode, frag_buf, frag_compressed = None, bytearray(), False
                if not ok:
                    if not (_s2l_state.WS_AUTO_RECONNECT and not tunnel.client_gone):
                        break
                    # Upstream dropped but transparent reconnect is enabled —
                    # keep reading the browser. Wait (bounded) for the
                    # reconnect loop in _handle_ws_direct to swap in a fresh
                    # upstream, then resume forwarding. Frames sent during the
                    # outage window are relayed once the new upstream is in
                    # place; giving up only if it never comes back.
                    _dead_srv = tunnel.srv
                    _waited = 0.0
                    while (_waited < 15.0 and not tunnel.client_gone
                           and tunnel.srv is _dead_srv):
                        time.sleep(0.25)
                        _waited += 0.25
                    if tunnel.srv is _dead_srv:
                        break   # reconnect never happened — end this relay
                    continue
            elif opcode == _s2l_state._WS_OPCODE_PING:
                # Browser pings us → respond with pong directly
                try:
                    client_sock.sendall(_s2l_state._ws_make_frame(_s2l_state._WS_OPCODE_PONG, payload))
                except Exception:
                    tunnel.client_gone = True
                    break
            elif opcode == _s2l_state._WS_OPCODE_PONG:
                # Browser PONG — unsolicited (we never PING the browser).
                # Do NOT update tunnel.last_pong here: that field tracks
                # UPSTREAM PONGs (responses to our keepalive pings sent via
                # _ws_keepalive_loop). A browser PONG has nothing to do with
                # upstream health — updating last_pong here would mask a dead
                # upstream and prevent the keepalive thread from detecting it.
                pass
        tunnel.stop.set()

    def _fwd_srv_to_client():
        """Upstream server → browser.

        Upstream frames are always plain (rsv1=False in practice — we never
        negotiate permessage-deflate with the upstream, Sec-WebSocket-Extensions
        is stripped from the outbound handshake). We never inflate. If the
        browser negotiated permessage-deflate, we re-compress the message and
        SET RSV1 on the outgoing frame so the browser knows to inflate.
        """
        frag_opcode = None
        frag_buf = bytearray()
        while not tunnel.stop.is_set():
            frame = srv.recv_frame()
            if frame is None:
                break
            fin, rsv1, opcode, payload = frame
            if _s2l_state.WS_LOG_FRAMES:
                _op_name = _s2l_state._WS_OP_NAMES.get(opcode, f"?{opcode}")
                _s2l_state.log(f"WS← {ws_url} fin={fin} rsv1={int(rsv1)} op={_op_name} len={len(payload)}", "DEBUG")
            if opcode == _s2l_state._WS_OPCODE_CLOSE:
                try:
                    client_sock.sendall(_s2l_state._ws_make_frame(_s2l_state._WS_OPCODE_CLOSE, payload))
                except Exception:
                    pass
                break
            if opcode == _s2l_state._WS_OPCODE_PONG:
                tunnel.last_pong = time.time()
                continue
            if opcode == _s2l_state._WS_OPCODE_PING:
                # Upstream pings us → respond with pong to upstream
                if not srv.send_frame(_s2l_state._WS_OPCODE_PONG, payload):
                    break
                continue
            if opcode in (_s2l_state._WS_OPCODE_CONTINUATION, _s2l_state._WS_OPCODE_TEXT, _s2l_state._WS_OPCODE_BINARY):
                if opcode != _s2l_state._WS_OPCODE_CONTINUATION:
                    frag_opcode = opcode
                    frag_buf = bytearray(payload)
                else:
                    frag_buf += payload
                # Same reassembly cap as the c2s direction (memory DoS guard).
                if len(frag_buf) > _s2l_state.WS_MAX_MSG_BYTES:
                    _s2l_state.log(f"WS← reassembled message exceeds {_s2l_state._fmt_size(_s2l_state.WS_MAX_MSG_BYTES)} "
                        f"— dropping tunnel {ws_url}", "WARN")
                    break
                if not fin:
                    continue
                msg = bytes(frag_buf)
                # Upstream never compresses (we didn't negotiate), so there is
                # nothing to inflate here. (If curl_cffi DID auto-negotiate
                # permessage-deflate upstream-side, it already decompressed
                # the payload for us — rsv1 is reported False either way, and
                # we must not double-inflate.)
                _s2l_state._run_ws_msg_hooks("out", frag_opcode or opcode, msg, tunnel.tunnel_id)
                # Capture the original frame for the GUI before applying any
                # replacement, so the When panel can retain the exact source.
                # Wrapped in try/except so a hook error NEVER kills the tunnel.
                original_msg = msg
                try:
                    msg = _s2l_state._apply_gui_ws_hooks("out", frag_opcode or opcode, msg,
                                              ws_url, tunnel.tunnel_id)
                except Exception as _hk:
                    _s2l_state.log(f"WS hook error (out): {_s2l_state._short_exc(_hk)}", "WARN")
                try:
                    _s2l_state._gui_ws_push("out", frag_opcode or opcode, original_msg,
                                 ws_url, tunnel.tunnel_id)
                except Exception as _push_err:
                    _s2l_state.log(f"WS GUI capture error (out): {_s2l_state._short_exc(_push_err)}", "WARN")
                _s2l_state._maybe_capture_ws("out", frag_opcode or opcode, msg, ws_url, tunnel.tunnel_id)
                tunnel.msgs_out  += 1
                tunnel.bytes_out += len(msg)
                # Re-compress for the browser leg if it negotiated deflate,
                # and SET RSV1 so the browser actually knows to inflate.
                out_payload = msg
                out_rsv1    = False
                if use_deflate:
                    out_payload = _s2l_state._ws_deflate_msg(tunnel.tunnel_id, msg)
                    out_rsv1    = True
                try:
                    client_sock.sendall(_s2l_state._ws_make_frame(
                        frag_opcode or opcode, out_payload, fin=fin, rsv1=out_rsv1))
                except Exception:
                    break
                frag_opcode, frag_buf = None, bytearray()
        tunnel.stop.set()

    # The browser→upstream relay is the SINGLE reader on the client socket —
    # a second concurrent reader would interleave frames and corrupt the
    # stream. It reads `tunnel.srv` dynamically, so it survives upstream
    # reconnects (tunnel.srv is swapped in-place). It's stored on the tunnel:
    # if it gave up during a long outage, the next round spawns a fresh one.
    if new_tunnel:
        tunnel.c2s_thread = threading.Thread(target=_fwd_client_to_srv,
                                             daemon=True, name="ws-c2s")
        tunnel.c2s_thread.start()
    elif tunnel.c2s_thread is None or not tunnel.c2s_thread.is_alive():
        tunnel.c2s_thread = threading.Thread(target=_fwd_client_to_srv,
                                             daemon=True, name="ws-c2s")
        tunnel.c2s_thread.start()
    t2 = threading.Thread(target=_fwd_srv_to_client, daemon=True, name="ws-s2c")
    t2.start()
    tunnel.stop.wait()

    # Upstream-only drop + reconnect enabled → keep the BROWSER socket open
    # (and the deflate contexts — the browser's compressor state is per-
    # connection) so the reconnect loop in _handle_ws_direct can resume the
    # pump transparently. Any client-side end (CLOSE / socket error / send
    # failure / reassembly cap) tears everything down exactly as before.
    _reconnectable = (_s2l_state.WS_AUTO_RECONNECT and not tunnel.client_gone)
    tunnel.close(keep_client=_reconnectable)
    # Join relay threads with a timeout — they may be blocked in recv() and
    # closing the sockets doesn't always unblock curl_cffi's recv on all
    # platforms. Without this join, daemon threads accumulate (one pair per
    # WS connection) and leak file descriptors / memory. In the reconnectable
    # case the browser reader is intentionally NOT joined — it must survive
    # across rounds; t2 (old upstream reader) is dead by definition.
    t2.join(timeout=3.0)
    if keepalive_t is not None:
        keepalive_t.join(timeout=2.0)
    if not _reconnectable:
        if tunnel.c2s_thread is not None:
            tunnel.c2s_thread.join(timeout=3.0)
        _s2l_state.log(f"WS tunnel closed ← {ws_url}  (in={tunnel.msgs_in}/{_s2l_state._fmt_size(tunnel.bytes_in)}, "
            f"out={tunnel.msgs_out}/{_s2l_state._fmt_size(tunnel.bytes_out)})", "INFO")
    return tunnel


