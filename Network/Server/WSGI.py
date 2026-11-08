"""Site2Local: WSGI server and request handler."""
from __future__ import annotations

from werkzeug.serving import ThreadedWSGIServer
import socket
import threading

from ... import Runtime as _s2l_state

# Slot accounting for the shared request pool. Long-lived connections
# (WebSocket tunnels, SSE streams) hold a pool thread for their entire
# lifetime; with keepalive off and no idle eviction, enough of them would
# starve ALL HTTP serving — the main app and every CDN mini-server share
# the one _http_pool. When every slot is busy, an overflow connection gets
# its own daemon thread instead of queueing behind the pool: unbounded,
# but only ever as many as real client connections, and it can never
# starve. Under normal load (≤ MAX_HTTP_THREADS concurrent requests)
# behavior is identical to the pure pool.
_pool_slots = threading.BoundedSemaphore(_s2l_state.MAX_HTTP_THREADS)


class _PooledWSGIServer(ThreadedWSGIServer):
    """ThreadedWSGIServer that uses a bounded thread pool instead of spawning
    a new thread per request. Prevents 'can't start new thread' under load.

    Also upgrades IPv4-only 0.0.0.0 binds to dual-stack IPv6 (:: with
    IPV6_V6ONLY=0) so the server is reachable at BOTH 127.0.0.1 and ::1.
    Many systems (glibc on Linux, macOS) resolve `localhost` to ::1 first;
    an IPv4-only bind makes the proxy unreachable at http://localhost:PORT,
    which is the canonical URL advertised in the banner and the web viewer.
    """
    def server_bind(self):
        # Swap the pre-made IPv4 socket for a dual-stack IPv6 one when the
        # configured bind is 0.0.0.0 (the default). This makes the listener
        # accept both IPv4-mapped and native IPv6 connections on one socket.
        try:
            host = self.server_address[0] if isinstance(self.server_address, tuple) else ""
            if self.socket.family == socket.AF_INET and host in ("0.0.0.0", ""):
                try:
                    self.socket.close()
                except OSError:
                    pass
                v6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                # IPV6_V6ONLY=0 is what turns an IPv6 socket into dual-stack.
                # On every Linux kernel >= 2.4 this is supported; the try/except
                # guards exotic platforms where the option is missing.
                try:
                    v6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
                except (OSError, AttributeError):
                    pass
                try:
                    v6.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                except OSError:
                    pass
                self.socket = v6
                self.address_family = socket.AF_INET6
                self.server_address = ("::", self.server_address[1])
        except Exception:
            # Any failure here falls through to werkzeug's default IPv4 bind.
            pass
        super().server_bind()

    def process_request(self, request, client_address):
        if _pool_slots.acquire(blocking=False):
            def _pooled():
                try:
                    self.process_request_thread(request, client_address)
                finally:
                    _pool_slots.release()
            _s2l_state._http_pool.submit(_pooled)
        else:
            # Pool saturated (likely by long-lived WS/SSE holds) — serve
            # this connection on its own thread rather than let it queue
            # indefinitely behind them.
            threading.Thread(target=self.process_request_thread,
                             args=(request, client_address),
                             daemon=True, name="http-overflow").start()
    def server_close(self):
        # Don't shut down the shared pool on one server's close — other CDN
        # mini-servers may still be using it. Pool is cleaned up at exit.
        super().server_close()
