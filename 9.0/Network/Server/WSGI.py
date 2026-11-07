"""Site2Local: WSGI server and request handler."""
from __future__ import annotations

from urllib.parse import urlparse
from werkzeug.serving import ThreadedWSGIServer
from werkzeug.serving import WSGIRequestHandler
import socket
import time

from ... import Runtime as _s2l_state


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
        _s2l_state._http_pool.submit(self.process_request_thread, request, client_address)
    def server_close(self):
        # Don't shut down the shared pool on one server's close — other CDN
        # mini-servers may still be using it. Pool is cleaned up at exit.
        super().server_close()
