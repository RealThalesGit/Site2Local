"""Pinned outbound connections shared by HTTP, WebSockets and raw tunnels.

HTTP backends use an authenticated loopback proxy so libcurl and urllib3
cannot independently re-resolve a checked hostname (DNS rebinding).
"""
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import secrets
import select
import socket
import threading
import time
from urllib.parse import urlsplit

from .. import Runtime as _s2l_state

_lock = threading.Lock()
_proxy_url = None
_proxy_auth = "Basic " + base64.b64encode(("s2l:" + secrets.token_hex(32)).encode()).decode()


def _outbound_addresses(host, port, socktype=socket.SOCK_STREAM):
    host = host.strip("[]").lower().rstrip(".")
    if not host or "%" in host or not 1 <= int(port) <= 65535:
        raise PermissionError("Invalid outbound address")
    authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    allowed = authority in _s2l_state.OUTBOUND_PRIVATE_ALLOWLIST
    addresses = socket.getaddrinfo(host, port, type=socktype)
    if not addresses:
        raise PermissionError("No outbound address")
    for _, _, _, _, address in addresses:
        ip = ipaddress.ip_address(address[0])
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
            ip = ip.ipv4_mapped
        embedded = getattr(ip, "sixtofour", None)
        if not allowed and (not ip.is_global or ip.is_multicast or ip.is_reserved
                            or getattr(ip, "is_site_local", False)
                            or (embedded is not None and (not embedded.is_global or embedded.is_multicast))):
            raise PermissionError("Private or non-global outbound address blocked")
    return addresses


def _connect_outbound(host, port, timeout=15, use_udp=False):
    kind = socket.SOCK_DGRAM if use_udp else socket.SOCK_STREAM
    addresses = _outbound_addresses(host, port, kind)
    last = None
    for family, socktype, proto, _, address in addresses:
        stream = socket.socket(family, socktype, proto)
        try:
            stream.settimeout(timeout)
            # Connect to the validated numeric sockaddr, never to the hostname.
            stream.connect(address)
            return stream
        except OSError as exc:
            last = exc
            stream.close()
    raise last or ConnectionError("No outbound address")


class _OutboundProxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    rbufsize = 0  # Do not read tunnel/body bytes ahead of the relay.

    def log_message(self, *args):
        pass

    def handle(self):
        self.connection.settimeout(30)
        super().handle()

    def _forward(self, tunnel=False):
        if not secrets.compare_digest(self.headers.get("Proxy-Authorization", ""), _proxy_auth):
            self.send_error(407)
            return
        try:
            parsed = urlsplit("//" + self.path if tunnel else self.path)
            if parsed.username or parsed.password or (not tunnel and parsed.scheme != "http"):
                raise PermissionError("Unsupported proxy target")
            host, port = parsed.hostname, parsed.port or (443 if tunnel else 80)
            upstream = _connect_outbound(host or "", port, timeout=15)
        except (OSError, ValueError, TypeError):
            self.send_error(403, "Outbound target denied or unavailable")
            return
        with upstream:
            if tunnel:
                self.connection.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            else:
                path = parsed.path or "/"
                if parsed.query:
                    path += "?" + parsed.query
                host_header = f"[{host}]" if ":" in host else host
                if port != 80:
                    host_header += f":{port}"
                connection = "Upgrade" if self.headers.get("Upgrade", "").lower() == "websocket" else "close"
                lines = [f"{self.command} {path} HTTP/1.1", f"Host: {host_header}", f"Connection: {connection}"]
                for key, value in self.headers.items():
                    if key.lower() not in ("host", "connection", "proxy-authorization", "proxy-connection"):
                        lines.append(f"{key}: {value}")
                upstream.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1"))
            peers = [self.connection, upstream]
            last = time.monotonic()
            while peers and time.monotonic() - last < _s2l_state.TUNNEL_TIMEOUT:
                ready, _, _ = select.select(peers, [], [], 1)
                for source in ready:
                    try:
                        data = source.recv(65536)
                        if not data:
                            return
                        destination = upstream if source is self.connection else self.connection
                        destination.sendall(data)
                        last = time.monotonic()
                    except OSError:
                        return
        self.close_connection = True

    def do_CONNECT(self):
        self._forward(tunnel=True)

    do_GET = do_POST = do_HEAD = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = _forward


def _secure_session(session):
    global _proxy_url
    with _lock:
        if _proxy_url is None:
            server = ThreadingHTTPServer(("127.0.0.1", 0), _OutboundProxy)
            server.daemon_threads = True
            threading.Thread(target=server.serve_forever, daemon=True, name="outbound-guard").start()
            credential = _proxy_auth[6:]
            password = base64.b64decode(credential).decode().split(":", 1)[1]
            _proxy_url = f"http://s2l:{password}@127.0.0.1:{server.server_port}"
    session.trust_env = False
    session.proxies = {"http": _proxy_url, "https": _proxy_url, "ws": _proxy_url, "wss": _proxy_url}
    # libcurl otherwise honors NO_PROXY even when using an explicit proxy.
    if hasattr(session, "curl_options"):
        from curl_cffi.const import CurlOpt
        session.curl_options[CurlOpt.NOPROXY] = ""
    return session
