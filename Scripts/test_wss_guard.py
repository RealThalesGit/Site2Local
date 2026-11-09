"""Local WSS handshake regression (cryptography is optional for test certs)."""
import base64
import datetime
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import ssl
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
import unittest

from test_cached_mime import helpers


class WssGuardTests(unittest.TestCase):
    def test_raw_and_curl_wss_with_explicit_local_exception(self):
        try:
            from cryptography import x509
            from cryptography.x509.oid import NameOID
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
        except ImportError:
            self.skipTest("cryptography is needed to generate a local test certificate")
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(101)
                self.send_header("Upgrade", "websocket")
                self.send_header("Connection", "Upgrade")
                key = self.headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                self.send_header("Sec-WebSocket-Accept", base64.b64encode(hashlib.sha1(key.encode()).digest()).decode())
                self.end_headers()
                self.close_connection = True
            def log_message(self, *args):
                pass
        with TemporaryDirectory() as directory:
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
            now = datetime.datetime.now(datetime.timezone.utc)
            cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                    .public_key(key.public_key()).serial_number(x509.random_serial_number())
                    .not_valid_before(now - datetime.timedelta(minutes=1))
                    .not_valid_after(now + datetime.timedelta(days=1)).sign(key, hashes.SHA256()))
            root = Path(directory)
            (root / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            (root / "key.pem").write_bytes(key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
            server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(root / "cert.pem", root / "key.pem")
            server.socket = context.wrap_socket(server.socket, server_side=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            state = SimpleNamespace(OUTBOUND_PRIVATE_ALLOWLIST=[f"localhost:{server.server_port}"],
                                    TUNNEL_TIMEOUT=3, TLS_VERIFY=True, _is_ip_literal=lambda host: False)
            outbound = helpers("Network/Outbound.py", state)
            state._connect_outbound = outbound["_connect_outbound"]
            state._strip_browser_cookie = lambda headers: headers
            raw = helpers("WebSockets/Upstream/Raw.py", state)["_RawSocketUpstreamWS"]
            url = f"wss://localhost:{server.server_port}/"
            try:
                with self.assertRaises(ssl.SSLCertVerificationError):
                    raw(url, {})
                state.TLS_VERIFY = False
                connection = raw(url, {})
                connection.close()
                try:
                    from curl_cffi import requests as curl_requests
                except ImportError:
                    return
                with outbound["_secure_session"](curl_requests.Session(verify=False)) as session:
                    connection = session.ws_connect(url, timeout=3)
                    connection.close()
            finally:
                server.shutdown()
                server.server_close()


if __name__ == "__main__":
    unittest.main()
