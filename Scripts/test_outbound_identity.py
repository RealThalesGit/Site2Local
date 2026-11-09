"""Security regressions using only local sockets and deterministic DNS."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask, jsonify
import requests
from test_cached_mime import helpers


class OutboundIdentityTests(unittest.TestCase):
    def test_non_global_dns_answers_and_mixed_records_are_denied(self):
        state = SimpleNamespace(OUTBOUND_PRIVATE_ALLOWLIST=[])
        functions = helpers("Network/Outbound.py", state)
        for address in ("127.0.0.1", "10.0.0.1", "169.254.169.254", "0.0.0.0",
                        "::1", "fc00::1", "::ffff:127.0.0.1", "224.0.0.1",
                        "64:ff9b::a00:1", "fec0::1", "2002:7f00:1::"):
            with self.subTest(address=address):
                records = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 443))]
                with patch("socket.getaddrinfo", return_value=records):
                    with self.assertRaises(PermissionError):
                        functions["_outbound_addresses"]("public-name.example", 443)
        records = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, 443))
                   for a in ("8.8.8.8", "127.0.0.1")]
        with patch("socket.getaddrinfo", return_value=records):
            with self.assertRaises(PermissionError):
                functions["_outbound_addresses"]("mixed.example", 443)

    def test_connection_uses_checked_numeric_address_without_second_dns_lookup(self):
        state = SimpleNamespace(OUTBOUND_PRIVATE_ALLOWLIST=[])
        functions = helpers("Network/Outbound.py", state)
        from unittest.mock import Mock
        stream = Mock()
        records = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]
        with patch("socket.getaddrinfo", return_value=records) as resolve, \
             patch("socket.socket", return_value=stream):
            self.assertIs(functions["_connect_outbound"]("changing.example", 443), stream)
        resolve.assert_called_once()
        stream.connect.assert_called_once_with(("8.8.8.8", 443))

    def test_http_backend_cannot_follow_redirect_to_unapproved_private_host(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/secret")
                    self.end_headers()
                else:
                    self.server.hits.append(self.path)
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(b"allowed")
            def log_message(self, *args):
                pass
        upstream = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        upstream.hits = []
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(upstream.server_close)
        self.addCleanup(upstream.shutdown)
        state = SimpleNamespace(OUTBOUND_PRIVATE_ALLOWLIST=[f"localhost:{upstream.server_port}"], TUNNEL_TIMEOUT=3)
        functions = helpers("Network/Outbound.py", state)
        proxy = ThreadingHTTPServer(("127.0.0.1", 0), functions["_OutboundProxy"])
        proxy.daemon_threads = True
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        import base64
        credentials = base64.b64decode(functions["_proxy_auth"][6:]).decode()
        url = f"http://{credentials}@127.0.0.1:{proxy.server_port}"
        with requests.Session() as session:
            session.trust_env = False
            session.proxies = {"http": url, "https": url}
            response = session.get(f"http://localhost:{upstream.server_port}/ok", timeout=3)
            self.assertEqual(response.content, b"allowed")
            response = session.get(f"http://localhost:{upstream.server_port}/redirect", timeout=3)
            self.assertEqual(response.status_code, 403)
            self.assertNotIn("/secret", upstream.hits)
            with self.assertRaises(requests.exceptions.ProxyError):
                session.get("https://127.0.0.1/", timeout=3)
        # curl_cffi must obey the same policy, even with NO_PROXY=*.
        try:
            from curl_cffi import requests as curl_requests
            from curl_cffi.const import CurlOpt
        except ImportError:
            return
        with patch.dict("os.environ", {"NO_PROXY": "*"}):
            with curl_requests.Session(trust_env=False, proxies={"http": url, "https": url},
                                       curl_options={CurlOpt.NOPROXY: ""}) as session:
                response = session.get(f"http://localhost:{upstream.server_port}/redirect", timeout=3)
                self.assertEqual(response.status_code, 403)
                self.assertNotIn("/secret", upstream.hits)

    def test_browser_sessions_are_distinct_stable_and_not_forwarded(self):
        state = SimpleNamespace(_CLIENT_LOCK=threading.Lock(), _CLIENT_LAST={},
                                _CLIENT_SESSIONS={}, _CLIENT_TTL=300,
                                _make_session=lambda: requests.Session())
        functions = helpers("HTTP/Sessions/Clients.py", state)
        state._client_id = functions["_client_id"]
        app = Flask(__name__)
        app.before_request(functions["_ensure_browser_session"])
        app.after_request(functions["_set_browser_cookie"])
        @app.get("/")
        def identity():
            session = functions["_get_client_session"]()
            response = jsonify(client=state._client_id(), upstream=id(session))
            response.set_cookie("__s2l_browser", "upstream-fixation")
            response.set_cookie("target", "keep")
            return response
        first, second = app.test_client(), app.test_client()
        a = first.get("/")
        b = second.get("/")
        self.assertNotEqual(a.json["client"], b.json["client"])
        self.assertEqual(first.get("/").json, a.json)
        self.assertNotEqual(a.json["upstream"], b.json["upstream"])
        self.assertTrue(any("HttpOnly" in cookie and "__s2l_browser=" in cookie
                            for cookie in a.headers.getlist("Set-Cookie")))
        signed = first.get_cookie("__s2l_browser").value
        self.assertEqual(state._client_id(signed), a.json["client"])
        self.assertNotEqual(state._client_id(signed + "tampered"), a.json["client"])
        self.assertNotEqual(state._client_id(signed.split(".")[0] + ".\u00e9"), a.json["client"])
        self.assertEqual(first.get_cookie("__s2l_browser").value, signed)
        with app.test_request_context(headers={"Cookie": f"__s2l_browser={signed}; target=keep"}):
            self.assertEqual(functions["_browser_cookies"](), {"target": "keep"})
            filtered = functions["_strip_browser_cookie"]({"Cookie": f"__s2l_browser={signed}; target=keep"})
            self.assertEqual(filtered, {"Cookie": "target=keep"})
        for session in state._CLIENT_SESSIONS.values():
            session.close()

    def test_session_storage_is_bounded_and_retired_sessions_close(self):
        from unittest.mock import Mock
        state = SimpleNamespace(_CLIENT_LOCK=threading.Lock(), _CLIENT_LAST={},
                                _CLIENT_SESSIONS={}, _CLIENT_TTL=300,
                                _make_session=Mock)
        functions = helpers("HTTP/Sessions/Clients.py", state)
        state._client_id = functions["_client_id"]
        first = functions["_get_client_session"]("invalid-first-token")
        for _ in range(520):
            functions["_get_client_session"]("invalid-token")
        self.assertEqual(len(state._CLIENT_SESSIONS), 512)
        first.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
