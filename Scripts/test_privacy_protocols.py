"""Private cache policy and end-to-end explicit HTTP/HTTPS startup."""
import ast
import queue
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask
import requests
from test_cached_mime import helpers
import test_live_proxy as live
from local_test_server import create_server, create_test_tls_context


class PrivacyProtocolTests(unittest.TestCase):
    def test_real_https_redirect_never_reaches_plain_http_origin(self):
        try:
            import cryptography
        except ImportError:
            self.skipTest("cryptography is needed for the local TLS certificate")
        sink = create_server()
        threading.Thread(target=sink.serve_forever, daemon=True).start()
        self.addCleanup(sink.server_close)
        self.addCleanup(sink.shutdown)
        secure = create_server(tls_context=create_test_tls_context())
        secure.startup_redirect_url = f"http://127.0.0.1:{sink.server_port}/"
        fixture = type("DowngradeFixture", (live.LiveProxyTests,), {"CONFIG_OVERRIDES": {
            "SITE": f"https://127.0.0.1:{secure.server_port}/startup-redirect", "TLS_VERIFY": False,
            "OUTBOUND_PRIVATE_ALLOWLIST": [f"127.0.0.1:{secure.server_port}", f"127.0.0.1:{sink.server_port}"]}})
        try:
            with patch.object(live, "create_server", return_value=secure):
                with self.assertRaisesRegex(RuntimeError, "configured protocol"):
                    fixture.setUpClass()
            self.assertGreater(secure.hits["/startup-redirect"], 0)
            self.assertEqual(sink.hits["/"], 0)
        finally:
            fixture.doClassCleanups()

    def test_same_scheme_redirect_and_http_upgrade_remain_available(self):
        for initial in ("http://example.com", "https://example.com"):
            session = Mock()
            session.get.side_effect = [
                SimpleNamespace(status_code=302, headers={"Location": "https://example.com/canonical"}, close=Mock()),
                SimpleNamespace(status_code=200, headers={}, url="https://example.com/canonical", close=Mock())]
            state = SimpleNamespace(OFFLINE=False, TLS_VERIFY=True, TIMEOUT_CONN=1, TIMEOUT_READ=1,
                log=lambda *args: None, _short_exc=str, _make_session=lambda: session,
                detect_platform=lambda url: "Unavailable", set_last_platform=lambda value: None,
                resolve_ip=lambda host: "test")
            build = helpers("Core/URL/Rewrite.py", state)["build_base_url"]
            self.assertEqual(build(initial), "https://example.com/canonical")
            self.assertEqual(session.get.call_count, 2)
    def test_private_request_stays_private_after_session_removal(self):
        state = SimpleNamespace(save_queue=queue.Queue(), _client_id=lambda: "browser",
                                _CLIENT_LOCK=threading.Lock(), _CLIENT_TTL=300,
                                _CLIENT_LAST={"browser": time.time()},
                                _CLIENT_SESSIONS={"browser": SimpleNamespace(cookies={"auth": "secret"})})
        functions = helpers("Core/Cache/Policy.py", state)
        app = Flask(__name__)
        with app.test_request_context("/"):
            self.assertFalse(functions["_shared_cache_request_allowed"]())
            state._CLIENT_SESSIONS.clear()
            self.assertFalse(functions["_queue_cache_response"]("https://example.com/profile", b"private", "text/html"))
        with app.test_request_context("/"):
            self.assertTrue(functions["_shared_cache_request_allowed"]())

    def test_session_authentication_is_captured_before_fetch(self):
        session = SimpleNamespace(cookies={"auth": "secret"})
        state = SimpleNamespace(save_queue=queue.Queue(), _client_id=lambda: "browser",
                                _CLIENT_LOCK=threading.Lock(), _CLIENT_TTL=300,
                                _CLIENT_LAST={"browser": time.time()}, _CLIENT_SESSIONS={"browser": session})
        clients = helpers("HTTP/Sessions/Clients.py", state)
        policy = helpers("Core/Cache/Policy.py", state)
        with Flask(__name__).test_request_context("/"):
            self.assertIs(clients["_get_client_session"](), session)
            session.cookies.clear()
            state._CLIENT_SESSIONS.clear()
            self.assertFalse(policy["_queue_cache_response"]("https://example.com/profile", b"private", "text/html"))
        self.assertTrue(state.save_queue.empty())

    def test_startup_redirect_downgrade_is_blocked_before_http_request(self):
        for verify in (True, False):
            with self.subTest(verify=verify):
                state = SimpleNamespace(OFFLINE=False, TLS_VERIFY=verify, TIMEOUT_CONN=1,
                                        TIMEOUT_READ=1, log=lambda *args: None, _short_exc=str)
                response = SimpleNamespace(status_code=302, headers={"Location": "http://example.com/plain"}, close=Mock())
                session = Mock()
                session.get.return_value = response
                state._make_session = lambda: session
                build = helpers("Core/URL/Rewrite.py", state)["build_base_url"]
                self.assertIsNone(build("https://example.com"))
                session.get.assert_called_once_with("https://example.com", timeout=(1, 1), verify=verify, allow_redirects=False)
                response.close.assert_called_once()

    def test_cache_policy_rejects_private_responses_and_credentials(self):
        state = SimpleNamespace(save_queue=queue.Queue())
        functions = helpers("Core/Cache/Policy.py", state)
        put = functions["_queue_cache_response"]
        for headers in ({"Cache-Control": "private, max-age=60"},
                        {"cache-control": "no-store"}, {"Set-Cookie": "session=secret"},
                        {"Vary": "Cookie"}, {"Vary": "Authorization"}, {"Vary": "*"}):
            with self.subTest(headers=headers):
                self.assertFalse(put("https://example.com/", b"private", "text/html", response_headers=headers))
        self.assertFalse(put("https://example.com/", b"private", "text/html", authenticated=True))
        app = Flask(__name__)
        for headers in ({"Authorization": "Bearer secret"}, {"Cookie": "session=secret"}):
            with app.test_request_context(headers=headers):
                self.assertFalse(functions["_shared_cache_request_allowed"]())
        self.assertTrue(state.save_queue.empty())
        self.assertTrue(put("https://example.com/app.js", b"public", "text/javascript"))

    def test_runtime_does_not_fallback_to_http_when_probe_fails(self):
        tree = ast.parse((live.ROOT / "Runtime.py").read_text(encoding="utf-8"))
        index = next(i for i, node in enumerate(tree.body) if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == "SITE_URL" for target in node.targets))
        module = ast.Module(body=tree.body[index:index + 2], type_ignores=[])
        with self.assertRaisesRegex(RuntimeError, "configured protocol"):
            exec(compile(module, "Runtime.py", "exec"), {"SITE": "example.com", "build_base_url": lambda site: None})

    def test_explicit_http_and_https_are_preserved_offline(self):
        state = SimpleNamespace(OFFLINE=True, log=lambda *args: None)
        build = helpers("Core/URL/Rewrite.py", state)["build_base_url"]
        for site in ("http://example.com", "https://example.com"):
            self.assertEqual(build(site), site)
        self.assertEqual(build("example.com"), "https://example.com")
        with self.assertRaises(ValueError):
            build("ftp://example.com")

    def test_live_https_origin_and_invalid_certificate_fail_closed(self):
        try:
            import cryptography
        except ImportError:
            self.skipTest("cryptography is needed for the local HTTPS certificate")
        for verify in (False, True):
            fixture = type("HttpsFixture", (live.LiveProxyTests,),
                           {"HTTPS_UPSTREAM": True, "CONFIG_OVERRIDES": {"TLS_VERIFY": verify}})
            try:
                if verify:
                    with self.assertRaisesRegex(RuntimeError, "configured protocol"):
                        fixture.setUpClass()
                else:
                    fixture.setUpClass()
                    response = fixture.client.get(fixture.base + "/", timeout=8)
                    self.assertEqual(response.status_code, 200)
                    self.assertIn("Live upstream", response.text)
                    self.assertGreater(fixture.upstream.hits["/"], 0)
            finally:
                fixture.doClassCleanups()


if __name__ == "__main__":
    unittest.main()
