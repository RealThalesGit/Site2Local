"""Offline regressions for TLS policy and untrusted forwarded headers."""
import socket
import ssl
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from flask import Flask
from test_cached_mime import helpers


class SecuritySettingsTests(unittest.TestCase):
    def state(self, verify):
        return SimpleNamespace(
            TLS_VERIFY=verify, _effective_device=lambda: "desktop",
            _secure_session=lambda session: session,
            _MOBILE_DEVICES=set(), _sanitize_ua=lambda value: value,
            UA_PROFILES={"desktop": "test", "macintosh": "test"},
            _SEC_CH_UA={}, _BROTLI_OK=False, _RETRY_POLICY=0,
            POOL_CONNECTIONS=1, POOL_MAXSIZE=1, _CLOUDSCRAPER_OK=True,
            _CURL_CFFI_OK=False, _next_cf_config=lambda mobile: {},
            log=lambda *args: None, _cffi_impersonate_cache={},
            _IMPERSONATE_BY_DEVICE={"desktop": ("chrome136",)})

    def test_every_http_factory_obeys_tls_setting(self):
        for verify in (True, False):
            state = self.state(verify)
            state.cloudscraper = SimpleNamespace(create_scraper=lambda **kw: Mock(headers={}))
            state._cffi_requests = SimpleNamespace(Session=lambda **kw: Mock(headers={}, verify=kw["verify"]))
            functions = helpers("HTTP/Sessions/Factory.py", state)
            for name in ("_make_requests_session", "_make_cloudscraper_session", "_make_session"):
                with self.subTest(verify=verify, factory=name):
                    session = functions[name]()
                    self.assertIs(session.verify, verify)
                    session.close()
            session = functions["_make_cffi_session"]("desktop", {})
            self.assertIs(session.verify, verify)

    def test_raw_wss_keeps_verification_and_ip_identity(self):
        for verify in (True, False):
            for host in ("example.com", "127.0.0.1", "[::1]"):
                with self.subTest(verify=verify, host=host):
                    state = SimpleNamespace(TLS_VERIFY=verify, _is_ip_literal=lambda h: h != "example.com",
                                            _connect_outbound=lambda h, p, timeout: socket.create_connection((h, p), timeout))
                    cls = helpers("WebSockets/Upstream/Raw.py", state)["_RawSocketUpstreamWS"]
                    context = ssl.create_default_context()
                    raw = Mock()
                    # Stop exactly at the TLS handshake and inspect its policy.
                    with patch("socket.create_connection", return_value=raw), \
                         patch("ssl.create_default_context", return_value=context), \
                         patch.object(context, "wrap_socket", side_effect=ssl.SSLCertVerificationError("invalid certificate")) as wrap:
                        with self.assertRaises(ssl.SSLCertVerificationError):
                            cls(f"wss://{host}/socket", {})
                    self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED if verify else ssl.CERT_NONE)
                    self.assertEqual(context.check_hostname, verify)
                    expected = host.strip("[]") if verify or host == "example.com" else None
                    self.assertEqual(wrap.call_args.kwargs["server_hostname"], expected)
                    raw.close.assert_called_once()

    def test_probe_cannot_bypass_verification_or_downgrade(self):
        state = SimpleNamespace(TLS_VERIFY=True, OFFLINE=False, TIMEOUT_CONN=1,
                                TIMEOUT_READ=1, log=lambda *a: None, _short_exc=str)
        session = Mock()
        session.get.side_effect = ssl.SSLCertVerificationError("bad cert")
        state._make_session = lambda: session
        probe = helpers("Core/URL/Rewrite.py", state)["build_base_url"]
        self.assertIsNone(probe("example.com"))
        session.get.assert_called_once_with("https://example.com", timeout=(1, 1), verify=True, allow_redirects=False)
        session.close.assert_called_once()

    def test_forwarded_for_cannot_select_another_peer_session(self):
        namespace = helpers("HTTP/Sessions/Clients.py", SimpleNamespace())
        client_id = namespace["_client_id"]
        signed = namespace["_signed_browser_token"]()
        app = Flask(__name__)
        with app.test_request_context(headers={"X-Forwarded-For": "192.0.2.8", "Cookie": "__s2l_browser=" + signed},
                                      environ_base={"REMOTE_ADDR": "192.0.2.1"}):
            self.assertEqual(client_id(), signed.split(".")[0])

    def test_viewer_rejects_untrusted_origins_before_reset(self):
        state = SimpleNamespace(WEB_CDNVIEWER=True, WEB_CDNVIEWER_IP_WHITELIST=[],
                                WEB_CDNVIEWER_ALLOWED_ORIGINS=["https://trusted.example"],
                                stats=Mock(), _ALL_METHODS=["GET", "POST", "OPTIONS"])
        common = helpers("Viewers/Common.py", state)
        state._viewer_ip_allowed = common["_viewer_ip_allowed"]
        state._viewer_ip_denied_response = common["_viewer_ip_denied_response"]
        endpoint = helpers("Viewers/Reset.py", state)["_s2l_reset_stats_endpoint"]
        app = Flask(__name__)
        for origin in ("https://untrusted.example", "null"):
            for method in ("POST", "OPTIONS"):
                with app.test_request_context(method=method, headers={"Origin": origin}):
                    self.assertEqual(endpoint().status_code, 403)
        state.stats.reset.assert_not_called()
        for origin in (None, "http://localhost", "https://trusted.example"):
            with app.test_request_context(headers={"Origin": origin} if origin else {}):
                self.assertTrue(state._viewer_ip_allowed())
        state.WEB_CDNVIEWER_IP_WHITELIST = ["192.0.2.1"]
        with app.test_request_context(headers={"Origin": "https://trusted.example"},
                                      environ_base={"REMOTE_ADDR": "192.0.2.2"}):
            self.assertFalse(state._viewer_ip_allowed())

    def test_optional_waf_scanner_uses_guarded_tls_session(self):
        class Scanner:
            def __init__(self, target, timeout):
                self.target, self.timeout = target, timeout
                self.headers, self.allowredir, self.requestnumber = {}, True, 0
                self.rq = self.Request()
            def identwaf(self):
                return ["Test WAF"], None
        response = Mock()
        response.iter_content.return_value = [b"bounded response"]
        session = Mock()
        session.get.return_value = response
        session.__enter__ = Mock(return_value=session)
        session.__exit__ = Mock(return_value=False)
        state = SimpleNamespace(_WAFW00F=Scanner, TIMEOUT_READ=3, TLS_VERIFY=True,
                                _make_requests_session=lambda: session)
        detect = helpers("Core/URL/Platform.py", state)["detect_platform"]
        self.assertEqual(detect("https://example.com"), "Test WAF")
        self.assertTrue(session.get.call_args.kwargs["verify"])
        response.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
