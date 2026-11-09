"""Offline COEP consistency checks for Firefox and Chromium request shapes."""
from types import SimpleNamespace
import unittest

from flask import Flask, Response
import test_cached_mime as fixtures


class IsolationHeaderTests(unittest.TestCase):
    def setUp(self):
        self.state = s = SimpleNamespace(CROSS_ORIGIN_ISOLATION=True)
        ci = fixtures.helpers("HTTP/Headers/CI.py", s)
        s._ci_remove_header = ci["_ci_remove_header"]
        s._ci_get_header = ci["_ci_get_header"]
        isolation = fixtures.helpers("HTTP/Headers/Isolation.py", s)
        self.isolation = isolation
        for name in ("_is_document_ct", "_is_document_request", "_is_worker_request",
                     "_coep_value", "_apply_isolation_headers"):
            setattr(s, name, isolation[name])
        s._apply_security_headers = lambda headers, *args, **kwargs: None
        s._apply_corp_header = fixtures.helpers("HTTP/Headers/CORS.py", s)["_apply_corp_header"]
        finalize = fixtures.helpers("HTTP/Headers/Response.py", s)["_finalize_security_headers"]
        self.app = Flask(__name__)
        # The main app and every CDN server register this same finalizer.
        self.app.after_request(finalize)

        @self.app.route("/<int:status>/<kind>")
        def response(status, kind):
            mime = "text/html" if kind == "html" else "application/octet-stream"
            out = Response(b"<html>game</html>", status=status, content_type=mime)
            out.headers["cross-origin-embedder-policy"] = "unsafe-none"
            return out

        self.agents = ("Mozilla/5.0 Firefox/132.0", "Mozilla/5.0 Chrome/140.0.0.0")

    def assert_policy(self, response):
        self.assertEqual(response.headers["Cross-Origin-Embedder-Policy"], "credentialless")
        self.assertEqual(response.headers["Cross-Origin-Opener-Policy"], "same-origin")
        self.assertEqual(response.headers["Cross-Origin-Resource-Policy"], "cross-origin")
        self.assertEqual(len(response.headers.getlist("Cross-Origin-Embedder-Policy")), 1)

    def test_navigation_policy_survives_response_shapes_and_nested_frames(self):
        client = self.app.test_client()
        for agent in self.agents:
            for destination in ("document", "iframe"):
                for status in (200, 302, 304, 404):
                    with self.subTest(agent=agent, destination=destination, status=status):
                        response = client.get(f"/{status}/binary", headers={
                            "User-Agent": agent, "Sec-Fetch-Dest": destination})
                        self.assert_policy(response)

    def test_html_keeps_policy_without_fetch_metadata(self):
        for agent in self.agents:
            with self.subTest(agent=agent):
                response = self.app.test_client().get("/200/html", headers={"User-Agent": agent})
                self.assert_policy(response)

    def test_worker_and_ordinary_assets_keep_distinct_policies(self):
        for agent in self.agents:
            with self.app.test_request_context(headers={"User-Agent": agent}):
                headers = {}
                self.state._apply_isolation_headers(headers)
                self.assertEqual(headers, {})
                self.state._apply_isolation_headers(headers, is_worker_script=True)
                self.assertEqual(headers["Cross-Origin-Embedder-Policy"], "credentialless")
                self.assertNotIn("Cross-Origin-Opener-Policy", headers)

    def test_compatibility_mode_removes_enforcement_on_every_response_shape(self):
        self.state.CROSS_ORIGIN_ISOLATION = False
        for agent in self.agents:
            for destination in ("document", "iframe", "worker"):
                for status in (200, 302, 304, 404):
                    with self.subTest(agent=agent, destination=destination, status=status):
                        response = self.app.test_client().get(f"/{status}/html", headers={
                            "User-Agent": agent, "Sec-Fetch-Dest": destination})
                        self.assertNotIn("Cross-Origin-Embedder-Policy", response.headers)
                        self.assertNotIn("Cross-Origin-Opener-Policy", response.headers)
                        self.assertEqual(response.headers["Cross-Origin-Resource-Policy"], "cross-origin")

    def test_compatibility_mode_removes_upstream_headers_case_insensitively(self):
        self.state.CROSS_ORIGIN_ISOLATION = False
        headers = {"cross-origin-embedder-policy": "require-corp",
                   "cross-origin-opener-policy": "same-origin",
                   "X-Content-Type-Options": "nosniff"}
        self.state._apply_isolation_headers(headers, is_top_level_html=True)
        self.assertEqual(headers, {"X-Content-Type-Options": "nosniff"})

    def test_auto_keeps_plain_pages_and_workers_compatible(self):
        self.state.CROSS_ORIGIN_ISOLATION = "auto"
        for agent in self.agents:
            for destination in ("document", "iframe", "worker"):
                with self.app.test_request_context(headers={
                        "User-Agent": agent, "Sec-Fetch-Dest": destination}):
                    headers = {}
                    self.state._apply_isolation_headers(headers, b"<html>game.wasm</html>",
                        is_top_level_html=destination != "worker", is_worker_script=destination == "worker")
                    self.assertNotIn("Cross-Origin-Embedder-Policy", headers)

    def test_auto_detects_html_and_keeps_cached_descendants_consistent(self):
        self.state.CROSS_ORIGIN_ISOLATION = "auto"
        for destination, body in (("document", b"<html>SharedArrayBuffer</html>"),
                                  ("iframe", b"<html>cached</html>"), ("worker", b"")):
            with self.app.test_request_context(headers={
                    "User-Agent": self.agents[0], "Sec-Fetch-Dest": destination}):
                headers = {}
                self.state._apply_isolation_headers(headers, body,
                    is_top_level_html=destination != "worker", is_worker_script=destination == "worker")
                self.assertEqual(headers["Cross-Origin-Embedder-Policy"], "credentialless")

    def test_auto_honors_upstream_requirement_without_latching_from_iframe(self):
        self.state.CROSS_ORIGIN_ISOLATION = "auto"
        with self.app.test_request_context(headers={"Sec-Fetch-Dest": "iframe"}):
            headers = {"cross-origin-embedder-policy": "require-corp"}
            self.state._apply_isolation_headers(headers, is_top_level_html=True)
            self.assertEqual(headers["Cross-Origin-Embedder-Policy"], "require-corp")
            self.assertFalse(self.isolation["_auto_isolation_required"])
        with self.app.test_request_context(headers={"Sec-Fetch-Dest": "document"}):
            headers = {"cross-origin-embedder-policy": "require-corp"}
            self.state._apply_isolation_headers(headers, is_top_level_html=True)
            self.assertTrue(self.isolation["_auto_isolation_required"])


if __name__ == "__main__":
    unittest.main()
