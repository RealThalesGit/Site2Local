"""Offline regressions for hook revalidation and cached If-Range handling."""
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

import test_cached_mime as fixtures


class CachedHttpTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.CachedMimeTests()
        self.fixture.setUp()
        self.state = s = self.fixture.state
        self.app = self.fixture.app
        s._REQ_HOOKS = []
        s._RESP_HOOKS = []
        s.HOOK_GUI = False
        s.HookContext = SimpleNamespace
        s._is_document_ct = lambda ct: ct.startswith("text/html")
        s.filter_fwd = dict
        s.stats = SimpleNamespace(inc=lambda *args: None)
        s.log = lambda *args: None
        s._gui_push_raw = lambda *args, **kw: None
        s._gui_push = lambda *args: None
        s._maybe_capture = lambda *args: None
        s._apply_gui_hooks = lambda ctx: None
        s._run_hooks = lambda group, ctx: [hook(ctx) for hook in group]
        s._fmt_host = lambda host: host
        s._fmt_path_qs = lambda path: path
        s._fmt_size = str
        s.MAIN_HOST = "example.com"
        s._serve_cached = self.fixture.serve["_serve_cached"]
        s._conditional_304 = self.fixture.validators["_conditional_304"]
        s._make_flask_resp = fixtures.helpers("Proxy/Upstream/Response.py", s)["_make_flask_resp"]
        self.ranges = fixtures.helpers("Proxy/Cache/Range.py", s)
        s._parse_range_header = self.ranges["_parse_range_header"]
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "asset.js"
        self.body = b"console.log('original');"
        self.path.write_bytes(self.body)
        ct = s._resolve_mime_with_sniff(str(self.path))
        self.etag, self.modified = s._disk_validators(str(self.path), ct)

    def full(self):
        return self.fixture.serve["_cached_response"](
            str(self.path), "https://example.com/asset.js", "GET", "/asset.js")

    def partial(self):
        return self.ranges["_serve_range_from_disk"](
            str(self.path), "https://example.com/asset.js", "GET", "/asset.js")

    def test_each_hook_mode_runs_despite_matching_browser_validator(self):
        def edit(ctx):
            ctx.resp_body = b"console.log('hooked');"
        for mode in ("_REQ_HOOKS", "_RESP_HOOKS", "HOOK_GUI"):
            with self.subTest(mode=mode):
                self.state._REQ_HOOKS = []
                self.state._RESP_HOOKS = []
                self.state.HOOK_GUI = mode == "HOOK_GUI"
                self.state._apply_gui_hooks = edit if mode == "HOOK_GUI" else lambda ctx: None
                if mode != "HOOK_GUI":
                    setattr(self.state, mode, [edit])
                with self.app.test_request_context(headers={"If-None-Match": self.etag}):
                    response = self.full()
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.data, b"console.log('hooked');")

    def test_no_hooks_preserves_fast_304(self):
        with self.app.test_request_context(headers={"If-None-Match": self.etag}):
            self.assertEqual(self.full().status_code, 304)

    def test_current_if_range_or_no_validator_serves_exact_slice(self):
        for validator in (None, self.etag, self.modified):
            with self.subTest(validator=validator):
                headers = {"Range": "bytes=2-4"}
                if validator:
                    headers["If-Range"] = validator
                with self.app.test_request_context(headers=headers):
                    response = self.partial()
                    self.assertEqual(response.status_code, 206)
                    self.assertEqual(response.data, self.body[2:5])
                    self.assertEqual(response.headers["Content-Range"], f"bytes 2-4/{len(self.body)}")
                    self.assertEqual(response.headers["ETag"], self.etag)

    def test_stale_weak_or_invalid_if_range_falls_back_to_full_200(self):
        for validator in ('"obsolete"', 'W/' + self.etag,
                          "Wed, 01 Jan 2020 00:00:00 GMT", "invalid-date"):
            with self.subTest(validator=validator):
                # Even a matching conditional must not turn this fallback into 304.
                with self.app.test_request_context(headers={
                        "Range": "bytes=2-4", "If-Range": validator,
                        "If-None-Match": self.etag}):
                    self.assertIsNone(self.partial())
                    response = self.full()
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.data, self.body)
                    self.assertNotIn("Content-Range", response.headers)


if __name__ == "__main__":
    unittest.main()
