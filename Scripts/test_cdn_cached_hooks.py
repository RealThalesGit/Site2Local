"""Both CDN cache routes must run request, response and GUI hooks."""
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from flask import Flask
import test_cached_http as http_fixtures
import test_cached_mime as fixtures


class CdnCachedHookTests(unittest.TestCase):
    def setUp(self):
        self.fixture = http_fixtures.CachedHttpTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.state = s = self.fixture.state
        s._cached_response = self.fixture.fixture.serve["_cached_response"]
        s._raw_path_after = lambda prefix: None
        s._EXT_PREFIX = "/__s2l_ext__"
        s._SAFE_METHODS = {"GET", "HEAD"}
        s._ALL_METHODS = ["GET", "HEAD", "OPTIONS"]
        s.CACHE_CDN = True
        s.OFFLINE = False
        s.PROXY_CDN = True
        s.MAX_BODY_BYTES = 1024
        s._finalize_security_headers = lambda response: response
        self.calls = []
        s.rewrite_doc_relative = lambda body, host, **kw: body.replace(b"/image.png", b"/cdn/image.png")
        s._rewrite_ext_urls = lambda body, **kw: body
        s._REQ_HOOKS = [lambda ctx: self.calls.append(("request", ctx.url))]
        def response_hook(ctx):
            self.calls.append(("response", ctx.url))
            ctx.resp_body += b"/*response*/"
        s._RESP_HOOKS = [response_hook]
        def gui_hook(ctx):
            self.calls.append(("gui", ctx.url))
            ctx.resp_body += b"/*gui*/"
        s._apply_gui_hooks = gui_hook
        s.HOOK_GUI = True
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        path = Path(self.directory.name) / "style.css"
        path.write_bytes(b"body{background:url(/image.png)}")
        s.local_path = lambda url: str(path)

    def check_response(self, response):
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"/cdn/image.png", response.data)
        self.assertTrue(response.data.endswith(b"/*response*//*gui*/"))
        self.assertEqual([kind for kind, url in self.calls], ["request", "response", "gui"])
        self.assertTrue(all(url == "https://cdn.test/style.css?v=1" for kind, url in self.calls))
        self.assertIn("no-store", response.headers["Cache-Control"])

    def test_external_route_cache_runs_all_hooks(self):
        namespace = fixtures.helpers("Proxy/Assets.py", self.state)
        with self.fixture.app.test_request_context("/__s2l_ext__/cdn.test/style.css?v=1"):
            response = namespace["ext_asset"]("cdn.test/style.css")
            self.check_response(response)

    def test_dedicated_cdn_cache_runs_all_hooks(self):
        namespace = fixtures.helpers("CDN/Server.py", self.state)
        apps = []
        def create_app(name, **kwargs):
            app = Flask(name, **kwargs)
            apps.append(app)
            return app
        namespace["Flask"] = create_app
        namespace["threading"] = SimpleNamespace(Thread=lambda **kwargs: SimpleNamespace(start=lambda: None))
        self.state._ensure_browser_session = lambda: None
        self.state._set_browser_cookie = lambda response: response
        namespace["_start_cdn_server"]("cdn.test", 8081)
        response = apps[0].test_client().get("/style.css?v=1")
        self.check_response(response)

    def test_safe_names_obey_limit_even_with_huge_extension(self):
        for limit in (8, 16, 180):
            namespace = fixtures.helpers("Core/Cache/Naming.py", SimpleNamespace(MAX_FNAME=limit))
            safe = namespace["_safe_seg"]
            for value in ("a." + "x" * 300, "x" * 300 + ".js", "long" * 100):
                self.assertLessEqual(len(safe(value)), limit)
                self.assertNotEqual(safe(value), safe(value + "z"))
        self.assertTrue(safe("x" * 300 + ".js").endswith(".js"))
        self.assertEqual(safe("normal.js"), "normal.js")


if __name__ == "__main__":
    unittest.main()
