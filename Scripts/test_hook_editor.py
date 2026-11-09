"""HTTP hook editor hides proxy scripts and safely reapplies edited HTML."""
import re
import threading
from types import SimpleNamespace
import unittest

import test_cached_mime as fixtures


class HookEditorTests(unittest.TestCase):
    def setUp(self):
        self.script = b'<script id="__s2l__">freshProxy();</script>'
        self.state = s = SimpleNamespace(
            HOOK_GUI=True, PROXY_CDN=True, MAIN_HOST="example.com",
            _gui_hooks_lock=threading.Lock(), _gui_hooks=[],
            _s2l_js_lock=threading.Lock(), _build_s2l_injector=lambda: self.script,
            _EDITOR_MAX_BYTES=1024, _fmt_size=str, log=lambda *args: None,
            _is_document_ct=lambda ct: "text/html" in ct,
            _rewrite_ext_urls=lambda body, **kw: body.replace(b"https://cdn.test", b"http://localhost:8081"),
            rewrite_abs_urls=lambda body: body,
            rewrite_doc_relative=lambda body, host, **kw: body)
        self.sw = fixtures.helpers("Rewriting/HTML/ServiceWorker.py", s)
        self.strip = self.sw["_strip_s2l_injector"]
        s._inject_sw_clear = self.sw["_inject_sw_clear"]
        self.store = fixtures.helpers("Hooks/Hooks/Store.py", s)
        self.store["_strip_s2l_injector"] = self.strip
        self.hooks = fixtures.helpers("Hooks/Hooks/GUIHooks.py", s)
        self.hooks["_strip_s2l_injector"] = self.strip
        s._origin_matches = self.hooks["_origin_matches"]

    def context(self, path="/", ct="text/html"):
        return SimpleNamespace(url="https://example.com" + path, method="GET", path=path,
            resp_status=200, resp_ct=ct, resp_body=b"original",
            resp_headers={"Content-Length": "8", "content-length": "8"})

    def hook(self, body, pattern=r"^/$"):
        self.state._gui_hooks = [dict(enabled=True, method="GET", status=0,
            origin_url="target", pattern=pattern, body_bytes=body, name="edit")]

    def test_editor_strips_injector_before_size_cap(self):
        html = b'<html><head><script id="__s2l__">' + b"x" * 2000 + b'</script></head><body>editable</body></html>'
        text, truncated = self.store["_editor_preview"](html, "text/html")
        self.assertFalse(truncated)
        self.assertNotIn("__s2l__", text)
        self.assertIn("editable", text)
        self.assertIn("__s2l__", html.decode())

    def test_strip_preserves_site_scripts_and_similarly_named_attributes(self):
        html = b'<script data-id="__s2l__">site();</script><script id="app">app();</script>' + self.script
        self.assertEqual(self.strip(html), html[:-len(self.script)])

    def test_edited_html_rewrites_and_injects_exactly_once(self):
        for stale in (b"", b'<script id="__s2l__">oldProxy();</script>'):
            body = b'<html><head><meta charset="utf-8">' + stale + b'</head><body>edited<img src="https://cdn.test/a.png"></body></html>'
            self.hook(body)
            ctx = self.context()
            self.hooks["_apply_gui_hooks"](ctx)
            self.assertEqual(ctx.resp_body.count(b'id="__s2l__"'), 1)
            self.assertIn(b"freshProxy()", ctx.resp_body)
            self.assertNotIn(b"oldProxy()", ctx.resp_body)
            self.assertIn(b"http://localhost:8081/a.png", ctx.resp_body)
            self.assertEqual(ctx.resp_headers["Content-Length"], str(len(ctx.resp_body)))
            self.assertNotIn("content-length", ctx.resp_headers)
            self.assertEqual(self.state._gui_hooks[0]["body_bytes"], body)

    def test_exact_root_hook_does_not_replace_stylesheets(self):
        pattern = "^" + re.escape("/") + "$"
        self.hook(b"<html>edited</html>", pattern)
        ctx = self.context("/assets/site.css", "text/css")
        self.hooks["_apply_gui_hooks"](ctx)
        self.assertEqual(ctx.resp_body, b"original")

    def test_css_and_js_hooks_preserve_original_mime(self):
        for ct, body in (("text/css", b"body{color:red}"),
                         ("application/javascript", b"run();")):
            self.hook(body)
            ctx = self.context(ct=ct)
            self.hooks["_apply_gui_hooks"](ctx)
            self.assertEqual(ctx.resp_ct, ct)
            self.assertEqual(ctx.resp_body, body)


if __name__ == "__main__":
    unittest.main()
