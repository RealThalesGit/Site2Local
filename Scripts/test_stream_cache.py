"""Streaming cache and rewritable-response regressions.

Run: python -B -m unittest discover -s Scripts -p test_stream_cache.py
"""
import queue
from types import SimpleNamespace
import unittest

from flask import Flask
import test_cached_mime as fixtures


class Upstream:
    def __init__(self, body, status=200, headers=None, fail=False):
        self.body = body
        self.status_code = status
        self.headers = {"Content-Type": "application/octet-stream", **(headers or {})}
        self.closed = False
        self.fail = fail

    def iter_content(self, chunk_size=None):
        yield self.body
        if self.fail:
            raise OSError("interrupted download")

    def close(self):
        self.closed = True


class StreamCacheTests(unittest.TestCase):
    def setUp(self):
        self.state = s = SimpleNamespace(
            _ensure_wasm_content_type=lambda ct, path: ct,
            _resp_headers_dict=lambda response: dict(response.headers),
            filter_resp=lambda headers, *args, **kw: dict(headers),
            _is_worker_request=lambda: False, _STREAM_CACHE_MAX_BYTES=1024 * 1024,
            decompress_body=lambda body, encoding: body,
            _is_wire_payload=lambda body: False, _is_bot_page=lambda *args: False,
            _effective_device=lambda: "desktop", save_queue=queue.Queue(),
            log=lambda *args: None, _short_exc=str, _fmt_host=lambda host: host,
            _fmt_path_qs=lambda path, query: path, _fmt_size=str,
            HOOK_GUI=False, CACHE_CDN=True, MAIN_HOST="example.com")
        self.stream_helpers = fixtures.helpers("HTTP/Streaming/Response.py", s)
        self.stream = self.stream_helpers["_stream_resp"]
        self.app = Flask(__name__)

    def consume(self, upstream, target="https://example.com/asset.bin"):
        with self.app.test_request_context("/"):
            response = self.stream(upstream, "GET", target)
            body = response.get_data()
            response.close()
        self.assertTrue(upstream.closed)
        return body

    def test_partial_and_malformed_206_are_never_cached(self):
        for content_range in ("bytes 0-3/100", "bytes 1-4/5", "bytes 0-3/*",
                              "bytes 0-3/4 garbage", "bytes 0-4/4", "", "bytes 0-0/0"):
            with self.subTest(content_range=content_range):
                upstream = Upstream(b"abcd", 206, {"Content-Range": content_range})
                self.assertEqual(self.consume(upstream), b"abcd")
                self.assertTrue(self.state.save_queue.empty())

    def test_complete_206_is_cached_only_with_exact_received_length(self):
        self.consume(Upstream(b"abcd", 206, {"Content-Range": "bytes 0-3/4"}))
        self.assertEqual(self.state.save_queue.get_nowait()[1], b"abcd")
        for body in (b"abc", b"abcde"):
            with self.subTest(body=body):
                self.consume(Upstream(body, 206, {"Content-Range": "bytes 0-3/4"}))
                self.assertTrue(self.state.save_queue.empty())

    def test_complete_200_still_caches(self):
        self.consume(Upstream(b"complete"))
        self.assertEqual(self.state.save_queue.get_nowait()[1], b"complete")

    def test_interrupted_stream_never_caches(self):
        self.consume(Upstream(b"abcd", 206, {"Content-Range": "bytes 0-3/4"}, fail=True))
        self.assertTrue(self.state.save_queue.empty())

    def test_issue_2_external_stream_should_respect_cache_cdn_disabled(self):
        self.state.CACHE_CDN = False
        self.consume(Upstream(b"external"), "https://cdn.example.com/asset.bin")
        self.assertTrue(self.state.save_queue.empty())

    def test_issue_3_streamed_html_should_rewrite_and_inject(self):
        html = b'<html><script src="https://external.test/game.js"></script></html>'
        self.state.PROXY_CDN = True
        self.state._rewrite_ext_urls = lambda body, **kw: body.replace(
            b"https://external.test", b"http://localhost:8081")
        self.state._inject_sw_clear = lambda body: b'<script id="__s2l__"></script>' + body
        s = self.state
        s._raw_path_after = lambda prefix: None
        s._EXT_PREFIX = "/__s2l_ext__"
        s._SAFE_METHODS = {"GET", "HEAD"}
        s._BODY_METHODS = {"POST", "PUT", "PATCH"}
        s.local_path = lambda url: "nonexistent-stream-test-cache"
        s.OFFLINE = False
        s.HookContext = SimpleNamespace
        s.filter_fwd = dict
        s._real_origin_for = lambda origin: "https://example.com"
        s._REQ_HOOKS = []
        s._RESP_HOOKS = [True]
        hook_calls = []
        s._run_hooks = lambda hooks, ctx: hook_calls.append(ctx.resp_body) or 1
        s._CONN_ERRORS = (ConnectionError,)
        s.TIMEOUT_CONN = 6
        s.TIMEOUT_READ = 12
        s.TIMEOUT_STREAM_READ = 120
        s.guess_mime = lambda path: "text/html"
        s._should_stream = lambda ct: False
        upstream = Upstream(html, headers={"Content-Type": "text/html"})
        s._get_client_session = lambda: SimpleNamespace(request=lambda *args, **kw: upstream)
        s._register_cdn_host = lambda host: None
        s.stats = SimpleNamespace(inc=lambda *args: None)
        s._is_document_ct = lambda ct: ct.startswith("text/html")
        s._apply_freshness = lambda *args: None
        s.rewrite_abs_urls = lambda body: body
        s.rewrite_doc_relative = lambda body, host, **kw: body
        s._apply_gui_hooks = lambda ctx: None
        s._gui_push_raw = lambda *args, **kw: None
        s.CAPTURE = False
        namespace = fixtures.helpers("Proxy/Assets.py", s)
        namespace["_buffer_rewritable_response"] = self.stream_helpers["_buffer_rewritable_response"]
        with self.app.test_request_context("/__s2l_ext__/cdn.example.com/assets/game.html"):
            result = namespace["ext_asset"]("cdn.example.com/assets/game.html").get_data()
        self.assertIn(b"__s2l__", result)
        self.assertNotIn(b"https://external.test", result)
        self.assertEqual(hook_calls, [result])
        self.assertEqual(s.save_queue.get_nowait()[1], html)
        self.assertTrue(upstream.closed)

    def test_rewritable_buffer_keeps_binary_ranges_and_sse_streaming(self):
        for mime, status, headers in (("application/wasm", 200, {}),
                                      ("text/css", 206, {"Content-Range": "bytes 0-3/4"}),
                                      ("text/event-stream", 200, {}),
                                      ("application/stream+json", 200, {})):
            upstream = Upstream(b"data", status, {"Content-Type": mime, **headers})
            self.assertIsNone(self.stream_helpers["_buffer_rewritable_response"](upstream))
            self.assertFalse(upstream.closed)

    def test_rewritable_buffer_consumes_css_json_and_empty_html(self):
        for mime, body in (("text/css", b"@import 'theme.css';"),
                           ("application/json", b'{"url":"/asset"}'), ("text/html", b"")):
            upstream = Upstream(body, headers={"Content-Type": mime})
            self.assertEqual(self.stream_helpers["_buffer_rewritable_response"](upstream), body)
            self.assertTrue(upstream.closed)

    def test_cache_cdn_disabled_does_not_disable_main_host_cache(self):
        self.state.CACHE_CDN = False
        self.consume(Upstream(b"main"))
        self.assertEqual(self.state.save_queue.get_nowait()[1], b"main")


if __name__ == "__main__":
    unittest.main()
