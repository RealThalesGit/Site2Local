"""Offline HTML/CSS/JS discovery regressions, without starting Runtime."""
from collections import deque
from pathlib import Path
import queue
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
import unittest

import test_cached_mime as fixtures


class LinkExtractionTests(unittest.TestCase):
    def setUp(self):
        self.state = s = SimpleNamespace(
            DUMP_ALL=True, SCAN_LIMIT=262144, PROXY_CDN=True,
            _LOCAL_HOSTS={"localhost", "127.0.0.1"},
            is_allowed_domain=lambda host: host == "example.com",
            is_external_domain=lambda host: bool(host),
            _ext_asset_queue=queue.Queue(), log=lambda *args: None, _short_exc=str)
        self.extract = fixtures.helpers("Crawler/Crawl/Extract.py", s)["extract_links"]
        self.main = []
        s.enqueue = self.main.append
        self.links = fixtures.helpers("Crawler/Crawl/Links.py", s)
        self.links["extract_links"] = self.extract
        s._discover_links = self.links["_discover_links"]

    def urls(self, body, ct="text/html", base="https://example.com/pages/index.html", dump=True):
        return dict(self.extract(body, base, ct, scan_text=dump))

    def test_base_entities_queries_and_fragments(self):
        result = self.urls(b'<base href="/static/"><img src="icon.png?v=a+b&amp;x=2#part"><a href="../next#top">next</a>')
        self.assertEqual(result, {"https://example.com/static/icon.png?v=a+b&x=2": True,
                                  "https://example.com/next": False})

    def test_srcset_retains_commas_inside_url_and_ignores_data(self):
        result = self.urls(b'<img srcset="data:image/png;base64,AAAA 1x, //cdn.test/image/q=78,w=300/a.webp 2x, large.webp 3x">')
        self.assertEqual(set(result), {"https://cdn.test/image/q=78,w=300/a.webp",
                                       "https://example.com/pages/large.webp"})

    def test_media_and_lazy_images(self):
        result = self.urls(b'<video src="movie.mp4" poster="cover.jpg"><track src="captions.vtt"></video><audio src="sound.ogg"></audio><img data-src="lazy.png"><source srcset="small.webp 1x, large.webp 2x">')
        self.assertEqual(len(result), 7)
        self.assertTrue(all(result.values()))

    def test_css_imports_escapes_and_comment_exclusion(self):
        css = br'''/* url(fake.png) */ @import "../theme.css";
            .x {background:url('../images/a(b).png?q=a+b');src:url(font\20 name.woff2)}
            .y:before {content:"url(not-a-link.png)"}'''
        result = self.urls(css, "text/css")
        self.assertEqual(set(result), {"https://example.com/theme.css",
            "https://example.com/images/a(b).png?q=a+b",
            "https://example.com/pages/font name.woff2"})

    def test_inline_css_uses_html_base(self):
        result = self.urls(b'<base href="/static/"><style>@import "theme.css";</style><div style="background:url(bg.png)"></div>')
        self.assertEqual(set(result), {"https://example.com/static/theme.css", "https://example.com/static/bg.png"})

    def test_js_quoted_urls_keep_plus_and_relative_prefix(self):
        js = br'''const api="/api/data?q=a+b"; const img="../images/bg.png";
            // "https://fake.test/comment"
            const partial="/api/"+id; const data="data:text/plain,x";
            const remote="https:\/\/cdn.test\/file.js?v=1";'''
        result = self.urls(js, "application/javascript")
        self.assertEqual(set(result), {"https://example.com/api/data?q=a+b",
            "https://example.com/images/bg.png", "https://cdn.test/file.js?v=1"})
        self.assertEqual(self.urls(js, "application/javascript", dump=False), {})

    def test_dedup_and_unsafe_schemes(self):
        result = self.urls(b'<a href="same">same</a><img src="same"><img src="same#part"><img src="data:image/png,x"><a href="javascript:alert(1)">x</a><a href="#only">x</a>')
        self.assertEqual(result, {"https://example.com/pages/same": True})

    def test_shared_policy_routes_assets_but_not_external_navigation(self):
        self.links["_extract_links_async"](
            b'<img src="/a.png"><iframe src="https://cdn.test/game.html"></iframe><a href="https://other.test/page">x</a><img src="http://localhost/debug">',
            "https://example.com/")
        self.assertEqual(self.main, ["https://example.com/a.png"])
        self.assertEqual(self.state._ext_asset_queue.get_nowait(), "https://cdn.test/game.html")
        self.assertTrue(self.state._ext_asset_queue.empty())

    def test_cached_crawl_uses_shared_extractor(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "index.html"
            path.write_bytes(b'<base href="/static/"><img srcset="a.webp 1x, b.webp 2x">')
            s = self.state
            s.visited_lock = threading.Lock()
            s.visited = set()
            s._visited_order = deque()
            s._VISITED_MAX = 100
            s.url_depth = lambda url: 1
            s.CRAWL_DEPTH = 7
            s.BLOCK_PATHS = []
            s._fmt_host = lambda host: host
            s._device_variant = lambda device: ""
            s._effective_device = lambda: "desktop"
            s.local_path = lambda *args: str(path)
            s._resolve_mime_with_sniff = lambda *args: "text/html"
            s.SHOW_HIDDEN = False
            s._is_wire_payload = lambda data: False
            s.is_html = lambda data, ct: "html" in ct
            fixtures.helpers("Crawler/Crawl/Jobs.py", s)["_crawl"]("https://example.com/page")
            self.assertEqual(set(self.main), {"https://example.com/static/a.webp", "https://example.com/static/b.webp"})

    def test_queue_accepts_post_startup_discovery_only_with_workers(self):
        s = self.state
        s._crawl_done = threading.Event()
        s._crawl_done.set()
        s._discovery_started = threading.Event()
        s.url_queue = queue.Queue()
        s.normalize_url = lambda url: url
        s.CRAWL = True
        s.OFFLINE = False
        enqueue = fixtures.helpers("Crawler/Crawl/Jobs.py", s)["enqueue"]
        enqueue("https://example.com/a")
        self.assertTrue(s.url_queue.empty())
        s._discovery_started.set()
        enqueue("https://example.com/b")
        self.assertEqual(s.url_queue.get_nowait(), "https://example.com/b")
        s.OFFLINE = True
        enqueue("https://example.com/c")
        self.assertTrue(s.url_queue.empty())


if __name__ == "__main__":
    unittest.main()
