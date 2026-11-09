"""Offline regressions for independent files, drain accounting and HEAD."""
from pathlib import Path
import queue
from tempfile import TemporaryDirectory
import threading
from types import SimpleNamespace
import unittest

from flask import Flask
import test_cached_mime as fixtures


class StopWorker(BaseException):
    pass


class FiniteQueue(queue.Queue):
    def __init__(self, items, before_get=lambda: None):
        super().__init__()
        self.before_get = before_get
        for item in items:
            self.put(item)

    def get(self, timeout=None):
        self.before_get()
        if self.empty():
            raise StopWorker()
        return super().get(timeout=timeout)


class SaveIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def worker(self, items, batch=1, before_get=lambda: None):
        q = FiniteQueue(items, before_get)
        s = SimpleNamespace(
            save_queue=q, SAVE_INTERVAL=999, SAVE_BATCH=batch,
            _is_bot_page=lambda data: False, _device_variant=lambda dev: "",
            save_lock=threading.Lock(), saved_keys=set(), _SAVED_KEYS_MAX=100,
            local_path=lambda url, dev="": str(self.root / url.rsplit("/", 1)[-1]),
            content_lock=threading.Lock(), content_hashes={}, _CONTENT_HASHES_MAX=100,
            _CACHE_TMP_SUFFIX=".s2l-tmp", stats=SimpleNamespace(inc=lambda *args: None),
            log=lambda *args: None)
        return s, fixtures.helpers("Crawler/Crawl/Save.py", s)

    def run_worker(self, state, namespace):
        with self.assertRaises(StopWorker):
            namespace["_save_worker"]()
        self.assertEqual(state.save_queue.unfinished_tasks, 0)
        state.save_queue.join()

    def item(self, name):
        return (f"https://example.com/{name}.js", b"original", "text/javascript")

    def test_duplicate_content_stays_independent_after_hand_edit(self):
        state, namespace = self.worker([self.item("a"), self.item("b")])
        self.run_worker(state, namespace)
        (self.root / "a.js").write_bytes(b"edited!!")
        self.assertEqual((self.root / "b.js").read_bytes(), b"original")

    def test_later_duplicate_never_copies_stale_hash_source(self):
        def edit_first():
            if (self.root / "b.js").exists():
                (self.root / "a.js").write_bytes(b"edited!!")
        state, namespace = self.worker([self.item("a"), self.item("b"), self.item("c")],
                                       before_get=edit_first)
        self.run_worker(state, namespace)
        self.assertEqual((self.root / "c.js").read_bytes(), b"original")

    def test_batch_tasks_remain_pending_until_each_disk_write(self):
        state, namespace = self.worker([self.item("a"), self.item("b")], batch=2)
        original_write = namespace["_atomic_write"]
        pending = []
        def checked_write(path, data):
            pending.append(state.save_queue.unfinished_tasks)
            original_write(path, data)
        namespace["_atomic_write"] = checked_write
        self.run_worker(state, namespace)
        self.assertEqual(pending, [2, 1])
        self.assertTrue((self.root / "a.js").exists())
        self.assertTrue((self.root / "b.js").exists())

    def test_failed_write_and_skipped_items_balance_queue(self):
        state, namespace = self.worker([self.item("a"), self.item("a"), ("invalid",)])
        def fail_write(path, data):
            raise OSError("disk unavailable")
        namespace["_atomic_write"] = fail_write
        self.run_worker(state, namespace)
        self.assertFalse(state.saved_keys)

    def test_head_preserves_representation_length_and_upstream_metadata(self):
        fixture = fixtures.CachedMimeTests()
        fixture.setUp()
        s = fixture.state
        s._is_document_ct = lambda ct: ct.startswith("text/html")
        s.HOOK_GUI = False
        make_response = fixtures.helpers("Proxy/Upstream/Response.py", s)["_make_flask_resp"]
        app = Flask(__name__)
        @app.route("/asset", methods=["GET", "HEAD"])
        def asset():
            from flask import request
            ctx = SimpleNamespace(resp_body=b"0123456789", resp_status=200,
                resp_headers={}, resp_ct="application/javascript", path="/asset")
            return make_response(ctx, request.method)
        client = app.test_client()
        self.assertEqual(client.get("/asset").headers["Content-Length"], "10")
        head = client.head("/asset")
        self.assertEqual(head.headers["Content-Length"], "10")
        self.assertEqual(head.data, b"")
        ctx = SimpleNamespace(resp_body=b"", resp_status=200,
            resp_headers={"content-length": "12345"}, resp_ct="application/javascript", path="/asset")
        self.assertEqual(make_response(ctx, "HEAD").headers["Content-Length"], "12345")


if __name__ == "__main__":
    unittest.main()
