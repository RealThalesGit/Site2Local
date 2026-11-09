"""Actual upstream -> outbound guard -> Site2Local -> HTTP client integration.

Runs the real launcher with OFFLINE=False in an isolated temporary checkout.
Run: python -X utf8 -B -m unittest discover -s Scripts -p test_live_proxy.py
"""
from pathlib import Path
import shutil
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
import time
import unittest

import requests
from local_test_server import create_server, create_test_tls_context

ROOT = Path(__file__).resolve().parents[1]


class LiveProxyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = TemporaryDirectory()
        cls.addClassCleanup(cls.directory.cleanup)
        checkout = Path(cls.directory.name) / "Renamed Live Proxy"
        checkout.mkdir()
        cls.cache_root = checkout / "site_src"
        for path in ROOT.iterdir():
            if path.is_file() and path.suffix == ".py":
                shutil.copy2(path, checkout / path.name)
            elif path.is_dir() and (path / "__init__.py").exists():
                shutil.copytree(path, checkout / path.name,
                                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        tls_context = create_test_tls_context() if getattr(cls, "HTTPS_UPSTREAM", False) else None
        cls.upstream = create_server(tls_context=tls_context)
        threading.Thread(target=cls.upstream.serve_forever, daemon=True).start()
        cls.addClassCleanup(cls.upstream.server_close)
        cls.addClassCleanup(cls.upstream.shutdown)
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        cls.base = f"http://127.0.0.1:{port}"
        scheme = "https" if tls_context is not None else "http"
        overrides = dict(SITE=f"{scheme}://127.0.0.1:{cls.upstream.server_port}", HOST="127.0.0.1", PORT=port,
                         OFFLINE=False, CRAWL=False, HOOK_GUI=False, PROXY_CDN=False,
                         MULTIPORT=False, TLS_VERIFY=True, CAPTURE=False, CAPTURE_WS=False,
                         SCAN_PATHS=False, TIMEOUT_CONN=1, TIMEOUT_READ=3,
                         OUTBOUND_PRIVATE_ALLOWLIST=[f"127.0.0.1:{cls.upstream.server_port}"])
        overrides.update(getattr(cls, "CONFIG_OVERRIDES", {}))
        with (checkout / "Config.py").open("a", encoding="utf-8") as config:
            config.write("\n# Isolated live test overrides\n")
            for key, value in overrides.items():
                config.write(f"{key} = {value!r}\n")
        log_path = Path(cls.directory.name) / "proxy.log"
        log = log_path.open("w", encoding="utf-8")
        cls.addClassCleanup(log.close)
        # Disable optional active WAF fingerprinting, not upstream fetching.
        script = "import runpy,sys,types; m=types.ModuleType('wafw00f.main'); m.WAFW00F=None; sys.modules['wafw00f.main']=m; runpy.run_path('Boot.py',run_name='__main__')"
        cls.process = subprocess.Popen([sys.executable, "-X", "utf8", "-B", "-u", "-c", script],
                                       cwd=checkout, stdout=log, stderr=subprocess.STDOUT)
        def stop():
            if cls.process.poll() is None:
                cls.process.terminate()
                try:
                    cls.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    cls.process.kill()
                    cls.process.wait(timeout=5)
        cls.addClassCleanup(stop)
        cls.client = requests.Session()
        cls.client.trust_env = False
        cls.addClassCleanup(cls.client.close)
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline and cls.process.poll() is None:
            try:
                if cls.client.get(cls.base + "/.__s2l_hp", timeout=0.5).status_code == 200:
                    return
            except requests.RequestException:
                pass
            time.sleep(0.1)
        raise RuntimeError("Live proxy failed to start:\n" + log_path.read_text(encoding="utf-8"))

    def test_live_html_assets_and_redirect(self):
        response = self.client.get(self.base + "/", timeout=8)
        self.assertEqual(response.status_code, 200)
        self.assertIn("Live upstream", response.text)
        self.assertGreater(self.upstream.hits["/"], 0)
        for path, marker in (("/app.js", "liveFixture"), ("/style.css", "background"),
                             ("/redirect", "liveFixture")):
            with self.subTest(path=path):
                response = self.client.get(self.base + path, timeout=8)
                self.assertEqual(response.status_code, 200)
                self.assertIn(marker, response.text)
                self.assertGreater(self.upstream.hits[path], 0)

    def test_live_browser_sessions_and_post_body(self):
        with requests.Session() as one, requests.Session() as two:
            one.trust_env = two.trust_env = False
            for client, user in ((one, "alice"), (two, "bob")):
                response = client.post(self.base + "/login", data={"user": user}, timeout=8)
                self.assertEqual(response.status_code, 200)
            for client, user in ((one, "alice"), (two, "bob")):
                response = client.post(self.base + "/echo", data=b"live payload", timeout=8)
                self.assertEqual(response.status_code, 200)
                data = response.json()
                self.assertEqual(data["body"], "live payload")
                self.assertEqual(data["cookies"].get("fixture_user"), user)
                self.assertNotIn("__s2l_browser", data["cookies"])

    def test_live_private_redirect_is_blocked(self):
        response = self.client.get(self.base + "/blocked-redirect", timeout=8)
        self.assertEqual(response.status_code, 403)
        self.assertGreater(self.upstream.hits["/blocked-redirect"], 0)
        self.assertEqual(self.upstream.hits["/private"], 0)

    def test_private_html_never_crosses_browser_sessions_or_hits_disk(self):
        with requests.Session() as alice, requests.Session() as bob:
            for client, user in ((alice, "alice"), (bob, "bob")):
                client.trust_env = False
                client.headers["Sec-Fetch-Dest"] = "empty"
                client.post(self.base + "/login", data={"user": user}, timeout=8).raise_for_status()
            for client, user in ((alice, "alice"), (bob, "bob")):
                response = client.get(self.base + "/private-profile.html", timeout=8)
                self.assertEqual(response.status_code, 200)
                self.assertIn("PRIVATE PROFILE: " + user, response.text)
            time.sleep(0.4)  # Allow the save worker's batch interval to pass.
            self.assertFalse(list(self.cache_root.rglob("private-profile.html")))

    def test_live_cache_range_and_head(self):
        body = self.client.get(self.base + "/app.js", timeout=8).content
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline and not list(self.cache_root.rglob("app.js")):
            time.sleep(0.05)
        self.assertTrue(list(self.cache_root.rglob("app.js")), "Save worker did not write the live asset")
        before = self.upstream.hits["/app.js"]
        cached = self.client.get(self.base + "/app.js", timeout=8)
        self.assertEqual(cached.content, body)
        self.assertEqual(self.upstream.hits["/app.js"], before)
        response = self.client.get(self.base + "/app.js", headers={"Range": "bytes=0-5"}, timeout=8)
        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, body[:6])
        response = self.client.head(self.base + "/app.js", timeout=8)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"")
        self.assertEqual(int(response.headers["Content-Length"]), len(body))
        # Online Range deliberately goes upstream; HEAD uses the disk copy.
        self.assertEqual(self.upstream.hits["/app.js"], before + 1)


if __name__ == "__main__":
    unittest.main()
