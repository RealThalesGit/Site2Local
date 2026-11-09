"""Small deterministic upstream for manual and live proxy tests.

Run: python Scripts/local_test_server.py --port 9090
"""
import argparse
from collections import Counter
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import threading
from urllib.parse import parse_qs, urlsplit


class TestHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.respond()

    def do_HEAD(self):
        self.respond()

    def do_POST(self):
        self.respond()

    def respond(self):
        parsed = urlsplit(self.path)
        with self.server.hits_lock:
            self.server.hits[parsed.path] += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        status, headers = 200, {}
        content_type = "application/json"
        if parsed.path == "/":
            content_type = "text/html; charset=utf-8"
            payload = b'<!doctype html><title>Site2Local live test</title><link rel="stylesheet" href="/style.css"><h1>Live upstream</h1><script src="/app.js"></script>'
        elif parsed.path == "/app.js":
            content_type, payload = "application/javascript", b'window.liveFixture = true;'
        elif parsed.path == "/style.css":
            content_type, payload = "text/css", b'body{background:#eef;color:#123}'
        elif parsed.path == "/redirect":
            status, payload = 302, b""
            headers["Location"] = "/app.js"
        elif parsed.path == "/blocked-redirect":
            status, payload = 302, b""
            headers["Location"] = f"http://localhost:{self.server.server_port}/private"
        elif parsed.path == "/login" and self.command == "POST":
            user = parse_qs(body.decode()).get("user", ["guest"])[0]
            cookie = SimpleCookie()
            cookie["fixture_user"] = user
            headers["Set-Cookie"] = cookie["fixture_user"].OutputString() + "; Path=/; HttpOnly"
            payload = json.dumps({"user": user}).encode()
        elif parsed.path == "/echo":
            cookie = SimpleCookie()
            cookie.load(self.headers.get("Cookie", ""))
            payload = json.dumps({"method": self.command, "body": body.decode(errors="replace"),
                                  "cookies": {key: value.value for key, value in cookie.items()}}).encode()
        elif parsed.path == "/private":
            payload = b'{"private":"must not be reached through redirect"}'
        else:
            status, payload = 404, b'{"error":"not found"}'
        if parsed.path == "/app.js" and self.headers.get("Range"):
            match = re.fullmatch(r"bytes=([0-9]+)-([0-9]*)", self.headers["Range"])
            size = len(payload)
            if match:
                start = int(match[1])
                end = min(int(match[2]) if match[2] else size - 1, size - 1)
                if 0 <= start <= end < size:
                    status = 206
                    headers["Content-Range"] = f"bytes {start}-{end}/{size}"
                    payload = payload[start:end + 1]
                else:
                    status, payload = 416, b""
                    headers["Content-Range"] = f"bytes */{size}"
            headers["Accept-Ranges"] = "bytes"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)


def create_server(port=0):
    server = ThreadingHTTPServer(("127.0.0.1", port), TestHandler)
    server.daemon_threads = True
    server.hits, server.hits_lock = Counter(), threading.Lock()
    return server


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9090)
    args = parser.parse_args()
    with create_server(args.port) as server:
        print(f"Local test upstream: http://localhost:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
