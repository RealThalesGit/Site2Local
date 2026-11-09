"""Offline checks for console encoding, byte ranges and SSE headers."""
import io
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from test_cached_mime import helpers


class ReviewRegressions(unittest.TestCase):
    def test_console_encoding(self):
        for encoding in ("cp1252", "ascii", "utf-8"):
            for progress, rainbow in ((False, False), (True, False), (True, True), (False, True)):
                with self.subTest(encoding=encoding, progress=progress, rainbow=rainbow):
                    raw = io.BytesIO()
                    stream = io.TextIOWrapper(raw, encoding=encoding)
                    state = SimpleNamespace(
                        _LEVEL_COLOR={}, _log_lock=threading.Lock(),
                        RAINBOW_LOGS=rainbow, _TQDM_OK=progress,
                        _active_scan_pbar=object() if progress else None,
                        _lolcat_real=lambda line: None,
                        _lolcat_internal=lambda line: line + " \u2605",
                        _tqdm=SimpleNamespace(write=lambda line, file: print(line, file=file)))
                    log = helpers("Core/Logging/Logger.py", state)["log"]
                    with patch("sys.stdout", stream), patch("sys.stderr", stream):
                        log("request \u2192 response \U0001f680", "\u2190")
                    stream.flush()
                    text = raw.getvalue().decode(encoding)
                    self.assertIn("response", text)
                    self.assertIn("\U0001f680" if encoding == "utf-8" else "\\U0001f680", text)
                    stream.close()

    def test_invalid_ranges(self):
        parse = helpers("Proxy/Cache/Range.py", SimpleNamespace())["_parse_range_header"]
        for value in ("bytes=0-2,5-7", "bytes=0-2junk", "bytes=\u0660-2",
                      "bytes=-0", "bytes=10-", "bytes=5-3", "bytes=-",
                      "bytes=0-2\nother", "bytes=" + "9" * 5000 + "-"):
            with self.subTest(value=value):
                self.assertIsNone(parse(value, 10))
        self.assertEqual(parse("bytes=-3", 10), (7, 9))
        self.assertEqual(parse("bytes=0-99", 10), (0, 9))
        self.assertEqual(parse("bytes=5-", 10), (5, 9))
        self.assertIsNone(parse("bytes=0-1", 0))

    def test_sse_logs_actual_upstream_status(self):
        calls = []
        state = SimpleNamespace(filter_resp=dict, _resp_headers_dict=lambda r: r.headers,
                                log_req=lambda *a, **k: calls.append(a))
        build = helpers("HTTP/Streaming/SSE.py", state)["_stream_sse_resp"]
        upstream = SimpleNamespace(status_code=503, headers={"Content-Type": "text/event-stream"})
        response = build(upstream, "GET", "https://example.com/events")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(calls[0][1], 503)

    def test_sse_removes_content_length_in_every_casing(self):
        state = SimpleNamespace(filter_resp=dict,
                                _resp_headers_dict=lambda r: r.headers,
                                log_req=lambda *a, **k: None)
        build = helpers("HTTP/Streaming/SSE.py", state)["_stream_sse_resp"]
        for key in ("Content-Length", "content-length", "CONTENT-LENGTH"):
            with self.subTest(key=key):
                upstream = SimpleNamespace(status_code=200, headers={
                    "Content-Type": "text/event-stream", key: "12",
                    "cache-control": "public, max-age=3600"})
                response = build(upstream, "GET", "https://example.com/events")
                self.assertNotIn("Content-Length", response.headers)
                self.assertEqual(response.headers.getlist("Cache-Control"),
                                 ["no-cache, no-transform"])


if __name__ == "__main__":
    unittest.main()
