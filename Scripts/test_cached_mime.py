"""Offline regressions for cached JavaScript mistaken for CSS.

Run from the project root: python -m unittest discover -s Scripts -p test_cached_mime.py
Loads the production helpers without starting Runtime or contacting upstream.
"""
import ast
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import os
import runpy

from flask import Flask


ROOT = Path(__file__).resolve().parents[1]
CONFIG = runpy.run_path(str(ROOT / "Config.py"))
MEMORY = {"ATOMIC_SERVE_MAX_BYTES": CONFIG["ATOMIC_SERVE_MAX_BYTES"],
          "parse_size": CONFIG["parse_size"]}
memory_tree = ast.parse((ROOT / "Core/Cache/Memory.py").read_text(encoding="utf-8"))
memory_tree.body = [n for n in memory_tree.body
                    if not (isinstance(n, ast.ImportFrom) and n.level)]
exec(compile(memory_tree, "Core/Cache/Memory.py", "exec"), MEMORY)


def helpers(path, state):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8"))
    tree.body = [n for n in tree.body
                 if not (isinstance(n, ast.ImportFrom) and n.level)]
    namespace = {"_s2l_state": state, "read_cached_file": MEMORY["read_cached_file"]}
    exec(compile(tree, path, "exec"), namespace)
    return namespace


class CachedMimeTests(unittest.TestCase):
    def setUp(self):
        self.state = SimpleNamespace(
            _ct_base=lambda ct: ct.split(";", 1)[0].strip().lower(),
            _is_wire_payload=lambda data: False,
            _is_valid_html=lambda data: data.lstrip().lower().startswith(b"<!doctype html"),
            _apply_cors_headers=lambda *a, **k: None,
            _apply_isolation_headers=lambda *a, **k: None,
            _apply_security_headers=lambda *a, **k: None,
            _is_worker_request=lambda: False,
            _ensure_wasm_content_type=lambda ct, path: ct,
            PROXY_CDN=False,
        )
        self.mime = helpers("Core/MIME/Guess.py", self.state)
        self.state.guess_mime = self.mime["guess_mime"]
        self.state._resolve_mime_with_sniff = self.mime["_resolve_mime_with_sniff"]
        self.validators = helpers("Proxy/Cache/Validators.py", self.state)
        self.state._disk_validators = self.validators["_disk_validators"]
        self.serve = helpers("Proxy/Cache/Serve.py", self.state)
        self.app = Flask(__name__)

    def resolve(self, path, body):
        return self.state._ct_base(self.state._resolve_mime_with_sniff(path, body))

    def test_small_webpack_class_map(self):
        for prefix in (b'"use strict";', b'', b'/* license */'):
            body = prefix + b'(this.webpackChunkdiscord_app=this.webpackChunkdiscord_app||[]).push([[1],{1:(e)=>{e.exports={root:"root_1",item:"item_2",label:"label_3",icon:"icon_4"}}}]);'
            with self.subTest(prefix=prefix):
                self.assertIn(self.resolve("chunk.js", body),
                              ("application/javascript", "text/javascript"))

    def test_css_in_js_strings_is_not_a_stylesheet(self):
        for body in (b'load("@media (min-width:1px){.a{color:red}}");',
                     b'load(`@font-face{font-family:test;src:url(x)}`);',
                     b'load(".a{color:red;display:block;margin:0}");',
                     b'load("@media {.a{color:red}}' + b' ' * 3000):
            with self.subTest(body=body):
                self.assertFalse(self.mime["_sniff_css"](body))
                self.assertIn(self.resolve("chunk.js", body),
                              ("application/javascript", "text/javascript"))

    def test_real_css_and_mislabeled_resources_still_resolve(self):
        css = b'/* theme */ @media (min-width:1px){.a{color:red;display:block;margin:0}}'
        for path in ("theme.css", "theme.js", "theme"):
            with self.subTest(path=path):
                self.assertEqual(self.resolve(path, css), "text/css")
        self.assertEqual(self.resolve("data.js", b'{"items":[1,2,3]}'), "application/json")
        self.assertEqual(self.resolve("asset.js", b'\x00asm\x01\x00\x00\x00'), "application/wasm")

    def test_js_words_in_css_comments_and_strings_do_not_change_mime(self):
        for body in (
                b'/* function return window document */ .a{color:red;display:block;margin:0}',
                b'.a:before{content:"function return window document";color:red;display:block;margin:0}',
                b'@media screen {.a{color:red}} /* function return window document',
        ):
            for path in ("theme.css", "theme"):
                with self.subTest(path=path, body=body):
                    self.assertFalse(self.mime["_sniff_js"](body))
                    self.assertEqual(self.resolve(path, body), "text/css")
        js = b'const url="https://example.com"; function run(){return window.document;}'
        self.assertTrue(self.mime["_sniff_js"](js))

    def test_extensionless_css_and_json_rewrite_with_and_without_ram_cache(self):
        external = b'https://cdn.example.com'
        proxied = b'http://localhost:8081'
        self.state.PROXY_CDN = True
        self.state._rewrite_ext_urls = lambda data, **kw: data.replace(external, proxied)
        self.state._rewrite_json_urls = lambda data: data.replace(external, proxied)
        with TemporaryDirectory() as directory:
            for atomic in (False, True):
                self.state.ATOMIC_SERVE = atomic
                for name, body, mime in (
                        ("style", b'@import "https://cdn.example.com/theme.css";', "text/css"),
                        ("data", b'{"url":"https://cdn.example.com/image.png"}', "application/json"),
                        ("binary", b'\x89PNG\r\n\x1a\nraw', "image/png"),
                ):
                    with self.subTest(atomic=atomic, name=name):
                        path = Path(directory) / name
                        path.write_bytes(body)
                        for _ in range(2):
                            data, ct = self.serve["_serve_cached"](str(path))
                            self.assertEqual(self.state._ct_base(ct), mime)
                            self.assertEqual(data, body.replace(external, proxied))
                        self.assertEqual(path.read_bytes(), body)

    def test_disk_read_and_revalidation_recover_old_css_validator(self):
        body = b'"use strict";(this.webpackChunkdiscord_app=this.webpackChunkdiscord_app||[]).push([[1],{1:e=>{e.exports={root:"r",item:"i",icon:"x",label:"l"}}}]);'
        with TemporaryDirectory() as directory:
            path = Path(directory) / "330938.js"
            path.write_bytes(body)
            data, ct = self.serve["_serve_cached"](str(path))
            self.assertEqual(data, body)
            self.assertIn(self.state._ct_base(ct), ("application/javascript", "text/javascript"))
            old_etag, last_modified = self.state._disk_validators(str(path), "text/css; charset=utf-8")
            with self.app.test_request_context(headers={"If-None-Match": old_etag,
                                                        "If-Modified-Since": last_modified}):
                self.assertIsNone(self.validators["_conditional_304"](str(path), ct))
            etag, _ = self.state._disk_validators(str(path), ct)
            with self.app.test_request_context(headers={"If-None-Match": etag}):
                self.assertEqual(self.validators["_conditional_304"](str(path), ct).status_code, 304)

    def test_atomic_mode_avoids_repeat_reads_and_sniffs_and_detects_edits(self):
        self.state.ATOMIC_SERVE = True
        body = b'"use strict";load("@media {.a{color:red}}");'
        with TemporaryDirectory() as directory:
            path = Path(directory) / "chunk.js"
            path.write_bytes(body)
            resolve = self.state._resolve_mime_with_sniff
            first = resolve(str(path))
            with patch("builtins.open", side_effect=AssertionError("unexpected disk read")):
                self.assertEqual(resolve(str(path)), first)
            data, ct = self.serve["_serve_cached"](str(path))
            with patch("builtins.open", side_effect=AssertionError("unexpected disk read")):
                self.assertEqual(self.serve["_serve_cached"](str(path)), (data, ct))
            # Same-size edits invalidate too; force a distinct filesystem timestamp.
            old_stat = path.stat()
            path.write_bytes(b' ' * len(body))
            os.utime(path, ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000))
            self.assertEqual(self.serve["_serve_cached"](str(path))[0], b' ' * len(body))
            path.write_bytes(b'@media {.a{color:red}}')
            self.assertEqual(self.state._ct_base(resolve(str(path))), "text/css")
            path.unlink()
            self.assertIsNone(self.serve["_serve_cached"](str(path)))

    def test_atomic_mode_disabled_uses_regular_reads(self):
        self.state.ATOMIC_SERVE = False
        with TemporaryDirectory() as directory:
            path = Path(directory) / "asset.js"
            path.write_bytes(b'"use strict";run();')
            self.serve["_serve_cached"](str(path))
            with patch("builtins.open", side_effect=OSError("disk unavailable")):
                self.assertIsNone(self.serve["_serve_cached"](str(path)))

    def test_memory_cache_is_bounded_and_replacement_is_visible(self):
        with TemporaryDirectory() as directory:
            read = MEMORY["read_cached_file"]
            MEMORY["_entries"].clear()
            MEMORY["_bytes"] = 0
            with patch.dict(MEMORY, {"_MAX_BYTES": 8, "_MAX_ENTRIES": 2}):
                for name in ("a", "b", "c"):
                    path = Path(directory) / name
                    path.write_bytes(b"1234")
                    self.assertEqual(read(path), b"1234")
                self.assertLessEqual(MEMORY["_bytes"], 8)
                self.assertLessEqual(len(MEMORY["_entries"]), 2)
                replacement = Path(directory) / "new"
                replacement.write_bytes(b"5678")
                os.replace(replacement, path)
                self.assertEqual(read(path), b"5678")
                MEMORY["_entries"].clear()


if __name__ == "__main__":
    unittest.main()
