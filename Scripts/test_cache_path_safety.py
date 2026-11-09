"""Cache paths must stay inside their host directory for literal dot segments."""
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from test_cached_mime import helpers


class CachePathSafetyTests(unittest.TestCase):
    def test_dot_segments_cannot_escape_host_directory(self):
        with TemporaryDirectory() as directory:
            state = SimpleNamespace(MAX_FNAME=180, SRC_FOLDER=directory,
                                    _FS_NAME_MAX=240, _QUERY_DIR="__query__",
                                    _QS_SAFE_RE=re.compile(r"[A-Za-z0-9_\-=&,+~@]"),
                                    _QS_PCT_RE=re.compile(r"%[0-9A-Fa-f]{2}"),
                                    normalize_url=lambda url: url)
            naming = helpers("Core/Cache/Naming.py", state)
            state._safe_seg = naming["_safe_seg"]
            state._beautify_query_name = naming["_beautify_query_name"]
            state._cap_query_fname = naming["_cap_query_fname"]
            state._encode_query_for_filename = naming["_encode_query_for_filename"]
            paths = helpers("Core/Cache/Paths.py", state)
            root = (Path(directory) / "example.com").resolve()
            for path in ("/../outside.js", "/a/../../../outside.js", "/./asset.js",
                         "/.../asset.js", "/a/../../../asset.js?v=1",
                         "/a/../../../asset?v=1"):
                with self.subTest(path=path):
                    result = Path(paths["local_path"]("https://example.com" + path)).resolve()
                    self.assertTrue(result.is_relative_to(root), str(result))
            self.assertEqual(state._safe_seg("normal-file.js"), "normal-file.js")
            self.assertNotIn(state._safe_seg(".."), (".", "..", ""))
            for authority in ("..", ".", "[::1]", "[2001:db8::1]"):
                with self.subTest(authority=authority):
                    result = Path(paths["local_path"](f"https://{authority}/file.js")).resolve()
                    self.assertTrue(result.is_relative_to(Path(directory).resolve()))
                    self.assertNotEqual(result.parent, Path(directory).resolve())


if __name__ == "__main__":
    unittest.main()
