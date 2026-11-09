"""Regression checks for launching a renamed checkout from another directory."""
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]


class LauncherTests(unittest.TestCase):
    def test_directory_names_and_relative_imports(self):
        with tempfile.TemporaryDirectory() as tmp:
            parent = pathlib.Path(tmp)
            # A decoy package must not override the checkout being launched.
            decoy = parent / "Site2Local"
            decoy.mkdir()
            (decoy / "__init__.py").write_text("raise RuntimeError('wrong package')")
            for name in ("S2L", "my proxy-9.0", "renamed_project"):
                with self.subTest(name=name):
                    project = parent / name
                    project.mkdir()
                    shutil.copy2(ROOT / "Boot.py", project / "Boot.py")
                    (project / "Config.py").write_text("VALUE = 42\n")
                    (project / "__init__.py").write_text(
                        "from .Config import VALUE\n"
                        "import os\n"
                        "from pathlib import Path\n"
                        "def main():\n"
                        "    assert VALUE == 42\n"
                        "    assert Path.cwd() == Path(__file__).parent\n"
                        "    print('launcher OK')\n"
                    )
                    result = subprocess.run(
                        [sys.executable, str(project / "Boot.py")],
                        cwd=parent, capture_output=True, text=True, timeout=15,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("launcher OK", result.stdout)


if __name__ == "__main__":
    unittest.main()
