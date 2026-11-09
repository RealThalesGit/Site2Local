#!/usr/bin/env python3
"""Site2Local launcher, independent of the project directory's name.
Run with: python Boot.py, or python /path/to/project/Boot.py.
Everything — code, mirror (site_src/), metadata (site_data/), wordlists —
lives in this one folder. (Equivalent to `python -m Site2Local` from the
parent folder; the monolith `python s2l.py` also still works standalone.)
"""
import importlib.util
import os
import sys
# Locate the package beside this launcher, regardless of the working directory.
_HERE = os.path.dirname(os.path.abspath(__file__))
# Make every relative path the project uses (site_src/, site_data/,
# Wordlists/, hook stores) resolve against this folder, no matter which
# directory the user invokes Boot.py from.
if os.getcwd() != _HERE:
    os.chdir(_HERE)
# Importing the package assembles the shared state (Runtime), the Flask app
# and preserves the historical `import s2l` API (the monolith sits next to
# this file, so it is importable when running from inside the folder). The
# PYTHONWARNINGS resource-tracker suppression inside Runtime runs during
# this import, exactly like it does for `python -m Site2Local`.
# Keep a stable package identity while locating its code by this file's path.
# This also supports directory names containing spaces, dots or hyphens.
if "Site2Local" not in sys.modules:
    _spec = importlib.util.spec_from_file_location(
        "Site2Local", os.path.join(_HERE, "__init__.py"),
        submodule_search_locations=[_HERE],
    )
    _package = importlib.util.module_from_spec(_spec)
    sys.modules[_spec.name] = _package
    try:
        _spec.loader.exec_module(_package)
    except BaseException:
        sys.modules.pop(_spec.name, None)
        raise
Site2Local = sys.modules["Site2Local"]
if __name__ == "__main__":
    Site2Local.main()
