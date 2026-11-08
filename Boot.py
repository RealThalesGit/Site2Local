#!/usr/bin/env python3
"""Site2Local — simple launcher (lives INSIDE the Site2Local folder).
Run with:  python3 Site2Local/Boot.py     (from the parent folder)
or:  python3 Boot.py                (from inside Site2Local/)
Everything — code, mirror (site_src/), metadata (site_data/), wordlists —
lives in this one folder. (Equivalent to `python -m Site2Local` from the
parent folder; the monolith `python s2l.py` also still works standalone.)
"""
import os
import sys
# This file lives INSIDE the package folder, and the package is importable
# from its PARENT — so make sure the parent is on sys.path no matter which
# directory the user invokes Boot.py from.
_HERE = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
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
import Site2Local
if __name__ == "__main__":
    Site2Local.main()
