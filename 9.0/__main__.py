"""Run with python -m Site2Local (from the parent folder)."""
import os

if __name__ == "__main__":
    # Keep runtime data (site_src/, site_data/, wordlists) inside the
    # package folder — same behavior as Boot.py, which chdirs before
    # importing. Safe to do here: Runtime only touches the filesystem
    # from main() onwards.
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    from . import main
    main()
