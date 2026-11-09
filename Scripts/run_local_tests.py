"""Run every regression script in isolated processes, without external sites.

python -X utf8 -B Scripts/run_local_tests.py
Use --legacy-only to run the historical batteries only.
"""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "Scripts"


def is_unittest(path):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"))
    return any(isinstance(node, ast.ClassDef) and any(
        isinstance(base, ast.Attribute) and base.attr == "TestCase"
        for base in node.bases) for node in tree.body)


def run(path):
    command = [sys.executable, "-X", "utf8", "-B"]
    try:
        unit = is_unittest(path)
    except (SyntaxError, OSError) as exc:
        return path.name, 1, str(exc)
    if unit:
        command += ["-m", "unittest", "discover", "-s", "Scripts", "-p", path.name]
    else:
        command += [str(path)]
    try:
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True,
                                encoding="utf-8", timeout=120)
        return path.name, result.returncode, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return path.name, 124, "Timed out after 120 seconds"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-only", action="store_true")
    parser.add_argument("--jobs", type=int, choices=range(1, 5), default=3)
    args = parser.parse_args()
    paths = sorted(SCRIPTS.glob("test_*.py"))
    if args.legacy_only:
        paths = [path for path in paths if not is_unittest(path)]
    failed, skipped = [], []
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        for future in as_completed([pool.submit(run, path) for path in paths]):
            name, status, output = future.result()
            skip = status == 77 or (status == 0 and "skipped=" in output)
            if skip:
                skipped.append(name)
            if status not in (0, 77):
                failed.append(name)
            label = "FAIL" if status not in (0, 77) else "SKIP" if skip else "PASS"
            print(f"[{label}] {name}", flush=True)
            if status or skip:
                print(output[-8000:], flush=True)
    print(f"\n{len(paths)} scripts: {len(paths)-len(failed)-len(skipped)} passed, "
          f"{len(failed)} failed, {len(skipped)} with skips", flush=True)
    return bool(failed)


if __name__ == "__main__":
    sys.exit(main())
