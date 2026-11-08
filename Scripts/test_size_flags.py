#!/usr/bin/env python3
"""Byte-size flag battery for Site2Local 9.0 (Fix 12).

  S1  parse_size matrix — "512", "512B", "512BYTE", "5MB", "1GB", "1.5GB",
      "2 kb", "1KiB", case-insensitivity, plain-int passthrough, and loud
      ValueErrors for garbage ("banana"), negative ("-5MB") and unknown
      units ("5XB").
  S2  parity — every flag's runtime value equals the historical hardcoded
      expression (ZERO behavior change from the 256*1024 era).
  S3  everything is still an int at the consumer surface (Flask
      MAX_CONTENT_LENGTH, WS caps, stream thresholds, editor cap).
  S4  Config.py itself now uses human literals (no "* 1024" arithmetic
      left in the user-editable file).
  S5  parse_size is re-exported through Runtime.
"""
import os
import re
import shutil
import sys

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_sizes"


def build_test_env():
    if os.path.isdir(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    shutil.copytree(SRC_TREE, os.path.join(WORK, "Site2Local"),
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    cfg = os.path.join(WORK, "Site2Local", "Config.py")
    txt = open(cfg, encoding="utf-8").read()
    txt = re.sub(r'SITE\s*=\s*"[^"]*"', 'SITE = "aya.nodium.lol"', txt)
    txt = txt.replace("OFFLINE = False", "OFFLINE = True")
    txt = txt.replace("CRAWL = True", "CRAWL = False")
    open(cfg, "w", encoding="utf-8").write(txt)


def main():
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from Site2Local import Runtime as R

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    has_parser = hasattr(R, "parse_size")

    # ── S1: parse matrix ──────────────────────────────────────────────────
    if has_parser:
        p = R.parse_size
        cases = [
            ("512", 512), ("512B", 512), ("512BYTE", 512), ("512BYTES", 512),
            ("512 byte", 512), ("5MB", 5 * 1024 * 1024), ("5mb", 5 * 1024 * 1024),
            ("1GB", 1024 ** 3), ("1.5GB", round(1.5 * 1024 ** 3)),
            ("2 kb", 2048), ("1KiB", 1024), ("256KB", 256 * 1024),
            ("1TB", 1024 ** 4), (4096, 4096),
        ]
        bad = [(v, p(v), want) for v, want in cases if p(v) != want]
        errs = []
        for bad_v in ("banana", "-5MB", "5XB", "", "MB"):
            try:
                p(bad_v)
                errs.append(bad_v)      # no error = FAIL
            except ValueError:
                pass
        check("S1 parse_size matrix (units, case, floats, ints, errors)",
              not bad and not errs,
              f"wrong={bad} missing-error={errs}")
    else:
        check("S1 parse_size matrix (units, case, floats, ints, errors)",
              False, "parse_size not exported by Runtime")

    # ── S2: parity with the historical values ─────────────────────────────
    parity = [
        ("SCAN_LIMIT",             R.SCAN_LIMIT,             256 * 1024),
        ("WS_MAX_MSG_BYTES",       R.WS_MAX_MSG_BYTES,       8 * 1024 * 1024),
        ("MAX_BODY_BYTES",         R.MAX_BODY_BYTES,         256 * 1024 * 1024),
        ("MAX_DECOMPRESSED_BYTES", R.MAX_DECOMPRESSED_BYTES, 512 * 1024 * 1024),
        ("STREAM_CACHE_MAX_BYTES", R.STREAM_CACHE_MAX_BYTES, 256 * 1024 * 1024),
        ("_EDITOR_MAX_BYTES",      R._EDITOR_MAX_BYTES,      512 * 1024),
        ("_STREAM_MIN_BYTES",      R._STREAM_MIN_BYTES,      5 * 1024 * 1024),
    ]
    bad = [(n, got, want) for n, got, want in parity if got != want]
    check("S2 zero behavior change vs the 256*1024-era values", not bad,
          f"mismatches={bad}" if bad else "all 7 flags identical")

    # ── S3: consumer surface sees ints ────────────────────────────────────
    vals = [v for _, v, _ in parity]
    check("S3 all size flags are ints at the consumer surface",
          all(isinstance(v, int) and not isinstance(v, bool) for v in vals))

    # ── S4: Config.py uses human literals ─────────────────────────────────
    cfg_src = open(os.path.join(WORK, "Site2Local", "Config.py"),
                   encoding="utf-8").read()
    flags = re.findall(
        r"^(SCAN_LIMIT|WS_MAX_MSG_BYTES|MAX_BODY_BYTES|MAX_DECOMPRESSED_BYTES"
        r"|STREAM_CACHE_MAX_BYTES)\s*=\s*(.+)$", cfg_src, re.M)
    # the flag VALUE (before any trailing comment) must be a pure string
    # literal — the *1024 arithmetic era is over in the user-editable file
    pure_str = []
    for name, raw in flags:
        val = raw.split("#", 1)[0].strip()
        pure_str.append(bool(re.fullmatch(r'"[^"]+"', val)))
    check("S4 Config.py: the 5 flags are pure human string literals",
          len(flags) == 5 and all(pure_str),
          f"flags={[(n, v.split('#')[0].strip()) for n, v in flags]}")

    # ── S5: re-export present ─────────────────────────────────────────────
    check("S5 parse_size re-exported via Runtime", has_parser)

    passed = sum(1 for _, ok in results if ok)
    print(f"\n=== SUMMARY ===\n{passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
