#!/usr/bin/env python3
"""Access-log query-string battery (log-only enhancement, no version bump).

Field complaint (2026-11-08): N cache hits on the same URL path with different
query strings log IDENTICALLY —
    07:49:46 [←] GET HIT aya.nodium.lol/asset/ 25.3KB
    07:49:46 [←] GET HIT aya.nodium.lol/asset/ 8.9KB   (was ?exemplo=1)
    07:49:46 [←] GET HIT aya.nodium.lol/asset/ 6.1KB   (was ?exemplo2=3)
— so you cannot tell which __query__ variant was served. Desired shape:
    07:49:46 [←] GET HIT aya.nodium.lol/asset/?exemplo=1 8.9KB
Lines WITHOUT a query must stay byte-identical to the old format.

Covers every access-log shape: HIT 200 / 304 / 206 on the main proxy, the
no-extension __query__ layout, the [EXT] route (HIT + offline 404), and the
_fmt_path_qs() unit matrix (explicit qs, empty qs, outside request context,
live-request default). MISS/→/CDN-live/STREAM/SSE shapes share the same
helper and pass the query explicitly (verified by code review — they need a
live upstream, which an OFFLINE battery cannot drive).
"""
import contextlib
import io
import os
import shutil
import sys

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_logtest"

JS_PLAIN = b"// no-query build\n" + b"console.log('plain');\n" * 60
JS_A     = b"// variant aaa111\n" + b"console.log('aaa');\n" * 40
JS_B     = b"// variant bbb222\n" + b"console.log('bbb');\n" * 20


def build_test_env():
    if os.path.isdir(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    shutil.copytree(SRC_TREE, os.path.join(WORK, "Site2Local"),
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    cfg = os.path.join(WORK, "Site2Local", "Config.py")
    txt = open(cfg, encoding="utf-8").read()
    txt = txt.replace('SITE = "jeskojets.com"', 'SITE = "aya.nodium.lol"')
    txt = txt.replace("OFFLINE = False", "OFFLINE = True")
    txt = txt.replace("CRAWL = True", "CRAWL = False")
    open(cfg, "w", encoding="utf-8").write(txt)

    host = os.path.join(WORK, "site_src", "aya_nodium_lol", "aya.nodium.lol")
    # no-query cache file (plain layout)
    os.makedirs(os.path.join(host, "asset"), exist_ok=True)
    with open(os.path.join(host, "asset", "app.js"), "wb") as f:
        f.write(JS_PLAIN)
    # beautified __query__ variants of the SAME path (9.2 corrected layout)
    os.makedirs(os.path.join(host, "__query__", "asset"), exist_ok=True)
    with open(os.path.join(host, "__query__", "asset", "app_v_aaa111.js"), "wb") as f:
        f.write(JS_A)
    with open(os.path.join(host, "__query__", "asset", "app_v_bbb222.js"), "wb") as f:
        f.write(JS_B)
    # no-extension __query__ layout (the /data?id=7 binary-endpoint family)
    with open(os.path.join(host, "__query__", "data_id_7"), "wb") as f:
        f.write(b'{"id": 7, "items": ["a", "b", "c"], "ok": true}')


def main():
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from Site2Local import Runtime as R

    c = R.app.test_client()
    FF = "Mozilla/5.0 (X11; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0"
    results = []

    def check(tag, ok, detail=""):
        results.append((tag, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    def lastline(out):
        lines = [ln for ln in out.splitlines() if ln.strip()]
        return lines[-1] if lines else "<no log>"

    def get(url, headers=None):
        """GET + capture the console log lines this request produced."""
        h = {"User-Agent": FF}
        if headers:
            h.update(headers)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r = c.get(url, headers=h)
        return r, buf.getvalue()

    # ── L1: no-query HIT — line must NOT grow a '?' (old format preserved) ──
    r, out = get("/asset/app.js")
    check("L1 no-query HIT line unchanged (no '?')",
          r.status_code == 200
          and "GET HIT aya.nodium.lol/asset/app.js " in out
          and "app.js?" not in out,
          lastline(out))

    # ── L2/L3: query-variant hits are distinguishable from each other ──────
    r, out = get("/asset/app.js?v=aaa111")
    check("L2 HIT shows ?v=aaa111",
          r.status_code == 200
          and "GET HIT aya.nodium.lol/asset/app.js?v=aaa111 " in out,
          lastline(out))
    r, out = get("/asset/app.js?v=bbb222")
    check("L3 HIT shows ?v=bbb222",
          r.status_code == 200
          and "GET HIT aya.nodium.lol/asset/app.js?v=bbb222 " in out,
          lastline(out))

    # ── L4: no-extension __query__ layout (binary endpoint + query) ────────
    r, out = get("/data?id=7")
    check("L4 no-ext HIT shows ?id=7",
          r.status_code == 200
          and "GET HIT aya.nodium.lol/data?id=7 " in out,
          lastline(out))

    # ── L5: conditional 304 revalidation keeps the query ────────────────────
    r1, _ = get("/asset/app.js?v=aaa111")
    etag = r1.headers.get("ETag", "")
    r2, out = get("/asset/app.js?v=aaa111", headers={"If-None-Match": etag})
    check("L5 HIT 304 shows query",
          r2.status_code == 304
          and "HIT 304 aya.nodium.lol/asset/app.js?v=aaa111" in out,
          f"{r2.status_code} {lastline(out)}")

    # ── L6: 206 partial-content serve keeps the query ──────────────────────
    r, out = get("/asset/app.js?v=aaa111", headers={"Range": "bytes=0-99"})
    check("L6 HIT 206 shows query",
          r.status_code == 206
          and "GET HIT aya.nodium.lol/asset/app.js?v=aaa111 206" in out,
          f"{r.status_code} {lastline(out)}")

    # ── L7: [EXT] route HIT (fonts css2 family) shows the query ────────────
    fonts_url = "https://fonts.googleapis.com/css2?family=Test:wght@400&display=swap"
    lp = R.local_path(fonts_url)          # beautified name, no legacy present
    os.makedirs(os.path.dirname(lp), exist_ok=True)
    with open(lp, "wb") as f:
        f.write(b"body{font-family:Test,sans-serif;font-weight:400}")
    r, out = get("/__s2l_ext__/fonts.googleapis.com/css2?family=Test:wght@400&display=swap")
    check("L7 [EXT] HIT shows query",
          r.status_code == 200
          and "[EXT] GET HIT fonts.googleapis.com/css2?family=Test:wght@400&display=swap" in out,
          f"{r.status_code} {lastline(out)}")

    # ── L8: [EXT] offline 404 (not cached) shows the query too ─────────────
    r, out = get("/__s2l_ext__/cdn.example.net/x.css?token=zzz9")
    check("L8 EXT 404 shows query",
          r.status_code == 404
          and "EXT GET 404 cdn.example.net/x.css?token=zzz9" in out,
          f"{r.status_code} {lastline(out)}")

    # ── L9: _fmt_path_qs unit matrix ────────────────────────────────────────
    F = getattr(R, "_fmt_path_qs", None)
    try:
        unit_ok = (F is not None
                   and F("/a/b", "k=1") == "/a/b?k=1"
                   and F("/a/b", "") == "/a/b"
                   and F("/a/b") == "/a/b")          # outside request context
    except Exception as e:
        unit_ok = False
    check("L9 _fmt_path_qs unit (explicit/empty/outside-ctx)",
          unit_ok, "missing" if F is None else "ok")

    # ── L10: _fmt_path_qs defaults to the LIVE request's query ─────────────
    live_ok = False
    if F is not None:
        try:
            with R.app.test_request_context("/a/b?k=1"):
                live_ok = F("/a/b") == "/a/b?k=1"
        except Exception:
            live_ok = False
    check("L10 _fmt_path_qs reads the live request query", live_ok)

    print("\n=== SUMMARY ===")
    fails = [t for t, ok in results if not ok]
    print(f"{len(results)-len(fails)}/{len(results)} passed")
    if fails:
        print("FAILING:", ", ".join(fails))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
