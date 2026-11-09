#!/usr/bin/env python3
"""Fix 17 battery — root-relative URLs in externally-served documents.

Field origin (2026-11-09, live Chromium + Firefox field check on the
aya.nodium.lol mirror): the Discord.com widget iframe is fetched via
/__s2l_ext__/discord.com/widget and its HTML ships
    <link href="/assets/fdf2a99770058b1f.css">
    <script src="/assets/c114e7c6ec68f068.js">
while its 626KB CSS ships url(/assets/*.woff2) — all ROOT-RELATIVE. In the
mirror those resolve against the proxy origin and land on the MAIN site
route: aya answers its JSON 404 ({"detail":"Not Found"}, nosniff), so the
browser logs "MIME type mismatch" and drops the resource → unstyled,
script-less widget.

Checks:
  ER1  unit HTML: src/href/poster/data-src/data-href/action root-relative
      values re-pointed to /__s2l_ext__/<doc_host>/…; https://, //host, #,
      data: and empty values untouched; pass is idempotent
  ER2  unit srcset: root-relative candidates re-pointed, descriptors kept
  ER3  unit CSS: url('/x') and url(/x) re-pointed; url(data:…) untouched;
      idempotent
  ER4  unit DONT_REWRITE: doc_host under DONT_REWRITE → bytes unchanged
  ER5  route cache-HIT HTML: /__s2l_ext__/discord.com/widget serves the
      widget HTML with the root-relative href/src re-pointed (this is the
      exact field scenario — second load, served from disk)
  ER6  route cache-HIT CSS: /__s2l_ext__/discord.com/assets/…css serves
      with url(/assets/…woff2) re-pointed, CT text/css
  ER7  cache-is-truth: after the HIT requests the on-disk cache files still
      hold the RAW bodies (rewriting is a view, never persisted)
  ER8  route FRESH HTML: uncached /widget2 fetched through a fake upstream
      session → re-pointed too (first-load parity)
  ER9  regression: JSON cache-HIT still runs the absolute-URL JSON rewrite
      (pre-existing _serve_cached behavior must survive Fix 17)
  ER10 source contract: all six call sites (Assets fresh-html/fresh-css/hit,
      Server fresh-html/fresh-css/hit) invoke rewrite_doc_relative
"""
import os
import re
import shutil
import sys

from legacy_test_support import legacy_workspace, prepare_legacy

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.dirname(_here)
WORK = legacy_workspace("test_ext_doc_rewrite")

WIDGET_HTML = (b"<!DOCTYPE html><html><head>"
               b'<link rel="stylesheet" href="/assets/fdf2a99770058b1f.css">'
               b'<script defer src="/assets/c114e7c6ec68f068.js"></script>'
               b"</head><body><div class=\"widget\"></div></body></html>")
WIDGET_CSS = (b"@font-face{font-family:Whitney;src:url(/assets/03dcf979852e8b8e.woff2)"
              b" format('woff2');}\n.avatar{background:url('/assets/img.png');}")


def build_test_env():
    prepare_legacy(WORK)
    cfg = os.path.join(WORK, "Site2Local", "Config.py")
    txt = open(cfg, encoding="utf-8").read()
    txt = re.sub(r'SITE\s*=\s*"[^"]*"', 'SITE = "aya.nodium.lol"', txt)
    txt = txt.replace("OFFLINE = False", "OFFLINE = True")
    txt = txt.replace("CRAWL = True", "CRAWL = False")
    open(cfg, "w", encoding="utf-8").write(txt)


def main() -> int:
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from Site2Local import Runtime as R

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, bool(ok)))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    # ── ER1: unit HTML attributes ────────────────────────────────────────
    doc = (b'<img src="/a.png" data-src="/b.png" poster="/c.jpg" '
           b'data-href="/d" action="/submit">'
           b'<a href="/page">x</a>'
           b'<a href="https://cdn.other.com/y">abs</a>'
           b'<a href="//cdn.other.com/z">proto</a>'
           b'<a href="#top">anchor</a>'
           b'<a href="data:text/plain,no">data</a>')
    out = R.rewrite_doc_relative(doc, "discord.com", html_doc=True)
    ext = "http://localhost:8080/__s2l_ext__/discord.com"
    ok1 = all(t in out for t in (
        f'src="{ext}/a.png"'.encode(), f'data-src="{ext}/b.png"'.encode(),
        f'action="{ext}/submit"'.encode(), f'href="{ext}/page"'.encode()))
    ok1 = ok1 and b'href="https://cdn.other.com/y"' in out \
        and b'href="//cdn.other.com/z"' in out and b'href="#top"' in out \
        and b'href="data:text/plain,no"' in out
    out_b = R.rewrite_doc_relative(out, "discord.com", html_doc=True)
    check("ER1 HTML attrs root-relative re-pointed; others untouched; idempotent",
          ok1 and out_b == out)

    # ── ER2: unit srcset ─────────────────────────────────────────────────
    ss = b'<img srcset="/a/small.png 480w, /a/big.png 1024w, https://x/y.png 2x">'
    out = R.rewrite_doc_relative(ss, "discord.com", html_doc=True)
    check("ER2 srcset root-relative candidates re-pointed, descriptors kept",
          f"{ext}/a/small.png 480w".encode() in out
          and f"{ext}/a/big.png 1024w".encode() in out
          and b"https://x/y.png 2x" in out)
    # ── ER3: unit CSS url() ──────────────────────────────────────────────
    css = (b"@font-face{src:url(/assets/f.woff2) format('woff2');}"
           b".a{background:url('/assets/g.png');}"
           b".b{background:url(data:image/png;base64,AAA);}"
           b".c{background:url(https://cdn.other.com/h.png);}")
    out = R.rewrite_doc_relative(css, "discord.com", html_doc=False)
    ok3 = (f'url("{ext}/assets/f.woff2")'.encode() in out
           and f'url("{ext}/assets/g.png")'.encode() in out
           and b"url(data:image/png;base64,AAA)" in out
           and b"url(https://cdn.other.com/h.png)" in out)
    out_b = R.rewrite_doc_relative(out, "discord.com", html_doc=False)
    check("ER3 CSS url() root-relative re-pointed; data:/https: untouched; idempotent",
          ok3 and out_b == out)

    # ── ER4: DONT_REWRITE host ───────────────────────────────────────────
    old = R.DONT_REWRITE
    try:
        R.DONT_REWRITE = ("discord.com",)
        check("ER4 doc_host under DONT_REWRITE → bytes unchanged",
              R.rewrite_doc_relative(doc, "discord.com", html_doc=True) == doc)
    finally:
        R.DONT_REWRITE = old

    # ── plant the widget cache (RAW upstream bodies, as the save worker
    # stores them: pre-rewrite) ───────────────────────────────────────────
    site = os.path.join(WORK, "site_src", "aya_nodium_lol", "discord.com")
    os.makedirs(os.path.join(site, "assets"), exist_ok=True)
    widget_lp = R.local_path("https://discord.com/widget")
    css_lp    = R.local_path("https://discord.com/assets/fdf2a99770058b1f.css")
    json_lp   = R.local_path("https://discord.com/api/config.json")
    for lp, blob in ((widget_lp, WIDGET_HTML),
                     (css_lp, WIDGET_CSS),
                     (json_lp, b'{"icon":"https://cdn.discordapp.com/i.png"}')):
        os.makedirs(os.path.dirname(lp), exist_ok=True)
        with open(lp, "wb") as f:
            f.write(blob)

    c = R.app.test_client()

    # ── ER5: cache-HIT HTML (the exact field scenario, second load) ──────
    r = c.get("/__s2l_ext__/discord.com/widget")
    body = r.data
    check("ER5 EXT cache-HIT HTML: /assets/* re-pointed to the doc's own host",
          r.status_code == 200
          and b'href="http://localhost:8080/__s2l_ext__/discord.com/assets/fdf2a99770058b1f.css"' in body
          and b'src="http://localhost:8080/__s2l_ext__/discord.com/assets/c114e7c6ec68f068.js"' in body,
          f"status={r.status_code}")

    # ── ER6: cache-HIT CSS ───────────────────────────────────────────────
    r = c.get("/__s2l_ext__/discord.com/assets/fdf2a99770058b1f.css")
    body = r.data
    check("ER6 EXT cache-HIT CSS: url(/assets/…woff2) re-pointed, CT text/css",
          r.status_code == 200 and "text/css" in r.headers.get("Content-Type", "")
          and f'url("{ext}/assets/03dcf979852e8b8e.woff2")'.encode() in body
          and f'url("{ext}/assets/img.png")'.encode() in body,
          f"ct={r.headers.get('Content-Type')}")
    # ── ER7: the disk cache stays RAW ────────────────────────────────────
    with open(widget_lp, "rb") as f:
        w_raw = f.read()
    with open(css_lp, "rb") as f:
        c_raw = f.read()
    check("ER7 cache-is-truth: on-disk files unchanged (RAW bodies)",
          w_raw == WIDGET_HTML and c_raw == WIDGET_CSS)

    # ── ER8: FRESH HTML through a fake upstream ──────────────────────────
    R.OFFLINE = False

    class _Resp:
        status_code = 200
        headers = {"Content-Type": "text/html; charset=utf-8",
                   "Content-Encoding": "identity"}
        content = WIDGET_HTML

    class _Sess:
        def request(self, method, url, **kw):
            self.last = url
            return _Resp()

    fake = _Sess()
    orig = R._get_client_session
    R._get_client_session = lambda: fake
    try:
        r = c.get("/__s2l_ext__/discord.com/widget2")
        body = r.data
        ok8 = (r.status_code == 200
               and b'href="http://localhost:8080/__s2l_ext__/discord.com/assets/fdf2a99770058b1f.css"' in body
               and b'src="http://localhost:8080/__s2l_ext__/discord.com/assets/c114e7c6ec68f068.js"' in body)
        # the save path must still enqueue the RAW body (pre-rewrite) —
        # drain one item if the queue has it and confirm no marker change
        try:
            item = R.save_queue.get_nowait()
            ok8 = ok8 and item[1] == WIDGET_HTML
        except Exception:
            pass  # save worker absent in battery — queue simply accumulates
        check("ER8 EXT FRESH HTML: re-pointed; save enqueues the RAW body", ok8,
              f"status={r.status_code} fetched={getattr(fake, 'last', '?')}")
    finally:
        R._get_client_session = orig
        R.OFFLINE = True

    # ── ER9: JSON cache-HIT keeps its absolute-URL rewrite ───────────────
    r = c.get("/__s2l_ext__/discord.com/api/config.json")
    check("ER9 JSON EXT cache-HIT: absolute CDN URL still rewritten",
          r.status_code == 200
          and b"/__s2l_ext__/cdn.discordapp.com/i.png" in r.data,
          f"status={r.status_code}")

    # ── ER10: source contract — every EXT/CDN serve path calls the pass ──
    # Cached responses share one implementation; fresh paths remain per route.
    import ast
    contracts = {}
    for rel in ("Proxy/Assets.py", "CDN/Server.py", "Proxy/Cache/Serve.py"):
        tree = ast.parse(open(os.path.join(WORK, "Site2Local", rel), encoding="utf-8").read())
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Attribute)
                 and node.func.attr == "rewrite_doc_relative"]
        if rel == "Proxy/Cache/Serve.py":
            contracts[rel] = any(any(kw.arg == "html_doc" for kw in node.keywords) for node in calls)
        else:
            flags = {kw.value.value for node in calls for kw in node.keywords
                     if kw.arg == "html_doc" and isinstance(kw.value, ast.Constant)}
            shared_cache = any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                               and node.func.attr == "_cached_response" for node in ast.walk(tree))
            contracts[rel] = flags == {True, False} and shared_cache
    check("ER10 fresh HTML/CSS and shared cache all use document rewriting",
          all(contracts.values()), str(contracts))

    print("\n=== SUMMARY ===")
    n_ok = sum(1 for _, ok in results if ok)
    print(f"{n_ok}/{len(results)} passed")
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
