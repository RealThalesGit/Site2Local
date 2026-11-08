#!/usr/bin/env python3
"""Full E2E + unit battery for Site2Local 9.0 ("new generation" contract).

Uses the REAL files downloaded from aya.nodium.lol (+ Google Fonts CSS) to
reproduce the field scenarios from the 2026-11-07/08 test sessions:

  - aya.nodium.lol  : WebAssembly.Memory DOMException (COOP/COEP isolation)
  - marketplace     : Google Fonts css2 served as octet-stream (Fix 6R)
  - lying-extension : .js file that is actually CSS (Fix 5 family, unified)
  - __query__ names : beautified layout (every param joins NAME_VALUE) with
                      .q sidecar collision guard
  - 9.0 CUT         : the migration bridges are GONE — pre-9.1 / 9.1a /
                      09/10 caches are NOT renamed and NOT served; the
                      startup sweep deletes them as unreachable garbage
                      (liveness proven via local_path; provable-safe keeps:
                      live pretty files, collision fallbacks, orphan .q
                      cleanup, capped-encoding guard, idempotence)
  - ETag CT crc32   : MIME auto-heal via validator change
  - worklets / UA-dependent COEP value (credentialless vs require-corp)
  - filename caps   : post-join _cap_query_fname keeps overlong names writable
"""
import os, re, shutil, sys

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_test"
# Fixtures resolve in this order: S2L_AYA_FIXTURES env var → the packed
# scripts/fixtures/ folder shipped next to this battery → the maintainer's
# /tmp/aya drop. Needs: index.html, app.css, fonts.css, Aya.App.js, and
# Aya.App.wasm (only its first 2 MiB are consumed).
_fixdir  = os.path.join(_here, "fixtures")
AYA      = os.environ.get("S2L_AYA_FIXTURES",
                          _fixdir if os.path.isdir(_fixdir) else "/tmp/aya")

FONTS_URL = "https://fonts.googleapis.com/css2?family=Montserrat%3Awght@400%3B500%3B600%3B700&display=swap"
FONTS_QS  = "family=Montserrat%3Awght@400%3B500%3B600%3B700&display=swap"
# the same URL the way the browser sends it (percent-encoded) — the ext
# route and the Range check use this form too
FONTS_EXT = "/__s2l_ext__/fonts.googleapis.com/css2?family=Montserrat%3Awght@400%3B500%3B600%3B700&display=swap"


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

    site = os.path.join(WORK, "site_src", "aya_nodium_lol")
    host = os.path.join(site, "aya.nodium.lol")
    # main documents
    os.makedirs(os.path.join(host, "runtime"), exist_ok=True)
    shutil.copy(os.path.join(AYA, "index.html"), os.path.join(host, "index.html"))
    shutil.copy(os.path.join(AYA, "app.css"),   os.path.join(host, "app.css"))
    with open(os.path.join(AYA, "Aya.App.wasm"), "rb") as f:
        head = f.read(2 * 1024 * 1024)
    with open(os.path.join(host, "runtime", "Aya.App.wasm"), "wb") as f:
        f.write(head)
    # OLD-LAYOUT fixtures — 9.0 must IGNORE these (no migration), and the
    # sweep must DELETE them (no URL resolves to any of them anymore).
    os.makedirs(os.path.join(host, "__query__", "runtime"), exist_ok=True)
    # pre-9.1 layout: Aya.App_=v=<hash>.js
    shutil.copy(os.path.join(AYA, "Aya.App.js"),
                os.path.join(host, "__query__", "runtime",
                             "Aya.App_=v=26b389885145fb927a677cca.js"))
    # second pre-9.1 entry (different query) — proves per-file liveness
    shutil.copy(os.path.join(AYA, "Aya.App.js"),
                os.path.join(host, "__query__", "runtime",
                             "Aya.App_=v=oldcafe.js"))
    # 9.1a layout: value-only pretty name + .q sidecar (value chosen NOT to
    # end in 12 hex chars — that is the cap signature and would make the
    # sweep conservatively keep the file as unprovable)
    with open(os.path.join(AYA, "Aya.App.js"), "rb") as f:
        body91a = f.read() + b"\n/*9.1a-marker*/"
    with open(os.path.join(host, "__query__", "runtime",
                           "Aya.App_deadbeef99.js"), "wb") as f:
        f.write(body91a)
    with open(os.path.join(host, "__query__", "runtime",
                           "Aya.App_deadbeef99.js.q"), "w") as f:
        f.write("v=deadbeef99\n")
    # pre-9.1 bare layout for Google Fonts css2 (the marketplace case)
    gq = os.path.join(site, "fonts.googleapis.com", "__query__", "css2")
    os.makedirs(gq, exist_ok=True)
    qenc = "family=Montserrat%3Awght@400%3B500%3B600%3B700&display=swap"
    shutil.copy(os.path.join(AYA, "fonts.css"), os.path.join(gq, qenc))
    # lying extension: a .js cache file whose content is pure CSS (LIVE —
    # no-query file, the sweep must never touch it)
    lie = os.path.join(host, "assets")
    os.makedirs(lie, exist_ok=True)
    with open(os.path.join(lie, "bundle.js"), "wb") as f:
        f.write(b"/* injected banner */\n.foo{color:red}\n"
                b".bar{margin:0 auto;padding:4px}\n"
                b"@media screen and (max-width:600px){.baz{display:none}}\n")


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

    def hdrs(h, *names):
        return {n: h.get(n, "<MISSING>") for n in names}

    nav = {
        "User-Agent": FF,
        "Sec-Fetch-Dest": "document", "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none", "Upgrade-Insecure-Requests": "1",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    site = os.path.join(WORK, "site_src", "aya_nodium_lol")
    host = os.path.join(site, "aya.nodium.lol")
    col_host = os.path.join(site, "example.com")

    # ── C1: normal Firefox navigation → COOP+COEP ─────────────────────────
    r = c.get("/", headers=nav)
    hh = hdrs(r.headers, "Cross-Origin-Opener-Policy", "Cross-Origin-Embedder-Policy")
    check("C1 doc-nav COOP/COEP",
          hh["Cross-Origin-Opener-Policy"] == "same-origin"
          and hh["Cross-Origin-Embedder-Policy"] in ("require-corp", "credentialless")
          and r.status_code == 200, str(hh))

    # ── C2: iframe document load → COOP+COEP ─────────────────────────────
    r2 = dict(nav); r2["Sec-Fetch-Dest"] = "iframe"; r2["Sec-Fetch-Site"] = "same-origin"
    r = c.get("/", headers=r2)
    hh = hdrs(r.headers, "Cross-Origin-Opener-Policy", "Cross-Origin-Embedder-Policy")
    check("C2 doc-iframe COOP/COEP",
          hh["Cross-Origin-Opener-Policy"] == "same-origin"
          and hh["Cross-Origin-Embedder-Policy"] in ("require-corp", "credentialless"), str(hh))

    # ── C3: no Fetch Metadata → body-signal fallback ──────────────────────
    r = c.get("/", headers={"User-Agent": FF, "Accept": "text/html"})
    hh = hdrs(r.headers, "Cross-Origin-Opener-Policy", "Cross-Origin-Embedder-Policy")
    check("C3 doc-nosec COOP/COEP (body signal)",
          hh["Cross-Origin-Opener-Policy"] == "same-origin"
          and hh["Cross-Origin-Embedder-Policy"] in ("require-corp", "credentialless"), str(hh))

    # ── C4: THE 9.0 CUT — pre-9.1 cache is IGNORED (no migration) ─────────
    # The only on-disk copy of ?v=26b389… sits in the pre-9.1 layout; OFFLINE
    # mode must return 404 (a plain MISS), leaving the old file untouched and
    # creating nothing at the current pretty name.
    url = "/runtime/Aya.App.js?v=26b389885145fb927a677cca"
    r = c.get(url, headers={"User-Agent": FF})
    p_pre91 = os.path.join(host, "__query__", "runtime",
                           "Aya.App_=v=26b389885145fb927a677cca.js")
    p_pretty = os.path.join(host, "__query__", "runtime",
                            "Aya.App_v_26b389885145fb927a677cca.js")
    check("C4 pre-9.1 cache NOT served (plain MISS, 404 offline)",
          r.status_code == 404 and os.path.isfile(p_pre91)
          and not os.path.isfile(p_pretty),
          f"{r.status_code}")

    # ── C4b: current pretty NAME formula (pure naming, no side effects) ──
    check("C4b local_path names the corrected pretty file",
          R.local_path("https://aya.nodium.lol/runtime/Aya.App.js?v=26b389885145fb927a677cca")
              .endswith(os.path.join("__query__", "runtime",
                                     "Aya.App_v_26b389885145fb927a677cca.js")))

    # ── C4c: 9.1a value-only cache is IGNORED too (no steal, no rename) ──
    r = c.get("/runtime/Aya.App.js?v=deadbeef99", headers={"User-Agent": FF})
    p91a = os.path.join(host, "__query__", "runtime", "Aya.App_deadbeef99.js")
    p91a_new = os.path.join(host, "__query__", "runtime", "Aya.App_v_deadbeef99.js")
    check("C4c 9.1a cache NOT served/migrated (404, files intact)",
          r.status_code == 404 and os.path.isfile(p91a)
          and os.path.isfile(p91a + ".q") and not os.path.isfile(p91a_new),
          f"{r.status_code}")

    # ── C10-family: naming semantics on the example.com fixture host ─────
    os.makedirs(os.path.join(col_host, "__query__", "user"), exist_ok=True)
    # live collision owner + its sidecar
    with open(os.path.join(col_host, "__query__", "user_q_a_b"), "wb") as f:
        f.write(b"content-A")
    R._write_query_sidecar(os.path.join(col_host, "__query__", "user_q_a_b"), "q=a+b")
    # live collision FALLBACK file (no sidecar — raw-decode liveness path)
    with open(os.path.join(col_host, "__query__", "user", "q=a%20b"), "wb") as f:
        f.write(b"content-B")
    # 9.1a leftovers (dead) with sidecars
    with open(os.path.join(col_host, "__query__", "user_1"), "wb") as f:
        f.write(b"content-91a")
    R._write_query_sidecar(os.path.join(col_host, "__query__", "user_1"), "a=1")
    with open(os.path.join(col_host, "__query__", "user_2"), "wb") as f:
        f.write(b"content-X")
    R._write_query_sidecar(os.path.join(col_host, "__query__", "user_2"), "x=2")
    # pre-9.1 bare leftover (dead)
    os.makedirs(os.path.join(col_host, "__query__", "v1", "asset"), exist_ok=True)
    with open(os.path.join(col_host, "__query__", "v1", "asset", "id=7"), "wb") as f:
        f.write(b"asset-7-bytes")
    # orphan sidecar (dead)
    with open(os.path.join(col_host, "__query__", "user", "ghost.q"), "w") as f:
        f.write("z=9\n")
    # LIVE no-ext pretty file whose beautified VALUE contains a dot
    os.makedirs(os.path.join(col_host, "__query__", "api"), exist_ok=True)
    with open(os.path.join(col_host, "__query__", "api", "asset_v_1.2"), "wb") as f:
        f.write(b'{"v": "1.2"}')
    R._write_query_sidecar(os.path.join(col_host, "__query__", "api", "asset_v_1.2"), "v=1.2")
    # capped-looking pre-9.1 file (unprovable → the sweep must KEEP it)
    os.makedirs(os.path.join(col_host, "__query__", "big"), exist_ok=True)
    with open(os.path.join(col_host, "__query__", "big",
                           "data_=x_abcdef123456.js"), "wb") as f:
        f.write(b"// capped-looking legacy")
    # 09/10 layout: query file straight in the host tree (dead)
    os.makedirs(os.path.join(col_host, "assets"), exist_ok=True)
    with open(os.path.join(col_host, "assets", "bundle_=x=1.js"), "wb") as f:
        f.write(b"// 09/10 layout")

    # C10: colliding query falls back to the reversible encoding
    lp_b = R.local_path("https://example.com/user?q=a%20b")
    check("C10 collision: ?q=a%20b falls back to the raw-encoding name",
          lp_b.endswith(os.path.join("__query__", "user", "q=a%20b")),
          os.path.basename(lp_b))
    # C10b: the owner keeps the pretty name
    lp_a = R.local_path("https://example.com/user?q=a+b")
    check("C10b owner ?q=a+b keeps the pretty name",
          lp_a.endswith(os.path.join("__query__", "user_q_a_b")),
          os.path.basename(lp_a))
    # C10c: 9.1a name is NOT resolved, NOT renamed (?a=1 → user_a_1)
    lp_m = R.local_path("https://example.com/user?a=1")
    check("C10c ?a=1 resolves to user_a_1 (9.1a user_1 untouched)",
          lp_m.endswith(os.path.join("__query__", "user_a_1"))
          and os.path.isfile(os.path.join(col_host, "__query__", "user_1")),
          os.path.basename(lp_m))
    # C10d: no content theft (?b=2 gets its own name; user_2 stays)
    lp_z = R.local_path("https://example.com/user?b=2")
    check("C10d ?b=2 does NOT steal the 9.1a user_2 (owned by x=2)",
          lp_z.endswith(os.path.join("__query__", "user_b_2"))
          and os.path.isfile(os.path.join(col_host, "__query__", "user_2")),
          os.path.basename(lp_z))

    # ── C5: CURRENT-layout cache serves normally (the post-refetch state) ─
    # Simulate what an online re-fetch (or a fresh 9.0 mirror) writes: the
    # pretty file + its owning sidecar. Everything the old battery verified
    # through migration must still hold through the current layout.
    os.makedirs(os.path.dirname(p_pretty), exist_ok=True)
    shutil.copy(os.path.join(AYA, "Aya.App.js"), p_pretty)
    R._write_query_sidecar(p_pretty, "v=26b389885145fb927a677cca")
    r = c.get(url, headers={"User-Agent": FF})
    hh = hdrs(r.headers, "Content-Type", "Cross-Origin-Resource-Policy")
    check("C5 pretty cache HIT: CT+CORS",
          r.status_code == 200 and "javascript" in hh["Content-Type"]
          and hh["Cross-Origin-Resource-Policy"] == "cross-origin", str(hh))
    r = c.get(url, headers={"User-Agent": FF, "Sec-Fetch-Dest": "worker",
                            "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin"})
    check("C5b worker COEP",
          r.headers.get("Cross-Origin-Embedder-Policy") in ("require-corp", "credentialless"))
    r = c.get(url, headers={"User-Agent": FF, "Sec-Fetch-Dest": "audio-worklet",
                            "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin"})
    check("C5c audio-worklet COEP",
          r.headers.get("Cross-Origin-Embedder-Policy") in ("require-corp", "credentialless"))

    # ── C6: WASM CT ────────────────────────────────────────────────────────
    r = c.get("/runtime/Aya.App.wasm", headers={"User-Agent": FF})
    check("C6 wasm CT", r.headers.get("Content-Type", "").startswith("application/wasm"),
          r.headers.get("Content-Type", ""))

    # ── C7: Google Fonts css2 in the CURRENT layout → text/css ────────────
    lp_fonts = R.local_path(FONTS_URL)
    os.makedirs(os.path.dirname(lp_fonts), exist_ok=True)
    shutil.copy(os.path.join(AYA, "fonts.css"), lp_fonts)
    R._write_query_sidecar(lp_fonts, FONTS_QS)
    ct = R._resolve_mime_with_sniff(lp_fonts)
    check("C7 fonts css2 cached at the pretty name (+ sidecar)",
          os.path.isfile(lp_fonts) and os.path.isfile(lp_fonts + ".q"),
          os.path.basename(lp_fonts))
    check("C7b fonts css2 MIME (Fix 6R)", ct.startswith("text/css"), ct)
    r = c.get(FONTS_EXT, headers={"User-Agent": FF})
    check("C7c fonts css2 served as CSS via ext_asset",
          "text/css" in r.headers.get("Content-Type", "") and r.status_code == 200,
          f"{r.status_code} {r.headers.get('Content-Type','')}")

    # ── C8: lying extension (.js file that is CSS) ─────────────────────────
    lp_lie = os.path.join(host, "assets", "bundle.js")
    ct = R._resolve_mime_with_sniff(lp_lie)
    check("C8 lying .js → text/css", ct.startswith("text/css"), ct)

    # ── C8b: obscure extension (.gl mapped to video/gl) serving JS ────────
    import urllib.request
    try:
        gg = urllib.request.urlopen("https://unpkg.com/globe.gl", timeout=15).read(4096)
    except Exception:
        gg = (b"(function (global, factory) { typeof exports === 'object' && typeof module !== 'undefined'"
              b" ? factory(exports) : typeof define === 'function' && define.amd ? define(['exports'], factory)"
              b" : (global = typeof globalThis !== 'undefined' ? globalThis : global || self,"
              b" factory(global.Globe = {})); })(this, (function (exports) { 'use strict';"
              b" var THREE = window.THREE; function sel() { return typeof document; }"
              b" exports.Globe = Globe; Object.defineProperty(exports, '__esModule', { value: true });}));")
    lp_gl = os.path.join(host, "assets", "globe.gl")
    with open(lp_gl, "wb") as f:
        f.write(gg)
    ct = R._resolve_mime_with_sniff(lp_gl)
    check("C8b globe.gl obscure-ext rescue → JS", ct.startswith("application/javascript"), ct)
    lp_glv = os.path.join(host, "assets", "clip.gl")
    with open(lp_glv, "wb") as f:
        f.write(b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00isomiso2" + bytes(256))
    ct = R._resolve_mime_with_sniff(lp_glv)
    check("C8c real video .gl keeps video/mp4", ct.startswith("video/mp4"), ct)

    # ── C8d: large JSON truncated at the sniff window ─────────────────────
    bigjson = b"".join(
        (b'[{"id":%d,"name":"item%d","tags":["a","b"],"meta":{"k":"v"}},' % (i, i))
        for i in range(20)) + b'{"id":999}]'
    lp_bj = os.path.join(host, "assets", "feed")
    with open(lp_bj, "wb") as f:
        f.write(bigjson)
    ct = R._resolve_mime_with_sniff(lp_bj)
    check("C8d truncated big JSON → application/json", ct.startswith("application/json"), ct)

    # ── C9: ETag folds CT crc32 (MIME auto-heal) ──────────────────────────
    e1 = R._disk_validators(lp_fonts, "application/octet-stream")[0]
    e2 = R._disk_validators(lp_fonts, "text/css")[0]
    check("C9 ETag changes with CT", e1 != e2, f"{e1} vs {e2}")

    # ── C11: 206 CT consistent with 200 ───────────────────────────────────
    ext_url = FONTS_EXT
    r200 = c.get(ext_url, headers={"User-Agent": FF})
    r206 = c.get(ext_url, headers={"User-Agent": FF, "Range": "bytes=0-99"})
    check("C11 206 CT == 200 CT",
          r206.status_code == 206
          and r206.headers.get("Content-Type") == r200.headers.get("Content-Type"),
          f"{r206.status_code} {r206.headers.get('Content-Type','')} vs {r200.headers.get('Content-Type','')}")

    # ── C12: COEP value by UA ─────────────────────────────────────────────
    saf = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
    rff = c.get("/", headers=dict(nav))
    coep_ff = rff.headers.get("Cross-Origin-Embedder-Policy", "")
    nav_s = dict(nav); nav_s["User-Agent"] = saf
    rsf = c.get("/", headers=nav_s)
    coep_sf = rsf.headers.get("Cross-Origin-Embedder-Policy", "")
    check("C12 COEP: Firefox 132 → credentialless", coep_ff == "credentialless", coep_ff)
    check("C12b COEP: Safari → require-corp", coep_sf == "require-corp", coep_sf)

    # ── C13: CDN-live HTML call-site parity ────────────────────────────────
    with R.app.test_request_context(
            "/", headers={"User-Agent": FF, "Sec-Fetch-Dest": "iframe",
                          "Sec-Fetch-Mode": "navigate", "Accept": "text/html"}):
        out = R.filter_resp({"Content-Type": "text/html; charset=utf-8"},
                            b"<html><body>embedded player</body></html>",
                            is_top_level_html=R._is_document_ct("text/html; charset=utf-8"))
    check("C13 CDN/ext live HTML gets isolation",
          out.get("Cross-Origin-Opener-Policy") == "same-origin"
          and out.get("Cross-Origin-Embedder-Policy") in ("require-corp", "credentialless"))

    # ── C14: XHTML document CT detection ───────────────────────────────────
    check("C14 _is_document_ct covers xhtml",
          R._is_document_ct("application/xhtml+xml; charset=utf-8")
          and R._is_document_ct("text/html")
          and not R._is_document_ct("application/javascript"))

    # ── C15: beautify unit matrix ──────────────────────────────────────────
    B = R._beautify_query_name
    check("C15 1-param joins [K]_[V]",
          B("v=26b389") == "v_26b389" and B("id=7") == "id_7")
    check("C15b flags / empty value / value without key",
          B("debug") == "debug" and B("k=") == "k" and B("=x") == "x")
    check("C15c 2+ params and the Google Fonts case",
          B("a=1&b=2") == "a_1_N_b_2"
          and B("family=Montserrat:wght@400&display=swap")
              == "family_Montserrat_wght@400_N_display_swap")

    # ── C16: post-join filename caps ───────────────────────────────────────
    seg180 = "a" * 180
    lp_ov1 = R.local_path(f"https://example.com/{seg180}?q={'z' * 200}")
    lp_ov2 = R.local_path(f"https://example.com/{seg180}?q={'z' * 200}")
    name_ov = os.path.basename(lp_ov1)
    writable = False
    try:
        os.makedirs(os.path.dirname(lp_ov1), exist_ok=True)
        with open(lp_ov1, "wb") as f:
            f.write(b"x")
        writable = True
    except OSError:
        pass
    check("C16 no-ext pretty name capped (≤ limit, deterministic, writable)",
          len(name_ov) <= R._FS_NAME_MAX and lp_ov1 == lp_ov2 and writable,
          f"len={len(name_ov)}")
    cap = R._cap_query_fname
    capped = cap("b" * 300 + ".js")
    check("C16b _cap_query_fname unit (identity short, head+hash, ext kept)",
          cap("short_name.js") == "short_name.js"
          and len(capped) <= R._FS_NAME_MAX and capped.endswith(".js")
          and capped != cap("c" * 300 + ".js"))

    # ── SW: THE STARTUP SWEEP (the 9.0 garbage collector) ─────────────────
    # Every fixture built above is still on disk — run the sweep NOW and
    # verify it deletes exactly the unreachable files and keeps the rest.
    n, freed = R._sweep_legacy_cache()

    def gone(*parts):
        return not os.path.isfile(os.path.join(*parts))

    check("SW1 pre-9.1 caches swept (both ?v= variants + oldcafe)",
          gone(host, "__query__", "runtime", "Aya.App_=v=26b389885145fb927a677cca.js")
          and gone(host, "__query__", "runtime", "Aya.App_=v=oldcafe.js"))
    check("SW1b pre-9.1 bare swept (fonts css2 + v1/asset/id=7)",
          gone(os.path.join(site, "fonts.googleapis.com", "__query__", "css2"),
               "family=Montserrat%3Awght@400%3B500%3B600%3B700&display=swap")
          and gone(col_host, "__query__", "v1", "asset", "id=7"))
    check("SW2 9.1a caches swept with their sidecars (Aya + user_1 + user_2)",
          gone(host, "__query__", "runtime", "Aya.App_deadbeef99.js")
          and gone(host, "__query__", "runtime", "Aya.App_deadbeef99.js.q")
          and gone(col_host, "__query__", "user_1")
          and gone(col_host, "__query__", "user_1.q")
          and gone(col_host, "__query__", "user_2")
          and gone(col_host, "__query__", "user_2.q"))
    check("SW3 09/10 host-tree file swept + empty dirs pruned",
          gone(col_host, "assets", "bundle_=x=1.js")
          and not os.path.isdir(os.path.join(col_host, "__query__", "v1")))
    check("SW4 orphan sidecar swept",
          gone(col_host, "__query__", "user", "ghost.q"))
    check("SW5 LIVE files kept (pretty owners, fallback, dot-in-beauty, "
          "capped-guard, capped pretty C16, no-query assets)",
          os.path.isfile(os.path.join(col_host, "__query__", "user_q_a_b"))
          and os.path.isfile(os.path.join(col_host, "__query__", "user", "q=a%20b"))
          and os.path.isfile(os.path.join(col_host, "__query__", "api", "asset_v_1.2"))
          and os.path.isfile(os.path.join(col_host, "__query__", "api", "asset_v_1.2.q"))
          and os.path.isfile(os.path.join(col_host, "__query__", "big",
                                          "data_=x_abcdef123456.js"))
          and os.path.isfile(p_pretty) and os.path.isfile(p_pretty + ".q")
          and os.path.isfile(lp_fonts) and os.path.isfile(lp_fonts + ".q")
          and os.path.isfile(lp_ov1)
          and os.path.isfile(os.path.join(host, "assets", "bundle.js"))
          and os.path.isfile(os.path.join(host, "runtime", "Aya.App.wasm")))
    n2, freed2 = R._sweep_legacy_cache()
    check("SW6 idempotent (second run removes nothing)",
          n2 == 0 and freed2 == 0, f"second run: {n2}")
    check("SW7 accounting (9 removals, bytes freed > 0)",
          n == 9 and freed > 0, f"n={n} freed={freed}B")
    check("SW8 config surface (VERSION 9.0, CLEAN_LEGACY_CACHE knob removed)",
          R.VERSION == "9.0" and not hasattr(R, "CLEAN_LEGACY_CACHE"))

    # ── post-sweep sanity: the live mirror still serves ────────────────────
    r = c.get("/", headers=nav)
    r2 = c.get(url, headers={"User-Agent": FF})
    check("SW9 post-sweep serving intact (doc 200 + pretty JS 200)",
          r.status_code == 200 and r2.status_code == 200
          and "javascript" in r2.headers.get("Content-Type", ""))

    print("\n=== SUMMARY ===")
    fails = [t for t, ok in results if not ok]
    print(f"{len(results)-len(fails)}/{len(results)} passed")
    if fails:
        print("FAILING:", ", ".join(fails))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
