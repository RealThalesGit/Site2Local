#!/usr/bin/env python3
"""Fix 18 battery — plain-http absolute URLs must route through the proxy.

Field origin (2026-11-09, live Chromium+Firefox field check, round 2): with
Fix 17 in place the Discord widget's HTML/CSS/JS finally load through
/__s2l_ext__/discord.com — and the (now running) widget JS immediately XHRs
    http://discord.com/api/guilds/…/widget.json
PLAIN http, absolute, third-party. The main injector's rw() only routed
https: URLs of third-party hosts (line 103: p.protocol==="https:"), so the
http: request leaked DIRECT to the real host. On the real site that request
is same-origin (the widget iframe IS discord.com); in the mirror the iframe
origin is the proxy, so the browser killed it on CORS (the Discord API
sends no ACAO header — it never needed one). Fix 18 widens the branch to
http: as well (the EXT route always fetches the upstream over https).

The rw() function is real JavaScript living inside the built injector, so
this battery runs it under Node.js (v24) with the core extracted verbatim.

Checks:
  HW1 source contract: built injector carries the widened condition
  HW2 rw() matrix under Node: http third→EXT (the fix), https third→EXT
     (regression), http/https MAIN→PX, localhost→unchanged, relative and
     protocol-relative passthrough, DNR host→unchanged, HP host→dedicated
     port, ws://→ws-ext
  HW3 XHR/fetch patches apply rw (source contract inside the core)
  HW4 idempotence: rw() never re-wraps an already-proxied URL
  HW5 integration: an EXT HTML cache-HIT serves the injector carrying the
     widened condition (the _serve_cached injection path picks it up)
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_httpxhr"

CORE_START = "function __s2l_core("
CORE_END = "return rw;}"


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


def main() -> int:
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from Site2Local import Runtime as R

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, bool(ok)))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    js = R._build_s2l_injector().decode("utf-8")

    # ── HW1: source contract ─────────────────────────────────────────────
    check("HW1 built injector routes http: like https: for third parties",
          '(p.protocol==="https:"||p.protocol==="http:")' in js)

    # ── HW2 + HW4: run the REAL core under Node ──────────────────────────
    # The built injector is one long line — extract by markers, not regex.
    _i0 = js.find(CORE_START)
    _i1 = js.find(CORE_END, _i0) if _i0 != -1 else -1
    core_src = js[_i0:_i1 + len(CORE_END)] if (_i0 != -1 and _i1 != -1) else None
    check("HW2a core extracted from the built injector", bool(core_src))
    if core_src:
        harness = (
            "const core = " + json.dumps(core_src) + ";\n"
            "const fn = new Function(core + '; return __s2l_core;')();\n"
            "const rw = fn('aya.nodium.lol', {'cdn.reg.com': 8085},"
            " '/__s2l_ext__', 'localhost', 8080,"
            " '^(?:[^/]*\\.)?hcaptcha\\.com(?:[:/]|$)', null);\n"
            "const T = [\n"
            " ['http://discord.com/api/guilds/1/widget.json',"
            " 'http://localhost:8080/__s2l_ext__/discord.com/api/guilds/1/widget.json'],\n"
            " ['https://cdn.plain.com/x.png',"
            " 'http://localhost:8080/__s2l_ext__/cdn.plain.com/x.png'],\n"
            " ['http://aya.nodium.lol/home', 'http://localhost:8080/home'],\n"
            " ['https://aya.nodium.lol/home?q=1', 'http://localhost:8080/home?q=1'],\n"
            " ['http://localhost:8080/__s2l_ext__/discord.com/already',"
            " 'http://localhost:8080/__s2l_ext__/discord.com/already'],\n"
            " ['/relative/path', '/relative/path'],\n"
            " ['//cdn.proto.com/p',"
            " 'http://localhost:8080/__s2l_ext__/cdn.proto.com/p'],\n"
            " ['http://api.hcaptcha.com/captcha/x', 'http://api.hcaptcha.com/captcha/x'],\n"
            " ['http://cdn.reg.com/f.js', 'http://localhost:8085/f.js'],\n"
            " ['ws://discord.com/socket',"
            " 'ws://localhost:8080/__s2l_ws_ext_plain__/discord.com/socket'],\n"
            "];\n"
            "let bad = [];\n"
            "for (const [u, want] of T) {\n"
            "  const got = rw(u);\n"
            "  if (got !== want) bad.push(u + ' -> ' + got + ' (want ' + want + ')');\n"
            "  const got2 = rw(got);\n"
            "  if (got2 !== got) bad.push('NOT IDEMPOTENT: ' + got + ' -> ' + got2);\n"
            "}\n"
            "console.log(JSON.stringify(bad));\n"
        )
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(harness)
            hp = f.name
        try:
            out = subprocess.run(["node", hp], capture_output=True, text=True,
                                 timeout=30)
            bad = json.loads(out.stdout.strip() or "[]") if out.returncode == 0 else None
            check("HW2 rw() matrix under Node (10 cases) + HW4 idempotence",
                  out.returncode == 0 and not bad,
                  "; ".join(bad[:3]) if bad else f"rc={out.returncode} {out.stderr[:120]}")
        finally:
            os.unlink(hp)
    else:
        check("HW2 rw() matrix under Node (10 cases) + HW4 idempotence", False,
              "core not extracted")

    # ── HW3: XHR/fetch patches route through rw ──────────────────────────
    check("HW3 XHR open() and fetch() apply rw() to the URL argument",
          "a[1]=rw(a[1])" in js and 'if(typeof i==="string")i=rw(i);' in js)

    # ── HW5: EXT HTML cache-HIT serves the widened injector ──────────────
    site = os.path.join(WORK, "site_src", "aya_nodium_lol", "discord.com")
    widget_lp = R.local_path("https://discord.com/widget")
    os.makedirs(os.path.dirname(widget_lp), exist_ok=True)
    with open(widget_lp, "wb") as f:
        f.write(b"<!DOCTYPE html><html><head><title>w</title></head><body></body></html>")
    c = R.app.test_client()
    r = c.get("/__s2l_ext__/discord.com/widget")
    check("HW5 EXT cache-HIT HTML carries the widened injector",
          r.status_code == 200
          and '(p.protocol==="https:"||p.protocol==="http:")' in r.data.decode())

    print("\n=== SUMMARY ===")
    n_ok = sum(1 for _, ok in results if ok)
    print(f"{n_ok}/{len(results)} passed")
    return 0 if n_ok == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
