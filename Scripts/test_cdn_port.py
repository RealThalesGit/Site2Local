#!/usr/bin/env python3
"""CDN port-sentinel leak battery for Site2Local 9.0 (Fix 11).

Reproduces the "localhost:-1" report: when a CDN host is registered in
MULTIPORT mode, its registry entry is briefly the -1 "server starting"
sentinel. That sentinel leaked into the browser-side URL rewriter:

  CP1  the injector serialized the RAW registry (sentinels included) into
       the JS ``var HP=…`` — a page served inside the registration race
       window carried ``HP[host] = -1``.
  CP2  the JS rewriter used truthiness checks (``if(HP[p.host])``) — and
       -1 is truthy in JS, so rw() emitted ``http://localhost:-1/…``
       (and the WS twin ``ws://localhost:-1``). Guards must be ``>0``.
  CP3  the live-refresh poll ``if(!HP[h])`` refused to overwrite the
       stuck -1 — the endpoint only ever sends live ports, so a changed
       port must REPLACE the stale entry.
  CP4  the /.__s2l_hp live endpoint filters to live ports (regression —
       this side was already correct).
  CP5  the Python-side rewriter (Origins._proxy_target) already treats
       -1/0 as "no port" and falls back to /__s2l_ext__/ (regression).
"""
import json
import os
import re
import shutil
import sys

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_cdnport"


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


def main():
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from flask import Flask
    from Site2Local import Runtime as R

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    # The race-window registry: one live port, one -1 sentinel
    # (server starting), one 0 (non-MULTIPORT registration).
    with R._cdn_port_lock:
        R._cdn_host_port.clear()
        R._cdn_host_port["live.cdn.example"] = 8081
        R._cdn_host_port["starting.cdn.example"] = -1
        R._cdn_host_port["unported.cdn.example"] = 0

    _app = Flask("cdnport")
    with _app.test_request_context("/", headers={"Origin": "http://localhost:8080"}):
        # ── CP1: injector HP snapshot excludes sentinels ───────────────────
        js = R._build_s2l_injector().decode("utf-8", "replace")
        m = re.search(r"var HP=(\{.*?\});", js)
        hp = json.loads(m.group(1)) if m else None
        check("CP1 injector HP excludes the -1 sentinel and 0 entries",
              hp is not None and hp == {"live.cdn.example": 8081},
              f"HP={hp}")

        # ── CP2: JS rewriter guards are >0, not truthiness ─────────────────
        src = open(os.path.join(WORK, "Site2Local", "Rewriting", "Injector.py"),
                   encoding="utf-8").read()
        ok_http = 'if(HP[p.host]>0)return "http://"' in src
        ok_ws = "Math.max(0,HP[p.hostname]||HP[p.host]||0)" in src
        check("CP2 JS rw() guards: HP[p.host]>0 + WS cdnp normalized",
              ok_http and ok_ws, f"http-guard={ok_http} ws-guard={ok_ws}")

        # ── CP3: live refresh replaces a stale/changed port ────────────────
        ok_refresh = "if(!HP[h]||HP[h]!==d[h])HP[h]=d[h]" in src
        check("CP3 live HP poll replaces changed ports (heals stuck entries)",
              ok_refresh)

        # ── CP4: /.__s2l_hp endpoint filters to live ports ─────────────────
        from Site2Local.Viewers.Hosts import _s2l_hp_endpoint
        resp = _s2l_hp_endpoint()
        data = json.loads(resp.get_data(as_text=True))
        check("CP4 /.__s2l_hp endpoint serves only live ports",
              data == {"live.cdn.example": 8081}, f"data={data}")

        # ── CP5: Python-side rewriter falls back on -1 ─────────────────────
        from Site2Local.CDN.Registry.Origins import _proxy_target
        out = _proxy_target("starting.cdn.example", "/app.js")
        ok = out is not None and "/__s2l_ext__/starting.cdn.example/app.js" in out
        out0 = _proxy_target("unported.cdn.example", "/app.js")
        ok0 = out0 is not None and "/__s2l_ext__/unported.cdn.example/app.js" in out0
        outl = _proxy_target("live.cdn.example", "/app.js")
        okl = outl is not None and ":8081/app.js" in outl and "__s2l_ext__" not in outl
        check("CP5 _proxy_target: -1/0 → /__s2l_ext__/, live → :port",
              ok and ok0 and okl,
              f"-1→ext:{ok} 0→ext:{ok0} live→port:{okl}")

    passed = sum(1 for _, ok in results if ok)
    print(f"\n=== SUMMARY ===\n{passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
