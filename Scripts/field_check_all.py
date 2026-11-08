#!/usr/bin/env python3
"""Self-contained field check: spawns the proxy, probes Chromium + Firefox.

Everything in ONE process so no tool-shell background quirks:
  1. Popen(python3 -m Site2Local) with its own session, log to file
  2. poll http://localhost:8080/ until it answers
  3. per browser: goto (domcontentloaded, 45s) → poll up to 75s for
     readyState complete → screenshot + crossOriginIsolated/SAB/errors
  4. SIGTERM the proxy in a finally (graceful drain)

Usage: field_check_all.py [chromium|firefox|both]
"""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

from playwright.sync_api import sync_playwright

RUN = "/tmp/field/run"
LOG = "/tmp/field/server.log"
URL = "http://localhost:8080/"
OUT = "/tmp/field"
WHICH = sys.argv[1] if len(sys.argv) > 1 else "both"


def say(*a):
    print(*a, flush=True)


def wait_port(timeout=40):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            urllib.request.urlopen(URL, timeout=3).read(64)
            return True
        except Exception:
            time.sleep(1)
    return False


def probe(browser, name):
    errors, pageerrors = [], []
    pg = browser.new_page()
    pg.on("console", lambda m: errors.append(f"[{m.type}] {m.text[:240]}")
          if m.type in ("error", "warning") else None)
    pg.on("pageerror", lambda e: pageerrors.append(str(e)[:300]))
    t0 = time.time()
    try:
        pg.goto(URL, wait_until="domcontentloaded", timeout=45_000)
    except Exception as e:
        say(f"[{name}] goto FAILED after {time.time()-t0:.0f}s: {str(e)[:160]}")
        pg.close()
        return False, {}
    say(f"[{name}] domcontentloaded in {time.time()-t0:.1f}s — polling readiness")
    # Poll: readyState complete (cap 75s); crossOriginIsolated needs only the
    # main document, but the hero/wasm render needs the full load.
    t1 = time.time()
    while time.time() - t1 < 75:
        try:
            if pg.evaluate("document.readyState") == "complete":
                break
        except Exception:
            pass
        pg.wait_for_timeout(2_000)
    pg.wait_for_timeout(4_000)
    load_s = time.time() - t1
    iso = pg.evaluate("window.crossOriginIsolated")
    sab = pg.evaluate("typeof SharedArrayBuffer")
    title = pg.title()
    dom = pg.evaluate("document.querySelectorAll('*').length")
    hero = pg.evaluate(
        "!!document.querySelector('canvas, #app, #root, main, [class*=hero]')")
    shot = f"{OUT}/shot_{name}.png"
    pg.screenshot(path=shot)
    rep = dict(iso=iso, sab=sab, title=title, dom=dom, hero=hero,
               load_s=round(load_s, 1), errors=errors, pageerrors=pageerrors)
    pg.close()
    say(f"[{name}] iso={iso}  sab={sab}  dom={dom}  hero={hero}  "
        f"load={load_s:.0f}s  title={title!r}")
    for e in pageerrors[:8]:
        say(f"  PAGEERROR {e}")
    for e in errors[:12]:
        say(f"  CONSOLE   {e}")
    if len(errors) > 12:
        say(f"  … +{len(errors)-12} more console items")
    ok = bool(iso) and sab == "function" and not pageerrors
    return ok, rep


def main() -> int:
    say(f"== field check ({WHICH}) — spawning proxy from {RUN}")
    with open(LOG, "w") as lf:
        srv = subprocess.Popen(
            ["python3", "-m", "Site2Local"], cwd=RUN,
            stdout=lf, stderr=subprocess.STDOUT,
            start_new_session=True)
    ok, report = {}, {}
    try:
        if not wait_port():
            say("PROXY NEVER ANSWERED — log tail:")
            os.system(f"tail -15 {LOG}")
            return 2
        say("proxy UP")
        with sync_playwright() as p:
            if WHICH in ("chromium", "both"):
                ok["chromium"], report["chromium"] = probe(
                    p.chromium.launch(
                        args=["--enable-features=SharedArrayBuffer"]),
                    "chromium")
            if WHICH in ("firefox", "both"):
                ok["firefox"], report["firefox"] = probe(
                    p.firefox.launch(), "firefox")
    finally:
        say("signalling proxy (SIGTERM) …")
        srv.send_signal(signal.SIGTERM)
        try:
            srv.wait(timeout=30)
            say(f"proxy exited rc={srv.returncode} (graceful drain done)")
        except subprocess.TimeoutExpired:
            srv.kill()
            say("proxy KILLED after drain timeout")
    with open(f"{OUT}/report.json", "w") as f:
        json.dump(report, f, indent=2)
    say("\nVERDICT " + "  ".join(f"{k}: {'PASS' if v else 'FAIL'}"
                                 for k, v in ok.items()))
    return 0 if ok and all(ok.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
