#!/usr/bin/env python3
"""Drive a REAL headless Chromium against the live S2L mirror of
aya.nodium.lol and report crossOriginIsolated + every console error."""
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.launch(args=["--enable-features=SharedArrayBuffer"])
    pg = b.new_page()
    errors, pageerrors = [], []
    pg.on("console", lambda m: errors.append(f"[{m.type}] {m.text[:300]}") if m.type in ("error", "warning") else None)
    pg.on("pageerror", lambda e: pageerrors.append(str(e)[:400]))
    pg.goto("http://localhost:8080/", wait_until="domcontentloaded", timeout=60000)
    pg.wait_for_timeout(8000)
    iso = pg.evaluate("window.crossOriginIsolated")
    sab = pg.evaluate("typeof SharedArrayBuffer")
    print("crossOriginIsolated:", iso)
    print("SharedArrayBuffer:", sab)
    print("--- page errors ---")
    for e in pageerrors[:10]:
        print(" *", e)
    print("--- console errors/warnings ---")
    for e in errors[:15]:
        print(" *", e)
    b.close()
