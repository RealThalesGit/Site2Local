#!/usr/bin/env python3
"""Hop-by-hop contract battery for Site2Local 9.0 (Fix 10).

  H1  RFC 7230 §6.1 (response leg) — header names listed in the upstream's
      Connection field are hop-by-hop for that message and must be stripped
      from the mirrored response, not just "connection" itself.
  H2  RFC 7230 §6.1 (request leg) — the same for the browser's Connection
      field: listed names are not forwarded upstream.
  H3  WebSocket exemption — "upgrade" listed in Connection must NOT strip
      the Upgrade header (the WS handler owns it; see _HOP_BY_HOP note).
  H4  RFC 7230 §4.4 — the hop-by-hop header is named "Trailer" (singular).
      The static list carried a "trailers" typo inherited from the frozen
      monolith, so real Trailer headers were passed through to the browser.
  H5  Robustness — list-valued Connection fields (the shape curl_cffi
      sometimes produces) are honored too.
  H6  Regression guard — a keyword-only Connection ("close") strips
      nothing beyond the field itself.
"""
import os
import re
import shutil
import sys

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_hbh"


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
    from flask import Flask
    from Site2Local import Runtime as R
    from Site2Local.HTTP.Headers.Response import filter_resp
    from Site2Local.HTTP.Headers.Forwarding import filter_fwd

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    def keys_ci(d):
        return {k.lower() for k in d}

    _app = Flask("hbh")
    with _app.test_request_context(
            "/", headers={"Origin": "http://localhost:8080"}):

        # ── H1: response-side dynamic Connection tokens (§6.1) ────────────
        out = filter_resp({
            "Content-Type": "text/html; charset=utf-8",
            "ETag": '"abc123"',
            "Connection": "keep-alive, x-acme-hop",
            "x-acme-hop": "session-xyz",
            "X-Keep-Me": "yes",
        }, body=b"<html><body>ok</body></html>", is_top_level_html=True)
        k = keys_ci(out)
        check("H1 upstream Connection-listed header stripped (RFC 7230 §6.1)",
              "x-acme-hop" not in k and "connection" not in k
              and "x-keep-me" in k and "etag" in k,
              f"x-acme-hop present={'x-acme-hop' in k}, "
              f"connection present={'connection' in k}")

        # ── H2: request-side dynamic Connection tokens (§6.1) ─────────────
        fwd = filter_fwd({
            "Accept": "text/html,application/xhtml+xml",
            "Connection": "keep-alive, x-browser-hop",
            "x-browser-hop": "1",
            "X-Keep-Fwd": "yes",
        })
        k = keys_ci(fwd)
        check("H2 browser Connection-listed header not forwarded upstream",
              "x-browser-hop" not in k and "connection" not in k
              and "x-keep-fwd" in k and "accept" in k,
              f"x-browser-hop present={'x-browser-hop' in k}")

        # ── H3: the upgrade exemption (WS routing owns Upgrade) ───────────
        fwd = filter_fwd({"Connection": "Upgrade", "Upgrade": "websocket",
                          "Sec-WebSocket-Key": "k", "Sec-WebSocket-Version": "13"})
        k = keys_ci(fwd)
        check("H3 'upgrade' in Connection does NOT strip Upgrade (WS route)",
              "upgrade" in k and "sec-websocket-key" in k,
              f"upgrade present={'upgrade' in k}")

        # ── H4: the RFC 7230 §4.4 name is "Trailer", not "Trailers" ───────
        out = filter_resp({"Content-Type": "text/plain",
                           "Trailer": "X-Checksum", "X-Normal": "1"})
        k = keys_ci(out)
        check("H4 Trailer (RFC 7230 §4.4) stripped from responses",
              "trailer" not in k and "x-normal" in k,
              f"trailer present={'trailer' in k}")

        # ── H5: list-valued Connection field (curl_cffi shape) ────────────
        fwd = filter_fwd({"Accept": "*/*",
                          "connection": ["keep-alive", "x-listed-hop"],
                          "x-listed-hop": "1"})
        check("H5 list-valued Connection field honored",
              "x-listed-hop" not in keys_ci(fwd),
              f"x-listed-hop present={'x-listed-hop' in keys_ci(fwd)}")

        # ── H6: keyword-only Connection strips nothing extra ──────────────
        out = filter_resp({"Content-Type": "text/plain", "Connection": "close",
                           "X-Normal": "1"})
        k = keys_ci(out)
        check("H6 keyword-only Connection ('close') strips nothing extra",
              "x-normal" in k and "connection" not in k)

    passed = sum(1 for _, ok in results if ok)
    print(f"\n=== SUMMARY ===\n{passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
