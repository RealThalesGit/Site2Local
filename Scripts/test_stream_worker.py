#!/usr/bin/env python3
"""Fix 13 battery — filter_resp(is_worker_script=...) + the _stream_resp path.

Root cause (reported live by the user as a 500 on /api/cdn/dl/... .zip):
HTTP/Streaming/Response.py:33 has passed `is_worker_script=` to filter_resp()
since the Worker hardening landed in the Stable/9.0 branch, but the signature
in HTTP/Headers/Response.py never accepted the keyword. Every STREAMED
response (large downloads, media, 206 ranges) raised
    TypeError: filter_resp() got an unexpected keyword argument 'is_worker_script'
No prior battery exercised _stream_resp, so it survived until real usage.

Checks:
  W1  kwarg accepted + auto-detect (None) preserved for existing callers
  W2  full _stream_resp happy path — the user's exact scenario (zip, GET)
  W3  _stream_resp + Sec-Fetch-Dest: worker → COEP on the streamed response
  W4  explicit bool overrides auto-detect (True forces, False suppresses)
  W5  Fix 10 intact inside the streaming path (dynamic Connection tokens,
      Trailer typo fix, gzip decompression + Content-Length drop)
  W6  whole-tree keyword/signature audit — no OTHER caller passes a keyword
      its target doesn't accept (same class of bug; would have caught W1's
      bug the day it was written). Covers the package (_s2l_state.X calls,
      checked via inspect against the live Runtime) and every file's local
      by-name calls (checked statically via AST, s2l.py included).
"""
import ast
import inspect
import sys
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "download" / "Site2Local"
sys.path.insert(0, str(ROOT.parent))

from flask import Flask  # noqa: E402
from Site2Local import Runtime as S  # noqa: E402

app = Flask(__name__)  # only for test_request_context

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


def lower_keys(headers) -> set:
    return {k.lower() for k in headers.keys()}


class FakeUpstream:
    """Minimal stand-in for a curl_cffi/requests streamed response."""

    def __init__(self, headers, status_code=200, chunks=()):
        self.headers = dict(headers)
        self.status_code = status_code
        self._chunks = list(chunks)
        self.closed = False

    def iter_content(self, chunk_size=None):
        yield from self._chunks

    def close(self):
        self.closed = True


# --------------------------------------------------------------------------
# W1 — kwarg accepted + auto-detect (None default) still works
# --------------------------------------------------------------------------
with app.test_request_context("/worker.js", headers={"Sec-Fetch-Dest": "worker"}):
    out_auto = S.filter_resp({"Content-Type": "application/javascript"})
with app.test_request_context("/plain.js"):
    out_plain = S.filter_resp({"Content-Type": "application/javascript"})
coep_auto = S._ci_get_header(out_auto, "Cross-Origin-Embedder-Policy", "")
coep_plain = S._ci_get_header(out_plain, "Cross-Origin-Embedder-Policy", "")
check("W1 kwarg accepted + None=auto-detect (worker→COEP, plain→none)",
      coep_auto != "" and coep_plain == "",
      f"auto={coep_auto!r} plain={coep_plain!r}")

# --------------------------------------------------------------------------
# W2 — full _stream_resp happy path (the user's scenario: big zip via GET)
# --------------------------------------------------------------------------
payload = b"PK\x03\x04" + b"eaglercraft-wasm-payload" * 5000
up = FakeUpstream(
    {"Content-Type": "application/zip",
     "Content-Length": str(len(payload)),
     "Connection": "close",
     "Trailer": "X-T",
     "X-Custom": "keep-me"},
    chunks=(payload[:65536], payload[65536:131072], payload[131072:]))
with app.test_request_context("/api/cdn/dl/1.8.8/eaglercraft.zip", method="GET"):
    resp = S._stream_resp(up, "GET", "https://cdn.example.com/1.8.8/eaglercraft.zip")
    body = b"".join(resp.response)
hk = lower_keys(resp.headers)
check("W2 _stream_resp: 200, body intact, hop-by-hop stripped, custom kept",
      resp.status_code == 200 and body == payload
      and "connection" not in hk and "trailer" not in hk
      and "x-custom" in hk
      and resp.headers.get("Cache-Control") == "no-cache, no-transform"
      and "cross-origin-embedder-policy" not in hk,
      f"sc={resp.status_code} body={len(body)}B keys={sorted(hk)}")

# --------------------------------------------------------------------------
# W3 — streamed WORKER script gets COEP (the hardening's intent, finally
#      active on the streaming path)
# --------------------------------------------------------------------------
up2 = FakeUpstream(
    {"Content-Type": "application/javascript",
     "Content-Length": "1200",
     "Connection": "close"},
    chunks=(b"self.onmessage = e => postMessage(e.data);",))
with app.test_request_context("/sw.js", method="GET",
                              headers={"Sec-Fetch-Dest": "serviceworker"}):
    resp2 = S._stream_resp(up2, "GET", "https://cdn.example.com/sw.js")
    _ = b"".join(resp2.response)
coep_stream = resp2.headers.get("Cross-Origin-Embedder-Policy", "")
check("W3 streamed worker script carries COEP",
      coep_stream in ("credentialless", "require-corp"),
      f"COEP={coep_stream!r}")

# --------------------------------------------------------------------------
# W4 — explicit bool overrides auto-detect
# --------------------------------------------------------------------------
with app.test_request_context("/forced.js"):  # no Sec-Fetch-Dest
    out_force = S.filter_resp({"Content-Type": "application/javascript"},
                              is_worker_script=True)
with app.test_request_context("/suppressed.js",
                              headers={"Sec-Fetch-Dest": "worker"}):
    out_supp = S.filter_resp({"Content-Type": "application/javascript"},
                             is_worker_script=False)
check("W4 explicit override (True forces COEP, False suppresses)",
      S._ci_get_header(out_force, "Cross-Origin-Embedder-Policy", "") != ""
      and S._ci_get_header(out_supp, "Cross-Origin-Embedder-Policy", "") == "")

# --------------------------------------------------------------------------
# W5 — Fix 10 intact inside the streaming path + gzip handling
# --------------------------------------------------------------------------
plain5 = b"A" * 70000 + b"B" * 70000
co = zlib.compressobj(9, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
gz1 = co.compress(plain5[:70000])
gz2 = co.compress(plain5[70000:]) + co.flush()
up5 = FakeUpstream(
    {"Content-Type": "application/zip",
     "Content-Encoding": "gzip",
     "Content-Length": str(len(gz1) + len(gz2)),
     "Connection": "X-Hop-Custom, close",
     "X-Hop-Custom": "must-die"},
    chunks=(gz1, gz2))
with app.test_request_context("/big.zip", method="GET"):
    resp5 = S._stream_resp(up5, "GET", "https://cdn.example.com/big.zip")
    body5 = b"".join(resp5.response)
hk5 = lower_keys(resp5.headers)
check("W5 Fix10 in stream: dyn Connection token stripped, gzip decoded, "
      "CL/CE dropped",
      body5 == plain5
      and "x-hop-custom" not in hk5 and "connection" not in hk5
      and "content-length" not in hk5 and "content-encoding" not in hk5,
      f"body_ok={body5 == plain5} keys={sorted(hk5)}")

# --------------------------------------------------------------------------
# W6 — whole-tree keyword/signature audit (same class of bug)
# --------------------------------------------------------------------------
problems = []

# (a) package: every `_s2l_state.X(...)` call vs the live signature of X
py_files = sorted(p for p in ROOT.rglob("*.py")
                  if "__pycache__" not in p.parts)
for py in py_files:
    try:
        tree = ast.parse(py.read_text(encoding="utf-8"))
    except SyntaxError as e:
        problems.append(f"{py.name}: SyntaxError {e}")
        continue
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "_s2l_state"):
            continue
        fname = node.func.attr
        fn = getattr(S, fname, None)
        if fn is None or not callable(fn):
            continue
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            continue  # builtins / C funcs — not checkable
        params = sig.parameters
        has_kw = any(p.kind is p.VAR_KEYWORD for p in params.values())
        for kw in node.keywords:
            if kw.arg is None or has_kw or kw.arg in params:
                continue
            problems.append(f"{py.relative_to(ROOT)}:{node.lineno} "
                            f"_s2l_state.{fname}(..., {kw.arg}=...) "
                            f"not in signature({fname})")

# (b) every file (package + s2l.py): local by-name calls vs local defs
for py in [*py_files, ROOT / "s2l.py"]:
    try:
        tree = ast.parse(py.read_text(encoding="utf-8"))
    except SyntaxError:
        continue
    local_defs = {}  # name -> union of accepted keywords across overloads
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            names = ([p.arg for p in a.posonlyargs] + [p.arg for p in a.args]
                     + [p.arg for p in a.kwonlyargs])
            entry = local_defs.setdefault(node.name, set())
            entry.update(names)
            if a.kwarg is not None:
                entry.add("**")
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in local_defs):
            continue
        accepted = local_defs[node.func.id]
        for kw in node.keywords:
            if kw.arg is None or "**" in accepted or kw.arg in accepted:
                continue
            problems.append(f"{py.relative_to(ROOT)}:{node.lineno} "
                            f"{node.func.id}(..., {kw.arg}=...) "
                            f"not accepted by local def")

check("W6 whole-tree keyword/signature audit clean", not problems,
      "; ".join(problems[:6]) if problems else
      f"scanned {len(py_files) + 1} files, 0 mismatches")

# --------------------------------------------------------------------------
print(f"\n{sum(1 for _, ok in RESULTS if ok)}/{len(RESULTS)} checks passed")
sys.exit(0 if all(ok for _, ok in RESULTS) else 1)
