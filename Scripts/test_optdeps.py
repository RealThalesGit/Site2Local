#!/usr/bin/env python3
"""Fix 14 battery — optional-dependency tiers (the "_brotli/_zstd/_ai_cloudscraper
are missing" report from another AI).

Audit verdict being verified here:
  * _BROTLI_OK/_ZSTD_OK/_CLOUDSCRAPER_OK are NOT missing code — they are
    optional-dependency tiers with graceful fallback, and every claim about
    them in requirements.txt matches PyPI reality (checked 2026-11-09:
    cloudscraper last released 2023-04-25, ai-cloudscraper 3.8.4 on
    2026-02-05 shipping module `cloudscraper`, curl-cffi 0.17.0 alive).
  * REAL garbage found instead: (1) the brotli "tolerant streaming fallback"
    called Decompressor.finish(), a method the official brotli package NEVER
    exposed (verified 1.1.0 and 1.2.0) — the branch was 100% dead code;
    (2) requirements.txt never listed zstandard although the code supports
    Content-Encoding: zstd; (3) the WAFW00F not-installed notice logged at
    ERROR while every other optional dep logs WARN.

Checks:
  OD1 brotli one-shot regression (real package present)
  OD2 revived fallback: brotlipy-shaped shim (process+finish) and official
     shape (process only) both work, no AttributeError can escape
  OD3 zstd: one-shot frame + streaming frame WITHOUT content size (the
     documented stream_reader rationale) both decode
  OD4 Accept-Encoding negotiation tracks _BROTLI_OK (br advertised or not)
  OD5 cloudscraper tier chain semantics: exact source block exec'd under
     controlled sys.modules (fork present / original present / neither)
  OD6 WAFW00F not-installed notice is WARN (source contract, RF9-style)
     and no ERROR variant remains in the package (frozen s2l.py excluded)
  OD7 zstd streamed through _stream_resp end-to-end
"""
import re
import sys
import types
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent / "download" / "Site2Local"
sys.path.insert(0, str(ROOT.parent))

from flask import Flask  # noqa: E402
from Site2Local import Runtime as S  # noqa: E402

app = Flask(__name__)
RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" — {detail}" if detail else ""))


import brotli       # noqa: E402  (present in this sandbox)
import zstandard    # noqa: E402  (installed for this battery)

# --------------------------------------------------------------------------
# OD1 — brotli one-shot regression
# --------------------------------------------------------------------------
payload = (b"the quick brown fox jumps over the lazy dog. " * 300
           + bytes(range(0x80, 0x100)) * 50)
br_comp = brotli.compress(payload)


def _gate_like_s2l(d: bytes) -> bool:
    """Local copy of decompress_body's nested _looks_json_or_text gate."""
    if not d:
        return True
    if d[0:1] in (b'{', b'[', b'<', b'"', b' ', b'\n', b'\r', b'\t'):
        return True
    if b'\x00' in d[:64]:
        return False
    if d[:2] == b'\x1f\x8b' or (d[0] == 0x78 and d[1] in (0x01, 0x9c, 0xda)):
        return False
    return 0x20 <= d[0] <= 0x7E


assert not _gate_like_s2l(br_comp), "test data unexpectedly looks like text"
out1 = S.decompress_body(br_comp, "br")
check("OD1 brotli one-shot decompress_body(br) == payload", out1 == payload,
      f"len {len(out1)}/{len(payload)}")

# --------------------------------------------------------------------------
# OD2 — revived fallback branch (Fix 14): one-shot fails → streaming saves
# --------------------------------------------------------------------------
class _BrotliPyShim:  # brotlipy-shaped: process() + finish()
    def __init__(self, blob):
        self._d = brotli.Decompressor()
        self._buf = blob

    def process(self, _chunk):
        return self._d.process(self._buf)

    def finish(self):
        return b""


def _run_fallback(shim_factory):
    """Force the one-shot to fail; the streaming fallback must save the day."""
    real_brotli = S._brotli
    real_ok = S._BROTLI_OK
    class _NS:
        decompress = staticmethod(lambda data: (_ for _ in ()).throw(
            RuntimeError("one-shot intentionally failed")))
        Decompressor = staticmethod(shim_factory)
    try:
        S._brotli = _NS
        S._BROTLI_OK = True
        return S.decompress_body(br_comp, "br")
    finally:
        S._brotli = real_brotli
        S._BROTLI_OK = real_ok

out_py = _run_fallback(lambda: _BrotliPyShim(br_comp))
out_official = _run_fallback(brotli.Decompressor)   # no finish() at all
check("OD2 revived fallback: brotlipy-shape and official-shape both decode, "
      "no AttributeError",
      out_py == payload and out_official == payload,
      f"brotlipy={out_py == payload} official={out_official == payload}")

# --------------------------------------------------------------------------
# OD3 — zstd: one-shot + frame WITHOUT content size (stream_reader rationale)
# --------------------------------------------------------------------------
cctx = zstandard.ZstdCompressor()
z1 = cctx.compress(payload)                       # frame with content size
cobj = cctx.compressobj()                         # streaming frame: NO size
z2 = cobj.compress(payload) + cobj.flush()
out_z1 = S.decompress_body(z1, "zstd")
one_shot_raises = False
try:
    zstandard.ZstdDecompressor().decompress(z2)
except Exception:
    one_shot_raises = True                        # the documented rationale
out_z2 = S.decompress_body(z2, "zstd")

# zstd bomb guard (mirrors the gzip round-2 guard): cap patched down so the
# test doesn't need a 512MB allocation. Declared-size frame + streaming
# size-less frame must both be refused with the bomb log, served raw.
_cap = S.MAX_DECOMPRESSED_BYTES
_bomb_logs: list = []
_real_log = S.log
try:
    S.MAX_DECOMPRESSED_BYTES = 10_000
    S.log = lambda msg, lvl="INFO": _bomb_logs.append((lvl, msg))
    bomb_plain = b"\x00" * 20_000
    bomb_sized = zstandard.ZstdCompressor().compress(bomb_plain)
    bobj = zstandard.ZstdCompressor().compressobj()
    bomb_sizeless = bobj.compress(bomb_plain) + bobj.flush()
    r_sized = S.decompress_body(bomb_sized, "zstd")
    r_sizeless = S.decompress_body(bomb_sizeless, "zstd")
finally:
    S.MAX_DECOMPRESSED_BYTES = _cap
    S.log = _real_log
bomb_ok = (r_sized == bomb_sized and r_sizeless == bomb_sizeless
           and sum(1 for lvl, _ in _bomb_logs
                   if lvl == "ERROR" and "bomb" in _) == 2)
check("OD3 zstd: sized frame + size-less stream frame decode; bomb guard fires",
      out_z1 == payload and out_z2 == payload and one_shot_raises and bomb_ok,
      f"sized={out_z1 == payload} sizeless={out_z2 == payload} "
      f"one-shot-raises={one_shot_raises} bomb={bomb_ok} "
      f"logs={[l for l, _ in _bomb_logs]}")

# --------------------------------------------------------------------------
# OD4 — Accept-Encoding negotiation tracks _BROTLI_OK
# --------------------------------------------------------------------------
def _ae_for_flag(flag):
    real = S._BROTLI_OK
    try:
        S._BROTLI_OK = flag
        with app.test_request_context("/"):
            s = S._make_session()
        return s.headers.get("Accept-Encoding", "")
    finally:
        S._BROTLI_OK = real

ae_on, ae_off = _ae_for_flag(True), _ae_for_flag(False)
check("OD4 Accept-Encoding: br advertised iff brotli available",
      ae_on == "gzip, deflate, br" and ae_off == "gzip, deflate",
      f"on={ae_on!r} off={ae_off!r}")

# --------------------------------------------------------------------------
# OD5 — cloudscraper tier chain semantics (exact source block, fake modules)
# --------------------------------------------------------------------------
src = (ROOT / "Runtime.py").read_text(encoding="utf-8")
m = re.search(r"(try:\n    import ai_cloudscraper as cloudscraper\n"
              r".*?_CLOUDSCRAPER_OK = False\n)", src, re.S)
assert m, "tier block not found in Runtime.py"
TIER_BLOCK = m.group(1)

def _tier_scenario(mods):
    saved = {k: sys.modules.get(k) for k in ("ai_cloudscraper", "cloudscraper")}
    try:
        for k in mods:
            sys.modules[k] = types.ModuleType(k)
        ns: dict = {}
        exec(TIER_BLOCK, ns)
        bound = ns.get("cloudscraper")
        return ns["_CLOUDSCRAPER_OK"], getattr(bound, "__name__", None)
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

ok_fork, name_fork = _tier_scenario({"cloudscraper"})       # fork's real shape
ok_ai, name_ai = _tier_scenario({"ai_cloudscraper"})        # future rename
ok_none, name_none = _tier_scenario({})                     # nothing installed
check("OD5 tier chain: fork binds via 2nd import, rename via 1st, none→False",
      ok_fork is True and name_fork == "cloudscraper"
      and ok_ai is True and name_ai == "ai_cloudscraper"
      and ok_none is False and name_none is None,
      f"fork=({ok_fork},{name_fork}) rename=({ok_ai},{name_ai}) none=({ok_none},{name_none})")

# --------------------------------------------------------------------------
# OD6 — WAFW00F not-installed notice: WARN level, no ERROR variant left
# (ast-based: the message spans two string literals, regex would miss it)
# --------------------------------------------------------------------------
import ast  # noqa: E402

rw_src = (ROOT / "Core" / "URL" / "Rewrite.py").read_text(encoding="utf-8")
warn_ok = False
for node in ast.walk(ast.parse(rw_src)):
    if not (isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "log"):
        continue
    msg = "".join(a.value for a in node.args
                  if isinstance(a, ast.Constant) and isinstance(a.value, str))
    if "WAFW00F not installed" in msg:
        level = node.args[-1]
        warn_ok = (isinstance(level, ast.Constant)
                   and level.value == "WARN")
err_left = []
for p in ROOT.rglob("*.py"):
    if p.name == "s2l.py":
        continue  # FROZEN parity monolith — fixes never land there
    t = p.read_text(encoding="utf-8", errors="replace")
    if "WAFW00F is not installed" in t and '"ERROR"' in t:
        err_left.append(p.name)
check("OD6 WAFW00F absence notice is WARN; no ERROR variant in package",
      warn_ok and not err_left,
      f"warn_present={warn_ok} error_variants={err_left or 'none'}")

# --------------------------------------------------------------------------
# OD7 — zstd streamed end-to-end through _stream_resp
# --------------------------------------------------------------------------
class FakeUpstream:
    def __init__(self, headers, chunks=()):
        self.headers = dict(headers)
        self.status_code = 200
        self._chunks = list(chunks)

    def iter_content(self, chunk_size=None):
        yield from self._chunks

    def close(self):
        pass


up = FakeUpstream({"Content-Type": "application/zip",
                   "Content-Encoding": "zstd",
                   "Connection": "close"},
                  chunks=(z2[:4096], z2[4096:]))
with app.test_request_context("/big.bin", method="GET"):
    resp = S._stream_resp(up, "GET", "https://cdn.example.com/big.bin")
    body = b"".join(resp.response)
hk = {k.lower() for k in resp.headers.keys()}
check("OD7 zstd through _stream_resp: body decoded, CE dropped",
      body == payload and "content-encoding" not in hk,
      f"body_ok={body == payload} keys={sorted(hk)}")

# --------------------------------------------------------------------------
print(f"\n{sum(1 for _, ok in RESULTS if ok)}/{len(RESULTS)} checks passed")
sys.exit(0 if all(ok for _, ok in RESULTS) else 1)
