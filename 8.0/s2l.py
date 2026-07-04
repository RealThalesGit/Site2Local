#!/usr/bin/env python3
import os, sys, re, csv, json, time, socket, queue, zlib
import hashlib, mimetypes, logging, threading, warnings
from dataclasses import dataclass, field, replace as _dc_replace
from typing import Callable
import base64
import struct as _struct
import ssl as _ssl_mod
import hashlib as _hashlib_ws
import base64 as _base64
from urllib.parse import urljoin, urlparse, urldefrag
import requests
import requests.adapters
import urllib3
import urllib3.util.retry
import cloudscraper
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from flask import Flask, Response, stream_with_context, request as flask_request, redirect
from werkzeug.serving import WSGIRequestHandler
import colorama
from colorama import Fore, Style
try:
    import brotli as _brotli
    _BROTLI_OK = True
except ImportError:
    _BROTLI_OK = False
# curl_cffi: real Chrome TLS fingerprinting — much more effective vs Cloudflare
# than cloudscraper (which uses Python ssl and has a detectable JA3 fingerprint).
# Install with: pip install curl-cffi --break-system-packages (not recommended on linux, use venv.)
try:
    from curl_cffi import requests as _cffi_requests
    _CURL_CFFI_OK = True
except ImportError:
    _cffi_requests = None
    _CURL_CFFI_OK  = False

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

sys.setrecursionlimit(20_000)
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RAINBOW_LOGS = False   # early default so log() works before CONFIG

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

_LOLCAT_PALETTE = (Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.CYAN, Fore.BLUE, Fore.MAGENTA)
_ANSI_ESC       = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_log_lock       = threading.Lock()

_LEVEL_COLOR: dict[str, str] = {
    "→":     Fore.BLUE,                      # proxied upstream request
    "←":     Fore.CYAN    + Style.DIM,        # cache hit
    "CDN":   Fore.MAGENTA + Style.BRIGHT,
    "HOOK":  Fore.YELLOW  + Style.BRIGHT,
    "SCAN":  Fore.RED     + Style.BRIGHT,
    "CRAWL": Fore.CYAN,
    "INFO":  Fore.GREEN,
    "WARN":  Fore.YELLOW,
    "ERROR": Fore.RED     + Style.BRIGHT,
    "DEBUG": Fore.CYAN    + Style.DIM,
}

def _lolcat_internal(text: str) -> str:
    plain = _ANSI_ESC.sub("", text)
    out, i = [], 0
    for ch in plain:
        if ch.isspace():
            out.append(ch)
        else:
            out.append(_LOLCAT_PALETTE[i % len(_LOLCAT_PALETTE)] + ch)
            i += 1
    return "".join(out) + Style.RESET_ALL

def log(msg: str, level: str = "INFO") -> None:
    ts    = time.strftime("%H:%M:%S")
    color = _LEVEL_COLOR.get(level, Fore.WHITE)
    tag   = f"{color}[{level}]{Style.RESET_ALL}"
    line  = f"{Style.DIM}{ts}{Style.RESET_ALL} {tag} {msg}"
    with _log_lock:
        print(_lolcat_internal(line) if RAINBOW_LOGS else line)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

SITE = "example.com"          # target domain or URL
HOST = "0.0.0.0"               # listen address
PORT = 8080                    # listen port
DEVICE = "auto"                # UA profile: auto|mobile|macintosh|ie11|iphone|ipad|bot — auto mirrors the requesting browser's own UA
MIME_FILE = "mimetypes.csv"
TIMEOUT_READ = 12              # upstream read timeout (s)
TIMEOUT_CONN = 6               # upstream connect timeout (s)
WORKERS = 40                   # crawler thread-pool size
MAX_FNAME = 180                # max on-disk path-segment length
SAVE_BATCH = 32                # files per save-flush
SAVE_INTERVAL = 0.15           # max seconds between save flushes
CRAWL_DEPTH = 7                # max URL path depth to follow
SCAN_LIMIT = 256 * 1024        # body bytes scanned in DUMP_ALL mode
RETRIES = 2                    # retry count on 5xx / timeout
BACKOFF = 0.4                  # exponential backoff base (s)
CRAWL = True                   # crawl at startup; False = proxy-on-demand only
OFFLINE = False                # never hit upstream — serve disk only
SAVE_ERRORS = False            # cache 4xx/5xx responses
DUMP_ALL = False               # extract + crawl every URL found in any response body
PROXY_CDN = True                # proxy external CDN/third-party assets
CACHE_CDN = True                # cache CDN assets to disk (False = live-proxy, no disk)
MULTIPORT = True                # each CDN host gets a dedicated port (False = /__s2l_ext__/)
HOOK_GUI = False                # Tkinter traffic inspector + live hook editor
RAINBOW_LOGS = False            # lolcat-style terminal output
SHOW_HIDDEN = False             # un-hide display:none / disabled elements in HTML
SCAN_PATHS = False              # probe /.git/.env/admin/etc at startup → JSON report
CAPTURE = False                  # record every request+response as JSON
CAPTURE_CDN = False              # include CDN responses in captures
CAPTURE_BODIES = True            # include request bodies in captures
CAPTURE_SKIP_STATIC = True       # skip images/fonts/JS/CSS from captures
COOP_COEP = False                # COOP + COEP + CORP headers (for WASM threads / SharedArrayBuffer)
EXTRA_PATHS: list = []           # extra paths to probe in SCAN_PATHS mode

# ──────────────────────────────────────────────────────────────────────────────
# Filters / constants
# ──────────────────────────────────────────────────────────────────────────────

CF_BLOCK_PATHS   = ("/cdn-cgi/challenge-platform", "/__cf_chl_", "/cdn-cgi/im/", "/cdn-cgi/login")
# Paths served by CF Image Resizing — legitimate, must NOT be in CF_BLOCK_PATHS above.
# Handled by the client-session interception block in proxy() (needs browser cf_clearance).
CF_IMG_PREFIXES  = ("/f=auto/", "/cdn-cgi/image/", "/f=webp/", "/f=avif/")
CDN_BLOCK    = ()
DUMP_TARGETS = {}

_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding",
    # "upgrade" is NOT stripped here — it's consumed by the WebSocket handler
    # which routes WS upgrades before reaching filter_fwd.
    "host", "content-length", "accept-encoding",
    # Strip conditional-GET headers from proxy→upstream direction.
    # The browser's cache validators are only valid for the browser↔proxy leg.
    # Forwarding them to upstream causes upstream to return 304 with no body,
    # triggering our expensive re-fetch loop. Stripping forces a fresh 200.
    "if-none-match", "if-modified-since", "if-unmodified-since", "if-match",
    "if-range",
    # Next.js RSC / SPA headers — strip so server returns full HTML, not wire format
    "rsc", "next-router-state-tree", "next-router-prefetch",
    "next-router-segment-prefetch", "next-url", "next-action",
    "x-nextjs-data",
    "x-nuxt-no-ssr", "x-remix-worker",
})

_STATIC_CTS = frozenset({
    "text/css",
    "application/javascript", "application/x-javascript", "text/javascript",
    "application/wasm",
    "font/woff", "font/woff2", "font/ttf", "font/otf",
    "application/font-woff", "application/font-woff2",
    "image/png", "image/jpeg", "image/gif", "image/webp",
    "image/svg+xml", "image/x-icon", "image/vnd.microsoft.icon",
    "audio/mpeg", "audio/ogg", "video/mp4", "video/webm",
})

_CONN_ERRORS_BASE = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.SSLError,
    requests.exceptions.ChunkedEncodingError,   # "connection ended prematurely"
    requests.exceptions.ContentDecodingError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.IncompleteRead,
    urllib3.exceptions.ProtocolError,
    ConnectionRefusedError,
    OSError,
)

# curl_cffi raises its OWN exception classes (separate hierarchy from requests.*),
# so without these, a connection failure from the curl_cffi backend — which is the
# DEFAULT bypass engine whenever it's installed (see _CURL_CFFI_OK below) — falls
# through every `except _CONN_ERRORS:` handler in this file (dead-host tracking,
# conn_errors stat, CF retry logic) and gets treated as a generic unexpected error
# instead of a recognised, recoverable connection failure.
if _CURL_CFFI_OK:
    try:
        from curl_cffi.requests.exceptions import (
            ConnectionError  as _CffiConnError,
            Timeout          as _CffiTimeout,
            RequestException as _CffiReqExc,
        )
        _CONN_ERRORS = _CONN_ERRORS_BASE + (_CffiConnError, _CffiTimeout, _CffiReqExc)
    except ImportError:
        _CONN_ERRORS = _CONN_ERRORS_BASE
else:
    _CONN_ERRORS = _CONN_ERRORS_BASE

_EXT_PREFIX  = "/__s2l_ext__"


def _raw_path_after(marker: str) -> str | None:
    """Return the RAW (still percent-encoded) request path following `marker`.

    Werkzeug's <path:...> route converter can't help here: by the time a WSGI
    request reaches Flask, the server has already percent-decoded PATH_INFO per
    PEP 3333, so an intentionally-encoded slash (%2F) inside a single opaque path
    segment is irreversibly indistinguishable from a real path separator.

    Firebase Storage is the textbook case: an object key that legitimately
    contains a slash (e.g. "listings/foo.jpg") is transmitted as ONE percent-
    encoded segment ("listings%2Ffoo.jpg") specifically so it is NOT split into
    two path segments. Once PATH_INFO decodes that %2F, we've already lost the
    distinction — re-emitting it as a literal "/" makes the real API see two
    segments instead of one opaque key, and it rejects the request outright.

    REQUEST_URI / RAW_URI are de-facto (non-PEP-3333) extensions that Werkzeug's
    dev server and most production WSGI servers populate with the untouched,
    still-encoded request target straight off the wire — use that instead
    whenever it's available; PATH_INFO remains the only option otherwise.
    """
    raw = (flask_request.environ.get("REQUEST_URI")
           or flask_request.environ.get("RAW_URI"))
    if not raw:
        return None
    raw_path = raw.split("?", 1)[0]
    idx = raw_path.find(marker)
    if idx == -1:
        return None
    return raw_path[idx + len(marker):]
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})

# RSC wire format signatures (Next.js streaming protocol)
_WIRE_FORMAT_SIGNATURES: tuple = (
    b":HL[", b":H[", b":I[", b":E[", b'0:{"',
    b"<turbo-stream", b'hx-swap-oob=',
)

def _is_wire_payload(data: bytes) -> bool:
    if not data: return False
    # Strip a UTF-8 BOM before checking signatures — some Next.js RSC streams
    # are emitted with one, which previously shifted every signature off the
    # front of the scanned window and made this check silently miss them.
    head = data.lstrip(b"\xef\xbb\xbf")[:64]
    return any(head.startswith(sig) for sig in _WIRE_FORMAT_SIGNATURES)

def _is_valid_html(data: bytes) -> bool:
    """Quick check that cached data is real HTML, not RSC or other junk."""
    if not data:
        return False
    head = data[:512].lower()
    return (b"<html" in head or b"<!doctype" in head or b"<head" in head)

# Extra headers to strip from forwarded requests (browser mode/navigation indicators).
# sec-fetch-* are stripped because they can expose proxy behaviour to the upstream server.
# sec-ch-ua* (Client Hints) are intentionally KEPT — they help CF fingerprinting.
# x-requested-with is intentionally kept — servers use it as CSRF defense.
_STRIP_FWD_EXTRA = frozenset({
    # sec-fetch-* are intentionally NOT stripped here —
    # they're needed by CF and Google for legitimate bot detection bypass.
    # The _do_upstream function adds them from the real browser request.
    "purpose",
})

# ──────────────────────────────────────────────────────────────────────────────
# Platform detection
# ──────────────────────────────────────────────────────────────────────────────

def detect_platform(headers: dict) -> str:
    h      = {k.lower(): v.lower() for k, v in headers.items()}
    server = h.get("server", "")
    via    = h.get("via", "")
    cf_ray = headers.get("CF-RAY", "")

    if "cloudflare" in server or cf_ray:                                      return "Cloudflare"
    if "cloudfront" in server or "cloudfront" in via \
       or any("x-amz-cf" in k for k in h):                                   return "AWS CloudFront"
    if "akamai" in server or "akamaized" in server or "akamai" in via:       return "Akamai"
    if "fastly" in server or "fastly" in via:                                 return "Fastly"
    if "incapsula" in server or "imperva" in server:                          return "Imperva/Incapsula"
    if "sucuri" in server:                                                    return "Sucuri WAF"
    if "cf-cache-status" in h:                                                return "Cloudflare (Cache)"
    if "oracle" in server:                                                    return "Oracle Cloud"
    if "varnish" in server or "varnish" in via:                               return "Varnish"
    if "edgecast" in server:                                                   return "EdgeCast CDN"
    if "nginx"  in server:                                                    return "Nginx"
    if "apache" in server:                                                    return "Apache"
    return "Unknown"

# ──────────────────────────────────────────────────────────────────────────────
# Hook system
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class HookContext:
    method:       str
    url:          str
    path:         str
    query:        str
    req_headers:  dict
    req_body:     bytes
    resp_status:  int   = 200
    resp_headers: dict  = field(default_factory=dict)
    resp_body:    bytes = b""
    resp_ct:      str   = "application/octet-stream"

    def json(self) -> object:
        return json.loads(self.resp_body)

    def set_json(self, obj: object) -> None:
        self.resp_body = json.dumps(obj, ensure_ascii=False).encode()
        self.resp_ct   = "application/json; charset=utf-8"
        self.resp_headers.pop("content-length", None)

    def req_json(self) -> object:
        return json.loads(self.req_body)

    def set_req_json(self, obj: object) -> None:
        self.req_body = json.dumps(obj, ensure_ascii=False).encode()
        self.req_headers["content-type"] = "application/json; charset=utf-8"


_REQ_HOOKS:  list[tuple[re.Pattern, frozenset, Callable]] = []
_RESP_HOOKS: list[tuple[re.Pattern, frozenset, Callable]] = []


def _norm_methods(methods) -> frozenset:
    if methods in ("*", None):
        return frozenset({"*"})
    if isinstance(methods, str):
        methods = [methods]
    return frozenset(m.upper() for m in methods)


def on_request(pattern: str = r".*", methods="*") -> Callable:
    pat  = re.compile(pattern, re.IGNORECASE)
    mset = _norm_methods(methods)
    def decorator(fn: Callable) -> Callable:
        _REQ_HOOKS.append((pat, mset, fn))
        log(f"req  hook [{','.join(sorted(mset))}] {pattern} → {fn.__name__}()", "HOOK")
        return fn
    return decorator


def on_response(pattern: str = r".*", methods="*") -> Callable:
    pat  = re.compile(pattern, re.IGNORECASE)
    mset = _norm_methods(methods)
    def decorator(fn: Callable) -> Callable:
        _RESP_HOOKS.append((pat, mset, fn))
        log(f"resp hook [{','.join(sorted(mset))}] {pattern} → {fn.__name__}()", "HOOK")
        return fn
    return decorator


def _run_hooks(hooks: list, ctx: HookContext) -> int:
    n = 0
    for pat, mset, fn in hooks:
        if "*" not in mset and ctx.method not in mset:
            continue
        if not pat.search(ctx.path):
            continue
        try:
            fn(ctx)
            n += 1
        except Exception as exc:
            log(f"hook {fn.__name__} raised: {exc}", "ERROR")
    return n

# ═══════════════════════════════════════════════════════════════════════════════
#  YOUR HOOKS GO HERE
#
#  `pattern` matches against ctx.path ONLY (e.g. "/gameinfo/@sdk"), never the
#  host — so a hook fires the same way whether that path is served by
#  MAIN_HOST, a /__s2l_ext__/ external asset, or a MULTIPORT CDN host's own
#  dedicated port. Check ctx.url if a hook needs to know which host it was.
# ═══════════════════════════════════════════════════════════════════════════════

# @on_request(methods="POST", pattern=r"/api/login")
# def _inject_remember(ctx: HookContext) -> None:
#     try:
#         body = ctx.req_json()
#         body["remember_me"] = True
#         ctx.set_req_json(body)
#     except Exception: pass

# @on_response(pattern=r"/api/user/me")
# def _patch_premium(ctx: HookContext) -> None:
#     try:
#         d = ctx.json(); d["premium"] = True; ctx.set_json(d)
#     except Exception: pass

# Works the same for a CDN-hosted endpoint, e.g. a first-party API on a
# completely different host than the page itself:
# @on_response(pattern=r"/gameinfo/@sdk")
# def _patch_sdk_info(ctx: HookContext) -> None:
#     try:
#         d = ctx.json(); d["debug"] = False; ctx.set_json(d)
#     except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────────
# Hook GUI  (HOOK_GUI)
#
# Workflow:
#   1. Traffic log shows every request.  Click a row → body appears in editor.
#   2. Edit the body, set a name / method filter / status code override.
#   3. "Save Hook" → appears in Active Hooks list with an Enabled toggle.
#   4. On each matching response the hook replaces resp_body + resp_status.
# ──────────────────────────────────────────────────────────────────────────────

# Each entry pushed to the GUI: full body included for editor display
_gui_log_queue: queue.Queue = queue.Queue(maxsize=2000)

# Active GUI request hooks (list of dicts, mutated only on GUI thread)
# {name, method, pattern, body_bytes, enabled: bool}
# Mirrors _gui_hooks below but overrides the body WE SEND to the real upstream,
# applied before the fetch — not the body the browser receives back.
_gui_req_hooks: list[dict] = []
_gui_req_hooks_lock = threading.Lock()


def _apply_gui_req_hooks(ctx: HookContext) -> None:
    """Apply the first matching enabled GUI request hook. Thread-safe.

    Call this BEFORE the upstream fetch (_do_upstream) — it overwrites
    ctx.req_body with the saved override so the real origin server receives
    the hook's body instead of whatever the browser actually sent.
    """
    if not HOOK_GUI:
        return
    with _gui_req_hooks_lock:
        hooks = list(_gui_req_hooks)

    for h in hooks:
        if not h["enabled"]:
            continue
        mf = h["method"].strip().upper()
        if mf not in ("", "*") and ctx.method.upper() != mf:
            continue
        try:
            if not re.search(h["pattern"], ctx.path, re.IGNORECASE):
                continue
        except re.error:
            continue

        new_body = h["body_bytes"]
        ctx.req_body = new_body
        ctx.req_headers["Content-Length"] = str(len(new_body))
        # Only touch Content-Type when the override actually parses as JSON —
        # avoids corrupting e.g. a multipart/form-data upload's boundary header
        # when the override wasn't meant to change the encoding at all.
        try:
            json.loads(new_body)
            ctx.req_headers["Content-Type"] = "application/json; charset=utf-8"
        except Exception:
            pass
        log(f"req hook '{h['name']}' matched {ctx.method} {ctx.path}  "
            f"{_fmt_size(len(new_body))}", "HOOK")
        break


# Active GUI hooks  (list of dicts, mutated only on GUI thread)
_gui_hooks: list[dict] = []   # {name, method, status, pattern, body_bytes, enabled: bool}
_gui_hooks_lock = threading.Lock()


def _gui_push(ctx: HookContext) -> None:
    """Push a proxied request/response to the GUI traffic log with body."""
    if not HOOK_GUI:
        return

    body = ctx.resp_body if ctx.resp_body is not None else b""

    # Show a readable placeholder in the Body Editor for truly empty responses
    # (e.g. API polling endpoints that 200 with no payload).
    # This is what was causing the "0B / blank body editor" confusion in the GUI.
    display_body = body
    if len(body) == 0:
        ct = (ctx.resp_ct or "").lower()
        if "json" in ct:
            display_body = b"(empty JSON response -- upstream returned 200 with no body)"
        elif "html" in ct:
            display_body = b"(empty HTML response -- upstream returned 200 with no body)"
        else:
            display_body = b"(empty response)"

    try:
        _gui_log_queue.put_nowait({
            "ts":     time.strftime("%H:%M:%S"),
            "method": ctx.method,
            "path":   ctx.path,
            "query":  ctx.query,
            "status": ctx.resp_status,
            "ct":     ctx.resp_ct.split(";")[0].strip() if ctx.resp_ct else "",
            "size":   len(body),           # real size (0 = correct)
            "body":   display_body,        # what the editor shows
        })
    except queue.Full:
        log(f"GUI queue full — dropped {ctx.method} {ctx.path}", "WARN")


def _gui_push_raw(method: str, path: str, status: int, ct: str, body: bytes,
                  display_tag: str = "") -> None:
    """Lightweight push for CDN mini-server / ext_asset routes.

    display_tag is shown as a prefix in the Content-Type column (e.g. "[cdn:8081]")
    so the path column always contains the bare path that hook patterns match against.
    Previously the tag was embedded in `path` itself, which meant the GUI traffic log
    showed e.g. "[cdn:8081] /generate_204" and users trying to write a hook pattern
    from it would include "[cdn:8081]" — causing their patterns to never match because
    _apply_gui_hooks (and _run_hooks) test against ctx.path which is always the bare path.
    """
    if not HOOK_GUI:
        return

    if body is None:
        body = b""
    elif not isinstance(body, bytes):
        try:
            body = str(body).encode("utf-8")
        except Exception:
            body = b""

    display_body = body if body else b"(empty response)"
    ct_clean = ct.split(";")[0].strip() if ct else ""
    if display_tag:
        ct_clean = f"{display_tag} {ct_clean}"

    try:
        _gui_log_queue.put_nowait({
            "ts":     time.strftime("%H:%M:%S"),
            "method": method,
            "path":   path,
            "query":  "",
            "status": status,
            "ct":     ct_clean,
            "size":   len(body),
            "body":   display_body,
        })
    except queue.Full:
        pass


def _apply_gui_hooks(ctx: HookContext) -> None:
    """Apply the first matching enabled GUI hook. Thread-safe."""
    if not HOOK_GUI:
        return
    with _gui_hooks_lock:
        hooks = list(_gui_hooks)

    for h in hooks:
        if not h["enabled"]:
            continue
        # Method filter
        mf = h["method"].strip().upper()
        if mf not in ("", "*") and ctx.method.upper() != mf:
            continue
        # Status filter — hook fires only when upstream status matches
        _rs = h.get("status", 0)
        hook_status = 0 if _rs in (0,"*","any","") else int(_rs)
        if hook_status != 0 and hook_status != ctx.resp_status:
            continue
        # Path + query pattern (match against full path?query so hooks can target specific params)
        try:
            if not re.search(h["pattern"], ctx.path, re.IGNORECASE):
                continue
        except re.error:
            continue

        # Apply hook — replace body, auto-detect content-type
        new_body = h["body_bytes"]
        ctx.resp_body = new_body
        # Only override HTTP status when hook specifies a real numeric code (>0)
        _hs = h.get("status", 0)
        if isinstance(_hs, int) and _hs > 0:
            ctx.resp_status = _hs
        ctx.resp_headers["content-length"] = str(len(new_body))
        ctx.resp_headers["_hooked"] = "1"
        try:
            json.loads(new_body)
            ctx.resp_ct = "application/json; charset=utf-8"
        except Exception:
            head = new_body[:64].lstrip()
            ctx.resp_ct = ("text/html; charset=utf-8" if head.startswith(b"<")
                           else "text/plain; charset=utf-8")
        log(f"hook '{h['name']}' matched {ctx.method} {ctx.path}  "
            f"[{ctx.resp_status}]  {_fmt_size(len(new_body))}", "HOOK")
        break


def _launch_hook_gui() -> None:
    """
    Launch the Tkinter GUI. Must be called from the MAIN thread on Linux/X11.
    When HOOK_GUI=True, Flask moves to a daemon thread and this blocks main.
    """
    try:
        import tkinter as tk
        from tkinter import ttk, scrolledtext, messagebox, simpledialog
    except ImportError:
        log("tkinter not available — HOOK_GUI ignored", "WARN")
        return

    def _gui_main() -> None:
        root = tk.Tk()
        root.title("S2L — Hook Inspector")

        # ── Responsive sizing: fill 90% of screen ────────────────────────────
        sw = root.winfo_screenwidth()
        sh = root.winfo_screenheight()
        w  = max(1000, int(sw * 0.90))
        h  = max(640,  int(sh * 0.88))
        x  = (sw - w) // 2
        y  = max(0, (sh - h) // 2 - 20)
        root.geometry(f"{w}x{h}+{x}+{y}")
        root.minsize(900, 560)

        # ── Theme: black background, white accent ─────────────────────────────
        _BG  = "#000000"
        _PNL = "#0d0d0d"
        _ACC = "#ffffff"
        _FG  = "#e0e0e0"
        _DIM = "#666677"
        _GRN = "#69ff69"
        _YLW = "#ffcc00"
        _RED = "#ff5555"
        _ENT = "#1a1a1a"   # entry/input background

        root.configure(bg=_BG)

        style = ttk.Style(root)
        style.theme_use("clam")
        style.configure("Treeview",
                        background=_PNL, foreground=_FG,
                        fieldbackground=_PNL, rowheight=22,
                        font=("Consolas", 10))
        style.configure("Treeview.Heading",
                        background="#111111", foreground=_ACC,
                        font=("Consolas", 10, "bold"))
        style.map("Treeview", background=[("selected", "#222222")])
        style.configure("TNotebook",         background=_BG, borderwidth=0)
        style.configure("TNotebook.Tab",     background="#111111", foreground=_FG,
                        padding=[10, 4], font=("Consolas", 10, "bold"))
        style.map("TNotebook.Tab",
                  background=[("selected", _PNL)],
                  foreground=[("selected", _ACC)])
        style.configure("TCombobox", fieldbackground=_ENT, background=_ENT,
                        foreground=_ACC, selectbackground="#333333",
                        selectforeground="#000000")
        style.configure("Vertical.TScrollbar",   background=_ENT, troughcolor=_BG)
        style.configure("Horizontal.TScrollbar", background=_ENT, troughcolor=_BG)
        # Fix combobox dropdown listbox — selected text must be black (visible)
        root.option_add("*TCombobox*Listbox.background",       "#ffffff")
        root.option_add("*TCombobox*Listbox.foreground",       "#000000")
        root.option_add("*TCombobox*Listbox.selectBackground", "#3a7bd5")
        root.option_add("*TCombobox*Listbox.selectForeground", "#ffffff")

        def _btn(parent, text, cmd, **kw):
            return tk.Button(parent, text=text, command=cmd,
                             bg=kw.pop("bg", "#1a1a1a"), fg=kw.pop("fg", _ACC),
                             relief="flat", activebackground="#2a2a2a",
                             activeforeground=_ACC,
                             font=kw.pop("font", ("Consolas", 9)), **kw)

        def _lbl(parent, text=None, **kw):
            font = kw.pop("font", ("Consolas", 9))
            bg   = kw.pop("bg", _PNL)
            fg   = kw.pop("fg", _DIM)
            kwargs = {"bg": bg, "fg": fg, "font": font}
            if text is not None:
                kwargs["text"] = text
            kwargs.update(kw)
            return tk.Label(parent, **kwargs)

        def _entry(parent, **kw):
            return tk.Entry(parent, bg=_ENT, fg=_ACC, insertbackground=_ACC,
                            relief="flat", font=("Consolas", 10), **kw)

        def _text_box(parent, **kw):
            return scrolledtext.ScrolledText(
                parent, bg="#050505", fg=_FG, insertbackground=_ACC,
                font=("Consolas", 10), relief="flat", **kw)

        def _bind_editor_keys(widget):
            """Bind Ctrl+A/C/V/X on Linux for any ScrolledText."""
            def _sa(e): widget.tag_add("sel","1.0","end"); return "break"
            def _sc(e):
                try:
                    s = widget.get("sel.first","sel.last")
                    root.clipboard_clear(); root.clipboard_append(s)
                except tk.TclError: pass
                return "break"
            def _sv(e):
                try:
                    t = root.clipboard_get()
                    try: widget.delete("sel.first","sel.last")
                    except tk.TclError: pass
                    widget.insert("insert", t)
                except tk.TclError: pass
                return "break"
            def _sx(e):
                _sc(e)
                try: widget.delete("sel.first","sel.last")
                except tk.TclError: pass
                return "break"
            for seq in ("<Control-a>","<Control-A>"): widget.bind(seq, _sa)
            for seq in ("<Control-c>","<Control-C>"): widget.bind(seq, _sc)
            for seq in ("<Control-v>","<Control-V>"): widget.bind(seq, _sv)
            for seq in ("<Control-x>","<Control-X>"): widget.bind(seq, _sx)

        # ── Notebook (tabs) ───────────────────────────────────────────────────
        nb = ttk.Notebook(root)
        nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # ══════════════════════════════════════════════════════════════════════
        # TAB 1: Traffic + Hooks
        # ══════════════════════════════════════════════════════════════════════
        tab_traffic = tk.Frame(nb, bg=_BG)
        nb.add(tab_traffic, text="  Traffic / Hooks")

        # Vertical split: top area (traffic + editor) | bottom (active hooks)
        v_pane = tk.PanedWindow(tab_traffic, orient=tk.VERTICAL, bg=_BG, sashwidth=5,
                                sashrelief="flat")
        v_pane.pack(fill=tk.BOTH, expand=True)

        top_area = tk.Frame(v_pane, bg=_BG)
        bot_area = tk.Frame(v_pane, bg=_PNL)
        v_pane.add(top_area, minsize=320)
        v_pane.add(bot_area, minsize=160)

        # Horizontal split inside top_area: left (traffic log) | right (body editor)
        h_pane = tk.PanedWindow(top_area, orient=tk.HORIZONTAL, bg=_BG, sashwidth=5,
                                sashrelief="flat")
        h_pane.pack(fill=tk.BOTH, expand=True)

        left  = tk.Frame(h_pane, bg=_PNL)
        right = tk.Frame(h_pane, bg=_PNL)
        h_pane.add(left,  minsize=460)
        h_pane.add(right, minsize=340)

        # ── LEFT: traffic log ─────────────────────────────────────────────────
        _lbl(left, "Traffic Log", fg=_ACC, font=("Consolas", 11, "bold"),
             bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))

        tree_frame = tk.Frame(left, bg=_PNL)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=4)

        cols = ("ts", "method", "path", "status", "ct", "size")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")
        for c, w, label in (
            ("ts",     70,  "Time"),
            ("method", 60,  "Method"),
            ("path",   0,   "Path"),   # 0 = stretch
            ("status", 54,  "Status"),
            ("ct",     130, "Content-Type"),
            ("size",   68,  "Size"),
        ):
            tree.heading(c, text=label)
            tree.column(c, width=w, anchor="w", stretch=(c == "path"))

        tree.tag_configure("GET",    foreground=_GRN)
        tree.tag_configure("POST",   foreground=_YLW)
        tree.tag_configure("PUT",    foreground="#ff9944")
        tree.tag_configure("PATCH",  foreground="#ff9944")
        tree.tag_configure("DELETE", foreground=_RED)
        tree.tag_configure("err",    foreground=_RED)
        tree.tag_configure("hooked", foreground="#ff44ff", font=("Consolas",10,"bold"))
        tree.tag_configure("api",    foreground="#44ddff")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical",   command=tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tree.pack(fill=tk.BOTH, expand=True)

        ctrl = tk.Frame(left, bg=_PNL)
        ctrl.pack(fill=tk.X, padx=8, pady=4)

        auto_scroll = tk.BooleanVar(value=True)
        paused      = tk.BooleanVar(value=False)
        tk.Checkbutton(ctrl, text="Auto-scroll", variable=auto_scroll,
                       bg=_PNL, fg=_FG, selectcolor=_BG,
                       activebackground=_PNL).pack(side=tk.LEFT)
        tk.Checkbutton(ctrl, text="Pause", variable=paused,
                       bg=_PNL, fg=_FG, selectcolor=_BG,
                       activebackground=_PNL).pack(side=tk.LEFT, padx=8)
        _btn(ctrl, "Clear", lambda: (
            [tree.delete(i) for i in tree.get_children()],
            _row_data.clear()
        )).pack(side=tk.LEFT)

        # ── Traffic Ctrl+F search bar ─────────────────────────────────────────
        search_frame = tk.Frame(left, bg=_PNL)
        search_frame.pack(fill=tk.X, padx=4, pady=(0, 2))
        _lbl(search_frame, "Find:", fg=_DIM, bg=_PNL).pack(side=tk.LEFT, padx=(4, 2))
        traffic_search_var = tk.StringVar()
        traffic_search_entry = _entry(search_frame)
        traffic_search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        search_frame.pack_forget()   # hidden until Ctrl+F

        def _traffic_search_apply(*_):
            q = traffic_search_var.get().lower()
            for iid in _row_data:
                d = _row_data[iid]
                visible = (not q or q in d["path"].lower()
                           or q in str(d["status"])
                           or q in d["ct"].lower()
                           or q in d["method"].lower())
                if not visible:
                    try:
                        tree.detach(iid)
                    except Exception:
                        pass
                else:
                    try:
                        tree.reattach(iid, "", "end")
                    except Exception:
                        pass

        def _traffic_search_close(*_):
            traffic_search_var.set("")
            _traffic_search_apply()
            search_frame.pack_forget()
            tree.focus_set()

        traffic_search_var.trace_add("write", _traffic_search_apply)
        traffic_search_entry.bind("<Escape>", _traffic_search_close)
        traffic_search_entry.bind("<Return>", lambda e: tree.focus_set())

        def _show_traffic_search(event=None):
            search_frame.pack(fill=tk.X, padx=4, pady=(0, 2),
                              after=ctrl)
            traffic_search_entry.focus_set()
            return "break"

        left.bind_all("<Control-f>", _show_traffic_search)
        left.bind_all("<Control-F>", _show_traffic_search)

        _row_data: dict[str, dict] = {}
        _MAX_ROWS = 800

        # ── RIGHT: body editor ────────────────────────────────────────────────
        _lbl(right, "Body Editor", fg=_ACC, font=("Consolas", 11, "bold"),
             bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))

        info_var = tk.StringVar(value="← Select a request")
        _lbl(right, textvariable=info_var, fg=_ACC, bg=_PNL).pack(anchor="w", padx=8)

        body_box = _text_box(right)
        body_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)
        _bind_editor_keys(body_box)

        # ── Body editor Ctrl+F find bar ───────────────────────────────────────
        body_find_frame = tk.Frame(right, bg=_PNL)
        body_find_frame.pack(fill=tk.X, padx=8, pady=(0, 2))
        _lbl(body_find_frame, "Find:", fg=_DIM, bg=_PNL).pack(side=tk.LEFT, padx=(0, 2))
        body_find_entry = _entry(body_find_frame, width=22)
        body_find_entry.pack(side=tk.LEFT, padx=(0, 4))
        body_find_match_lbl = _lbl(body_find_frame, text="", fg=_DIM, bg=_PNL)
        body_find_match_lbl.pack(side=tk.LEFT)
        body_find_frame.pack_forget()  # hidden until Ctrl+F

        _body_find_positions: list = []
        _body_find_idx       = [0]

        def _body_find_apply(*_):
            body_box.tag_remove("find_hi", "1.0", "end")
            _body_find_positions.clear()
            q = body_find_entry.get()
            if not q:
                body_find_match_lbl.config(text="")
                return
            start = "1.0"
            while True:
                pos = body_box.search(q, start, stopindex="end", nocase=True)
                if not pos:
                    break
                end_pos = f"{pos}+{len(q)}c"
                body_box.tag_add("find_hi", pos, end_pos)
                _body_find_positions.append(pos)
                start = end_pos
            body_box.tag_config("find_hi", background="#ffcc00", foreground="#000000")
            n = len(_body_find_positions)
            body_find_match_lbl.config(text=f"{n} match{'es' if n != 1 else ''}")
            if _body_find_positions:
                _body_find_idx[0] = 0
                body_box.see(_body_find_positions[0])

        def _body_find_next(event=None):
            if not _body_find_positions:
                return "break"
            _body_find_idx[0] = (_body_find_idx[0] + 1) % len(_body_find_positions)
            body_box.see(_body_find_positions[_body_find_idx[0]])
            return "break"

        def _body_find_prev(event=None):
            if not _body_find_positions:
                return "break"
            _body_find_idx[0] = (_body_find_idx[0] - 1) % len(_body_find_positions)
            body_box.see(_body_find_positions[_body_find_idx[0]])
            return "break"

        def _body_find_close(event=None):
            body_box.tag_remove("find_hi", "1.0", "end")
            body_find_frame.pack_forget()
            body_box.focus_set()

        body_find_entry.bind("<Return>",       _body_find_next)
        body_find_entry.bind("<Shift-Return>", _body_find_prev)
        body_find_entry.bind("<Escape>",       _body_find_close)
        body_find_entry.bind("<KeyRelease>",   _body_find_apply)

        def _show_body_find(event=None):
            body_find_frame.pack(fill=tk.X, padx=8, pady=(0, 2), before=body_box)
            body_find_entry.focus_set()
            sel = ""
            try:
                sel = body_box.get("sel.first", "sel.last")
            except tk.TclError:
                pass
            if sel:
                body_find_entry.delete(0, "end")
                body_find_entry.insert(0, sel)
                _body_find_apply()
            return "break"

        body_box.bind("<Control-f>", _show_body_find)
        body_box.bind("<Control-F>", _show_body_find)

        # Hook save controls
        save_frame = tk.Frame(right, bg=_PNL)
        save_frame.pack(fill=tk.X, padx=8, pady=(0, 2))

        row1 = tk.Frame(save_frame, bg=_PNL); row1.pack(fill=tk.X, pady=2)
        row2 = tk.Frame(save_frame, bg=_PNL); row2.pack(fill=tk.X, pady=2)

        _lbl(row1, "Name:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        name_entry = _entry(row1, width=14)
        name_entry.pack(side=tk.LEFT, padx=(2, 10))

        _lbl(row1, "Method:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        _METHOD_OPTS = ["*", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
        method_var = tk.StringVar(value="*")
        method_menu = ttk.Combobox(row1, textvariable=method_var, values=_METHOD_OPTS,
                                   width=7, state="readonly", font=("Consolas", 10))
        method_menu.pack(side=tk.LEFT, padx=(2, 10))

        _lbl(row1, "Status:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        _STATUS_OPTS = ["*", "200", "201", "204", "301", "302", "304",
                        "400", "401", "403", "404", "405", "419", "422",
                        "500", "502", "503"]
        status_var = tk.StringVar(value="*")
        status_menu = ttk.Combobox(row1, textvariable=status_var, values=_STATUS_OPTS,
                                   width=6, state="readonly", font=("Consolas", 10))
        status_menu.pack(side=tk.LEFT, padx=(2, 0))

        _lbl(row2, "Pattern (regex):", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        pat_entry = _entry(row2, width=34)
        pat_entry.insert(0, r".*")
        pat_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)

        save_status = _lbl(right, text="", fg=_DIM, bg=_PNL)
        save_status.pack(anchor="w", padx=8, pady=(0, 2))

        def _on_select(event):
            sel = tree.selection()
            if not sel:
                return
            iid  = sel[0]
            data = _row_data.get(iid)
            if not data:
                return

            path_qs = data["path"] + (f"?{data['query']}" if data.get("query") else "")
            info_var.set(f"{data['method']}  {path_qs}  [{data['status']}]  {data['ct']}")

            raw_body = data["body"]
            body_box.config(state="normal")
            body_box.delete("1.0", "end")
            try:
                txt = raw_body.decode("utf-8", errors="replace")
            except Exception:
                txt = raw_body.hex()
            if "json" in data["ct"]:
                try:
                    txt = json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
                except Exception:
                    pass
            body_box.insert("1.0", txt)

            # Auto-populate hook editor fields from selected request
            method_var.set(data["method"] if data["method"] in _METHOD_OPTS else "*")
            sc = str(data["status"])
            status_var.set(sc if sc in _STATUS_OPTS else "*")
            # Path pattern: insert raw path — user can add anchors/wildcards manually.
            # re.escape() was here before and produced ugly \/ and \. escapes.
            pat_entry.delete(0, "end")
            pat_entry.insert(0, data["path"])
            # Auto-suggest name from path (only if name field is empty).
            # Use the deepest meaningful segment, skipping hashes/UUIDs.
            if not name_entry.get().strip():
                segs = [s for s in data["path"].split("/") if s]
                name_candidate = ""
                for seg in reversed(segs):
                    # Skip hash-like or UUID-like segments (long, mostly hex/dots)
                    clean = seg.split("=")[-1]   # strip k= prefix
                    if len(clean) > 32 and re.search(r'^[A-Za-z0-9._-]+$', clean):
                        continue   # looks like a hash — skip
                    name_candidate = clean[:28]
                    break
                if not name_candidate and segs:
                    # Fallback: use last two segments joined
                    name_candidate = "_".join(s[:12] for s in segs[-2:])[:28]
                if name_candidate:
                    name_entry.delete(0, "end")
                    name_entry.insert(0, name_candidate)

        tree.bind("<<TreeviewSelect>>", _on_select)

        # ── Toolbar: Copy as cURL + Export/Import hooks ───────────────────────
        toolbar = tk.Frame(right, bg=_PNL)
        toolbar.pack(fill=tk.X, padx=8, pady=(0, 4))

        def _copy_as_curl():
            sel = tree.selection()
            if not sel:
                return
            data = _row_data.get(sel[0])
            if not data:
                return
            # Build a minimal cURL command from traffic log entry
            url   = f"http://localhost:{PORT}{data['path']}"
            if data.get("query"):
                url += f"?{data['query']}"
            method = data["method"]
            parts  = [f"curl -X {method}"]
            # Include request headers in cURL command
            for k, v in (data.get("req_headers") or {}).items():
                kl = k.lower()
                if kl in ("host", "content-length", "transfer-encoding"):
                    continue
                safe_v = str(v).replace("'", "'\\''")
                parts.append(f"  -H '{k}: {safe_v}'")
            if data.get("body") and method not in _SAFE_METHODS:
                body_str = data["body"].decode("utf-8", errors="replace").replace("'", "'\\''")
                parts.append(f"  -d '{body_str}'")
            parts.append(f"  '{url}'")
            curl_str = " \\\n".join(parts)
            root.clipboard_clear()
            root.clipboard_append(curl_str)
            save_status.config(text="cURL copied to clipboard.", fg=_GRN)

        def _copy_req_headers():
            """Copy request headers of selected traffic log entry to clipboard."""
            sel = tree.selection()
            if not sel:
                save_status.config(text="Select a request first.", fg=_RED)
                return
            data = _row_data.get(sel[0])
            if not data:
                return
            hdrs = data.get("req_headers") or {}
            if not hdrs:
                save_status.config(text="No request headers captured for this entry.", fg=_RED)
                return
            lines = []
            # First line: method + path
            path_qs = data["path"] + (f"?{data['query']}" if data.get("query") else "")
            lines.append(f"{data['method']} {path_qs}")
            lines.append("")
            for k, v in hdrs.items():
                lines.append(f"{k}: {v}")
            txt = "\n".join(lines)
            root.clipboard_clear()
            root.clipboard_append(txt)
            save_status.config(text=f"Request headers copied ({len(hdrs)} headers).", fg=_GRN)

        def _export_hooks():
            from tkinter.filedialog import asksaveasfilename
            fp = asksaveasfilename(
                title="Export Hooks", defaultextension=".json",
                filetypes=[("JSON", "*.json"), ("All", "*.*")],
                initialfile=f"{SITE_NAME}_hooks.json",
            )
            if not fp:
                return
            with _gui_hooks_lock:
                export_data = [
                    {k: v for k, v in h.items() if k != "body_bytes"}
                    | {"body": h["body_bytes"].decode("utf-8", errors="replace")}
                    for h in _gui_hooks
                ]
            with open(fp, "w", encoding="utf-8") as f:
                json.dump(export_data, f, indent=2)
            save_status.config(text=f"Exported {len(export_data)} hook(s).", fg=_GRN)

        def _import_hooks():
            from tkinter.filedialog import askopenfilename
            fp = askopenfilename(
                title="Import Hooks",
                filetypes=[("JSON", "*.json"), ("All", "*.*")],
            )
            if not fp:
                return
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    items = json.load(f)
                imported = 0
                for item in items:
                    hook = {
                        "name":       item.get("name", "imported"),
                        "method":     item.get("method", "*"),
                        "status":     int(item.get("status", 200)),
                        "pattern":    item.get("pattern", ".*"),
                        "body_bytes": item.get("body", "").encode("utf-8"),
                        "enabled":    item.get("enabled", True),
                    }
                    with _gui_hooks_lock:
                        names = [h["name"] for h in _gui_hooks]
                        if hook["name"] not in names:
                            _gui_hooks.append(hook)
                            imported += 1
                _render_hook_rows()
                save_status.config(text=f"Imported {imported} hook(s).", fg=_GRN)
            except Exception as e:
                messagebox.showerror("S2L", f"Import failed: {e}")

        _btn(toolbar, "Copy as cURL",        _copy_as_curl,      font=("Consolas", 9)).pack(side=tk.LEFT, padx=(0, 4))
        _btn(toolbar, "Copy Request Headers", _copy_req_headers,  font=("Consolas", 9)).pack(side=tk.LEFT, padx=(0, 4))
        _btn(toolbar, "Export Hooks",         _export_hooks,      font=("Consolas", 9)).pack(side=tk.LEFT, padx=(0, 4))
        _btn(toolbar, "Import Hooks",         _import_hooks,      font=("Consolas", 9)).pack(side=tk.LEFT)


        # ── BOTTOM: Active Hooks ──────────────────────────────────────────────
        _lbl(bot_area, "Active Hooks", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(4, 2))

        hooks_scroll_frame = tk.Frame(bot_area, bg=_PNL)
        hooks_scroll_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))

        hksb = ttk.Scrollbar(hooks_scroll_frame, orient="vertical")
        hksb.pack(side=tk.RIGHT, fill=tk.Y)
        hooks_canvas = tk.Canvas(hooks_scroll_frame, bg=_PNL, bd=0,
                                 highlightthickness=0, yscrollcommand=hksb.set)
        hooks_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        hksb.config(command=hooks_canvas.yview)

        hooks_frame = tk.Frame(hooks_canvas, bg=_PNL)
        hf_win = hooks_canvas.create_window((0, 0), window=hooks_frame, anchor="nw")

        def _on_hf_configure(e):
            hooks_canvas.configure(scrollregion=hooks_canvas.bbox("all"))
        def _on_canvas_configure(e):
            hooks_canvas.itemconfig(hf_win, width=e.width)
        hooks_frame.bind("<Configure>", _on_hf_configure)
        hooks_canvas.bind("<Configure>", _on_canvas_configure)

        _HOOK_COLS   = ("Name", "Method", "Status", "Pattern", "On", "", "")
        _HOOK_WIDTHS = (18, 8, 7, 32, 4, 5, 5)

        def _render_hook_rows():
            for w in hooks_frame.winfo_children():
                w.destroy()
            for col_idx, (col_name, col_w) in enumerate(zip(_HOOK_COLS, _HOOK_WIDTHS)):
                _lbl(hooks_frame, col_name, fg=_ACC, bg=_PNL,
                     font=("Consolas", 9, "bold"),
                     width=col_w, anchor="w").grid(row=0, column=col_idx, sticky="w", padx=3, pady=2)
            with _gui_hooks_lock:
                hooks_copy = list(_gui_hooks)
            if not hooks_copy:
                _lbl(hooks_frame, "No hooks saved yet.", fg=_DIM, bg=_PNL).grid(
                    row=1, column=0, columnspan=7, sticky="w", padx=4, pady=4)
                return
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                _lbl(hooks_frame, h["name"], bg=_PNL, fg=row_fg,
                     width=18, anchor="w").grid(row=row_idx, column=0, sticky="w", padx=3)
                _lbl(hooks_frame, h["method"], bg=_PNL,
                     fg=_YLW if h["enabled"] else _DIM,
                     width=8, anchor="w").grid(row=row_idx, column=1, sticky="w", padx=3)
                _lbl(hooks_frame, str(h["status"]), bg=_PNL,
                     fg=_GRN if h["enabled"] else _DIM,
                     width=7, anchor="w").grid(row=row_idx, column=2, sticky="w", padx=3)
                _lbl(hooks_frame,
                     h["pattern"][:40] + ("…" if len(h["pattern"]) > 40 else ""),
                     bg=_PNL, fg=_DIM, width=32, anchor="w").grid(row=row_idx, column=3, sticky="w", padx=3)

                # Enabled toggle
                bv = tk.BooleanVar(value=h["enabled"])
                def _make_toggle(hook_ref, var):
                    def _toggle():
                        with _gui_hooks_lock:
                            hook_ref["enabled"] = var.get()
                        _hook_save_to_disk(hook_ref)
                        _render_hook_rows()
                    return _toggle
                tk.Checkbutton(hooks_frame, variable=bv, command=_make_toggle(h, bv),
                               bg=_PNL, fg=_FG, selectcolor=_BG,
                               activebackground=_PNL,
                               width=2).grid(row=row_idx, column=4, padx=3)

                # Edit — load hook back into the editor
                def _make_edit(hook_ref):
                    def _edit():
                        name_entry.delete(0, "end")
                        name_entry.insert(0, hook_ref["name"])
                        method_var.set(hook_ref["method"] if hook_ref["method"] in _METHOD_OPTS else "*")
                        sc = str(hook_ref["status"])
                        status_var.set(sc if sc in _STATUS_OPTS else "200")
                        pat_entry.delete(0, "end")
                        pat_entry.insert(0, hook_ref["pattern"])
                        body_box.delete("1.0", "end")
                        try:
                            txt = hook_ref["body_bytes"].decode("utf-8", errors="replace")
                            if hook_ref["body_bytes"].startswith(b"{") or hook_ref["body_bytes"].startswith(b"["):
                                try:
                                    txt = json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
                                except Exception:
                                    pass
                        except Exception:
                            txt = hook_ref["body_bytes"].hex()
                        body_box.insert("1.0", txt)
                        save_status.config(text=f"Editing '{hook_ref['name']}'", fg=_YLW)
                    return _edit
                _btn(hooks_frame, "Ed", _make_edit(h),
                     font=("Consolas", 8), bg="#051a05",
                     fg=_GRN).grid(row=row_idx, column=5, padx=3)

                # Delete
                def _make_del(hook_name):
                    def _del():
                        with _gui_hooks_lock:
                            idx = next((i for i, x in enumerate(_gui_hooks)
                                        if x["name"] == hook_name), None)
                            if idx is not None:
                                _gui_hooks.pop(idx)
                        _hook_delete_from_disk(hook_name)
                        _render_hook_rows()
                        save_status.config(text=f"Deleted '{hook_name}'.", fg=_YLW)
                    return _del
                _btn(hooks_frame, "X", _make_del(h["name"]),
                     bg="#1a0505", fg=_RED,
                     font=("Consolas", 8)).grid(row=row_idx, column=6, padx=3)

        _render_hook_rows()

        # ── Hook disk persistence helpers ─────────────────────────────────────
        _HOOKS_DIR = os.path.join("site_data", "MyHooks", MAIN_HOST)
        os.makedirs(_HOOKS_DIR, exist_ok=True)

        def _hook_disk_path(name: str, body_bytes: bytes) -> str:
            """Determine file extension from body content."""
            head = body_bytes[:64].lstrip()
            if head.startswith(b"<"):
                ext = ".html"
            else:
                try:
                    json.loads(body_bytes)
                    ext = ".json"
                except Exception:
                    ext = ".txt"
            safe = re.sub(r"[^\w\-.]", "_", name)
            return os.path.join(_HOOKS_DIR, f"{safe}{ext}")

        def _hook_save_to_disk(hook: dict) -> None:
            try:
                p = _hook_disk_path(hook["name"], hook["body_bytes"])
                meta = {
                    "name":    hook["name"],
                    "method":  hook["method"],
                    "status":  hook["status"],
                    "pattern": hook["pattern"],
                    "enabled": hook["enabled"],
                }
                meta_path = p + ".meta.json"
                with open(p, "wb") as f:
                    f.write(hook["body_bytes"])
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
            except Exception as e:
                log(f"Hook disk save failed: {e}", "ERROR")

        def _hook_delete_from_disk(name: str) -> None:
            try:
                for fn in os.listdir(_HOOKS_DIR):
                    safe = re.sub(r"[^\w\-.]", "_", name)
                    if fn.startswith(safe) and not fn.endswith(".meta.json"):
                        os.remove(os.path.join(_HOOKS_DIR, fn))
                    if fn == f"{safe}.meta.json" or fn.startswith(safe + "."):
                        try:
                            os.remove(os.path.join(_HOOKS_DIR, fn))
                        except OSError:
                            pass
            except Exception as e:
                log(f"Hook disk delete failed: {e}", "ERROR")

        def _load_hooks_from_disk() -> None:
            """Load previously saved hooks from site_data/MyHooks/{host}/ at startup."""
            try:
                for fn in sorted(os.listdir(_HOOKS_DIR)):
                    if not fn.endswith(".meta.json"):
                        continue
                    meta_path = os.path.join(_HOOKS_DIR, fn)
                    try:
                        with open(meta_path, "r", encoding="utf-8") as f:
                            meta = json.load(f)
                        body_file = meta_path[:-len(".meta.json")]
                        if not os.path.isfile(body_file):
                            continue
                        with open(body_file, "rb") as f:
                            body_bytes = f.read()
                        hook = {
                            "name":       meta.get("name", fn),
                            "method":     meta.get("method", "*"),
                            "status":     int(meta.get("status", 200)),
                            "pattern":    meta.get("pattern", ".*"),
                            "body_bytes": body_bytes,
                            "enabled":    meta.get("enabled", True),
                        }
                        with _gui_hooks_lock:
                            existing = [h["name"] for h in _gui_hooks]
                            if hook["name"] not in existing:
                                _gui_hooks.append(hook)
                        log(f"Loaded hook '{hook['name']}' from disk", "HOOK")
                    except Exception as e:
                        log(f"Hook load error {fn}: {e}", "WARN")
            except Exception:
                pass

        root.after(100, lambda: (_load_hooks_from_disk(), _render_hook_rows()))

        def _save_hook():
            name = name_entry.get().strip()
            if not name:
                messagebox.showwarning("S2L", "Hook name is required.")
                return
            with _gui_hooks_lock:
                existing_names = [h["name"] for h in _gui_hooks]
            if name in existing_names:
                if not messagebox.askyesno("S2L",
                        f"A hook named '{name}' already exists.\nReplace it?"):
                    return
                with _gui_hooks_lock:
                    for i, h in enumerate(_gui_hooks):
                        if h["name"] == name:
                            _gui_hooks.pop(i)
                            break
            body_txt = body_box.get("1.0", "end-1c")
            if not body_txt.strip():
                messagebox.showwarning("S2L", "Hook body is empty — nothing to inject.")
                return
            _sv = status_var.get().strip()
            status = 0 if _sv in ("*","any","") else (int(_sv) if _sv.isdigit() else None)
            if status is None:
                messagebox.showerror("S2L", "Status must be a number or *.")
                return
            pat_str = pat_entry.get().strip() or r".*"
            try:
                re.compile(pat_str)
            except re.error as e:
                messagebox.showerror("S2L", f"Invalid pattern: {e}")
                return
            hook = {
                "name":       name,
                "method":     method_var.get(),
                "status":     status,
                "pattern":    pat_str,
                "body_bytes": body_txt.encode("utf-8"),
                "enabled":    True,
            }
            with _gui_hooks_lock:
                _gui_hooks.append(hook)
            _hook_save_to_disk(hook)
            _render_hook_rows()
            save_status.config(
                text=f"Saved '{name}'  [{hook['method']}]  {hook['pattern']}",
                fg=_GRN)
            log(f"GUI hook saved: '{name}'  [{hook['method']}]  {hook['pattern']}", "HOOK")

        def _test_hook():
            pat_str   = pat_entry.get().strip()
            mf        = method_var.get()
            test_path = simpledialog.askstring("Test Hook", "Enter a path to test:", parent=root)
            if test_path is None:
                return
            try:
                matched_pat = bool(re.search(pat_str, test_path, re.IGNORECASE))
            except re.error as e:
                messagebox.showerror("S2L", f"Pattern error: {e}")
                return
            matched_meth = mf in ("", "*")
            result = "MATCH" if matched_pat else "NO MATCH"
            save_status.config(
                text=f"{result}  pattern={'yes' if matched_pat else 'no'}  method={'any' if matched_meth else mf}",
                fg=_GRN if matched_pat else _RED)

        btn_row2 = tk.Frame(right, bg=_PNL)
        btn_row2.pack(fill=tk.X, padx=8, pady=(0, 4))
        _btn(btn_row2, "Save Hook", _save_hook,
             font=("Consolas", 10, "bold")).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        _btn(btn_row2, "Test Pattern", _test_hook,
             font=("Consolas", 9)).pack(side=tk.LEFT)

        # ══════════════════════════════════════════════════════════════════════
        # TAB 2: Request Hook
        # Mirrors the Response Hook editor above, but overrides ctx.req_body
        # BEFORE the upstream fetch (_apply_gui_req_hooks / _gui_req_hooks)
        # instead of the response body after it. No Status field — there's no
        # upstream response yet at the point this fires.
        # ══════════════════════════════════════════════════════════════════════
        tab_reqhook = tk.Frame(nb, bg=_BG)
        nb.add(tab_reqhook, text="  Request Hook")

        rh_pane = tk.PanedWindow(tab_reqhook, orient=tk.VERTICAL, bg=_BG, sashwidth=5)
        rh_pane.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        rh_top = tk.Frame(rh_pane, bg=_PNL)
        rh_bot = tk.Frame(rh_pane, bg=_PNL)
        rh_pane.add(rh_top, minsize=280)
        rh_pane.add(rh_bot, minsize=160)

        # ── Editor ───────────────────────────────────────────────────────────
        _lbl(rh_top, "Request Hook Editor", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
        _lbl(rh_top, "Overrides the body SENT TO UPSTREAM for matching requests, "
                     "before the fetch — the browser's own request is never touched.",
             fg=_DIM, bg=_PNL, wraplength=760, justify="left").pack(anchor="w", padx=8, pady=(0, 6))

        rh_row1 = tk.Frame(rh_top, bg=_PNL); rh_row1.pack(fill=tk.X, padx=8, pady=2)
        rh_row2 = tk.Frame(rh_top, bg=_PNL); rh_row2.pack(fill=tk.X, padx=8, pady=2)

        _lbl(rh_row1, "Name:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        rh_name_entry = _entry(rh_row1, width=14)
        rh_name_entry.pack(side=tk.LEFT, padx=(2, 10))

        _lbl(rh_row1, "Method:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        _RH_METHOD_OPTS = ["*", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
        rh_method_var = tk.StringVar(value="*")
        rh_method_menu = ttk.Combobox(rh_row1, textvariable=rh_method_var, values=_RH_METHOD_OPTS,
                                      width=7, state="readonly", font=("Consolas", 10))
        rh_method_menu.pack(side=tk.LEFT, padx=(2, 0))

        _lbl(rh_row2, "Pattern (regex):", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        rh_pat_entry = _entry(rh_row2, width=34)
        rh_pat_entry.insert(0, r".*")
        rh_pat_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)

        _lbl(rh_top, "Request Body  (replaces what the real origin server receives):",
             fg=_ACC, bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
        rh_body_box = _text_box(rh_top)
        rh_body_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))
        _bind_editor_keys(rh_body_box)

        rh_save_status = _lbl(rh_top, text="", fg=_DIM, bg=_PNL)
        rh_save_status.pack(anchor="w", padx=8, pady=(0, 2))

        # ── Active Request Hooks ─────────────────────────────────────────────
        _lbl(rh_bot, "Active Request Hooks", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(4, 2))

        rh_scroll_frame = tk.Frame(rh_bot, bg=_PNL)
        rh_scroll_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))

        rh_sb = ttk.Scrollbar(rh_scroll_frame, orient="vertical")
        rh_sb.pack(side=tk.RIGHT, fill=tk.Y)
        rh_canvas = tk.Canvas(rh_scroll_frame, bg=_PNL, bd=0,
                              highlightthickness=0, yscrollcommand=rh_sb.set)
        rh_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        rh_sb.config(command=rh_canvas.yview)

        rh_rows_frame = tk.Frame(rh_canvas, bg=_PNL)
        rh_win = rh_canvas.create_window((0, 0), window=rh_rows_frame, anchor="nw")

        def _on_rh_configure(e):
            rh_canvas.configure(scrollregion=rh_canvas.bbox("all"))
        def _on_rh_canvas_configure(e):
            rh_canvas.itemconfig(rh_win, width=e.width)
        rh_rows_frame.bind("<Configure>", _on_rh_configure)
        rh_canvas.bind("<Configure>", _on_rh_canvas_configure)

        _RH_COLS   = ("Name", "Method", "Pattern", "On", "", "")
        _RH_WIDTHS = (18, 8, 40, 4, 5, 5)

        def _render_req_hook_rows():
            for w in rh_rows_frame.winfo_children():
                w.destroy()
            for col_idx, (col_name, col_w) in enumerate(zip(_RH_COLS, _RH_WIDTHS)):
                _lbl(rh_rows_frame, col_name, fg=_ACC, bg=_PNL,
                     font=("Consolas", 9, "bold"),
                     width=col_w, anchor="w").grid(row=0, column=col_idx, sticky="w", padx=3, pady=2)
            with _gui_req_hooks_lock:
                hooks_copy = list(_gui_req_hooks)
            if not hooks_copy:
                _lbl(rh_rows_frame, "No request hooks saved yet.", fg=_DIM, bg=_PNL).grid(
                    row=1, column=0, columnspan=6, sticky="w", padx=4, pady=4)
                return
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                _lbl(rh_rows_frame, h["name"], bg=_PNL, fg=row_fg,
                     width=18, anchor="w").grid(row=row_idx, column=0, sticky="w", padx=3)
                _lbl(rh_rows_frame, h["method"], bg=_PNL,
                     fg=_YLW if h["enabled"] else _DIM,
                     width=8, anchor="w").grid(row=row_idx, column=1, sticky="w", padx=3)
                _lbl(rh_rows_frame,
                     h["pattern"][:40] + ("…" if len(h["pattern"]) > 40 else ""),
                     bg=_PNL, fg=_DIM, width=40, anchor="w").grid(row=row_idx, column=2, sticky="w", padx=3)

                bv = tk.BooleanVar(value=h["enabled"])
                def _make_rh_toggle(hook_ref, var):
                    def _toggle():
                        with _gui_req_hooks_lock:
                            hook_ref["enabled"] = var.get()
                        _req_hook_save_to_disk(hook_ref)
                        _render_req_hook_rows()
                    return _toggle
                tk.Checkbutton(rh_rows_frame, variable=bv, command=_make_rh_toggle(h, bv),
                               bg=_PNL, fg=_FG, selectcolor=_BG,
                               activebackground=_PNL,
                               width=2).grid(row=row_idx, column=3, padx=3)

                def _make_rh_edit(hook_ref):
                    def _edit():
                        rh_name_entry.delete(0, "end")
                        rh_name_entry.insert(0, hook_ref["name"])
                        rh_method_var.set(hook_ref["method"] if hook_ref["method"] in _RH_METHOD_OPTS else "*")
                        rh_pat_entry.delete(0, "end")
                        rh_pat_entry.insert(0, hook_ref["pattern"])
                        rh_body_box.delete("1.0", "end")
                        try:
                            txt = hook_ref["body_bytes"].decode("utf-8", errors="replace")
                            if hook_ref["body_bytes"].startswith(b"{") or hook_ref["body_bytes"].startswith(b"["):
                                try:
                                    txt = json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
                                except Exception:
                                    pass
                        except Exception:
                            txt = hook_ref["body_bytes"].hex()
                        rh_body_box.insert("1.0", txt)
                        rh_save_status.config(text=f"Editing '{hook_ref['name']}'", fg=_YLW)
                    return _edit
                _btn(rh_rows_frame, "Ed", _make_rh_edit(h),
                     font=("Consolas", 8), bg="#051a05",
                     fg=_GRN).grid(row=row_idx, column=4, padx=3)

                def _make_rh_del(hook_name):
                    def _del():
                        with _gui_req_hooks_lock:
                            idx = next((i for i, x in enumerate(_gui_req_hooks)
                                        if x["name"] == hook_name), None)
                            if idx is not None:
                                _gui_req_hooks.pop(idx)
                        _req_hook_delete_from_disk(hook_name)
                        _render_req_hook_rows()
                        rh_save_status.config(text=f"Deleted '{hook_name}'.", fg=_YLW)
                    return _del
                _btn(rh_rows_frame, "X", _make_rh_del(h["name"]),
                     bg="#1a0505", fg=_RED,
                     font=("Consolas", 8)).grid(row=row_idx, column=5, padx=3)

        _render_req_hook_rows()

        # ── Disk persistence (own folder, separate from response hooks) ────
        _REQ_HOOKS_DIR = os.path.join("site_data", "MyReqHooks", MAIN_HOST)
        os.makedirs(_REQ_HOOKS_DIR, exist_ok=True)

        def _req_hook_disk_path(name: str, body_bytes: bytes) -> str:
            head = body_bytes[:64].lstrip()
            if head.startswith(b"<"):
                ext = ".html"
            else:
                try:
                    json.loads(body_bytes)
                    ext = ".json"
                except Exception:
                    ext = ".txt"
            safe = re.sub(r"[^\w\-.]", "_", name)
            return os.path.join(_REQ_HOOKS_DIR, f"{safe}{ext}")

        def _req_hook_save_to_disk(hook: dict) -> None:
            try:
                p = _req_hook_disk_path(hook["name"], hook["body_bytes"])
                meta = {
                    "name":    hook["name"],
                    "method":  hook["method"],
                    "pattern": hook["pattern"],
                    "enabled": hook["enabled"],
                }
                meta_path = p + ".meta.json"
                with open(p, "wb") as f:
                    f.write(hook["body_bytes"])
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
            except Exception as e:
                log(f"Request hook disk save failed: {e}", "ERROR")

        def _req_hook_delete_from_disk(name: str) -> None:
            try:
                safe = re.sub(r"[^\w\-.]", "_", name)
                for fn in os.listdir(_REQ_HOOKS_DIR):
                    if fn.startswith(safe):
                        try:
                            os.remove(os.path.join(_REQ_HOOKS_DIR, fn))
                        except OSError:
                            pass
            except Exception as e:
                log(f"Request hook disk delete failed: {e}", "ERROR")

        def _load_req_hooks_from_disk() -> None:
            """Load previously saved request hooks from site_data/MyReqHooks/{host}/."""
            try:
                for fn in sorted(os.listdir(_REQ_HOOKS_DIR)):
                    if not fn.endswith(".meta.json"):
                        continue
                    meta_path = os.path.join(_REQ_HOOKS_DIR, fn)
                    try:
                        with open(meta_path, "r", encoding="utf-8") as f:
                            meta = json.load(f)
                        body_file = meta_path[:-len(".meta.json")]
                        if not os.path.isfile(body_file):
                            continue
                        with open(body_file, "rb") as f:
                            body_bytes = f.read()
                        hook = {
                            "name":       meta.get("name", fn),
                            "method":     meta.get("method", "*"),
                            "pattern":    meta.get("pattern", ".*"),
                            "body_bytes": body_bytes,
                            "enabled":    meta.get("enabled", True),
                        }
                        with _gui_req_hooks_lock:
                            existing = [h["name"] for h in _gui_req_hooks]
                            if hook["name"] not in existing:
                                _gui_req_hooks.append(hook)
                        log(f"Loaded request hook '{hook['name']}' from disk", "HOOK")
                    except Exception as e:
                        log(f"Request hook load error {fn}: {e}", "WARN")
            except Exception:
                pass

        root.after(100, lambda: (_load_req_hooks_from_disk(), _render_req_hook_rows()))

        def _save_req_hook():
            name = rh_name_entry.get().strip()
            if not name:
                messagebox.showwarning("S2L", "Hook name is required.")
                return
            with _gui_req_hooks_lock:
                existing_names = [h["name"] for h in _gui_req_hooks]
            if name in existing_names:
                if not messagebox.askyesno("S2L",
                        f"A request hook named '{name}' already exists.\nReplace it?"):
                    return
                with _gui_req_hooks_lock:
                    for i, h in enumerate(_gui_req_hooks):
                        if h["name"] == name:
                            _gui_req_hooks.pop(i)
                            break
            body_txt = rh_body_box.get("1.0", "end-1c")
            if not body_txt.strip():
                messagebox.showwarning("S2L", "Hook body is empty — nothing to send.")
                return
            pat_str = rh_pat_entry.get().strip() or r".*"
            try:
                re.compile(pat_str)
            except re.error as e:
                messagebox.showerror("S2L", f"Invalid pattern: {e}")
                return
            hook = {
                "name":       name,
                "method":     rh_method_var.get(),
                "pattern":    pat_str,
                "body_bytes": body_txt.encode("utf-8"),
                "enabled":    True,
            }
            with _gui_req_hooks_lock:
                _gui_req_hooks.append(hook)
            _req_hook_save_to_disk(hook)
            _render_req_hook_rows()
            rh_save_status.config(
                text=f"Saved '{name}'  [{hook['method']}]  {hook['pattern']}",
                fg=_GRN)
            log(f"GUI request hook saved: '{name}'  [{hook['method']}]  {hook['pattern']}", "HOOK")

        def _test_req_hook():
            pat_str   = rh_pat_entry.get().strip()
            mf        = rh_method_var.get()
            test_path = simpledialog.askstring("Test Request Hook", "Enter a path to test:", parent=root)
            if test_path is None:
                return
            try:
                matched_pat = bool(re.search(pat_str, test_path, re.IGNORECASE))
            except re.error as e:
                messagebox.showerror("S2L", f"Pattern error: {e}")
                return
            matched_meth = mf in ("", "*")
            result = "MATCH" if matched_pat else "NO MATCH"
            rh_save_status.config(
                text=f"{result}  pattern={'yes' if matched_pat else 'no'}  method={'any' if matched_meth else mf}",
                fg=_GRN if matched_pat else _RED)

        rh_btn_row = tk.Frame(rh_top, bg=_PNL)
        rh_btn_row.pack(fill=tk.X, padx=8, pady=(0, 6))
        _btn(rh_btn_row, "Save Request Hook", _save_req_hook,
             font=("Consolas", 10, "bold")).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        _btn(rh_btn_row, "Test Pattern", _test_req_hook,
             font=("Consolas", 9)).pack(side=tk.LEFT)

        # ── Polling loop ──────────────────────────────────────────────────────
        def _poll():
            if not paused.get():
                count = 0
                while count < 60:
                    try:
                        entry = _gui_log_queue.get_nowait()
                    except queue.Empty:
                        break
                    method = entry["method"]
                    status = entry["status"]
                    sz   = entry["size"]
                    if sz >= 1024 * 1024:
                        sz_s = f"{sz / 1024 / 1024:.1f}MB"
                    elif sz >= 1024:
                        sz_s = f"{sz // 1024}KB"
                    elif sz > 0:
                        sz_s = f"{sz}B"
                    else:
                        sz_s = "—"    # genuinely empty response; not a read error
                    _tag = ("hooked" if entry.get("hooked") else "err" if status>=400 else "api" if any(x in entry.get("ct","") for x in ("json","xml","event-stream")) else method if method in ("GET","POST","PUT","PATCH","DELETE") else "")
                    iid  = tree.insert("", "end", values=(
                        entry["ts"], method, entry["path"],
                        status, entry["ct"], sz_s,
                    ), tags=(_tag,))
                    _row_data[iid] = entry
                    children = tree.get_children()
                    if len(children) > _MAX_ROWS:
                        old = children[0]
                        _row_data.pop(old, None)
                        tree.delete(old)
                    if auto_scroll.get():
                        tree.see(iid)
                    count += 1
            root.after(120, _poll)

        root.after(120, _poll)
        root.mainloop()

    _gui_main()   # blocks — caller must be on main thread

# ──────────────────────────────────────────────────────────────────────────────
# UA profiles + device detection
# ──────────────────────────────────────────────────────────────────────────────

UA_PROFILES: dict[str, str] = {
    "mobile":    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Mobile Safari/537.36",
    "tablet":    "Mozilla/5.0 (Linux; Android 15; SM-X916B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "desktop":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "macintosh": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "ie11":      "Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko",
    "iphone":    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1",
    "ipad":      "Mozilla/5.0 (iPad; CPU OS 18_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Mobile/15E148 Safari/604.1",
    "symbian":   "Mozilla/5.0 (Symbian/3; Series60/5.2 NokiaN8-00/012.002; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/533.4 (KHTML, like Gecko) NokiaBrowser/7.3.0 Mobile Safari/533.4 3gpp-gba",
    "bot":       "Googlebot/2.1 (+http://www.google.com/bot.html)",
}

# Sec-CH-UA hint headers — must match the UA above.  Chromium-based UAs only.
_SEC_CH_UA: dict[str, dict] = {
    "desktop":   {
        "Sec-CH-UA":          '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile":   "?0",
        "Sec-CH-UA-Platform": '"Windows"',
    },
    "macintosh": {
        "Sec-CH-UA":          '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile":   "?0",
        "Sec-CH-UA-Platform": '"macOS"',
    },
    "mobile": {
        "Sec-CH-UA":          '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile":   "?1",
        "Sec-CH-UA-Platform": '"Android"',
    },
    "tablet": {
        "Sec-CH-UA":          '"Chromium";v="136", "Google Chrome";v="136", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile":   "?0",
        "Sec-CH-UA-Platform": '"Android"',
    },
}

_MOBILE_DEVICES = frozenset({"mobile", "iphone", "ipad", "symbian", "tablet"})

def _sanitize_ua(ua: str) -> str:
    return re.sub(r"[\t\r\n]+", " ", ua).strip()

def _detect_device_from_ua(ua: str) -> str:
    ua = ua.lower()
    if "ipad"     in ua:                      return "ipad"
    if "iphone"   in ua or "ipod" in ua:      return "iphone"
    if "android"  in ua and "mobile" in ua:   return "mobile"
    if "android"  in ua:                      return "tablet"
    if "macintosh" in ua or "mac os x" in ua: return "macintosh"
    if "windows"  in ua:                      return "desktop"
    if "bot"      in ua or "crawl" in ua:     return "bot"
    return "desktop"

def _effective_device() -> str:
    if DEVICE != "auto":
        return DEVICE
    try:
        return _detect_device_from_ua(flask_request.headers.get("User-Agent", ""))
    except RuntimeError:
        return "macintosh"

# ──────────────────────────────────────────────────────────────────────────────
# Session factory  (per-thread, Cloudflare-aware)
# ──────────────────────────────────────────────────────────────────────────────

_RETRY_POLICY = urllib3.util.retry.Retry(
    total            = RETRIES,
    backoff_factor   = BACKOFF,
    status_forcelist = {429, 500, 502, 503, 504},
    allowed_methods  = {"GET", "HEAD", "POST", "PUT", "PATCH"},
    raise_on_status  = False,
)

_proxy_local  = threading.local()

_CF_BROWSER_CONFIGS = [
    {"browser": "chrome",  "platform": "darwin",  "desktop": True},
    {"browser": "chrome",  "platform": "windows", "desktop": True},
    {"browser": "firefox", "platform": "windows", "desktop": True},
    {"browser": "chrome",  "platform": "android", "desktop": False},
]
_cf_config_idx = 0
_cf_config_lock = threading.Lock()

def _next_cf_config(mobile: bool) -> dict:
    """Round-robin through browser configs to avoid CF fingerprint blacklisting."""
    global _cf_config_idx
    with _cf_config_lock:
        if mobile:
            return {"browser": "chrome", "platform": "android", "desktop": False}
        cfg = _CF_BROWSER_CONFIGS[_cf_config_idx % len(_CF_BROWSER_CONFIGS)]
        _cf_config_idx += 1
        return cfg

def _make_session(device: str | None = None):
    """Return a requests-compatible session (curl_cffi → cloudscraper → requests fallback)."""
    d = device or _effective_device()
    mobile = d in _MOBILE_DEVICES
    ua     = _sanitize_ua(UA_PROFILES.get(d, UA_PROFILES["macintosh"]))
    ch     = _SEC_CH_UA.get(d, _SEC_CH_UA.get("desktop", {}))
    base_headers = {
        "User-Agent":                ua,
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br" if _BROTLI_OK else "gzip, deflate",
        "DNT":                       "1",
        "Upgrade-Insecure-Requests": "1",
        **ch,
    }

    # This is the only reliable method against Cloudflare Bot Management v2+.
    # Install: pip install curl-cffi --break-system-packages
    if _CURL_CFFI_OK:
        try:
            s = _cffi_requests.Session(impersonate="chrome136")
            s.headers.update(base_headers)
            return s
        except Exception as e:
            log(f"curl_cffi init failed, falling back: {e}", "WARN")

    cfg = _next_cf_config(mobile)
    try:
        s = cloudscraper.create_scraper(browser=cfg, delay=0)
        # MUST override UA after create_scraper — cloudscraper injects old UAs
        # (often Chrome 80-83) that fail browser version checks on modern sites.
        s.headers.update(base_headers)
        s.keep_alive = True
        s.verify     = False
        adapter = requests.adapters.HTTPAdapter(
            max_retries      = _RETRY_POLICY,
            pool_connections = 16,
            pool_maxsize     = 32,
            pool_block       = False,
        )
        s.mount("https://", adapter)
        s.mount("http://",  adapter)
        return s
    except Exception as e:
        log(f"cloudscraper init failed, falling back to requests: {e}", "WARN")

    s = requests.Session()
    s.headers.update(base_headers)
    adapter = requests.adapters.HTTPAdapter(
        max_retries      = _RETRY_POLICY,
        pool_connections = 16,
        pool_maxsize     = 32,
        pool_block       = False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s


def _is_cf_block(body: bytes, status: int, headers: dict | None = None) -> bool:
    """Detect Cloudflare challenge/block pages.

    Covers three distinct signal classes:
      • Classic 403/503 HTML block pages (Ray ID, cf-browser-verification)
      • CF Managed Challenge / Turnstile — these return HTTP 200, not 403/503,
        with a JS challenge embedded in the body (the "q=78" / jschl_vc family
        reported against poki.com). A status-only check misses these entirely.
      • The cf-mitigated: challenge response header, when present — the most
        reliable single signal Cloudflare gives us, independent of body content.
    """
    if headers:
        h = {k.lower(): v.lower() for k, v in headers.items()}
        if h.get("cf-mitigated") == "challenge":
            return True

    if not body:
        return False

    head = body[:8192].lower()

    # Classic block — these always carry one of these markers on 403/503
    if status in (403, 503):
        if (b"cf-browser-verification" in head
                or b"please enable cookies" in head
                or b"checking your browser" in head
                or b"cf-error-overview" in head
                or (b"ray id" in head and b"cloudflare" in head)
                or (b"attention required" in head and b"cloudflare" in head)):
            return True

    # JS challenge / Managed Challenge — can return 200, so checked regardless
    # of status. Signature set includes the q=78 / jschl_vc challenge tokens.
    if (b"jschl_vc" in head
            or b"jschl_answer" in head
            or b"cf-challenge" in head
            or b"cf_chl_prog" in head
            or b"__cf_chl_tk__" in head
            or b"__cf_chl_f_tk" in head
            or b"chl_captcha_widget" in head
            or b"window._cf_chl_opt" in head
            or (b"challenges.cloudflare.com" in head and b"<script" in head)):
        return True

    return False


# Minimal block responses some edges/WAFs return as plain text with no HTML
# wrapper at all ("blocked", "Access Denied", "Error 1020", ...). These are
# short enough to fall under the normal 64-byte floor AND lack <html>, so the
# regular bot-page heuristics never see them — they'd otherwise sail through
# and get served/cached as if they were real content (a blank-looking page
# that just says "blocked").
_RAW_BLOCK_TEXT: tuple[bytes, ...] = (
    b"blocked", b"access denied", b"forbidden",
    b"error 1020", b"error 1006", b"error 1009",
)

def _is_raw_block_text(body: bytes, status: int) -> bool:
    if status not in (200, 403, 429, 503):
        return False
    stripped = body.strip().lower()
    if not stripped or len(stripped) > 256:
        return False
    return any(p in stripped for p in _RAW_BLOCK_TEXT)


_BOT_PAGE_SIGNATURES: tuple[bytes, ...] = (
    # Google automated-query / Sorry page (HTTP 200 — tricky!)
    b"your computer or network may be sending automated queries",
    b"our systems have detected unusual traffic",
    b"support.google.com/websearch/answer/86640",
    b"<title>sorry...</title>",
    # CAPTCHA / bot-gate phrases — kept specific to avoid false positives
    b"prove you're not a robot",
    b"please verify you are a human",
    b"complete the captcha",
    b"robot or a human",
    b"unusual traffic from your computer",
    # DDoS-Guard
    b"ddos-guard.net",
    # hCaptcha
    b"hcaptcha.com/captcha",
    # reCAPTCHA
    b"recaptcha.net/recaptcha",
    b"google.com/recaptcha",
)

# API path prefixes that should NEVER be treated as bot pages — they return
# legitimate 403/401 HTML error payloads that look like bot pages.
# API/RPC paths that return legitimate 4xx HTML errors — never treat as bot pages
_BOT_EXEMPT_PREFIXES: tuple[str, ...] = (
    "/api/", "/v1/", "/v2/", "/v3/",
    "/graphql", "/rpc/", "/ajax/", "/xhr/",
    "/generate_204", "/gen_204",   # connectivity checks (always empty)
    "/log?",                       # logging beacons
)

def _is_bot_page(body: bytes, status: int = 200, path: str = "") -> bool:
    """Return True if the body is a bot-detection / CAPTCHA page.

    Only fires on HTML responses — JSON API 403s are never bot pages.
    Path-exempt prefixes (YouTube internal API, /api/, etc.) are skipped.
    """
    # API endpoints return legitimate 4xx — never block them
    if path and any(path.startswith(p) for p in _BOT_EXEMPT_PREFIXES):
        return False
    if _is_raw_block_text(body, status):
        return True
    if len(body) < 64:
        return False
    # Must look like HTML
    head = body[:16384].lower()
    if b"<html" not in head and b"<!doctype" not in head:
        return False
    if _is_cf_block(body, status, None):
        return True
    for sig in _BOT_PAGE_SIGNATURES:
        if sig in head:
            return True
    return False


def _get_proxy_session():
    if not hasattr(_proxy_local, "s"):
        _proxy_local.s = _make_session()
    return _proxy_local.s

# Per-client sessions: each unique browser (identified by IP) gets its own
# session so cookies don't bleed between clients connected to the same S2L.
_CLIENT_SESSIONS:    dict[str, object] = {}
_CLIENT_LAST:        dict[str, float]  = {}
_CLIENT_LOCK  = threading.Lock()
_CLIENT_TTL   = 1800  # 30 min idle before session is evicted (was 900 — too short for crawling)

def _client_id() -> str:
    """Stable per-browser identifier derived from source IP."""
    try:
        ip  = flask_request.remote_addr or "unknown"
        xff = flask_request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        return hashlib.sha1((xff or ip).encode()).hexdigest()[:16]
    except RuntimeError:
        return "default"

def _get_client_session():
    """Return a session bound to the calling browser's IP, not the thread."""
    cid  = _client_id()
    now  = time.time()
    with _CLIENT_LOCK:
        # Evict stale sessions to avoid memory growth
        stale = [k for k, t in _CLIENT_LAST.items() if now - t > _CLIENT_TTL]
        for k in stale:
            _CLIENT_SESSIONS.pop(k, None)
            _CLIENT_LAST.pop(k, None)
        if cid not in _CLIENT_SESSIONS:
            _CLIENT_SESSIONS[cid] = _make_session()
        _CLIENT_LAST[cid] = now
        return _CLIENT_SESSIONS[cid]

def _short_exc(exc: Exception) -> str:
    msg = str(exc)
    for pat in (r"HTTPSConnectionPool\(host='([^']+)'",
                r"nodename nor servname provided",
                r"SSL: (.+)", r"Caused by (.+)"):
        m = re.search(pat, msg, re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    first = msg.splitlines()[0]
    return first[:120] + ("…" if len(first) > 120 else "")

# ──────────────────────────────────────────────────────────────────────────────
# Body decompression  (fixes blank-page bug when upstream sends gzip/br)
# ──────────────────────────────────────────────────────────────────────────────

def decompress_body(data: bytes, encoding: str) -> bytes:
    """Decompress an HTTP response body.

    curl_cffi with impersonate=True auto-decompresses internally but keeps the
    original Content-Encoding header in the response. We use magic-byte checks
    to detect whether data is still compressed or already plain text, preventing
    double-decompression bugs (which silently return garbled/empty data).

    Stacked encodings like "gzip, br" are handled by splitting on comma.
    """
    if not data:
        return data
    enc = (encoding or "").lower().strip()

    # Stacked encodings: "gzip, br" → decode br first, then gzip
    if enc and "," in enc:
        parts = [e.strip() for e in enc.split(",") if e.strip()]
        for part in reversed(parts):
            data = decompress_body(data, part)
        return data

    # Magic-byte helpers
    def _looks_gzip(d: bytes) -> bool:
        return len(d) >= 2 and d[:2] == b"\x1f\x8b"

    def _looks_zlib(d: bytes) -> bool:
        return len(d) >= 2 and d[0] == 0x78 and d[1] in (0x01, 0x9c, 0xda)

    def _looks_json_or_text(d: bytes) -> bool:
        """True if data looks like already-decoded UTF-8 text (JSON, HTML, etc).
        More reliable than byte-frequency heuristics:
        - starts with '{', '[', '<', or whitespace (JSON/HTML/XML)
        - OR starts with printable ASCII and has no null bytes in first 32 bytes
        This avoids false-positives with brotli/gzip compressed data that
        happen to have many high-bit bytes (>= 0x80).
        """
        if not d:
            return True
        # Fast path: common text starters
        if d[0:1] in (b'{', b'[', b'<', b'"', b' ', b'\n', b'\r', b'\t'):
            return True
        # Null bytes never appear in valid UTF-8 text responses
        if b'\x00' in d[:64]:
            return False
        # gzip/zlib magic = definitely NOT decoded yet
        if _looks_gzip(d) or _looks_zlib(d):
            return False
        # Brotli has no universal magic header, but brotli-compressed data
        # never starts with ASCII printable bytes (it starts with a bit-stream
        # header). If first byte is printable ASCII (0x20-0x7E), likely decoded.
        return 0x20 <= d[0] <= 0x7E

    try:
        if enc in ("", "identity"):
            # No compression declared — only decompress if gzip magic bytes present.
            # Do NOT try brotli speculatively: brotli has no magic and would corrupt
            # plain JSON bodies from curl_cffi that already auto-decoded.
            if _looks_gzip(data):
                try:
                    result = zlib.decompress(data, zlib.MAX_WBITS | 16)
                    if result:
                        return result
                except Exception:
                    pass
            return data

        if enc == "gzip":
            if not _looks_gzip(data):
                return data   # curl_cffi already decoded — body is plain text
            try:
                return zlib.decompress(data, zlib.MAX_WBITS | 16)
            except zlib.error:
                return data

        if enc == "deflate":
            if _looks_json_or_text(data):
                return data
            if _looks_zlib(data):
                try:
                    return zlib.decompress(data)
                except Exception:
                    pass
            if _looks_gzip(data):
                try:
                    return zlib.decompress(data, zlib.MAX_WBITS | 16)
                except Exception:
                    pass
            try:
                return zlib.decompress(data, -zlib.MAX_WBITS)
            except zlib.error:
                return data

        if enc in ("br", "brotli"):
            # Only skip decompression if data is already clearly decoded text.
            # Use _looks_json_or_text (NOT _looks_readable) to avoid false-positives
            # on brotli-compressed data that has many high-bit bytes.
            if _looks_json_or_text(data):
                return data   # curl_cffi already decoded it
            if _BROTLI_OK:
                try:
                    return _brotli.decompress(data)
                except Exception:
                    # Decompression failed — data may already be decoded (curl_cffi)
                    return data
            log("brotli response received but 'brotli' library not installed "
                "(pip install brotli --break-system-packages) — body may be garbled", "WARN")
            return data

        if enc == "zstd":
            if _looks_json_or_text(data):
                return data
            try:
                import zstandard as _zstd
                return _zstd.ZstdDecompressor().decompress(data)
            except ImportError:
                log("zstd response but 'zstandard' not installed "
                    "(pip install zstandard --break-system-packages)", "WARN")
            except Exception:
                pass
            return data

    except Exception:
        pass

    return data

_mime_lock:  threading.Lock = threading.Lock()
_mime_cache: set | None     = None

def _load_mimes() -> set:
    global _mime_cache
    with _mime_lock:
        if _mime_cache is not None:
            return _mime_cache
        mimes: set = set()
        try:
            with open(MIME_FILE, newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if row and not row[0].startswith("#"):
                        mimes.add(row[0].strip().lower())
            log(f"Loaded {len(mimes)} MIME types from {MIME_FILE}")
        except FileNotFoundError:
            log(f"{MIME_FILE} not found — extension detection only", "WARN")
        except Exception as e:
            log(f"MIME load failed: {e}", "WARN")
        _mime_cache = mimes
        return mimes

def guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

# local_path() collapses any extensionless URL to .../index.html so the cache
# lookup stays deterministic without knowing the real content-type up front
# (we can't know it before the first fetch). But plenty of real sites serve
# CSS/JS/JSON from hash-named, extensionless URLs — those would then get
# treated as HTML on every future cache hit: script-injected, content-type
# mislabeled, and broken in the browser. This sidecar file records the REAL
# upstream Content-Type next to the cached body whenever it disagrees with
# what the on-disk extension implies, so _serve_cached() can trust it instead.
_CTYPE_SIDECAR_EXT = ".s2l-ctype"

def _ctype_sidecar_path(lp: str) -> str:
    return lp + _CTYPE_SIDECAR_EXT

def _save_ctype_sidecar(lp: str, content_type: str) -> None:
    try:
        with open(_ctype_sidecar_path(lp), "w", encoding="utf-8") as f:
            f.write(_ct_base(content_type))
    except OSError:
        pass

def _load_ctype_sidecar(lp: str) -> str | None:
    sp = _ctype_sidecar_path(lp)
    try:
        with open(sp, "r", encoding="utf-8") as f:
            return f.read().strip() or None
    except OSError:
        return None

def resolve_mime(lp: str) -> str:
    """Prefer the recorded real Content-Type over the path-extension guess."""
    return _load_ctype_sidecar(lp) or guess_mime(lp)

def _ct_base(ct: str) -> str:
    return ct.split(";")[0].strip().lower()

def is_static_asset(ct: str) -> bool:
    return _ct_base(ct) in _STATIC_CTS

# Subresource = anything that's typically not interesting to log (images, fonts, wasm, video)
_SUBRESOURCE_CTS = _STATIC_CTS | frozenset({
    "application/wasm", "audio/mpeg", "audio/ogg",
    "video/mp4", "video/webm", "video/ogg",
})

def is_subresource(ct: str) -> bool:
    return _ct_base(ct) in _SUBRESOURCE_CTS

# ──────────────────────────────────────────────────────────────────────────────
# Header helpers
# ──────────────────────────────────────────────────────────────────────────────

def filter_fwd(headers: dict) -> dict:
    skip = _HOP_BY_HOP | _STRIP_FWD_EXTRA
    return {k: v for k, v in headers.items() if k.lower() not in skip}

def filter_resp(headers: dict) -> dict:
    # Strip security headers that block our local proxy, plus hop-by-hop headers
    skip = _HOP_BY_HOP | {
        "content-encoding",
        "content-security-policy", "content-security-policy-report-only",
        "x-frame-options", "strict-transport-security", "x-content-type-options",
        "cross-origin-opener-policy", "cross-origin-embedder-policy",
        "cross-origin-resource-policy", "permissions-policy",
        "nel", "report-to", "reporting-endpoints",
        # Strip upstream CORS — we set our own wildcard below so browser CORS
        # checks don't fail on Discord/Slack API responses going through the proxy.
        "access-control-allow-origin",
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-expose-headers",
        "access-control-max-age",
    }
    out = {k: v for k, v in headers.items() if k.lower() not in skip}
    # Always grant open CORS from the proxy
    out["Access-Control-Allow-Origin"]  = "*"
    out["Access-Control-Expose-Headers"] = "*"
    # Cross-Origin-Resource-Policy: cross-origin — required so that resources served
    # through S2L (images, scripts, fonts from CDN) can be consumed by cross-origin HTML
    # pages also going through S2L (game iframes, etc.). Without this, when COEP is
    # active any resource without CORP is blocked by the browser.
    out["Cross-Origin-Resource-Policy"] = "cross-origin"
    # COOP + COEP: games and apps that use SharedArrayBuffer or WASM threads require
    # Cross-Origin isolation. COEP=credentialless is less strict than require-corp
    # (it allows no-credentials sub-resources without explicit CORP) but still enables
    # isolation. COOP=same-origin prevents opener attacks.
    if COOP_COEP:
        out["Cross-Origin-Opener-Policy"]   = "same-origin"
        out["Cross-Origin-Embedder-Policy"] = "credentialless"
    if "Set-Cookie" in out:
        def _rewrite_cookie(c: str) -> str:
            c = re.sub(r";\s*SameSite=[^;]+", "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Secure\b",        "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Partitioned\b", "", c, flags=re.IGNORECASE)
            # Strip Domain= entirely rather than rewriting it — a Domain that
            # doesn't match the host the browser is actually on gets the WHOLE
            # cookie silently rejected. Dropping it makes the cookie host-only,
            # which is always valid for whatever host is currently in the
            # address bar (works the same whether the user reaches this proxy
            # via a spoofed MAIN_HOST hosts-file entry or directly via
            # localhost — no separate-mode handling needed).
            c = re.sub(r";\s*Domain=[^;]+", "", c, flags=re.IGNORECASE)
            return c
        cookie_val = out["Set-Cookie"]
        if isinstance(cookie_val, list):
            out["Set-Cookie"] = [_rewrite_cookie(c) for c in cookie_val]
        else:
            out["Set-Cookie"] = _rewrite_cookie(cookie_val)
    return out

# ──────────────────────────────────────────────────────────────────────────────
# General utils
# ──────────────────────────────────────────────────────────────────────────────

def inject_csrf_headers(fwd: dict) -> None:
    """Forward CSRF tokens from browser → upstream so POST/PUT requests don't 403."""
    for key in flask_request.headers.keys():
        kl = key.lower()
        if kl in ("x-csrf-token", "x-csrftoken", "x-xsrf-token",
                  "x-requested-with", "x-request-id"):
            fwd[key] = flask_request.headers[key]


def rewrite_origin(fwd: dict, origin_base: str) -> None:
    """Rewrite Origin/Referer from proxy localhost address → real target origin.

    Without this, CORS preflight checks see Origin: http://localhost:8080 and
    reject it.  Referer is rewritten similarly so hotlink-protection passes.
    """
    for key in list(fwd.keys()):
        kl = key.lower()
        if kl == "origin":
            fwd[key] = origin_base
        elif kl == "referer":
            try:
                p = urlparse(fwd[key])
                fwd[key] = origin_base + p.path + (f"?{p.query}" if p.query else "")
            except Exception:
                fwd[key] = origin_base + "/"


def rewrite_abs_urls(html: bytes) -> bytes:
    """Rewrite absolute MAIN_HOST URLs inside HTML *attribute values* → proxy-relative.

    Only targets attribute-value contexts (href=, src=, action=, data-href=, etc.)
    so that inline JSON/JS (ytInitialData, __NEXT_DATA__, etc.) is NOT modified.
    Touching bare JSON strings breaks YouTube/Next.js because their JS uses the
    full URLs for API calls and video manifests.

    Before: href="https://www.youtube.com/watch?v=abc"
    After:  href="/watch?v=abc"

    JSON (untouched): "url":"https://www.youtube.com/watch?v=abc"
    """
    if not html or not MAIN_HOST:
        return html
    # Match only when preceded by an HTML attribute value opener (=" or =')
    # This excludes bare JSON strings and JS string literals outside attributes.
    # The trailing negative lookahead requires the host to actually END there
    # (next char is /, ", ', :, ?, # or nothing) — without it, a bare prefix
    # match would also strip "https://poki.community/..." or any other domain
    # that merely starts with the same characters as MAIN_HOST.
    #
    # The replacement is an ABSOLUTE http://localhost:PORT URL rather than a
    # bare relative path: this function runs standalone whenever PROXY_CDN is
    # False (no _rewrite_ext_urls pass first), and also on CDN documents served
    # from a different origin (dedicated MULTIPORT port, /__s2l_ext__/...) —
    # a relative path only resolves correctly back to MAIN_HOST when the
    # current document already happens to be served from the main proxy port.
    host_b = re.escape(MAIN_HOST.encode())
    proxy_root = f"http://localhost:{PORT}".encode()
    pattern = (rb'((?:href|src|action|data-src|data-href|poster|srcset|content)\s*=\s*["\'])'
               rb'https?://' + host_b + rb'(?![A-Za-z0-9\-.])')
    html = re.sub(pattern, rb'\1' + proxy_root, html, flags=re.IGNORECASE)

    # <meta http-equiv="refresh" content="0;url=https://MAIN_HOST/path"> — the
    # URL is never the first thing after the quote ("N;url=..."), so the
    # pattern above never matches it. Same risk class as an unrewritten
    # location.href: the browser performs this navigation on its own.
    def _meta_refresh_rep(m: re.Match) -> bytes:
        tag = m.group(0)
        if not re.search(rb'http-equiv\s*=\s*["\']refresh["\']', tag, re.IGNORECASE):
            return tag
        return re.sub(
            rb'(content\s*=\s*["\'][^"\']*?url\s*=\s*)https?://' + host_b + rb'(?![A-Za-z0-9\-.])',
            rb'\1' + proxy_root, tag, flags=re.IGNORECASE)
    html = re.sub(rb'<meta\b[^>]*>', _meta_refresh_rep, html, flags=re.IGNORECASE)

    return html


def strip_fragment(u: str) -> str:
    return urldefrag(u)[0]

def normalize_url(u: str) -> str:
    u = strip_fragment(u).strip()
    return "https:" + u if u.startswith("//") else u

def resolve_ip(host: str) -> str:
    try:
        return socket.gethostbyname(host)
    except Exception:
        return "?"

def url_depth(u: str) -> int:
    return urlparse(u).path.count("/")

def is_html(data: bytes, ct: str) -> bool:
    if ct and "text/html" in ct.lower():
        return True
    head = data[:1024].lower()
    return b"<html" in head or b"<!doctype" in head

def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}GB"


def log_req(method: str, status: int, host: str, path: str, size: int, tag: str = "") -> None:
    """Structured one-line request log + GUI traffic-log entry.

    For routes that return a custom Response early (bypassing the normal
    ctx/_gui_push pipeline) but still want a visible, consistent log line —
    e.g. the Cloudflare Image Optimization shortcut. Referenced there but
    never actually defined (a dropped-during-merge regression), which meant
    every successful hit of that path 500'd on a NameError instead of
    returning the image.
    """
    prefix = f"[{tag}] " if tag else ""
    log(f"{prefix}{method:6} {status}  {host}{path}  {_fmt_size(size)}", "→")
    _gui_push_raw(method, path, status, "", b"", display_tag=f"[{tag}]" if tag else "")

# ──────────────────────────────────────────────────────────────────────────────
# HTML reveal  (SHOW_HIDDEN)
# ──────────────────────────────────────────────────────────────────────────────

_REVEAL_STYLE = (
    "*[style*='display:none'],*[style*='display: none'],"
    "*[style*='visibility:hidden'],*[style*='visibility: hidden'],"
    "*[style*='opacity:0']{"
    "display:block!important;visibility:visible!important;opacity:1!important;"
    "outline:2px dashed #f00!important;background:rgba(255,0,0,.04)!important}"
    "[hidden],[disabled],[readonly]{pointer-events:auto!important;opacity:1!important}"
)
_HIDDEN_SELECTORS = [
    "[style*='display:none']", "[style*='display: none']",
    "[style*='visibility:hidden']", "[style*='visibility: hidden']",
    "[style*='opacity:0']", "[hidden]", "[disabled]", "[readonly]",
]

def _reveal_hidden(html_bytes: bytes) -> bytes:
    try:
        soup = BeautifulSoup(html_bytes.decode("utf-8", "ignore"), "lxml")
        style_tag = soup.new_tag("style")
        style_tag.string = _REVEAL_STYLE
        head = soup.find("head")
        (head or soup).insert(0, style_tag)
        for sel in _HIDDEN_SELECTORS:
            for el in soup.select(sel):
                style = el.attrs.get("style", "")
                style = re.sub(r"display\s*:\s*none\s*;?",      "display:block;",      style, flags=re.I)
                style = re.sub(r"visibility\s*:\s*hidden\s*;?", "visibility:visible;", style, flags=re.I)
                style = re.sub(r"opacity\s*:\s*0\s*;?",         "opacity:1;",          style, flags=re.I)
                if style.strip():
                    el.attrs["style"] = style
                for attr in ("hidden", "disabled", "readonly"):
                    el.attrs.pop(attr, None)
        return soup.encode("utf-8")
    except Exception as e:
        log(f"reveal_hidden failed: {e}", "WARN")
        return html_bytes



def _build_s2l_injector() -> bytes:
    with _cdn_port_lock:
        hp = dict(_cdn_host_port)
    js = (
        '<script id="__s2l__">'
        '(function(){'
        'var M=' + json.dumps(MAIN_HOST) + ';'
        'var HP=' + json.dumps(hp) + ';'
        'var EXT=' + json.dumps(_EXT_PREFIX) + ';'
        'var MP=' + json.dumps(PORT) + ';'   # the REAL main-proxy port — see __s2l_core below
        'var PX="http://"+location.hostname+":"+MP;'
        # ── Shared core: rw() + fetch/XHR/WebSocket/Worker patches ─────────────
        # Defined as a standalone named function so its .toString() source can
        # also be re-executed inside a Worker's own global scope (workers don't
        # share window — see the Worker-wrapping block below). Must not close
        # over any outer variable; everything it needs is passed as an argument.
        #
        # CRITICAL: this same script is also injected into every CDN-multiport
        # sub-iframe (e.g. a game host gets its own port, :8087). Inside THAT
        # document, location.port is :8087, NOT the main proxy's port — but
        # /__s2l_ext__/ and /__s2l_ws_ext__/ only exist on the main app. A bare
        # relative path for the main-host case would also resolve against
        # :8087, not the main proxy. So proxyPort here must always be the TRUE
        # main port (MP), never the current document's own location.port —
        # the one case that doesn't need this is a registered CDN host's own
        # port, which is looked up explicitly from HP and is correct as-is.
        'function __s2l_core(M,HP,EXT,proxyHost,proxyPort){'
          'var PX="http://"+proxyHost+":"+proxyPort;'
          'function rw(u){'
          'if(!u||typeof u!="string"||u[0]=="/"||u.indexOf("://")<0)return u;'
          'try{var p=new URL(u,"http://"+proxyHost+"/");'
          'if(p.hostname===proxyHost)return u;'
          'if(p.host===M||p.host==="www."+M)return PX+p.pathname+(p.search||"")+(p.hash||"");'
          'if(p.protocol==="wss:"||p.protocol==="ws:"){'
            'var wh=p.host;'
            'var cdnp=HP[p.hostname]||HP[p.host];'
            'if(cdnp)return "ws://"+proxyHost+":"+cdnp+p.pathname+(p.search||"")+(p.hash||"");'
            'return "ws://"+proxyHost+":"+proxyPort+"/__s2l_ws_ext__/"+wh+p.pathname+(p.search||"")+(p.hash||"");'
          '}'
          'if(HP[p.host])return "http://"+proxyHost+":"+HP[p.host]+p.pathname+(p.search||"")+(p.hash||"");'
          'if(p.protocol==="https:"&&p.hostname!==proxyHost)return PX+EXT+"/"+p.host+p.pathname+(p.search||"")+(p.hash||"");'
          '}catch(e){}return u;}'
          'try{var _f=self.fetch;if(_f)self.fetch=function(i,o){'
            'if(typeof i==="string")i=rw(i);'
            'else if(i&&i.url){var r=rw(i.url);if(r!==i.url)i=new Request(r,i);}'
            'return _f.call(this,i,o);};'
          '}catch(e){}'
          'try{if(typeof XMLHttpRequest!=="undefined"){'
            'var _x=XMLHttpRequest.prototype.open;'
            'XMLHttpRequest.prototype.open=function(){'
              'var a=Array.prototype.slice.call(arguments);a[1]=rw(a[1]);'
              'return _x.apply(this,a);};'
          '}}catch(e){}'
          'try{var _WS=self.WebSocket;if(_WS){'
            'function S2LWebSocket(url,protos){'
              'var ru=(typeof url==="string")?rw(url):url;'
              'if(protos)return new _WS(ru,protos);'
              'return new _WS(ru);'
            '}'
            'S2LWebSocket.prototype=_WS.prototype;'
            '["CONNECTING","OPEN","CLOSING","CLOSED"].forEach(function(k){S2LWebSocket[k]=_WS[k];});'
            'self.WebSocket=S2LWebSocket;'
          '}}catch(e){}'
          # Nested workers (a worker spawning its own sub-worker): rewrite the
          # script URL at minimum. Not re-injecting the full prelude here to
          # avoid unbounded blob-wrapping recursion — diminishing returns.
          'try{var _W2=self.Worker;if(_W2){'
            'self.Worker=function(u,o){return new _W2(typeof u==="string"?rw(u):u,o);};'
            'self.Worker.prototype=_W2.prototype;'
          '}}catch(e){}'
          # importScripts() called BY CODE RUNNING INSIDE the worker (as
          # opposed to the entry URL handled above) was never patched at all.
          # A schemeless argument (root-relative paths from webpack-split
          # worker chunks are the common case) went straight to the native
          # importScripts and got resolved against the worker's own location —
          # which is the blob: URL built above, not a real hierarchical URL —
          # so Chromium throws "The URL '...' is invalid." Resolve schemeless
          # arguments against PX (the real proxy origin) instead; anything
          # that already has a scheme goes through rw() same as everywhere else.
          'try{var _is=self.importScripts;if(typeof _is==="function"){'
            'self.importScripts=function(){'
              'var args=Array.prototype.slice.call(arguments).map(function(u){'
                'if(typeof u!=="string")return u;'
                'if(/^[a-zA-Z][a-zA-Z0-9+.\\-]*:/.test(u))return rw(u);'
                'try{return new URL(u,PX+"/").href;}catch(e){return PX+u;}'
              '});'
              'return _is.apply(self,args);'
            '};'
          '}}catch(e){}'
          'return rw;'
        '}'
        'var rw=__s2l_core(M,HP,EXT,location.hostname,MP);'
        # ── location.protocol / origin override ────────────────────────────────
        # When SITE is served via https:// in production, sites often read
        # window.location.protocol to decide how to build absolute asset URLs:
        #   `new URL(path, location.origin)` or `location.protocol + "//" + host`
        # On a local HTTP proxy these produce "https://localhost:8080/..." which
        # hits the HTTP-only server with a TLS negotiation it can't speak, causing
        # ERR_SSL_PROTOCOL_ERROR. Patching these properties to always return the
        # http:// variant nips this at the source before any URL is even built.
        # Note: location.protocol is non-configurable in some engines; we guard
        # with try/catch so a failed defineProperty is silent rather than fatal.
        'try{'
          'var _locP2=Object.getPrototypeOf(location);'
          'Object.defineProperty(_locP2,"protocol",{'
            'configurable:true,get:function(){return "http:";}'
          '});'
          'Object.defineProperty(_locP2,"origin",{'
            'configurable:true,get:function(){'
              'return "http://"+location.host;'
            '}'
          '});'
        '}catch(e){}'
        # ── location.href / .assign() / .replace() / window.open() patch ───────
        # A frame can navigate itself WITHOUT ever touching fetch/XHR/src — e.g.
        # Poki-style "are we embedded on the right domain?" SDK checks that fall
        # back to `top.location.href = "https://poki.com/"`, or any game script
        # doing `location.replace(realCdnUrl)`. None of the patches above catch
        # a raw navigation, so the browser tries to connect to the REAL host
        # directly — and since only the spoofed MAIN_HOST is in the hosts file
        # (and CDN sub-iframes don't even run on that hostname, they run on
        # localhost:<port>), that connection is refused. Location.prototype's
        # href/assign/replace are configurable accessors/methods in every
        # major engine, so they can be safely overridden here.
        'try{'
          'var _locP=Object.getPrototypeOf(location);'
          'var _lhD=Object.getOwnPropertyDescriptor(_locP,"href");'
          'if(_lhD&&_lhD.set){'
            'Object.defineProperty(_locP,"href",{'
              'get:_lhD.get,'
              'set:function(v){return _lhD.set.call(this,typeof v==="string"?rw(v):v);}'
            '});'
          '}'
          'var _lAssign=_locP.assign;'
          'if(typeof _lAssign==="function"){'
            '_locP.assign=function(v){return _lAssign.call(this,typeof v==="string"?rw(v):v);};'
          '}'
          'var _lReplace=_locP.replace;'
          'if(typeof _lReplace==="function"){'
            '_locP.replace=function(v){return _lReplace.call(this,typeof v==="string"?rw(v):v);};'
          '}'
        '}catch(e){}'
        'try{'
          'var _wOpen=window.open;'
          'if(typeof _wOpen==="function"){'
            'window.open=function(u,n,s){return _wOpen.call(this,typeof u==="string"?rw(u):u,n,s);};'
          '}'
        '}catch(e){}'
        # ── Window.prototype.postMessage patch ─────────────────────────────────
        # PokiSDK uses:  window.parent.postMessage(msg, 'https://poki.com')
        #                iframe.contentWindow.postMessage(msg, 'https://gdn.poki.com')
        # The browser drops these because the actual receiving origin is localhost,
        # not poki.com / gdn.poki.com. The SDK never gets a response and falls back
        # to navigating poki.com directly → "connection refused".
        # Fix: change any targetOrigin string matching poki.com/poki.io domains to '*'
        # so the message actually arrives regardless of actual origin.
        # This patch applies in EVERY realm (main frame + game iframes), so both
        # directions (wrapper→game and game→wrapper) are covered.
        'try{'
          'var _pmD=Object.getOwnPropertyDescriptor(Window.prototype,"postMessage");'
          'if(!_pmD){'
            # Fallback: wrap window.postMessage directly
            'var _pm0=window.postMessage;'
            'window.postMessage=function(m,t,tr){'
              'if(typeof t==="string"&&(t.indexOf("poki.com")>-1||t.indexOf("poki.io")>-1))t="*";'
              'return _pm0.apply(this,[m,t,tr]);'
            '};'
          '} else if(typeof _pmD.value==="function"){'
            'var _pm1=_pmD.value;'
            'Object.defineProperty(Window.prototype,"postMessage",{'
              'configurable:true,writable:true,enumerable:true,'
              'value:function(m,t,tr){'
                'if(typeof t==="string"&&(t.indexOf("poki.com")>-1||t.indexOf("poki.io")>-1))t="*";'
                'return _pm1.apply(this,[m,t,tr]);'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── MessageEvent.prototype.origin spoof ────────────────────────────────
        # After fixing postMessage targetOrigin, messages arrive but the SDK checks:
        #   if (event.origin !== 'https://poki.com') return; // ignore
        # Since parent is localhost:8080, not poki.com, SDK ignores the wrapper's msg.
        # Fix: patch the origin getter so each localhost:PORT is reported as the
        # REAL host it corresponds to — main port → poki.com, CDN port → that
        # CDN host's real domain (e.g. localhost:8091 → 5dd...gdn.poki.com).
        #
        # CRITICAL: this must NOT collapse every localhost origin to poki.com.
        # The game wrapper (PageGame) does a postMessage handshake with the game
        # iframe and verifies event.origin matches the game's OWN expected CDN
        # origin before trusting it. If every message — including ones from the
        # game's CDN-port iframe — reports back as "https://poki.com" instead of
        # the game's real GDN host, that origin check fails, and the wrapper's
        # fallback/recovery path was observed re-pointing the game iframe itself
        # at event.origin (i.e. literally "https://poki.com/") — which is exactly
        # the "connection refused" page this fix resolves.
        'try{'
          'var _meO=Object.getOwnPropertyDescriptor(MessageEvent.prototype,"origin");'
          'if(_meO&&_meO.get&&_meO.configurable){'
            'var _meOg=_meO.get;'
            'Object.defineProperty(MessageEvent.prototype,"origin",{'
              'configurable:true,enumerable:true,'
              'get:function(){'
                'var o=_meOg.call(this);'
                'if(!o)return o;'
                'try{'
                  'var pu=new URL(o);'
                  'if(pu.hostname==="localhost"||pu.hostname==="127.0.0.1"){'
                    'var pp=pu.port?parseInt(pu.port,10):(pu.protocol==="https:"?443:80);'
                    'if(pp===MP)return "https://"+M;'
                    'for(var h in HP){if(HP[h]===pp)return "https://"+h;}'
                  '}'
                '}catch(_e){}'
                'return o;'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── innerHTML / outerHTML / insertAdjacentHTML / document.write patch ──
        # These all hand a raw HTML STRING to the browser's native parser, which
        # sets attributes (src=, href=, action=...) while building elements —
        # that parsing step never goes through the .src/.setAttribute property
        # patches above, since those only fire for JS-level property access, not
        # for attributes that arrive already-baked-in via markup. Until now the
        # only thing that caught this was the MutationObserver below, which is
        # reactive (fires after the nodes already exist) and can lose the race
        # against the browser eagerly starting an <iframe>/<img> fetch the
        # instant it's parsed — exactly how an unrewritten absolute URL like
        # https://poki.com/... can slip straight to the real internet. Rewrite
        # the markup STRING itself before the parser ever sees it.
        'function _rwHtmlStr(html){'
          'try{'
            'return html.replace('
              '/((?:src|href|poster|data-src|data-href|action)\\s*=\\s*)(["\\x27])https?:\\/\\/([a-zA-Z0-9\\-._]+)((?:(?!\\2)[^<>])*)\\2/gi,'
              'function(m,pre,q,host,tail){return pre+q+rw("https://"+host+tail)+q;}'
            ');'
          '}catch(e){return html;}'
        '}'
        'try{'
          'var _ihD=Object.getOwnPropertyDescriptor(Element.prototype,"innerHTML");'
          'if(_ihD&&_ihD.set){'
            'Object.defineProperty(Element.prototype,"innerHTML",{'
              'get:_ihD.get,'
              'set:function(v){return _ihD.set.call(this,typeof v==="string"?_rwHtmlStr(v):v);}'
            '});'
          '}'
          'var _ohD=Object.getOwnPropertyDescriptor(Element.prototype,"outerHTML");'
          'if(_ohD&&_ohD.set){'
            'Object.defineProperty(Element.prototype,"outerHTML",{'
              'get:_ohD.get,'
              'set:function(v){return _ohD.set.call(this,typeof v==="string"?_rwHtmlStr(v):v);}'
            '});'
          '}'
          'var _iah=Element.prototype.insertAdjacentHTML;'
          'if(typeof _iah==="function"){'
            'Element.prototype.insertAdjacentHTML=function(pos,html){'
              'return _iah.call(this,pos,typeof html==="string"?_rwHtmlStr(html):html);'
            '};'
          '}'
          'var _dw=document.write, _dwl=document.writeln;'
          'if(typeof _dw==="function")document.write=function(){'
            'var a=Array.prototype.slice.call(arguments).map(function(s){'
              'return typeof s==="string"?_rwHtmlStr(s):s;});'
            'return _dw.apply(document,a);'
          '};'
          'if(typeof _dwl==="function")document.writeln=function(){'
            'var a=Array.prototype.slice.call(arguments).map(function(s){'
              'return typeof s==="string"?_rwHtmlStr(s):s;});'
            'return _dwl.apply(document,a);'
          '};'
        '}catch(e){}'
        # ── Worker / SharedWorker constructor wrapping ──────────────────────────
        # A worker has its OWN global scope — it does NOT inherit window.fetch
        # etc. Without this, a worker's network calls bypass the proxy entirely
        # and hit the real external host directly. Since the user's hosts file
        # points that host at us on a port nothing real is listening on, those
        # calls hang — and if the page Atomics.wait()s on the worker (common in
        # WASM-thread apps unlocked by COOP/COEP), the whole tab freezes.
        # Fix: inject __s2l_core as a prelude before the worker's real script
        # runs, via a same-origin blob: URL wrapping importScripts().
        '[["Worker","__s2l__worker_orig"],["SharedWorker","__s2l__sharedworker_orig"]].forEach(function(pair){'
          'var Ctor=window[pair[0]];if(!Ctor)return;'
          'function Wrapped(scriptURL,opts){'
            'var abs;try{abs=new URL(scriptURL,location.href).href;}catch(e){abs=scriptURL;}'
            'var ru=typeof abs==="string"?rw(abs):abs;'
            # abs is always fully absolute (new URL(...).href guarantees this),
            # so rw() either returns it prefixed with PX (main-host / ext-fallback
            # branches) or returns it completely unchanged when it's some other
            # scheme rw() doesn't touch — most commonly blob: (workers created
            # from `new Worker(URL.createObjectURL(blob))`, extremely common for
            # wasm/audio-worklet loaders) or data:.
            # BUG THIS FIXES: the old check was `ru.indexOf("http")!==0`, which
            # is true both for a genuine relative path AND for any string where
            # "http" doesn't appear at position 0 — including "blob:http://..."
            # (http appears at index 5, not 0). That made every blob: worker URL
            # get PX blindly concatenated onto the front, producing garbage like
            # "http://localhost:8080blob:http://localhost:8080/<uuid>" — an
            # invalid URL that made every WASM/audio worker in every proxied
            # page throw "Failed to execute importScripts... is invalid" and
            # never start. Fix: only prefix when the string has NO URL scheme
            # at all (a true bare/relative path); leave every already-schemed
            # URL — blob:, data:, http:, https: — completely untouched.
            'if(typeof ru==="string"&&!/^[a-zA-Z][a-zA-Z0-9+.\\-]*:/.test(ru))ru=PX+ru;'
            'try{'
              'var prelude="("+__s2l_core.toString()+")("+JSON.stringify(M)+","+JSON.stringify(HP)+","+'
                'JSON.stringify(EXT)+","+JSON.stringify(location.hostname)+","+JSON.stringify(MP)+");\\n"'
                '+"importScripts("+JSON.stringify(ru)+");";'
              'var blob=new Blob([prelude],{type:"application/javascript"});'
              'var blobURL=URL.createObjectURL(blob);'
              'return new Ctor(blobURL,opts);'
            '}catch(e){return new Ctor(ru,opts);}'
          '}'
          'Wrapped.prototype=Ctor.prototype;'
          'window[pair[0]]=Wrapped;'
        '});'
        # ── src setter patch (Image/Script/IFrame/Video/Source) ────────────────
        # Catches dynamically-created elements whose .src is set directly via JS,
        # e.g. var f=document.createElement("iframe"); f.src="https://poki.com/...".
        # These never pass through the MutationObserver (not yet in the DOM when
        # set) so without this patch they'd connect straight to the real host.
        'try{'
          '[["HTMLImageElement","img"],["HTMLScriptElement","script"],'
          '["HTMLIFrameElement","iframe"],["HTMLMediaElement","media"],'
          '["HTMLSourceElement","source"]].forEach(function(pair){'
            'var ctor=window[pair[0]];if(!ctor||!ctor.prototype)return;'
            'var d=Object.getOwnPropertyDescriptor(ctor.prototype,"src");'
            'if(!d||!d.set)return;'
            'Object.defineProperty(ctor.prototype,"src",{'
              'get:d.get,'
              'set:function(v){return d.set.call(this,typeof v==="string"&&v.indexOf("http")===0?rw(v):v);}'
            '});'
          '});'
        '}catch(e){}'
        # ── HTMLIFrameElement.setAttribute("src", ...) patch ────────────────────
        # Some frameworks set iframe src via setAttribute instead of the .src
        # property — patch that path too for this element type specifically.
        'try{'
          'var _ifSA=HTMLIFrameElement.prototype.setAttribute;'
          'HTMLIFrameElement.prototype.setAttribute=function(name,value){'
            'if(name&&name.toLowerCase()==="src"&&typeof value==="string"&&value.indexOf("http")===0)value=rw(value);'
            'return _ifSA.call(this,name,value);'
          '};'
        '}catch(e){}'
        # ── CSS backgroundImage + cssText setter patch ────────────────────────
        # Catches: element.style.backgroundImage = "url(https://cdn.game.com/bg.png)"
        'try{'
          'var _cssRw=function(v){return typeof v==="string"?v.replace(/url\\((["\']?)(https?:\\/\\/[^)"\'\\s]+)\\1\\)/gi,function(m,q,u){var r=rw(u);return r!==u?"url("+q+r+q+")":m;}):v;};'
          'var _bgD=Object.getOwnPropertyDescriptor(CSSStyleDeclaration.prototype,"backgroundImage");'
          'if(_bgD&&_bgD.set){'
            'Object.defineProperty(CSSStyleDeclaration.prototype,"backgroundImage",{'
              'get:_bgD.get,set:function(v){return _bgD.set.call(this,_cssRw(v));}'
            '});'
          '}'
          'var _ctD=Object.getOwnPropertyDescriptor(CSSStyleDeclaration.prototype,"cssText");'
          'if(_ctD&&_ctD.set){'
            'Object.defineProperty(CSSStyleDeclaration.prototype,"cssText",{'
              'get:_ctD.get,set:function(v){return _ctD.set.call(this,_cssRw(v));}'
            '});'
          '}'
        '}catch(e){}'
        # ── DOM attribute scanner + MutationObserver ──────────────────────────
        'var _A=["src","href","poster","data-src","action"];'
        'function _rn(n){if(!n||n.nodeType!==1)return;'
        '_A.forEach(function(a){var v=n.getAttribute(a);if(v&&v.indexOf("http")===0){var r=rw(v);if(r!==v)n.setAttribute(a,r);}});'
        'var ss=n.getAttribute("srcset");'
        'if(ss){var rs=ss.split(/,\\s+/).map(function(e){var b=e.trim().split(/ +/);if(b[0]&&b[0].indexOf("http")===0){var r=rw(b[0]);if(r!==b[0])b[0]=r;}return b.join(" ");}).join(", ");if(rs!==ss)n.setAttribute("srcset",rs);}}'
        'function _ra(r){try{var e=r.querySelectorAll("[src],[href],[srcset],[poster],[data-src],[action]");for(var i=0;i<e.length;i++)_rn(e[i]);}catch(x){}}'
        # attributes:true + attributeFilter catches ANY element type whose
        # src/href/poster changes — via setAttribute OR via the reflected
        # property setter (e.g. a.href=..., link.href=..., video.poster=...)
        # which we did NOT individually patch for every element type. rw() is
        # idempotent on already-proxied/relative URLs, so the rewrite we make
        # here re-triggering this same observer is harmless (one no-op pass).
        'new MutationObserver(function(ms){ms.forEach(function(m){'
          'if(m.type==="attributes"){_rn(m.target);}'
          'else{m.addedNodes.forEach(function(n){_rn(n);_ra(n);});}'
        '});}).observe(document.documentElement,{'
          'childList:true,subtree:true,'
          'attributes:true,attributeFilter:["src","href","poster","data-src","srcset","action"]'
        '});'
        'if(document.readyState!=="loading")_ra(document);else document.addEventListener("DOMContentLoaded",function(){_ra(document);});'
        # ── HTMLFormElement.prototype.submit() patch ────────────────────────────
        # form.submit() called from JS bypasses the click-driven navigation path
        # (and the action attribute may have been built/changed at runtime after
        # our scanner last ran) — rewrite the action immediately before submit.
        'try{'
          'var _formSubmit=HTMLFormElement.prototype.submit;'
          'HTMLFormElement.prototype.submit=function(){'
            'var a=this.getAttribute("action");'
            'if(a&&a.indexOf("http")===0){var r=rw(a);if(r!==a)this.setAttribute("action",r);}'
            'return _formSubmit.call(this);'
          '};'
        '}catch(e){}'
        # ── iframe.srcdoc patch ─────────────────────────────────────────────────
        # srcdoc embeds a full HTML document as a literal STRING — typically
        # built at runtime from API-response data (e.g. a sandboxed game-embed
        # wrapper). It never passes through our server-side HTML rewriting
        # (it's not a separate HTTP response) and isn't caught by any
        # attribute/property patch above (srcdoc isn't itself a URL). Rewrite
        # any src/href/poster/data-src URLs found inside the HTML string
        # before assignment, so the iframe the browser parses out of it
        # already points at the proxy instead of the real host.
        'try{'
          'var _sdD=Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype,"srcdoc");'
          'if(_sdD&&_sdD.set){'
            'Object.defineProperty(HTMLIFrameElement.prototype,"srcdoc",{'
              'get:_sdD.get,'
              'set:function(html){'
                'if(typeof html==="string"){'
                  'html=html.replace(/((?:src|href|poster|data-src)\\s*=\\s*)(["\'])(https?:\\/\\/[^"\']+)\\2/gi,'
                    'function(m,attr,q,u){var r=rw(u);return r!==u?attr+q+r+q:m;});'
                '}'
                'return _sdD.set.call(this,html);'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── Service worker + cache clear ──────────────────────────────────────
        'if(navigator.serviceWorker)navigator.serviceWorker.getRegistrations().then(function(r){r.forEach(function(s){s.unregister();});});'
        'if(typeof caches!=="undefined")caches.keys().then(function(k){k.forEach(function(n){caches.delete(n);});});'
        # ── Live HP refresh — picks up CDN hosts registered after page load ───
        # Polls PX+/.__s2l_hp every 2s so game CDN hosts registered AFTER the
        # initial page serve are picked up without a reload. PX is absolute
        # (http://127.0.0.1:8080) so polling works from any sub-iframe port.
        '(function(){'
          'var _ht=0;'
          'function _hp_poll(){'
            '_ht++;'
            'fetch(PX+"/.__s2l_hp",{cache:"no-store"})'
              '.then(function(r){return r.json();})'
              '.then(function(d){for(var h in d)if(!HP[h]){HP[h]=d[h];}})'
              '.catch(function(){});'
            'setTimeout(_hp_poll,_ht<30?2000:10000);'
          '}'
          'setTimeout(_hp_poll,1500);'
        '})();'
        '})();</script>'
    )
    return js.encode("utf-8")


_s2l_js_lock = threading.Lock()


def _inject_sw_clear(html_bytes: bytes) -> bytes:
    if not html_bytes:
        return html_bytes
    with _s2l_js_lock:
        script = _build_s2l_injector()

    # ── Strip any stale __s2l__ injector from disk-cached files ──────────────
    # Without this, a cached page that had the old injector baked in gets a NEW
    # injector prepended; both run; the OLD (possibly with buggy rw()) executes
    # LAST and overwrites window.__s2l_rw / fetch / setAttribute / src-setters.
    html_bytes = re.sub(
        rb'<script\s+id=["\']__s2l__["\'][^>]*>.*?</script\s*>',
        b'', html_bytes,
        flags=re.DOTALL | re.IGNORECASE,
    )

    lower = html_bytes.lower()
    m = re.search(rb"<head[^>]*>", html_bytes, re.IGNORECASE)
    if m:
        idx = m.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

    m = re.search(rb"<html[^>]*>", html_bytes, re.IGNORECASE)
    if m:
        idx = m.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

    for marker in (b"</head>", b"</body>"):
        idx = lower.rfind(marker)
        if idx != -1:
            return html_bytes[:idx] + script + html_bytes[idx:]
    return script + html_bytes

# ──────────────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────────────

def _safe_seg(name: str) -> str:
    if len(name) <= MAX_FNAME:
        return name
    base, ext = os.path.splitext(name)
    h = hashlib.sha1(name.encode()).hexdigest()[:12]
    return f"{base[:32]}_{h}{ext}"

def local_path(u: str) -> str:
    p = urlparse(normalize_url(u))
    path = p.path or "/"
    if path.endswith("/") or not os.path.splitext(path)[1]:
        path = path.rstrip("/") + "/index.html"
    parts = [_safe_seg(x) for x in path.split("/") if x]
    # Include a query-string hash so URLs with different params (e.g. YouTube search,
    # paginated APIs) are cached as separate files — prevents stale search results.
    if p.query:
        qs_hash = hashlib.sha1(p.query.encode()).hexdigest()[:10]
        last = parts[-1] if parts else "index.html"
        base, ext = os.path.splitext(last)
        ext = ext or ".html"
        parts[-1] = f"{base}_q{qs_hash}{ext}"
    return os.path.join(SRC_FOLDER, p.netloc.replace("www.", ""), *parts)

# ──────────────────────────────────────────────────────────────────────────────
# Site init
# ──────────────────────────────────────────────────────────────────────────────

def build_base_url(raw: str) -> str | None:
    s = _make_session()
    for scheme in ("https://", "http://"):
        try:
            r = s.get(scheme + raw, timeout=(TIMEOUT_CONN, TIMEOUT_READ))
            if r.status_code < 500:
                platform = detect_platform(dict(r.headers))
                log(f"Resolved {raw} → {r.url}  [{platform}]  IP: {resolve_ip(urlparse(r.url).netloc)}")
                return r.url
        except Exception as e:
            log(f"Probe {scheme+raw}: {_short_exc(e)}", "WARN")
    return None

SITE_URL  = build_base_url(SITE) or f"http://{SITE}"
MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.replace("www.", "").replace(".", "_")
SRC_FOLDER  = os.path.join("site_src",  SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)
os.makedirs(SRC_FOLDER,  exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Hidden path scanner  (SCAN_PATHS)
# ──────────────────────────────────────────────────────────────────────────────

_DEFAULT_HIDDEN_PATHS = [
    "admin", "administrator", "login", "panel", "dashboard", "console",
    "wp-admin", "wp-login.php", "phpmyadmin",
    ".git", ".git/HEAD", ".git/config",
    ".env", ".env.local", ".env.production",
    "config", "config.php", "config.json",
    "backup", "backups", "db", "database",
    "private", "secret", "secrets",
    "api", "api/v1", "api/v2", "graphql",
    "swagger", "swagger-ui", "swagger.json", "openapi.json",
    "robots.txt", "sitemap.xml",
    "server-status", "server-info",
    "phpinfo.php", "info.php", "test.php",
    "debug", "trace", "health", "status", "metrics",
]

def _run_path_scanner() -> None:
    paths  = _DEFAULT_HIDDEN_PATHS + list(EXTRA_PATHS)
    sess   = _make_session()
    found  = []
    origin = f"{urlparse(SITE_URL).scheme}://{MAIN_HOST}"
    log(f"Path scanner — probing {len(paths)} paths on {MAIN_HOST}", "SCAN")
    for p in paths:
        url = f"{origin}/{p.lstrip('/')}"
        try:
            r = sess.head(url, timeout=(TIMEOUT_CONN, TIMEOUT_READ), allow_redirects=False, verify=False)
            if r.status_code == 405:
                r = sess.get(url, timeout=(TIMEOUT_CONN, min(TIMEOUT_READ,6)), allow_redirects=False, verify=False)
            if r.status_code not in (404, 410):
                found.append({"path": p, "url": url, "status": r.status_code,
                              "content_type": r.headers.get("Content-Type", "")})
                sc = r.status_code
                c  = Fore.GREEN if sc == 200 else Fore.YELLOW if sc in (301, 302, 403) else Fore.CYAN
                log(f"{c}{sc}{Style.RESET_ALL}  {url}", "SCAN")
        except _CONN_ERRORS:
            pass
        except Exception as e:
            log(f"scan error {url}: {_short_exc(e)}", "WARN")
    out = os.path.join(DATA_FOLDER, "hidden_paths.json")
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "target": MAIN_HOST, "results": found}, f, indent=2)
    except Exception as e:
        log(f"scan save failed: {e}", "ERROR")
    log(f"Scan done — {len(found)}/{len(paths)} found → {out}", "SCAN")

# ──────────────────────────────────────────────────────────────────────────────
# Domain policy
# ──────────────────────────────────────────────────────────────────────────────

def is_allowed_domain(netloc: str) -> bool:
    if not netloc:
        return False
    if netloc == MAIN_HOST:
        return True
    if any(bad in netloc for bad in CDN_BLOCK):
        return False
    return DUMP_ALL and (not DUMP_TARGETS or netloc in DUMP_TARGETS)

def is_external_domain(netloc: str) -> bool:
    host = netloc.split(":")[0]
    return (bool(netloc)
            and netloc != MAIN_HOST
            and host not in _LOCAL_HOSTS
            and not any(bad in netloc for bad in CDN_BLOCK))

# ──────────────────────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────────────────────

class _Stats:
    __slots__ = ("_lock", "crawled", "saved", "proxied",
                 "conn_errors", "http_errors", "hooks_run", "revealed", "cdn_fetched", "captured")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        for s in self.__slots__[1:]:
            setattr(self, s, 0)

    def inc(self, field: str, n: int = 1) -> None:
        with self._lock:
            setattr(self, field, getattr(self, field) + n)

    def snapshot(self) -> dict:
        with self._lock:
            return {s: getattr(self, s) for s in self.__slots__ if s != "_lock"}

stats = _Stats()

# ──────────────────────────────────────────────────────────────────────────────
# CDN host registry  (PROXY_CDN)
#
# When MULTIPORT=True:  each CDN gets a dedicated Flask mini-server on
#   PORT+N and HTML is rewritten to http://localhost:PORT+N/path
# When MULTIPORT=False: CDN assets are served via /__s2l_ext__/host/path
#   on the same PORT (simpler, no extra ports needed)
# ──────────────────────────────────────────────────────────────────────────────

_cdn_host_port: dict[str, int] = {}
_cdn_port_lock  = threading.Lock()
_cdn_next_port  = PORT + 1

def _real_origin_for(incoming: str | None) -> str:
    """Translate the Origin/Referer header the BROWSER actually sent for an
    outgoing CDN/API/WS request into the origin a real, unproxied browser
    would have sent to that upstream.

    The browser's own header reflects whatever local address is currently
    serving the calling document — http://localhost:PORT for the main page
    (or https://MAIN_HOST if the user's hosts file spoofs it), or
    http://localhost:<cdn-port> when the call originates from inside a CDN
    sub-iframe served on its own MULTIPORT port. Blindly forwarding THAT
    local value (or, just as wrong, blindly substituting the call's OWN
    target host — self-referencing) doesn't match what a real browser would
    send, and a surprising number of first-party APIs use Origin/Referer to
    decide what to return: an unrecognized value commonly gets a stripped /
    empty fallback response instead of real data, which is exactly the kind
    of thing that can leave a page's JS waiting forever on data that never
    arrives in the shape it expects.
    """
    if incoming:
        try:
            p = urlparse(incoming)
            host, port = p.hostname or "", p.port
            if host in _LOCAL_HOSTS:
                if port and port != PORT:
                    with _cdn_port_lock:
                        for h, pt in _cdn_host_port.items():
                            if pt == port:
                                return f"https://{h}"
                return f"https://{MAIN_HOST}"
            if host == MAIN_HOST or host == "www." + MAIN_HOST:
                return f"https://{MAIN_HOST}"
            with _cdn_port_lock:
                if host in _cdn_host_port:
                    return f"https://{host}"
        except Exception:
            pass
    # No usable incoming header, or it didn't match anything we recognize —
    # MAIN_HOST is correct for the overwhelming majority of first-party API
    # calls, which are made directly from the main page's own JS.
    return f"https://{MAIN_HOST}"

def _start_cdn_server(cdn_host: str, port: int) -> None:
    cdn_app = Flask(f"cdn_{cdn_host}", static_folder=None)

    @cdn_app.route("/", defaults={"p": ""}, methods=_ALL_METHODS)
    @cdn_app.route("/<path:p>",             methods=_ALL_METHODS)
    def _cdn_serve(p: str):
        method  = flask_request.method.upper()
        _raw_rest = _raw_path_after("/")
        if _raw_rest is not None:
            p = _raw_rest
        cdn_url = f"https://{cdn_host}/{p.lstrip('/')}"
        qs      = flask_request.query_string.decode("utf-8", "ignore")
        if qs:
            cdn_url += f"?{qs}"

        if flask_request.headers.get("Upgrade", "").lower() == "websocket":
            # A CDN host that already has its own dedicated MULTIPORT port gets
            # WS URLs pointed straight at it (rw()'s known-host branch) instead
            # of via /__s2l_ws_ext__/ — this app had no upgrade handling at all,
            # so those connections failed exactly like the main-port ones did.
            fwd_hdrs = filter_fwd(dict(flask_request.headers))
            return _handle_websocket_upgrade("/" + p.lstrip("/"), cdn_url, fwd_hdrs)

        # OPTIONS — fast CORS preflight
        if method == "OPTIONS":
            r = Response(status=204)
            r.headers.update({
                "Access-Control-Allow-Origin":  "*",
                "Access-Control-Allow-Methods": ", ".join(_ALL_METHODS),
                "Access-Control-Allow-Headers": flask_request.headers.get(
                    "Access-Control-Request-Headers", "*"),
                "Access-Control-Max-Age": "86400",
            })
            return r

        # Cache-hit only for GET/HEAD when CACHE_CDN is enabled
        # Use _serve_cached (not send_file) so HTML gets _rewrite_ext_urls + _inject_sw_clear
        lp = local_path(cdn_url)
        if method in _SAFE_METHODS and CACHE_CDN and os.path.isfile(lp):
            result = _serve_cached(lp, cdn_url)
            if result is not None:
                data, ct = result
                resp = Response(data, content_type=ct)
                resp.headers["Cache-Control"] = "public, max-age=86400"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                resp.headers["Access-Control-Expose-Headers"] = "*"
                resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
                return resp

        if OFFLINE:
            return Response("Offline", status=404)

        # Forward request to real CDN (all methods, including POST/PUT for uploads)
        try:
            req_body = flask_request.get_data(cache=False) if method in _BODY_METHODS else b""
            ctx = HookContext(
                method=method, url=cdn_url, path="/" + p.lstrip("/"), query=qs,
                req_headers=filter_fwd(dict(flask_request.headers)),
                req_body=req_body,
            )
            _real_origin = _real_origin_for(
                ctx.req_headers.get("Origin") or ctx.req_headers.get("Referer"))
            for _k in list(ctx.req_headers.keys()):
                _kl = _k.lower()
                if _kl == "origin": ctx.req_headers[_k] = _real_origin
                elif _kl == "referer": ctx.req_headers[_k] = _real_origin + "/"
            ctx.req_headers["Host"] = cdn_host

            if _REQ_HOOKS:
                stats.inc("hooks_run", _run_hooks(_REQ_HOOKS, ctx))

            kw: dict = {
                "headers":         ctx.req_headers,
                "timeout":         (TIMEOUT_CONN, TIMEOUT_READ),
                "allow_redirects": True,
                "verify":          False,
            }
            if method in _BODY_METHODS:
                kw["data"] = ctx.req_body
            # Per-client (not thread-local) session: session/consent cookies set
            # while the browser was loading the main site must be visible here
            # too, or a CDN host that gates content on that state (e.g. a game
            # host validating a session established on the wrapper page) sees
            # what looks like a fresh, unauthenticated request and rejects it.
            r    = _get_client_session().request(method, cdn_url, **kw)
            body = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            ct   = r.headers.get("Content-Type", "application/octet-stream")
            # Only save on real 200 responses with content — 304 has an empty body and
            # saving it would corrupt/truncate a previously-good cached file.
            if r.status_code == 200 and body and method in _SAFE_METHODS and CACHE_CDN:
                save_queue.put((cdn_url, body, ct))
                stats.inc("cdn_fetched")
            out_headers = filter_resp(dict(r.headers))
            out_headers["Access-Control-Allow-Origin"]  = "*"
            out_headers["Access-Control-Expose-Headers"] = "*"
            out_headers["Cross-Origin-Resource-Policy"] = "cross-origin"
            if method in _SAFE_METHODS:
                out_headers["Cache-Control"] = "public, max-age=86400"
            # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
            # through the proxy (critical for game iframes served from CDN hosts).
            _ct_base_c = ct.split(";")[0].strip().lower()
            if r.status_code < 400 and PROXY_CDN:
                if "text/html" in _ct_base_c:
                    body = _rewrite_ext_urls(body, cdn_url)
                    body = rewrite_abs_urls(body)
                    body = _inject_sw_clear(body)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)
                elif "text/css" in _ct_base_c:
                    body = _rewrite_ext_urls(body, cdn_url)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)
                elif "json" in _ct_base_c:
                    body = _rewrite_json_urls(body)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)

            ctx.resp_status, ctx.resp_headers, ctx.resp_body, ctx.resp_ct = (
                r.status_code, out_headers, body, ct)
            if _RESP_HOOKS:
                stats.inc("hooks_run", _run_hooks(_RESP_HOOKS, ctx))
            _apply_gui_hooks(ctx)
            if CAPTURE and CAPTURE_CDN:
                _maybe_capture(ctx)

            log(f"CDN  {method} {ctx.resp_status}  {cdn_host}/{p}  {_fmt_size(len(ctx.resp_body))}  :{port}", "CDN")
            _gui_push_raw(method, f"/{p}", ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                          display_tag=f"[cdn:{port}]")
            return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
        except Exception as exc:
            log(f"CDN error {cdn_host}/{p}: {_short_exc(exc)}", "WARN")
            return Response(str(exc), status=502)

    def _run():
        _wz = logging.getLogger("werkzeug")
        _wz.setLevel(logging.ERROR)
        try:
            from werkzeug.serving import make_server
            server = make_server(HOST, port, cdn_app, threaded=True,
                                  request_handler=_S2LWSGIRequestHandler)
        except Exception as exc:
            log(f"CDN {cdn_host}:{port} bind failed — {exc}", "ERROR")
            with _cdn_port_lock:
                _cdn_host_port.pop(cdn_host, None)   # clear sentinel
            return
        # Port is now bound — register so JS injector can start routing here
        with _cdn_port_lock:
            _cdn_host_port[cdn_host] = port
        log(f"CDN {cdn_host} → :{port}", "CDN")
        try:
            server.serve_forever()
        except Exception as exc:
            log(f"CDN {cdn_host}:{port} crashed — {exc}", "ERROR")
        finally:
            # If this mini-server dies after a successful bind, drop the stale
            # mapping so the next request for this host re-registers and spins
            # up a fresh instance instead of permanently hitting
            # ERR_CONNECTION_REFUSED on a port nothing is listening on anymore.
            with _cdn_port_lock:
                if _cdn_host_port.get(cdn_host) == port:
                    _cdn_host_port.pop(cdn_host, None)

    threading.Thread(target=_run, daemon=True, name=f"cdn-{cdn_host}").start()

def _register_cdn_host(netloc: str) -> None:
    global _cdn_next_port
    if not MULTIPORT:
        with _cdn_port_lock:
            if netloc not in _cdn_host_port:
                _cdn_host_port[netloc] = 0  # registered, routes via /__s2l_ext__/
        return
    with _cdn_port_lock:
        if netloc in _cdn_host_port:
            return
        port = _cdn_next_port
        _cdn_next_port += 1
        _cdn_host_port[netloc] = -1  # sentinel: server starting, not ready yet
    _start_cdn_server(netloc, port)

def _proxy_target(host: str, tail: str) -> str | None:
    """Resolve an external host+path into the correct local-proxy URL.

    Shared by the HTML/CSS rewriter and the JSON/webmanifest rewriter so both
    reach a real destination (MAIN_HOST, a registered CDN's dedicated
    MULTIPORT port, or the catch-all /__s2l_ext__/ route) through identical
    routing logic — one place to get this right instead of two copies that
    can quietly drift apart.

    Absolute https://MAIN_HOST/... baked into server-rendered content (e.g. an
    <iframe src="https://poki.com/..."> deep inside a nested game iframe) must
    be rewritten so the browser re-enters our proxy instead of connecting to
    the real internet host directly (→ "connection refused"). A bare relative
    path only works when the CURRENT document is itself served from the main
    proxy port — but this same rewriter also runs on CDN documents served via
    /__s2l_ext__/ or (MULTIPORT) a dedicated CDN port, where a relative path
    resolves against THAT origin instead, landing nowhere near MAIN_HOST. Use
    an absolute http://localhost:PORT/... URL so it's correct from any origin.
    """
    _mh = {MAIN_HOST, "www." + MAIN_HOST}
    if MAIN_HOST.startswith("www."):
        _mh.add(MAIN_HOST[4:])
    if host in _mh:
        return f"http://localhost:{PORT}{tail}"
    if any(bad in host for bad in CDN_BLOCK):
        return None
    with _cdn_port_lock:
        port = _cdn_host_port.get(host, 0)
    if port > 0 and MULTIPORT:
        return f"http://localhost:{port}{tail}"
    # MUST be absolute (see docstring above) — this exact line was previously
    # returning a bare "/__s2l_ext__/host/path" relative reference. On the
    # main-port page that happens to resolve correctly by coincidence, but the
    # SAME rewritten HTML/JSON is also served verbatim on every CDN sub-port
    # (MULTIPORT) and via /__s2l_ext__/ itself — there the browser resolves
    # the relative path against THAT origin instead, e.g. a game index.html
    # served from :8084 turned "/__s2l_ext__/other-cdn.com/x" into
    # "http://localhost:8084/__s2l_ext__/other-cdn.com/x", which 400'd because
    # port 8084's Flask instance only knows how to proxy its own single CDN
    # host, not arbitrary third parties. That 400 was exactly what broke
    # loading the next game on poki.com ({"error":"Invalid Game"}) — the
    # failed request was the game's own manifest/config fetch.
    return f"http://localhost:{PORT}{_EXT_PREFIX}/{host}{tail}"


def _rewrite_json_urls(data: bytes) -> bytes:
    """Rewrite absolute https?://host/path URLs embedded as JSON string values.

    Web App Manifests (.webmanifest) and a great many first-party API
    responses embed absolute asset URLs in JSON — icon lists, level data
    referencing textures, CMS responses with image fields. Unlike HTML/CSS,
    nothing about JSON content ever went through _rewrite_ext_urls (it's
    gated on text/html and text/css content types), so any such URL silently
    pointed straight at the real internet — invisible to JS-level interception
    too, since plenty of this is consumed by the BROWSER's own native code
    (e.g. PWA manifest icon prefetch for installability/"Add to Home Screen"),
    which page JavaScript never gets a chance to touch at all.
    """
    if not PROXY_CDN or not data:
        return data
    try:
        text = data.decode("utf-8", "ignore")
    except Exception:
        return data

    def _rep(m: re.Match) -> str:
        host, tail = m.group(1), m.group(2) or ""
        r = _proxy_target(host, tail)
        return f'"{r}"' if r else m.group(0)

    text = re.sub(
        r'"https?://([a-zA-Z0-9\-._]+)((?:/[^"\\]*)?)"',
        _rep, text, flags=re.IGNORECASE)
    return text.encode("utf-8")


def _rewrite_ext_urls(html_bytes: bytes, base_url: str) -> bytes:
    """Rewrite ALL external https:// URLs in HTML attributes, srcset, and CSS url()."""
    if not PROXY_CDN or not html_bytes:
        return html_bytes
    try:
        html = html_bytes.decode("utf-8", "ignore")
    except Exception:
        return html_bytes

    def _attr_rep(m):
        host, tail = m.group(3), m.group(4)
        r = _proxy_target(host, tail)
        return f"{m.group(1)}{m.group(2)}{r}{m.group(5)}" if r else m.group(0)
    html = re.sub(
        r'((?:src|href|poster|data-src|data-href|action)\s*=\s*)(["\'])https?://([a-zA-Z0-9\-._]+)(/[^"\'<>]*)(["\'])',
        _attr_rep, html, flags=re.IGNORECASE)

    def _ss_rep(m):
        parts = []
        # Split only on a comma followed by whitespace — the real srcset candidate
        # separator. Image-CDN URLs (Cloudflare /cdn-cgi/image/q=78,w=300,.../...)
        # embed un-spaced commas directly in the path; splitting on every comma
        # truncates those URLs at the first option.
        for e in re.split(r',\s+', m.group(2).strip()):
            e = e.strip()
            if not e: continue
            bits = e.split(None, 1)
            url = bits[0]; desc = (" " + bits[1]) if len(bits) > 1 else ""
            if url.startswith("https://"):
                p = urlparse(url)
                r = _proxy_target(p.netloc, p.path + (f"?{p.query}" if p.query else ""))
                if r: url = r
            parts.append(url + desc)
        return m.group(1) + ", ".join(parts) + m.group(3)
    html = re.sub(r'(srcset\s*=\s*["\'])([^"\']+)(["\'])', _ss_rep, html, flags=re.IGNORECASE)

    def _css_rep(m):
        host, tail = m.group(1), m.group(2)
        r = _proxy_target(host, tail)
        return f"url({r})" if r else m.group(0)
    html = re.sub(r'url\(["\']?https?://([a-zA-Z0-9\-._]+)(/[^"\'\)\s]*)["\']?\)',
                  _css_rep, html, flags=re.IGNORECASE)

    # Strip CSP delivered via <meta http-equiv="Content-Security-Policy" ...> —
    # the HTTP header is already stripped in filter_resp, but pages that set CSP
    # via meta tag (frame-ancestors, script-src, etc.) bypass that and still get
    # enforced by the browser, blocking framing/scripts loaded through the proxy.
    html = re.sub(
        r'<meta[^>]+http-equiv\s*=\s*["\']content-security-policy["\'][^>]*>',
        '', html, flags=re.IGNORECASE)

    # <meta http-equiv="refresh" content="0;url=https://host/path"> — the URL
    # here is never the first thing after the quote (it's "N;url=..."), so the
    # attribute regex above never matches it. A meta-refresh is a real, raw
    # navigation the browser performs on its own, same risk as an unrewritten
    # location.href: it goes straight at the real host and can be refused.
    def _meta_refresh_rep(m):
        tag = m.group(0)
        if not re.search(r'http-equiv\s*=\s*["\']refresh["\']', tag, re.IGNORECASE):
            return tag
        def _content_rep(cm):
            host, tail = cm.group(2), cm.group(3) or ""
            r = _proxy_target(host, tail)
            return f"{cm.group(1)}{r}{cm.group(4)}" if r else cm.group(0)
        return re.sub(
            r'(content\s*=\s*["\'][^"\']*?url\s*=\s*)https?://([a-zA-Z0-9\-._]+)(/[^"\'<>]*)?(["\'])',
            _content_rep, tag, flags=re.IGNORECASE)
    html = re.sub(r'<meta\b[^>]*>', _meta_refresh_rep, html, flags=re.IGNORECASE)

    # ── Rewrite CDN URLs inside <script> blocks ───────────────────────────────
    # __NEXT_DATA__ JSON, Redux state, Next.js hydration data, etc. embed absolute
    # CDN/poki.com URLs that React reads at hydration time to create iframes.
    # Those live inside <script> tags and are invisible to the HTML-attribute
    # regexes above. Without this pass the game iframe src remains the raw CDN
    # URL; React creates the iframe pointing there directly → "connection refused".
    def _script_url_rep(m2):
        host = m2.group(1)
        tail = m2.group(2) or "/"
        if any(bad in host for bad in CDN_BLOCK):
            return m2.group(0)
        r = _proxy_target(host, tail)
        return r if r else m2.group(0)

    def _script_block_rep(mb):
        return mb.group(1) + re.sub(
            r'https?://([a-zA-Z0-9\-._]{4,})((?:/[^\s"\'\\<>]*)?)',
            _script_url_rep,
            mb.group(2),
        ) + mb.group(3)

    html = re.sub(
        r'(<script(?:\s[^>]*)?>)(.*?)(</script\s*>)',
        _script_block_rep,
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )

    return html.encode("utf-8")

# ──────────────────────────────────────────────────────────────────────────────
# Queues & shared state  (MUST be defined before _save_worker thread starts)
# ──────────────────────────────────────────────────────────────────────────────

visited:        set = set()
saved_paths:    set = set()
content_hashes: set = set()

visited_lock = threading.Lock()
save_lock    = threading.Lock()
content_lock = threading.Lock()

url_queue = queue.Queue()

# Domains whose background-session fetches always fail (need auth cookies).
# Domains to skip in background CDN pre-fetch (background session lacks user cookies).
# Populated from CDN_BLOCK + any host that returns a bot page on first fetch.
# Override via CDN_BLOCK = ("captcha-host.com", ...) in CONFIG.
_NO_BG_FETCH_DOMAINS: frozenset = frozenset(CDN_BLOCK) | frozenset({
    "recaptcha.net",
    "hcaptcha.com",
})

_dead_hosts:      set            = set()
_dead_hosts_lock: threading.Lock = threading.Lock()

_crawl_done     = threading.Event()   # set when initial crawl finishes
_cdn_thread_sem = threading.Semaphore(32)  # cap concurrent CDN fetch threads

# Regex for extracting URLs from arbitrary response bodies (DUMP_ALL mode)
URL_REGEX = re.compile(
    rb"((?:https?:)?\/\/[a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+|\/[a-zA-Z0-9_\-\/\.]{2,})"
)

# HTML tags that represent external sub-resources worth fetching
_ASSET_TAGS = frozenset({"script", "img", "link", "source", "video", "audio"})

# save_queue must be defined before _save_worker starts
save_queue: queue.Queue = queue.Queue()

# ──────────────────────────────────────────────────────────────────────────────

def _fetch_external_asset(u: str) -> None:
    """PROXY_CDN: fetch a single CDN asset and cache it. No recursion."""
    netloc = urlparse(u).netloc
    # Skip domains that require user auth / real browser cookies.
    # Background sessions lack these cookies so the response is always a bot page.
    if any(d in netloc for d in _NO_BG_FETCH_DOMAINS):
        return
    if not _cdn_thread_sem.acquire(blocking=False):
        return   # too many concurrent CDN fetches, skip silently
    try:
        with visited_lock:
            if u in visited:
                return
            visited.add(u)
        with _dead_hosts_lock:
            if netloc in _dead_hosts:
                return
        try:
            r    = _get_proxy_session().get(u, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
            body = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            # Never cache bot/CAPTCHA pages — they would poison the asset cache
            if _is_bot_page(body, r.status_code, urlparse(u).path):
                log(f"Bot page from CDN {netloc} — skipping cache", "WARN")
                with _dead_hosts_lock:
                    _dead_hosts.add(netloc)
                return
            if r.status_code < 400:
                if CACHE_CDN:
                    save_queue.put((u, body, r.headers.get("Content-Type", "")))
                _register_cdn_host(netloc)
                stats.inc("cdn_fetched")
                log(f"{netloc}{urlparse(u).path}  [{_fmt_size(len(body))}]", "CDN")
                if CAPTURE and CAPTURE_CDN:
                    parsed_u = urlparse(u)
                    ctx_cdn  = HookContext(
                        method="GET", url=u, path=parsed_u.path,
                        query=parsed_u.query, req_headers={}, req_body=b"",
                        resp_status=r.status_code, resp_headers=filter_resp(dict(r.headers)),
                        resp_body=body,
                        resp_ct=r.headers.get("Content-Type", "application/octet-stream"),
                    )
                    _maybe_capture(ctx_cdn)
        except _CONN_ERRORS as e:
            with _dead_hosts_lock:
                _dead_hosts.add(netloc)
            log(f"cdn unreachable {netloc} — {_short_exc(e)}", "WARN")
        except Exception as e:
            log(f"cdn error {u} — {_short_exc(e)}", "WARN")
    finally:
        _cdn_thread_sem.release()


def _save_worker() -> None:
    batch:      list  = []
    last_flush: float = time.time()
    while True:
        try:
            u, data, ctype = save_queue.get(timeout=SAVE_INTERVAL)
            # Safety gate: never cache bot-detection / CAPTCHA pages.
            # These can arrive from background CDN fetches or upstream retries.
            if _is_bot_page(data):
                log(f"Bot page detected — NOT caching {urlparse(u).netloc}{urlparse(u).path}", "WARN")
                save_queue.task_done()
                continue
            p = local_path(u)
            h = hashlib.sha1(data).hexdigest()
            with content_lock:
                if h in content_hashes:
                    save_queue.task_done(); continue
                content_hashes.add(h)
            with save_lock:
                if p in saved_paths:
                    save_queue.task_done(); continue
                saved_paths.add(p)
            # Record the real upstream Content-Type when it disagrees with what
            # the on-disk path/extension implies (e.g. an extensionless CSS/JS
            # URL that local_path() collapsed to .../index.html) — see resolve_mime().
            if ctype and _ct_base(ctype) != _ct_base(guess_mime(p)):
                _save_ctype_sidecar(p, ctype)
            batch.append((p, data))
            save_queue.task_done()
            stats.inc("saved")
        except queue.Empty:
            pass
        now = time.time()
        if batch and (len(batch) >= SAVE_BATCH or now - last_flush >= SAVE_INTERVAL):
            for p, data in batch:
                try:
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    if os.path.isdir(p):
                        p = os.path.join(p, "index.html")
                    with open(p, "wb") as f:
                        f.write(data)
                except Exception as e:
                    log(f"save error {p}: {e}", "ERROR")
            batch.clear()
            last_flush = now

threading.Thread(target=_save_worker, daemon=True, name="save-worker").start()

# ──────────────────────────────────────────────────────────────────────────────
# Capture system  (CAPTURE)
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class _CaptureRecord:
    method:       str
    url:          str
    query:        str
    req_headers:  dict
    req_body:     bytes
    resp_status:  int
    resp_headers: dict
    resp_body:    bytes
    resp_ct:      str
    ts:           str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))

_capture_queue: queue.Queue = queue.Queue()

def _capture_path(method: str, url: str, qs: str) -> str:
    parsed = urlparse(url)
    segs   = [re.sub(r"[^\w.\-]", "_", s) for s in parsed.path.strip("/").split("/") if s] or ["_root_"]
    qs_tag = f"_qs{hashlib.sha1(qs.encode()).hexdigest()[:8]}" if qs else ""
    return os.path.join(DATA_FOLDER, "captures", *segs,
                        f"{method}{qs_tag}_{int(time.time()*1000)}.json")

def _encode_body(data: bytes) -> tuple[str, str]:
    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(data).decode(), "base64"

def _capture_worker() -> None:
    while True:
        try:
            rec: _CaptureRecord = _capture_queue.get(timeout=1)
        except queue.Empty:
            continue
        path = _capture_path(rec.method, rec.url, rec.query)
        rb_enc, rb_how = _encode_body(rec.req_body) if rec.req_body else ("", "none")
        pb_enc, pb_how = _encode_body(rec.resp_body)
        doc = {
            "s2l": "capture", "ts": rec.ts,
            "request":  {"method": rec.method, "url": rec.url, "query": rec.query,
                         "headers": rec.req_headers,
                         "body":    rb_enc if CAPTURE_BODIES else None,
                         "encoding": rb_how if CAPTURE_BODIES else "omitted"},
            "response": {"status": rec.resp_status, "ct": rec.resp_ct,
                         "headers": rec.resp_headers, "body": pb_enc,
                         "encoding": pb_how, "size": len(rec.resp_body)},
        }
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            stats.inc("captured")
            rel = os.path.relpath(path, DATA_FOLDER)
            log(f"{rec.method} {urlparse(rec.url).path or '/'}  {rec.resp_status}"
                f"  {_fmt_size(len(rec.resp_body))}  → {rel}", "CAPTURE")
        except Exception as e:
            log(f"capture write error: {e}", "ERROR")
        _capture_queue.task_done()

def _maybe_capture(ctx: HookContext) -> None:
    if not CAPTURE:
        return
    if CAPTURE_SKIP_STATIC and is_static_asset(ctx.resp_ct):
        return
    # Ensure body is not None
    body = ctx.resp_body if ctx.resp_body is not None else b""
    _capture_queue.put(_CaptureRecord(
        method=ctx.method, url=ctx.url, query=ctx.query,
        req_headers=ctx.req_headers, req_body=ctx.req_body,
        resp_status=ctx.resp_status, resp_headers=ctx.resp_headers,
        resp_body=body, resp_ct=ctx.resp_ct,
    ))

# ──────────────────────────────────────────────────────────────────────────────
# Crawler
# ──────────────────────────────────────────────────────────────────────────────

def enqueue(u: str) -> None:
    # Only feed the crawler during the initial crawl phase.
    # After crawl finishes, the proxy serves fresh from upstream — no need to re-crawl.
    if not _crawl_done.is_set():
        url_queue.put(normalize_url(u))

def _crawl(u: str) -> None:
    with visited_lock:
        if u in visited:
            return
        visited.add(u)

    if url_depth(u) > CRAWL_DEPTH:
        return
    if any(x in u for x in CF_BLOCK_PATHS):
        return

    netloc = urlparse(u).netloc
    if not is_allowed_domain(netloc):
        return
    with _dead_hosts_lock:
        if netloc in _dead_hosts:
            return

    try:
        r = _get_proxy_session().get(u, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
    except _CONN_ERRORS as e:
        log(f"Skip {netloc} — {_short_exc(e)}", "WARN")
        with _dead_hosts_lock:
            _dead_hosts.add(netloc)
        stats.inc("conn_errors")
        return
    except Exception as e:
        log(f"Crawl error {u} — {_short_exc(e)}", "WARN")
        stats.inc("conn_errors")
        return

    stats.inc("crawled")
    path_short = urlparse(u).path or "/"

    # Always decompress before processing
    data = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
    ct   = r.headers.get("Content-Type", "")

    sc_c = (Fore.GREEN if r.status_code < 300
            else Fore.YELLOW if r.status_code < 400
            else Fore.RED)
    log(f"GET {sc_c}{r.status_code}{Style.RESET_ALL}  {path_short}  {_fmt_size(len(data))}", "CRAWL")

    if r.status_code >= 400:
        stats.inc("http_errors")
        if SAVE_ERRORS:
            save_queue.put((u, data, ct))
        return

    if SHOW_HIDDEN and is_html(data, ct):
        data = _reveal_hidden(data)
        stats.inc("revealed")

    # Never cache RSC payloads
    if _is_wire_payload(data):
        log(f"RSC payload skipped (not cached): {path_short}", "WARN")
        return

    # Save original (never rewritten) so cached files stay clean
    save_queue.put((u, data, ct))

    if is_html(data, ct):
        soup = BeautifulSoup(data.decode("utf-8", "ignore"), "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
            v = tag.get("href") or tag.get("src")
            if not v:
                continue
            abs_url = normalize_url(urljoin(u, v))
            parsed  = urlparse(abs_url)
            if parsed.hostname in _LOCAL_HOSTS:
                continue
            if is_allowed_domain(parsed.netloc):
                enqueue(abs_url)
            elif PROXY_CDN and is_external_domain(parsed.netloc) and tag.name in _ASSET_TAGS:
                threading.Thread(target=_fetch_external_asset, args=(abs_url,), daemon=True).start()

    if DUMP_ALL:
        for m in URL_REGEX.findall(data[:SCAN_LIMIT]):
            try:
                found = m.decode("utf-8", "ignore")
                p = urlparse(found)
                if found.startswith(("data:", "javascript:")):
                    continue
                if p.hostname in _LOCAL_HOSTS:
                    continue
                enqueue(urljoin(u, found))
            except Exception:
                pass


def _crawl_worker() -> None:
    while True:
        try:
            u = url_queue.get(timeout=3)
        except queue.Empty:
            return
        _crawl(u)
        url_queue.task_done()


def crawl_parallel() -> None:
    workers = [threading.Thread(target=_crawl_worker, daemon=True, name=f"crawl-{i}")
               for i in range(WORKERS)]
    for w in workers:
        w.start()
    url_queue.join()
    save_queue.join()
    _crawl_done.set()   # signals enqueue() to stop feeding the queue

# ──────────────────────────────────────────────────────────────────────────────
# Streaming helper  (large binary responses — video, WASM, big downloads)
# ──────────────────────────────────────────────────────────────────────────────

# Content types that are streamed (stream=True in the request, never buffered).
# HTML / JSON / CSS / JS are always buffered so hooks and URL rewriting can work.
# Images are NOT here — they're usually small (<1MB) and buffering them is safe.
# Only true large binary formats that would OOM the process belong here.
_STREAM_CTS = frozenset({
    "video/mp4", "video/webm", "video/ogg", "video/mpeg",
    "audio/mpeg", "audio/ogg", "audio/webm",
    "application/octet-stream",
    "application/zip", "application/x-tar", "application/gzip",
    "application/wasm",
})

# Minimum Content-Length (bytes) that triggers streaming even for non-binary CTs
_STREAM_MIN_BYTES = 5 * 1024 * 1024  # 5 MB


def _should_stream(ct: str, cl: int = 0) -> bool:
    """Return True if this response should be streamed without full buffering."""
    if _ct_base(ct) in _STREAM_CTS:
        return True
    if cl and cl > _STREAM_MIN_BYTES:
        return True
    return False


def _stream_resp(upstream_r, method: str, target: str) -> Response:
    """Stream a large upstream response (fetched with stream=True) to the browser.

    Body is not buffered in RAM.  After the generator finishes the plain
    (decompressed) content is queued for disk caching.
    """
    ct    = upstream_r.headers.get("Content-Type", "application/octet-stream")
    sc    = upstream_r.status_code
    enc   = upstream_r.headers.get("Content-Encoding", "")
    out_h = filter_resp(dict(upstream_r.headers))
    out_h["Access-Control-Allow-Origin"]   = "*"
    out_h["Access-Control-Expose-Headers"] = "*"
    out_h["Cache-Control"] = "public, max-age=86400"
    # We decompress before yielding, so strip the header the browser would misuse
    out_h.pop("content-encoding", None)
    out_h.pop("Content-Encoding", None)

    def _gen():
        raw_chunks: list[bytes] = []
        total = 0
        try:
            for chunk in upstream_r.iter_content(chunk_size=65536):
                if chunk:
                    raw_chunks.append(chunk)
                    total += len(chunk)
                    yield chunk
        except Exception as e:
            log(f"Stream error {urlparse(target).path}: {_short_exc(e)}", "WARN")
        finally:
            if method == "GET" and sc < 400 and raw_chunks:
                full  = b"".join(raw_chunks)
                plain = decompress_body(full, enc)
                if not _is_wire_payload(plain) and not _is_bot_page(plain, sc):
                    save_queue.put((target, plain, ct))
                    stats.inc("saved")
                log(f"STREAM {method} {sc}  {urlparse(target).path}"
                    f"  [{_fmt_size(total)}]", "→")

    return Response(stream_with_context(_gen()), status=sc,
                    headers=out_h, content_type=ct)

# ──────────────────────────────────────────────────────────────────────────────
# WebSocket proxy  (Discord, Slack, real-time messaging apps)
# ──────────────────────────────────────────────────────────────────────────────

def _ws_handshake_accept(key: str) -> str:
    """Compute the Sec-WebSocket-Accept value for the handshake response."""
    magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    sha1  = _hashlib_ws.sha1((key + magic).encode()).digest()
    return _base64.b64encode(sha1).decode()


def _ws_read_frame(sock) -> tuple[int, bytes] | None:
    """Read a single WebSocket frame.  Returns (opcode, payload) or None on EOF."""
    try:
        h = b""
        while len(h) < 2:
            c = sock.recv(2 - len(h))
            if not c:
                return None
            h += c
        b0, b1 = h
        opcode   = b0 & 0x0F
        masked   = bool(b1 & 0x80)
        pay_len  = b1 & 0x7F
        if pay_len == 126:
            l2 = b""
            while len(l2) < 2:
                c = sock.recv(2 - len(l2)); l2 += c
            pay_len = _struct.unpack("!H", l2)[0]
        elif pay_len == 127:
            l8 = b""
            while len(l8) < 8:
                c = sock.recv(8 - len(l8)); l8 += c
            pay_len = _struct.unpack("!Q", l8)[0]
        mask_key = b""
        if masked:
            while len(mask_key) < 4:
                c = sock.recv(4 - len(mask_key)); mask_key += c
        payload = b""
        while len(payload) < pay_len:
            chunk = sock.recv(min(65536, pay_len - len(payload)))
            if not chunk:
                return None
            payload += chunk
        if masked:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
        return opcode, payload
    except Exception:
        return None


def _ws_make_frame(opcode: int, payload: bytes, mask: bool = False) -> bytes:
    """Build a WebSocket frame (server → client: no mask; client → server: masked)."""
    l = len(payload)
    h = bytearray()
    h.append(0x80 | opcode)   # FIN=1, opcode
    if l < 126:
        h.append((0x80 if mask else 0) | l)
    elif l < 65536:
        h.append((0x80 if mask else 0) | 126)
        h.extend(_struct.pack("!H", l))
    else:
        h.append((0x80 if mask else 0) | 127)
        h.extend(_struct.pack("!Q", l))
    if mask:
        mk = os.urandom(4)
        h.extend(mk)
        payload = bytes(b ^ mk[i % 4] for i, b in enumerate(payload))
    return bytes(h) + payload


def _ws_connect_upstream(ws_url: str, extra_headers: dict):
    """
    Establish the TCP/TLS connection AND complete the WS handshake with the
    real upstream server. Returns the connected, upgraded socket on success.
    Raises on any failure — caller must NOT have told the browser anything yet.
    """
    parsed   = urlparse(ws_url)
    use_ssl  = ws_url.startswith("wss://")
    host     = parsed.hostname
    port     = parsed.port or (443 if use_ssl else 80)
    path_qs  = parsed.path or "/"
    if parsed.query:
        path_qs += "?" + parsed.query

    raw = socket.create_connection((host, port), timeout=10)
    if use_ssl:
        try:
            # Default context has a distinctive non-browser TLS fingerprint that
            # anti-bot edges (Cloudflare, Discord's gateway WAF) can flag and
            # silently drop. Nudge ALPN + cipher ordering toward what a real
            # Chrome TLS ClientHello looks like — best-effort, not a full
            # JA3 match, but meaningfully less fingerprintable than the bare
            # interpreter default.
            ctx = _ssl_mod.SSLContext(_ssl_mod.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode    = _ssl_mod.CERT_NONE
            ctx.set_alpn_protocols(["http/1.1"])
            ctx.minimum_version = _ssl_mod.TLSVersion.TLSv1_2
            try:
                ctx.set_ciphers(
                    "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
                    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                    "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305"
                )
            except _ssl_mod.SSLError:
                pass
        except Exception:
            # Every other outbound request in this file uses verify=False —
            # this fallback context was the one place that still enforced real
            # certificate validation, silently rejecting upstreams the rest of
            # the proxy treats as fine.
            ctx = _ssl_mod.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode    = _ssl_mod.CERT_NONE
        srv = ctx.wrap_socket(raw, server_hostname=host)
    else:
        srv = raw

    key = _base64.b64encode(_hashlib_ws.sha1(os.urandom(16)).digest()).decode()
    hdrs = [
        f"GET {path_qs} HTTP/1.1",
        f"Host: {host}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    for k, v in extra_headers.items():
        kl = k.lower()
        if kl not in ("host", "upgrade", "connection",
                      "sec-websocket-key", "sec-websocket-version",
                      "sec-websocket-extensions", "accept-encoding"):
            hdrs.append(f"{k}: {v}")
    srv.sendall(("\r\n".join(hdrs) + "\r\n\r\n").encode())

    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = srv.recv(4096)
        if not chunk:
            raise ConnectionError("Server closed during WS handshake")
        resp += chunk
        if len(resp) > 65536:
            raise ConnectionError("WS handshake response too large")
    if b"101" not in resp.split(b"\r\n")[0]:
        try: srv.close()
        except Exception: pass
        raise ConnectionError(f"WS handshake rejected: {resp[:200]!r}")
    return srv


def _pump_ws_frames(client_sock, srv, ws_url: str) -> None:
    """Bidirectionally relay WS frames once both sides are connected and upgraded."""
    log(f"WS tunnel open → {ws_url}", "INFO")
    stop = threading.Event()

    def _fwd_client_to_srv():
        """Browser → upstream server: frames are already masked by browser."""
        while not stop.is_set():
            frame = _ws_read_frame(client_sock)
            if frame is None:
                break
            opcode, payload = frame
            if opcode == 8:   # close
                try:
                    srv.sendall(_ws_make_frame(8, payload, mask=True))
                except Exception:
                    pass
                break
            if opcode in (1, 2):   # text or binary
                try:
                    srv.sendall(_ws_make_frame(opcode, payload, mask=True))
                except Exception:
                    break
            elif opcode == 9:   # ping → pong
                try:
                    client_sock.sendall(_ws_make_frame(10, payload))
                except Exception:
                    break
        stop.set()

    def _fwd_srv_to_client():
        """Upstream server → browser: frames are unmasked from server."""
        while not stop.is_set():
            frame = _ws_read_frame(srv)
            if frame is None:
                break
            opcode, payload = frame
            if opcode == 8:
                try:
                    client_sock.sendall(_ws_make_frame(8, payload))
                except Exception:
                    pass
                break
            if opcode in (1, 2):
                try:
                    client_sock.sendall(_ws_make_frame(opcode, payload))
                except Exception:
                    break
            elif opcode == 9:
                try:
                    srv.sendall(_ws_make_frame(10, payload, mask=True))
                except Exception:
                    break
        stop.set()

    t1 = threading.Thread(target=_fwd_client_to_srv, daemon=True)
    t2 = threading.Thread(target=_fwd_srv_to_client, daemon=True)
    t1.start(); t2.start()
    stop.wait()
    # Close BOTH sides. Only closing `srv` here leaked the browser-side socket
    # on every WS teardown — harmless for a single game, but `.io` titles open
    # many short-lived WS connections per session and this exhausted file
    # descriptors over time, surfacing as unrelated-looking "connection refused"
    # errors later in the same run.
    try: srv.close()
    except Exception: pass
    try: client_sock.close()
    except Exception: pass
    log(f"WS tunnel closed ← {ws_url}", "INFO")


def _handle_websocket_upgrade(req_path: str, target: str, req_headers: dict) -> Response:
    """
    Complete a WebSocket upgrade from the browser and proxy all frames bidirectionally.

    Works with Werkzeug's threaded development server by hijacking the underlying
    socket via environ['werkzeug.socket'] (Werkzeug ≥ 2.1).
    """
    # Determine the real WS target URL — use target as the canonical source of
    # path+query so callers don't need to pass them separately. flask_request
    # .query_string is used only as a fallback for MAIN_HOST WS routes where
    # the target is built without a query string but the browser may add one.
    parsed = urlparse(target)
    ws_scheme = "wss" if parsed.scheme in ("https", "wss") else "ws"
    ws_path   = parsed.path or "/"
    # Build the query string: prefer what's in the target (set by ws_ext from
    # the URL path segment) over flask_request.query_string (correct for the
    # main proxy WS route).  Avoid duplication by checking both.
    target_qs = parsed.query
    req_qs    = flask_request.query_string.decode("utf-8", "ignore")
    if target_qs:
        ws_qs = target_qs
    else:
        ws_qs = req_qs
    ws_url = f"{ws_scheme}://{parsed.netloc}{ws_path}"
    if ws_qs:
        ws_url += "?" + ws_qs

    # Get the raw client socket from Werkzeug
    environ     = flask_request.environ
    client_sock = (environ.get("werkzeug.socket")          # Werkzeug ≥ 2.1
                   or environ.get("gunicorn.socket")       # gunicorn
                   or environ.get("HTTP_BODY"))            # other WSGI

    if client_sock is None:
        log(f"WS upgrade: no raw socket available for {req_path} — returning 426", "WARN")
        r = Response("WebSocket proxying requires Werkzeug ≥ 2.1", status=426)
        r.headers["Upgrade"] = "websocket"
        return r

    ws_key = flask_request.headers.get("Sec-WebSocket-Key", "")
    accept  = _ws_handshake_accept(ws_key)

    # Sub-protocols requested by the browser
    proto_req = flask_request.headers.get("Sec-WebSocket-Protocol", "")
    proto_hdrs = f"Sec-WebSocket-Protocol: {proto_req}\r\n" if proto_req else ""

    # Forward headers (strip hop-by-hop + WS-specific headers)
    extra_fwd = {}
    for k, v in req_headers.items():
        kl = k.lower()
        if kl in ("host", "upgrade", "connection",
                   "sec-websocket-key", "sec-websocket-version",
                   "sec-websocket-extensions", "accept-encoding"):
            continue
        extra_fwd[k] = v
    _incoming_origin = next((v for k, v in req_headers.items() if k.lower() == "origin"), None)
    extra_fwd["Origin"] = _real_origin_for(_incoming_origin)
    if proto_req:
        extra_fwd["Sec-WebSocket-Protocol"] = proto_req

    # Connect + complete the WS handshake with the REAL upstream server FIRST.
    # Only tell the browser "101 Switching Protocols" once that has actually
    # succeeded — otherwise a blocked/rejected upstream looks to the browser
    # like a connection that opened and then instantly died, instead of a
    # clean immediate failure (which retries faster and reports clearer).
    try:
        srv = _ws_connect_upstream(ws_url, extra_fwd)
    except Exception as exc:
        log(f"WS upstream connect failed {ws_url}: {_short_exc(exc)}", "WARN")
        try:
            client_sock.sendall(
                b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            )
        except Exception:
            pass
        return Response(status=200)  # socket already handled directly above

    # Complete the handshake with the browser now that upstream is confirmed live
    handshake = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        f"{proto_hdrs}"
        "\r\n"
    )
    try:
        client_sock.sendall(handshake.encode())
    except Exception as exc:
        log(f"WS handshake send failed: {exc}", "WARN")
        try: srv.close()
        except Exception: pass
        return Response("WS handshake error", status=500)

    log(f"WS proxy: {req_path} → {ws_url}", "INFO")
    _gui_push_raw("WS", req_path, 101, "websocket", b"(WebSocket tunnel opened)")

    # Proxy runs in the current Flask worker thread (already its own thread)
    _pump_ws_frames(client_sock, srv, ws_url)

    # After the WS closes, return a dummy response
    # (Flask won't actually send this since the socket was hijacked)
    return Response(status=200)


# ──────────────────────────────────────────────────────────────────────────────
# Flask proxy app
# ──────────────────────────────────────────────────────────────────────────────

class _S2LWSGIRequestHandler(WSGIRequestHandler):
    """WSGIRequestHandler that exposes the raw client socket via the WSGI environ.

    ROOT CAUSE FIX: _handle_websocket_upgrade() hijacks the connection via
    environ['werkzeug.socket'], but vanilla Werkzeug (even >= 2.1) never
    populates that key on its own — it requires exactly this kind of
    make_environ() override. Without it, environ.get("werkzeug.socket") is
    ALWAYS None, so every single WS upgrade attempt (Discord's gateway, its
    remote-auth-gateway, any game's own WS) fell straight into the
    "no raw socket available" branch and got an immediate 426 — which is why
    the connection never succeeded even once, for any WS target, anywhere in
    this proxy. This must be passed as request_handler= to every app.run()/
    make_server() call (main app AND each MULTIPORT CDN mini-server) for WS
    proxying to work on that listener.
    """
    def make_environ(self):
        environ = super().make_environ()
        environ["werkzeug.socket"] = self.connection
        return environ


app = Flask(__name__, static_folder=None)

@app.before_request
def _normalize_request_origin():
    # When HTTPS-Only mode is active, or when a site JS reads
    # window.location.protocol and rebuilds asset URLs as "https://localhost:PORT/..."
    # the browser connects to our HTTP-only Flask server over TLS, which makes
    # it immediately close with ERR_SSL_PROTOCOL_ERROR.  Detect this via the
    # X-Forwarded-Proto or the Flask request scheme — if the *client* believes
    # it's on HTTPS but we're running plain HTTP, issue a 307 to the http://
    # equivalent so the browser retries correctly without TLS.
    #
    # Flask/Werkzeug behind a plain TCP socket always reports scheme="http",
    # so the reliable signal is the X-Forwarded-Proto header — if the Upgrade
    # header is present (websocket), skip both checks below entirely; a
    # redirect response doesn't make sense mid-handshake.
    if flask_request.headers.get("Upgrade", "").lower() == "websocket":
        return None
    fwd_proto = flask_request.headers.get("X-Forwarded-Proto", "").lower()
    if fwd_proto == "https":
        # Strip the https in favour of http; preserve full path + query
        new_url = flask_request.url.replace("https://", "http://", 1)
        return redirect(new_url, code=307)

    # Browsers and sites frequently special-case the literal string "localhost"
    # in security checks (CSP frame-ancestors, secure-context requirements,
    # CORS allowlists) without treating "127.0.0.1" as equivalent, even though
    # both resolve to the same loopback interface. Poki's own CSP is a real,
    # observed example: it allowlists frame-ancestors http://localhost:8080
    # explicitly but NOT 127.0.0.1:8080. A request that looks identical except
    # for arriving via the IP literal gets silently blocked by the browser
    # with zero server-side trace — and because the game's own postMessage
    # handshake with its CDN iframe falls back to re-pointing the iframe at
    # whatever origin it last saw on a failed check, this surfaces as the
    # game's OWN iframe ending up pointed at the (CSP-blocked) real upstream
    # host — "connection refused", with no indication why.
    # Redirecting once, up front, means every other piece of this proxy can
    # keep assuming "the user is on localhost" without re-deriving it per
    # request — and the person never has to remember to type the right host.
    host = flask_request.host
    if host == "127.0.0.1" or host.startswith("127.0.0.1:"):
        new_host = "localhost" + host[len("127.0.0.1"):]
        new_url = flask_request.url.replace(host, new_host, 1)
        return redirect(new_url, code=302)
    return None

_BODY_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_SAFE_METHODS = frozenset({"GET", "HEAD"})
_ALL_METHODS  = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]


def _upstream_url(path: str) -> str:
    origin = f"{urlparse(SITE_URL).scheme}://{MAIN_HOST}"
    base   = f"{origin}/{path.lstrip('/')}"
    qs     = flask_request.query_string.decode("utf-8", "ignore")
    return f"{base}?{qs}" if qs else base


def _build_ctx(method: str, target: str, req_body: bytes) -> HookContext:
    parsed = urlparse(target)
    return HookContext(
        method      = method,
        url         = target,
        path        = parsed.path or "/",
        query       = flask_request.query_string.decode("utf-8", "ignore"),
        req_headers = filter_fwd(dict(flask_request.headers)),
        req_body    = req_body,
    )


_SW_PATHS = frozenset({
    "/service-worker.js", "/sw.js", "/serviceworker.js",
    "/firebase-messaging-sw.js", "/push-sw.js", "/workbox-sw.js", "/ngsw-worker.js", "/worker.js",
})

def _is_sw_path(path: str) -> bool:
    return (path in _SW_PATHS or path.endswith("-sw.js") or path.endswith("-worker.js") or "service-worker" in path)
# Minimal no-op service worker — unregisters itself immediately
_SW_NOOP = (
    b"/* S2L no-op SW: clears previous site caches and SWs on every target change */\n"
    b"self.addEventListener('install', e => e.waitUntil(self.skipWaiting()));\n"
    b"self.addEventListener('activate', e => e.waitUntil(\n"
    b"  Promise.all([\n"
    b"    self.clients.claim(),\n"
    b"    caches.keys().then(keys => Promise.all(keys.map(k => caches.delete(k))))\n"
    b"  ])\n"
    b"));\n"
    b"self.addEventListener('fetch', () => {});\n"  # don't intercept — pass through
)


def _do_upstream(method: str, target: str, ctx: HookContext,
                  stream: bool = False) -> requests.Response:
    """Execute upstream request and return response object with content consumed."""
    sess = _get_client_session()   # per-browser session (cookies isolated per IP)

    fwd = dict(ctx.req_headers)
    fwd["Host"] = MAIN_HOST

    # Rewrite Origin/Referer so upstream sees its real origin, not localhost:PORT
    origin_base = f"{urlparse(SITE_URL).scheme}://{MAIN_HOST}"
    rewrite_origin(fwd, origin_base)
    # Carry CSRF tokens from the real browser request → upstream
    inject_csrf_headers(fwd)

    browser_ua = flask_request.headers.get("User-Agent", "")
    if DEVICE == "auto":
        if browser_ua:
            fwd["User-Agent"]         = _sanitize_ua(browser_ua)
            sess.headers["User-Agent"] = _sanitize_ua(browser_ua)
        else:
            device = _effective_device()
            ua = _sanitize_ua(UA_PROFILES.get(device, UA_PROFILES["macintosh"]))
            fwd["User-Agent"]         = ua
            sess.headers["User-Agent"] = ua
    else:
        ua = _sanitize_ua(UA_PROFILES.get(DEVICE, UA_PROFILES["macintosh"]))
        fwd["User-Agent"]         = ua
        sess.headers["User-Agent"] = ua

    for hint_hdr in ("sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
                     "sec-ch-ua-arch", "sec-ch-ua-bitness", "sec-ch-ua-model",
                     "sec-fetch-dest", "sec-fetch-mode", "sec-fetch-site",
                     "sec-fetch-user"):
        if hint_hdr not in {k.lower() for k in fwd}:
            val = flask_request.headers.get(hint_hdr, "")
            if val:
                fwd[hint_hdr] = val

    orig_accept  = fwd.get("Accept", "")
    orig_purpose = flask_request.headers.get("Purpose", "")
    is_api_req = (
        "application/json" in orig_accept
        or "text/plain" in orig_accept
        or orig_purpose == "prefetch"
        or method in _BODY_METHODS
    )
    # Only inject a browser-like Accept when the original is missing or bare wildcard.
    # If the browser specified a typed Accept (text/css, image/*, etc.) preserve it —
    # asset bundle servers (e.g. YouTube /ss/ CSS endpoint) use it to pick format.
    _bare_accept = not orig_accept or orig_accept.strip() in ("*/*", "")
    if not is_api_req and _bare_accept:
        fwd["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

    # Accept-Encoding strategy:
    # curl_cffi impersonate mode uses libcurl under the hood. libcurl with
    # CURLOPT_ACCEPT_ENCODING set will auto-decompress AND strip Content-Encoding
    # from response headers (so decompress_body sees no encoding to act on).
    # Without it set, the raw compressed bytes are returned unchanged.
    #
    # Our decompress_body handles both cases via magic-byte detection, but the
    # safest approach is to request "identity" (no compression) so we always
    # receive plain bytes — no ambiguity, no double-decompress risk.
    #
    # Exception: curl_cffi's impersonate mode may override Accept-Encoding
    # internally to match Chrome's fingerprint. We use the session header
    # override to ensure identity wins.
    fwd["Accept-Encoding"] = "identity"
    # Also update the session-level header so curl_cffi impersonation doesn't
    # re-inject a compressed Accept-Encoding at the libcurl level.
    try:
        sess.headers["Accept-Encoding"] = "identity"
    except Exception:
        pass

    kwargs: dict = {
        "headers":         fwd,
        "timeout":         (TIMEOUT_CONN, TIMEOUT_READ),
        "allow_redirects": True,
        "verify":          False,
        "stream":          stream,
    }

    fwd.pop("Cookie",  None)
    fwd.pop("cookie",  None)
    merged_cookies = dict(sess.cookies)
    merged_cookies.update(flask_request.cookies)
    if merged_cookies:
        kwargs["cookies"] = merged_cookies

    if method in _BODY_METHODS and ctx.req_body:
        ct = flask_request.content_type or ""
        if "multipart/form-data" in ct:
            # Forward the raw body verbatim with the original Content-Type header
            # (which contains the boundary string). Do NOT let requests rebuild the
            # multipart — it would generate a new boundary and corrupt the payload.
            kwargs["data"] = ctx.req_body
            fwd["Content-Type"]   = ct
            fwd["Content-Length"] = str(len(ctx.req_body))
        elif "application/x-www-form-urlencoded" in ct:
            kwargs["data"] = ctx.req_body
            fwd["Content-Type"] = ct
        else:
            kwargs["data"] = ctx.req_body
            existing_ct = {k.lower() for k in fwd}
            if "content-type" not in existing_ct and ct:
                fwd["Content-Type"] = ct
    response = sess.request(method, target, **kwargs)
    if not stream:
        try:
            _ = response.content   # force-consume so connection is reusable
        except Exception:
            pass

    # ── HTTP/2 empty body bug workaround ─────────────────────────────────────
    # curl_cffi occasionally returns empty content on first request for HTTP/2
    # multiplexed responses (chunked API endpoints like Discord /api/v9/*).
    # Status 200/201 with 0 bytes and no explicit Content-Length: 0 → retry once.
    #
    # CRITICAL: reuse the SAME session (with cookies + Authorization) — do NOT
    # create a fresh unauthenticated session, which returns 401 and we would
    # mistakenly serve that 401 body instead of the real 200 data.
    _resp_cl_raw = response.headers.get("Content-Length", None) if not stream else None
    _resp_cl_explicit_zero = (_resp_cl_raw is not None and int(_resp_cl_raw or 0) == 0)
    if (not stream
            and not _resp_cl_explicit_zero
            and response.status_code in (200, 201)
            and response.content == b""
            and method in _SAFE_METHODS | {"POST"}):
        try:
            # Retry with the SAME authenticated session + identity encoding
            retry_fwd = dict(fwd)
            retry_fwd["Accept-Encoding"] = "identity"
            retry_kwargs = dict(kwargs)
            retry_kwargs["headers"] = retry_fwd
            retry_response = sess.request(method, target, **retry_kwargs)
            try:
                _ = retry_response.content
            except Exception:
                pass
            # Only promote the retry if it kept the same status code —
            # never replace a 200 with a 401/403 auth-error body.
            if retry_response.content and retry_response.status_code == response.status_code:
                response = retry_response
        except Exception:
            pass   # silently fall through — proxy layer will handle empty body

    # Cloudflare block detected → rotate to a fresh session and retry once.
    # IMPORTANT: when stream=True, DO NOT access response.content here —
    # accessing it would consume the entire stream before iter_content() is called,
    # causing an empty response. Classic 403/503 blocks are always HTML, never
    # binary, but Managed Challenge can also return 200 with a JS challenge body
    # (the q=78/jschl_vc family) — _is_cf_block checks for both via headers+body.
    if not stream and _is_cf_block(
            response.content[:8192] if response.content else b"",
            response.status_code,
            dict(response.headers)):
        log(f"CF block on {urlparse(target).netloc} — rotating session and retrying", "WARN")
        _proxy_local.s = _make_session()
        sess = _proxy_local.s
        sess.headers["User-Agent"] = fwd.get("User-Agent",
            _sanitize_ua(UA_PROFILES["macintosh"]))
        response = sess.request(method, target, **kwargs)
        if not stream:
            try:
                _ = response.content
            except Exception:
                pass

    return response


def _make_flask_resp(ctx: HookContext, method: str) -> Response:
    """Build Flask Response from HookContext with body validation."""
    # For HEAD, body is always empty (HTTP spec)
    body = b"" if method == "HEAD" else ctx.resp_body
    if body is None:
        body = b""
        log(f"Body was None in _make_flask_resp for: {method} {ctx.path}", "ERROR")

    # Ensure body is bytes
    if not isinstance(body, bytes):
        try:
            body = str(body).encode('utf-8')
        except Exception:
            body = b"(conversion error)"
        log(f"Body converted to bytes in: {method} {ctx.path}", "WARN")

    # Strip content-type from headers dict — we pass it explicitly via content_type
    # to avoid Flask receiving two conflicting Content-Type values
    clean_headers = {k: v for k, v in ctx.resp_headers.items()
                     if k.lower() != "content-type"}

    resp = Response(body, status=ctx.resp_status,
                    headers=clean_headers, content_type=ctx.resp_ct)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    # Always set here (not just on the live-fetch path) so a cache-hit refresh
    # never silently drops these — that mismatch is what makes cross-origin-
    # isolated apps (SharedArrayBuffer/WASM threads) report missing headers
    # only after the first reload.
    resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
    if COOP_COEP:
        resp.headers["Cross-Origin-Opener-Policy"]   = "same-origin"
        resp.headers["Cross-Origin-Embedder-Policy"] = "credentialless"

    is_html_resp = "text/html" in ctx.resp_ct
    is_hooked    = bool(ctx.resp_headers.get("_hooked"))

    # HOOK RELIABILITY: when GUI is active, all responses get no-store so the
    # browser never caches — hooks fire on every request, not just the first.
    if HOOK_GUI or is_html_resp or is_hooked:
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"]        = "no-cache"
        resp.headers["Expires"]       = "0"
        resp.headers.pop("ETag",          None)
        resp.headers.pop("Last-Modified", None)

    return resp


def _serve_cached(lp: str, target: str) -> tuple[bytes, str] | None:
    """"""
    sidecar_ct = _load_ctype_sidecar(lp)
    # A sidecar means the real upstream type disagreed with the on-disk path —
    # e.g. an extensionless CSS/JS URL that collapsed to .../index.html.
    # Trust the sidecar over the naive ".html" extension check.
    looks_html = lp.endswith(".html") or lp.endswith("/index.html")
    is_html_file = looks_html and (sidecar_ct is None or sidecar_ct.startswith("text/html"))

    if is_html_file:
        try:
            data = open(lp, "rb").read()
        except OSError:
            return None
        if _is_wire_payload(data) or not _is_valid_html(data):
            log(f"Stale RSC cache — purging {os.path.relpath(lp)}", "WARN")
            try:
                os.remove(lp)
                os.remove(_ctype_sidecar_path(lp))
            except OSError:
                pass
            return None
        # Purge bot/CAPTCHA pages that slipped into cache before this guard existed
        if _is_bot_page(data):
            log(f"Bot page in cache — purging {os.path.relpath(lp)}", "WARN")
            try:
                os.remove(lp)
                os.remove(_ctype_sidecar_path(lp))
            except OSError:
                pass
            return None
        if PROXY_CDN:
            data = _rewrite_ext_urls(data, target)
        data = rewrite_abs_urls(data)   # strip absolute MAIN_HOST URLs → proxy-relative
        data = _inject_sw_clear(data)
        return data, "text/html; charset=utf-8"

    try:
        data = open(lp, "rb").read()
    except OSError:
        return None
    ct = sidecar_ct or guess_mime(lp)
    # Rewrite external URLs inside cached CSS files so images/fonts load through the proxy
    if PROXY_CDN and (lp.endswith(".css") or "css" in ct):
        data = _rewrite_ext_urls(data, target)
    elif PROXY_CDN and (lp.endswith((".json", ".webmanifest")) or "json" in ct):
        data = _rewrite_json_urls(data)
    return data, ct


def _cached_response(lp: str, target: str, method: str, req_path: str) -> Response | None:
    """"""
    result = _serve_cached(lp, target)
    if result is None:
        return None

    data, ct = result
    log(f"{method:6} HIT   {req_path}  {_fmt_size(len(data))}", "←")
    ctx = HookContext(
        method       = method,
        url          = target,
        path         = req_path,
        query        = flask_request.query_string.decode("utf-8", "ignore"),
        req_headers  = filter_fwd(dict(flask_request.headers)),
        req_body     = b"",
        resp_status  = 200,
        resp_headers = {
            # HTML: never cache at browser level (multi-site on same origin)
            # Non-HTML: allow long caching (JS/CSS/images don't change per-site)
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
                             if ct.startswith("text/html")
                             else "public, max-age=86400"
        },
        resp_body    = data,
        resp_ct      = ct,
    )

    if _REQ_HOOKS:
        _run_hooks(_REQ_HOOKS, ctx)
    if _RESP_HOOKS:
        _run_hooks(_RESP_HOOKS, ctx)
    _apply_gui_hooks(ctx)
    _gui_push(ctx)
    _maybe_capture(ctx)
    return _make_flask_resp(ctx, method)


# ── /__s2l_ext__/<host>/<path>  —  CDN fallback (MULTIPORT=False) ─────

@app.route("/.__s2l_hp", methods=["GET"])
def _s2l_hp_endpoint() -> Response:
    """Live CDN host→port map for JS injector polling.
    The injector fetches PX+'/.__s2l_hp' every 2s so CDN hosts registered
    AFTER the initial page load are picked up without a refresh.
    """
    with _cdn_port_lock:
        hp = {h: p for h, p in _cdn_host_port.items() if p > 0}
    resp = Response(json.dumps(hp), content_type="application/json")
    resp.headers["Cache-Control"]               = "no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route(f"{_EXT_PREFIX}/<path:extpath>", methods=_ALL_METHODS)
def ext_asset(extpath: str) -> Response:
    slash = extpath.find("/")
    if slash == -1:
        return Response("Bad ext path", status=400)
    ext_host = extpath[:slash]
    ext_path = extpath[slash:]
    method   = flask_request.method.upper()
    qs       = flask_request.query_string.decode("utf-8", "ignore")

    # Recover a still-percent-encoded object path when possible — see
    # _raw_path_after() for why this matters (Firebase Storage %2F keys, etc.)
    _raw_rest = _raw_path_after(_EXT_PREFIX + "/")
    if _raw_rest is not None:
        _raw_slash = _raw_rest.find("/")
        if _raw_slash != -1:
            ext_path = _raw_rest[_raw_slash:]

    real_url = f"https://{ext_host}{ext_path}"
    if qs:
        real_url += f"?{qs}"

    if method == "OPTIONS":
        r = Response(status=204)
        r.headers.update({
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": ", ".join(_ALL_METHODS),
            "Access-Control-Allow-Headers": flask_request.headers.get(
                "Access-Control-Request-Headers", "*"),
            "Access-Control-Max-Age": "86400",
        })
        return r

    lp = local_path(real_url)
    if method in _SAFE_METHODS and CACHE_CDN and os.path.isfile(lp):
        result = _serve_cached(lp, real_url)
        if result is not None:
            data, ct = result
            _gui_push_raw("GET", f"[cdn] {ext_host}{ext_path}", 200, ct, data)
            resp = Response(data, content_type=ct)
            resp.headers["Cache-Control"] = "public, max-age=86400"
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Expose-Headers"] = "*"
            return resp
    if OFFLINE:
        return Response("Offline — not cached", status=404)
    try:
        req_body = flask_request.get_data(cache=False) if method in _BODY_METHODS else b""
        ctx = HookContext(
            method=method, url=real_url, path=ext_path, query=qs,
            req_headers=filter_fwd(dict(flask_request.headers)),
            req_body=req_body,
        )
        _real_origin = _real_origin_for(
            ctx.req_headers.get("Origin") or ctx.req_headers.get("Referer"))
        for _k in list(ctx.req_headers.keys()):
            _kl = _k.lower()
            if _kl == "origin": ctx.req_headers[_k] = _real_origin
            elif _kl == "referer": ctx.req_headers[_k] = _real_origin + "/"
        ctx.req_headers["Host"] = ext_host

        if _REQ_HOOKS:
            stats.inc("hooks_run", _run_hooks(_REQ_HOOKS, ctx))

        kw: dict = {
            "headers":         ctx.req_headers,
            "timeout":         (TIMEOUT_CONN, TIMEOUT_READ),
            "allow_redirects": True,
            "verify":          False,
        }
        if method in _BODY_METHODS:
            kw["data"] = ctx.req_body
        # Per-client session — see the matching comment in _cdn_serve. This is
        # the exact path a brand-new per-game host (e.g. Poki's
        # <uuid>.gdn.poki.com) goes through on its first request, before it
        # has its own MULTIPORT port, so it's the one most likely to need
        # whatever session/consent state the main page already established.
        r    = _get_client_session().request(method, real_url, **kw)
        body = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
        ct   = r.headers.get("Content-Type", "application/octet-stream")
        # Only save real 200 responses with content — 304 has empty body and saving
        # it would corrupt any existing cached file.
        if r.status_code == 200 and body and method in _SAFE_METHODS and CACHE_CDN:
            save_queue.put((real_url, body, ct))
            _register_cdn_host(ext_host)
            stats.inc("cdn_fetched")
        elif r.status_code < 400 and method in _SAFE_METHODS:
            # CACHE_CDN=False: still register host so URL rewriting works next time
            _register_cdn_host(ext_host)
        out_headers = filter_resp(dict(r.headers))
        out_headers["Access-Control-Allow-Origin"]  = "*"
        out_headers["Access-Control-Expose-Headers"] = "*"
        out_headers["Cross-Origin-Resource-Policy"] = "cross-origin"
        if method in _SAFE_METHODS:
            out_headers["Cache-Control"] = "public, max-age=86400"
        # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
        # through the proxy (critical for game iframes served via /__s2l_ext__/).
        _ct_base_e = ct.split(";")[0].strip().lower()
        if r.status_code < 400 and PROXY_CDN:
            if "text/html" in _ct_base_e:
                body = _rewrite_ext_urls(body, real_url)
                body = rewrite_abs_urls(body)
                body = _inject_sw_clear(body)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)
            elif "text/css" in _ct_base_e:
                body = _rewrite_ext_urls(body, real_url)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)
            elif "json" in _ct_base_e:
                body = _rewrite_json_urls(body)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)

        ctx.resp_status, ctx.resp_headers, ctx.resp_body, ctx.resp_ct = (
            r.status_code, out_headers, body, ct)
        if _RESP_HOOKS:
            stats.inc("hooks_run", _run_hooks(_RESP_HOOKS, ctx))
        _apply_gui_hooks(ctx)

        log(f"ext  {method} {ctx.resp_status}  {ext_host}{ext_path}  {_fmt_size(len(ctx.resp_body))}", "CDN")
        _gui_push_raw(method, ext_path, ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                      display_tag="[ext]")
        if CAPTURE and CAPTURE_CDN:
            _maybe_capture(ctx)
        return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
    except _CONN_ERRORS as exc:
        return Response(f"CDN unreachable: {_short_exc(exc)}", status=502)
    except Exception as exc:
        return Response(f"CDN error: {_short_exc(exc)}", status=502)


# ── /__s2l_ws_ext__/<host:port>/<path>  —  WebSocket proxy to arbitrary external hosts ──
#
# The S2L JS injector rewrites:
#     new WebSocket("wss://game-server.com:8443/ws?token=xxx")
# to:
#     new WebSocket("ws://localhost:PORT/__s2l_ws_ext__/game-server.com:8443/ws?token=xxx")
#
# This route connects upstream first, then completes the WS handshake with the
# browser and forwards all frames bidirectionally (see _handle_websocket_upgrade).

@app.route("/__s2l_ws_ext__/<path:wspath>", methods=["GET"])
def ws_ext(wspath: str) -> Response:
    if flask_request.headers.get("Upgrade", "").lower() != "websocket":
        return Response(
            "This path is a WebSocket tunnel — send Upgrade: websocket",
            status=426,
            headers={"Upgrade": "websocket"},
        )
    slash = wspath.find("/")
    if slash == -1:
        ws_host     = wspath
        ws_path_q   = "/"
    else:
        ws_host     = wspath[:slash]
        ws_path_q   = wspath[slash:]
    qs = flask_request.query_string.decode("utf-8", "ignore")
    if qs:
        ws_path_q += f"?{qs}"
    # _handle_websocket_upgrade expects an https:// target and maps it to wss://.
    # We pass https:// so the helper builds the correct wss:// connection to the real host.
    target_https = f"https://{ws_host}{ws_path_q}"
    fwd_hdrs     = filter_fwd(dict(flask_request.headers))
    req_display  = f"/__s2l_ws_ext__/{ws_host}{ws_path_q}"
    log(f"WS-ext upgrade → {ws_host}{ws_path_q}", "INFO")
    return _handle_websocket_upgrade(req_display, target_https, fwd_hdrs)


# ── Main proxy ────────────────────────────────────────────────────────────────

@app.route("/", defaults={"path": ""}, methods=_ALL_METHODS)
@app.route("/<path:path>",             methods=_ALL_METHODS)
def proxy(path: str) -> Response:
    method   = flask_request.method.upper()
    target   = _upstream_url(path)
    req_path = "/" + path.lstrip("/")
    stats.inc("proxied")
    purpose = flask_request.headers.get("Purpose", "").lower()
    if purpose in ("prefetch", "prerender"):
        _gui_push_raw(method, req_path, 204, "text/plain", b"")
        return Response(status=204)  # No Content
    if flask_request.headers.get("Upgrade", "").lower() == "websocket":
        # Real WebSocket proxy — handles Discord DMs, Slack, chat apps, etc.
        fwd_hdrs = filter_fwd(dict(flask_request.headers))
        return _handle_websocket_upgrade(req_path, target, fwd_hdrs)

    # Service worker — no-op SW
    if _is_sw_path(req_path):
        _gui_push_raw(method, req_path, 200, "application/javascript", _SW_NOOP)
        resp = Response(_SW_NOOP, content_type="application/javascript; charset=utf-8")
        resp.headers["Service-Worker-Allowed"] = "/"
        resp.headers["Cache-Control"] = "no-store"
        return resp

    if any(x in target for x in CF_BLOCK_PATHS):
        _gui_push_raw(method, req_path, 403, "text/plain", b"Blocked")
        return Response("Blocked", status=403)

    # ── Cloudflare Image Resizing paths (/f=auto/, /cdn-cgi/image/, …) ───────
    # poki.com's CF Image Optimization requires the browser's own cf_clearance.
    # Our proxy session lacks it → poki.com returns its full SSR 404 page
    # (~190KB HTML). Attempt with client-session cookies first; if still 404/HTML,
    # return an empty 404 immediately — no second fetch, no 190KB to browser, no
    # noisy log line.
    if method in _SAFE_METHODS and any(req_path.startswith(p) for p in CF_IMG_PREFIXES):
        try:
            _i_sess = _get_proxy_session()
            _i_hdrs = filter_fwd(dict(flask_request.headers))
            _i_hdrs["Host"] = MAIN_HOST
            _i_r = _i_sess.get(target, headers=_i_hdrs,
                               cookies={**dict(_i_sess.cookies), **dict(flask_request.cookies)},
                               timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                               allow_redirects=True, verify=False)
            _i_body = decompress_body(_i_r.content, _i_r.headers.get("Content-Encoding", ""))
            _i_ct   = _i_r.headers.get("Content-Type", "application/octet-stream")
            if _i_r.status_code == 200 and "text/html" not in _i_ct:
                if _i_body and CACHE_CDN:
                    save_queue.put((target, _i_body, _i_ct))
                _i_out = filter_resp(dict(_i_r.headers))
                _i_out["Cache-Control"] = "public, max-age=86400"
                log_req(method, 200, MAIN_HOST, req_path, len(_i_body), tag="IMG")
                return Response(_i_body, status=200, headers=_i_out, content_type=_i_ct)
        except Exception:
            pass
        # 404/HTML or error — return minimal 404, skip normal proxy (same result, double bandwidth)
        return Response(b"", status=404, content_type="text/plain")

    if method == "OPTIONS":
        _gui_push_raw("OPTIONS", req_path, 204, "text/plain", b"")
        r = Response(status=204)
        r.headers.update({
            "Allow":                        ", ".join(_ALL_METHODS),
            "Access-Control-Allow-Origin":  flask_request.headers.get("Origin", "*"),
            "Access-Control-Allow-Methods": ", ".join(_ALL_METHODS),
            "Access-Control-Allow-Headers": flask_request.headers.get(
                                            "Access-Control-Request-Headers", "*"),
            "Access-Control-Max-Age": "86400",
        })
        return r

    if OFFLINE:
        if method in _SAFE_METHODS:
            lp = local_path(target)
            if os.path.isfile(lp):
                r = _cached_response(lp, target, method, req_path)
                if r is not None:
                    return r
        _gui_push_raw(method, req_path, 404, "text/plain", b"Offline")
        return Response("Offline — not cached", status=404)

    if method in _SAFE_METHODS:
        lp = local_path(target)
        # IMPORTANT: Range requests MUST always hit upstream.
        # Serving a full file from disk cache in response to "Range: bytes=X-Y"
        # causes the browser to receive a 200 instead of 206 Partial Content —
        # video players (YouTube, HTML5 <video>) interpret this as a truncated
        # stream and loop / stall indefinitely.
        _has_range = bool(flask_request.headers.get("Range", ""))
        if os.path.isfile(lp) and not _has_range:
            cached = _cached_response(lp, target, method, req_path)
            if cached is not None:
                return cached

    raw_body = flask_request.get_data(cache=True) if method in _BODY_METHODS else b""
    ctx = _build_ctx(method, target, raw_body)

    if _REQ_HOOKS:
        stats.inc("hooks_run", _run_hooks(_REQ_HOOKS, ctx))

    # Decide whether to stream based on the URL path extension.
    # The real Content-Type check happens in _stream_resp after we get the response.
    _do_stream = False
    if method in _SAFE_METHODS:
        _path_no_qs = req_path.split("?")[0]
        _ext_ct     = guess_mime(_path_no_qs) or ""
        _do_stream  = _should_stream(_ext_ct)

    try:
        upstream_r = _do_upstream(method, target, ctx, stream=_do_stream)
    except _CONN_ERRORS as exc:
        short = _short_exc(exc)
        log(f"{method} {urlparse(target).path} — {short}", "WARN")
        stats.inc("conn_errors")
        # Retry once with a fresh session on connection-level errors
        try:
            _proxy_local.s = _make_session()
            upstream_r = _do_upstream(method, target, ctx, stream=_do_stream)
            log(f"Retry succeeded {urlparse(target).path}", "INFO")
        except Exception as exc2:
            short2 = _short_exc(exc2)
            log(f"Retry also failed {urlparse(target).path} — {short2}", "ERROR")
            err_body = (f"Connection error: {short}\n"
                        f"Retry: {short2}\n\n"
                        f"This may be caused by:\n"
                        f"  • The server closed the connection prematurely\n"
                        f"  • A network interruption or timeout\n"
                        f"  • Cloudflare or WAF blocking the request")
            return Response(err_body, status=502, content_type="text/plain; charset=utf-8")
    except Exception as exc:
        log(f"{method} {urlparse(target).path} — {_short_exc(exc)}", "ERROR")
        stats.inc("conn_errors")
        _r502 = Response(f"Upstream error: {_short_exc(exc)}", status=502)
        _r502.headers["Access-Control-Allow-Origin"] = "*"
        return _r502
    if hasattr(upstream_r, 'history') and upstream_r.history:
        for hist_resp in upstream_r.history:
            hist_url = hist_resp.url
            try:
                hist_body = decompress_body(
                    hist_resp.content,
                    hist_resp.headers.get("Content-Encoding", "")
                )
            except Exception:
                hist_body = b""

            hist_path = urlparse(hist_url).path or "/"
            hist_ct = hist_resp.headers.get("Content-Type", "application/octet-stream")
            _gui_push_raw(
                method,
                hist_path,
                hist_resp.status_code,
                hist_ct,
                hist_body if hist_body else f"(redirect {hist_resp.status_code})".encode()
            )


    # ── Streaming: hand off immediately for large/binary content ─────────────
    # Check the actual Content-Type now that we have the response headers.
    # If it should be streamed, do it NOW before consuming any body bytes.
    _real_ct = upstream_r.headers.get("Content-Type", "")
    _real_cl = int(upstream_r.headers.get("Content-Length", "0") or 0)
    if _do_stream and _should_stream(_real_ct, _real_cl) and method in _SAFE_METHODS:
        return _stream_resp(upstream_r, method, target)

    # Decompress upstream body before we do anything with it
    enc  = upstream_r.headers.get("Content-Encoding", "")
    body = decompress_body(upstream_r.content, enc)

    # ── Cloudflare / WAF block detection + retry ─────────────────────────────
    # Covers classic 403/503 block pages, plain-text "blocked"/"Access Denied"
    # bodies with no HTML wrapper, AND CF Managed Challenge / Turnstile pages
    # that return HTTP 200 with a JS challenge embedded (the q=78/jschl_vc
    # family reported against poki.com — a status-only check misses these).
    # Either way: rotate to a fresh session with a different browser config
    # and retry once.
    _looks_blocked = (_is_cf_block(body, upstream_r.status_code, dict(upstream_r.headers))
                       or _is_raw_block_text(body, upstream_r.status_code))
    if _looks_blocked and method in _SAFE_METHODS:
        log(f"Block page detected on {req_path} — rotating session and retrying", "WARN")
        try:
            _proxy_local.s = _make_session()  # fresh session, next CF config
            # Also forward all real browser hints on retry
            retry_sess = _get_proxy_session()
            browser_ua_r = flask_request.headers.get("User-Agent", "")
            if browser_ua_r:
                retry_sess.headers["User-Agent"] = _sanitize_ua(browser_ua_r)
            upstream_r2 = _do_upstream(method, target, ctx)
            body2 = decompress_body(upstream_r2.content,
                                    upstream_r2.headers.get("Content-Encoding", ""))
            if not (_is_cf_block(body2, upstream_r2.status_code, dict(upstream_r2.headers))
                    or _is_raw_block_text(body2, upstream_r2.status_code)):
                log(f"Block retry succeeded {req_path} [{upstream_r2.status_code}]", "INFO")
                upstream_r, body = upstream_r2, body2
            else:
                log(f"Block retry also blocked {req_path} — serving block page as-is", "WARN")
        except Exception as e:
            log(f"Block retry error {req_path}: {_short_exc(e)}", "ERROR")
    if upstream_r.status_code == 304:
        # Upstream returned 304 despite us not sending If-None-Match.
        # Serve from local cache if available, otherwise force a clean fetch.
        lp = local_path(target)
        if os.path.isfile(lp):
            cached = _cached_response(lp, target, method, req_path)
            if cached is not None:
                return cached
        log(f"304 with no local cache — re-fetching {req_path}", "WARN")
        try:
            forced_r = _get_proxy_session().get(
                target,
                headers={"Host": MAIN_HOST, "Cache-Control": "no-cache", "Pragma": "no-cache"},
                cookies=flask_request.cookies,
                timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                allow_redirects=True,
                verify=False,
            )
            body       = decompress_body(forced_r.content, forced_r.headers.get("Content-Encoding", ""))
            upstream_r = forced_r
            log(f"Re-fetch: {len(body)} bytes [{upstream_r.status_code}]", "INFO")
        except Exception as e:
            log(f"Re-fetch failed: {_short_exc(e)}", "ERROR")
            return Response("304 with no cached copy", status=502)
    # ── Empty body recovery ───────────────────────────────────────────────────
    # Cases that can produce a real 200 with an empty body:
    #
    #   A) Decompression failed: Content-Length > 0 but body is 0 after decompress.
    #      curl_cffi usually auto-decompresses, but some encodings (zstd, malformed
    #      brotli) slip through. Fix: re-fetch with Accept-Encoding: identity.
    #
    #   B) Discord / Slack API endpoints using chunked Transfer-Encoding.
    #      No Content-Length header, yet body should be JSON. curl_cffi occasionally
    #      returns empty content for HTTP/2 multiplexed responses on the first try.
    #      Fix: retry via _do_upstream with a fresh session & stripped Accept-Encoding.
    #
    #   C) HTML page that came back empty (Cloudflare challenge page stripped etc.)
    #      Already handled below.
    #
    # Legitimate empty-body responses we must NOT re-fetch:
    #   • 204 No Content, 304 Not Modified (already handled above)
    #   • POST/DELETE/PATCH endpoints that intentionally return empty 200/201
    #   • Responses where Content-Length is explicitly 0
    _ct_now     = upstream_r.headers.get("Content-Type", "")
    _cl_raw     = upstream_r.headers.get("Content-Length", None)
    _cl_header  = int(_cl_raw or 0)
    _cl_explicit_zero = (_cl_raw is not None and _cl_header == 0)
    _is_html_ct = "text/html" in _ct_now or not _ct_now.strip()
    _is_json_ct = "application/json" in _ct_now or "application/javascript" in _ct_now

    # Case A: Content-Length said non-zero but body came out empty → decompression bust
    _decomp_failed = (
        len(body) == 0
        and not _cl_explicit_zero
        and _cl_header > 0
        and upstream_r.status_code not in (204, 304)
    )

    # Case B: JSON API endpoint, empty body, chunked (no Content-Length), status 200
    # Only for GET/HEAD — POST/DELETE intentionally return empty 200.
    _empty_api = (
        method in _SAFE_METHODS
        and upstream_r.status_code == 200
        and len(body) == 0
        and not _cl_explicit_zero
        and _is_json_ct
    )

    # Case C: HTML page with empty body
    _empty_html = (
        method in _SAFE_METHODS
        and upstream_r.status_code == 200
        and len(body) == 0
        and _is_html_ct
        and not any(req_path.startswith(p) for p in _BOT_EXEMPT_PREFIXES)
    )

    if _decomp_failed or _empty_api or _empty_html:
        if _decomp_failed:
            reason = "decompression failure"
        elif _empty_api:
            reason = "empty API body"
        else:
            reason = "empty HTML body"
        log(f"Empty body ({reason}) on {req_path} — re-fetching", "WARN")
        try:
            # Re-use _do_upstream so the same session, cookies, and auth headers
            # are forwarded. We modify ctx to force identity encoding.
            retry_ctx      = ctx
            retry_fwd      = dict(ctx.req_headers)
            # Force no compression on retry — defeats decompression failures
            retry_fwd["Accept-Encoding"] = "identity"
            # Preserve Authorization header explicitly (Discord Bearer tokens)
            orig_auth = flask_request.headers.get("Authorization", "")
            if orig_auth and "authorization" not in {k.lower() for k in retry_fwd}:
                retry_fwd["Authorization"] = orig_auth
            retry_ctx = _dc_replace(ctx, req_headers=retry_fwd)
            refetch_r = _do_upstream(method, target, retry_ctx)
            body_r = refetch_r.content   # identity encoding → no decompress needed
            if not body_r:
                body_r = decompress_body(
                    refetch_r.content,
                    refetch_r.headers.get("Content-Encoding", ""),
                )
            if len(body_r) > 0:
                body       = body_r
                upstream_r = refetch_r
                log(f"Re-fetch successful: {_fmt_size(len(body))}", "INFO")
            else:
                log(f"Re-fetch also empty for {req_path}", "WARN")
        except Exception as e:
            log(f"Re-fetch failed: {_short_exc(e)}", "WARN")
    # If main host 404 and we have CDN hosts, try them
    if upstream_r.status_code == 404 and PROXY_CDN and method in _SAFE_METHODS:
        req_path_cdn = urlparse(target).path
        req_qs   = flask_request.query_string.decode("utf-8", "ignore")
        with _cdn_port_lock:
            snap = dict(_cdn_host_port)
        for cdn_host in snap:
            cdn_url = f"https://{cdn_host}{req_path_cdn}"
            if req_qs:
                cdn_url += f"?{req_qs}"
            cdn_lp = local_path(cdn_url)
            if os.path.isfile(cdn_lp):
                try:
                    # Read directly instead of send_file: send_file's mimetype
                    # guessing can disagree with our sidecar-resolved type, and it
                    # doesn't let us set CORP/Cache-Control in one place. A direct
                    # read also avoids a TOCTOU crash if the file is deleted by the
                    # save worker between the isfile() check above and the open.
                    cdn_data = open(cdn_lp, "rb").read()
                except OSError as e:
                    log(f"CDN disk hit unreadable {cdn_lp}: {_short_exc(e)}", "WARN")
                else:
                    log(f"CDN hit {cdn_host}{req_path_cdn}", "CDN")
                    resp = Response(cdn_data, content_type=resolve_mime(cdn_lp))
                    resp.headers["Access-Control-Allow-Origin"]  = "*"
                    resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
                    resp.headers["Cache-Control"] = "public, max-age=86400"
                    return resp
            try:
                cdn_r    = _get_proxy_session().get(cdn_url, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
                cdn_body = decompress_body(cdn_r.content, cdn_r.headers.get("Content-Encoding", ""))
                ct       = cdn_r.headers.get("Content-Type", "application/octet-stream")
                if cdn_r.status_code < 400:
                    save_queue.put((cdn_url, cdn_body, ct))
                    stats.inc("cdn_fetched")
                    log(f"CDN fallback {cdn_host}{req_path_cdn}  {cdn_r.status_code}", "CDN")
                    out = filter_resp(dict(cdn_r.headers))
                    out["Access-Control-Allow-Origin"]  = "*"
                    out["Cross-Origin-Resource-Policy"]  = "cross-origin"
                    return Response(cdn_body, status=cdn_r.status_code, headers=out, content_type=ct)
            except Exception:
                pass
    ctx.resp_status  = upstream_r.status_code
    ctx.resp_headers = filter_resp(dict(upstream_r.headers))
    ctx.resp_body    = body
    ctx.resp_ct      = upstream_r.headers.get("Content-Type", "application/octet-stream")
    platform         = detect_platform(dict(upstream_r.headers))

    # Detect RSC wire format — re-fetch with stronger HTML signal
    if _is_wire_payload(body) and method == "GET":
        log(f"RSC detected on {urlparse(target).path} — re-fetching as full HTML", "WARN")
        try:
            refetch_sess = _get_proxy_session()
            refetch_hdrs = {
                "Host":    MAIN_HOST,
                "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control":   "no-cache",
                "Pragma":          "no-cache",
                "Upgrade-Insecure-Requests": "1",
            }
            refetch_r = refetch_sess.get(
                target,
                headers=refetch_hdrs,
                cookies=flask_request.cookies,
                timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                allow_redirects=True,
                verify=False,
            )
            refetch_body = decompress_body(refetch_r.content, refetch_r.headers.get("Content-Encoding", ""))
            if not _is_wire_payload(refetch_body) and _is_valid_html(refetch_body):
                body = refetch_body
                ctx.resp_body = body
                ctx.resp_ct   = refetch_r.headers.get("Content-Type", "text/html")
                ctx.resp_headers = filter_resp(dict(refetch_r.headers))
                log(f"RSC re-fetch succeeded {urlparse(target).path}", "INFO")
            else:
                log(f"RSC re-fetch also returned RSC — serving as-is", "WARN")
        except Exception as exc:
            log(f"RSC re-fetch error: {_short_exc(exc)}", "WARN")
    sc_color = (Fore.GREEN if upstream_r.status_code < 300
                else Fore.YELLOW if upstream_r.status_code < 400
                else Fore.RED)
    _is_api  = any(x in ctx.resp_ct for x in ("json", "xml", "event-stream"))
    _lbl     = "API " if _is_api else "HTML" if "text/html" in ctx.resp_ct else "    "
    _suppress = is_subresource(ctx.resp_ct) and not _is_api
    if not _suppress:
        log(f"{method:6} {sc_color}{upstream_r.status_code}{Style.RESET_ALL}"
            f"  {_lbl}  {req_path}  {_fmt_size(len(body))}"
            + (f"  [{platform}]" if platform != "Unknown" else ""), "→")
    if upstream_r.status_code >= 400:
        stats.inc("http_errors")
    if SHOW_HIDDEN and is_html(ctx.resp_body, ctx.resp_ct):
        ctx.resp_body = _reveal_hidden(ctx.resp_body)
        ctx.resp_headers.pop("content-length", None)
        stats.inc("revealed")
    # ── Bot page detection ────────────────────────────────────────────────────
    # Must happen on raw body BEFORE _inject_sw_clear adds our script (which
    # pushes the page content past the 8192-byte scan window).
    # Also catches Google Sorry (HTTP 200) which is missed by the CF-block check.
    if _is_bot_page(ctx.resp_body, ctx.resp_status, req_path):
        log(f"Bot page on {req_path} [{ctx.resp_status}] — returning 503", "WARN")
        err_body = (
            b"<html><head><title>S2L: Blocked</title>"
            b"<style>body{font:16px sans-serif;padding:2em;background:#111;color:#eee}"
            b"h1{color:#f66}pre{background:#1a1a1a;padding:1em;border-radius:4px;"
            b"overflow:auto;font-size:13px;color:#aaa}</style></head><body>"
            b"<h1>Upstream returned a bot-detection page</h1>"
            b"<p>The target server blocked S2L's request (CAPTCHA / IP rate-limit).</p>"
            b"<p>This response has <strong>not</strong> been cached.</p>"
            b"<pre>Path:   " + req_path.encode() + b"\n"
            b"Status: " + str(ctx.resp_status).encode() + b"</pre>"
            b"</body></html>"
        )
        err_ctx = HookContext(
            method=method, url=target, path=req_path,
            query=flask_request.query_string.decode("utf-8", "ignore"),
            req_headers=ctx.req_headers, req_body=ctx.req_body,
            resp_status=503, resp_headers={},
            resp_body=err_body, resp_ct="text/html; charset=utf-8",
        )
        _gui_push(err_ctx)
        return _make_flask_resp(err_ctx, method)

    # Save raw body to disk BEFORE any hook/rewrite so the cache always holds
    # the real upstream response. Hooks re-run on every cache hit (see _cached_response),
    # so the browser always gets the hooked version even when served from cache.
    # Exception: don't save if SAVE_ERRORS=False and status >= 400.
    _should_save = (
        method == "GET"
        and not _is_wire_payload(ctx.resp_body)
        and ctx.resp_body
        and (ctx.resp_status < 400 or SAVE_ERRORS)
    )
    if _should_save:
        save_queue.put((target, ctx.resp_body, ctx.resp_ct))
        enqueue(target)

    # HTML + CSS + JSON post-processing
    _is_html = is_html(ctx.resp_body, ctx.resp_ct)
    _is_css  = "text/css" in ctx.resp_ct
    _is_json = "json" in ctx.resp_ct.lower()
    if _is_html or _is_css:
        _body = ctx.resp_body
        if PROXY_CDN:
            _body = _rewrite_ext_urls(_body, target)
        if _is_html:
            _body = rewrite_abs_urls(_body)
            _body = _inject_sw_clear(_body)
        if _body is not ctx.resp_body:
            ctx.resp_body = _body
            ctx.resp_headers.pop("content-length", None)
            ctx.resp_headers.pop("Content-Length", None)
    elif _is_json and PROXY_CDN:
        _body = _rewrite_json_urls(ctx.resp_body)
        if _body is not ctx.resp_body:
            ctx.resp_body = _body
            ctx.resp_headers.pop("content-length", None)
            ctx.resp_headers.pop("Content-Length", None)

    if _RESP_HOOKS:
        stats.inc("hooks_run", _run_hooks(_RESP_HOOKS, ctx))
    _apply_gui_hooks(ctx)

    if not isinstance(ctx.resp_body, (bytes, bytearray)):
        ctx.resp_body = b""
    if ctx.resp_body and "content-length" not in {k.lower() for k in ctx.resp_headers}:
        ctx.resp_headers["Content-Length"] = str(len(ctx.resp_body))

    _gui_push(ctx)
    _maybe_capture(ctx)
    return _make_flask_resp(ctx, method)

# ──────────────────────────────────────────────────────────────────────────────
# Startup banner
# ──────────────────────────────────────────────────────────────────────────────

def _banner() -> None:
    W, C, G = Style.BRIGHT+Fore.WHITE, Style.BRIGHT+Fore.CYAN, Style.BRIGHT+Fore.GREEN
    Y, M    = Style.BRIGHT+Fore.YELLOW, Style.BRIGHT+Fore.MAGENTA
    DIM, R  = Style.DIM, Style.RESET_ALL

    ip      = resolve_ip(MAIN_HOST)
    n_hooks = len(_REQ_HOOKS) + len(_RESP_HOOKS)

    flags = []
    if CRAWL:         flags.append(f"{G}crawl{R}")
    if OFFLINE:       flags.append(f"{Y}offline{R}")
    if DUMP_ALL:      flags.append(f"{M}dump-all{R}")
    if PROXY_CDN and CACHE_CDN:   flags.append(f"{M}cdn:cache{R}")
    elif PROXY_CDN:               flags.append(f"{Y}cdn:live{R}")
    if MULTIPORT:     flags.append(f"{M}multiport{R}")
    if SHOW_HIDDEN:   flags.append(f"{G}show-hidden{R}")
    if SCAN_PATHS:    flags.append(f"{Style.BRIGHT+Fore.RED}scan-paths{R}")
    if CAPTURE:       flags.append(f"{Style.BRIGHT+Fore.RED}capture{R}")
    if HOOK_GUI:      flags.append(f"{Y}hook-gui{R}")
    if COOP_COEP:     flags.append(f"{Style.BRIGHT+Fore.CYAN}coop-coep{R}")
    flag_str   = f"  {DIM}·{R}  ".join(flags) if flags else f"{DIM}none{R}"

    hook_str   = (f"{Y}{n_hooks} hook{'s' if n_hooks != 1 else ''}{R}"
                  if n_hooks else f"{DIM}none{R}")
    device_str = (f"{W}{DEVICE}{R}  {DIM}(auto-mirrors browser UA){R}"
                  if DEVICE == "auto" else f"{W}{DEVICE}{R}")

    bypass_eng = (f"{G}curl_cffi/chrome136{R}" if _CURL_CFFI_OK
                  else f"{Y}cloudscraper{R}  {DIM}(tip: pip install curl-cffi for better CF bypass){R}")


    print(f"""
{C}  ╔══════════════════════════════════════════════════════╗{R}
{C}  ║{W}           S I T E  2  L O C A L                    {C}║{R}
{C}  ╠══════════════════════════════════════════════════════╣{R}
{C}  ║{R}  {DIM}Target:  {R}  {G}{MAIN_HOST}{R}  {DIM}({ip}){R}
{C}  ║{R}  {DIM}Proxy:   {R}  {W}http://{HOST}:{PORT}{R}
{C}  ║{R}  {DIM}Device:  {R}  {device_str}
{C}  ║{R}  {DIM}Bypass:  {R}  {bypass_eng}
{C}  ║{R}  {DIM}Workers: {R}  {W}{WORKERS}{R}
{C}  ║{R}  {DIM}Timeout: {R}  {W}connect={TIMEOUT_CONN}s  read={TIMEOUT_READ}s{R}
{C}  ║{R}  {DIM}Retries: {R}  {W}{RETRIES}× backoff={BACKOFF}s{R}
{C}  ║{R}  {DIM}Hooks:   {R}  {hook_str}
{C}  ║{R}  {DIM}Flags:   {R}  {flag_str}
{C}  ╚══════════════════════════════════════════════════════╝{R}
""")

    if PROXY_CDN and MULTIPORT:
        print(f"  {M}▶ MULTIPORT{R}  {Style.DIM}CDN hosts get a dedicated port starting at {PORT+1}{Style.RESET_ALL}\n")
    elif PROXY_CDN:
        print(f"  {M}▶ CDN{R}  {Style.DIM}assets via {_EXT_PREFIX}/ (single port){Style.RESET_ALL}\n")
    if CAPTURE:
        skip   = "static skipped" if CAPTURE_SKIP_STATIC else "all captured"
        body   = "req body on" if CAPTURE_BODIES else "req body off"
        others = " · CDN included" if CAPTURE_CDN else ""
        print(f"  {Style.BRIGHT+Fore.RED}▶ CAPTURE{Style.RESET_ALL}  {Style.DIM}({skip} · {body}{others}){Style.RESET_ALL}")
        print(f"  {Style.DIM}→  {DATA_FOLDER}/captures/{Style.RESET_ALL}\n")
    if SHOW_HIDDEN:
        print(f"  {G}▶ SHOW_HIDDEN{Style.RESET_ALL}  {Style.DIM}hidden elements revealed in every HTML page{Style.RESET_ALL}\n")
    if SCAN_PATHS:
        total = len(_DEFAULT_HIDDEN_PATHS) + len(EXTRA_PATHS)
        print(f"  {Style.BRIGHT+Fore.RED}▶ SCAN_PATHS{Style.RESET_ALL}"
              f"  {Style.DIM}probing {total} paths → {DATA_FOLDER}/hidden_paths.json{Style.RESET_ALL}\n")
    if HOOK_GUI:
        print(f"  {Y}▶ HOOK_GUI{Style.RESET_ALL}  {Style.DIM}Tkinter traffic inspector + live hook editor{Style.RESET_ALL}\n")
    if n_hooks:
        print(f"  {Y}▶ HOOKS{Style.RESET_ALL}")
        for pat, mset, fn in _REQ_HOOKS:
            print(f"  {Style.DIM}  req  [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{Style.RESET_ALL}")
        for pat, mset, fn in _RESP_HOOKS:
            print(f"  {Style.DIM}  resp [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{Style.RESET_ALL}")
        print()

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import signal

    # Validate flag combinations
    if CACHE_CDN and not PROXY_CDN:
        log("CACHE_CDN=True requires PROXY_CDN=True — disabling CACHE_CDN", "WARN")
        CACHE_CDN = False
    if not PROXY_CDN and MULTIPORT:
        log("MULTIPORT=True has no effect without PROXY_CDN=True", "WARN")
    def _sigint_handler(sig, frame):
        print(f"\n{Fore.YELLOW}{Style.BRIGHT}Script finished by user command{Style.RESET_ALL}")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _sigint_handler)
    signal.signal(signal.SIGTERM, _sigint_handler)

    # Silence werkzeug — including TLS 400s from browser HTTPS-on-HTTP probes
    wz_log = logging.getLogger("werkzeug")
    wz_log.setLevel(logging.CRITICAL)

    class _WerkzeugFilter(logging.Filter):
        """Drop the noisy 400 Bad request version messages from TLS probes."""
        def filter(self, record: logging.LogRecord) -> bool:
            msg = record.getMessage()
            return "Bad request version" not in msg and "400" not in msg

    wz_log.addFilter(_WerkzeugFilter())
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    import click as _click
    _click.echo = lambda *a, **kw: None   # silence Flask startup banner

    if CAPTURE:
        os.makedirs(os.path.join(DATA_FOLDER, "captures"), exist_ok=True)
        threading.Thread(target=_capture_worker, daemon=True, name="capture-worker").start()

    _banner()
    _load_mimes()

    # Purge any bot/CAPTCHA pages that slipped into cache in previous runs
    def _purge_bot_cache() -> None:
        purged = 0
        for root, _dirs, files in os.walk(SRC_FOLDER):
            for fn in files:
                if not fn.endswith(".html"):
                    continue
                fp = os.path.join(root, fn)
                try:
                    with open(fp, "rb") as f:
                        data = f.read(8192)
                    if _is_bot_page(data):
                        os.remove(fp)
                        purged += 1
                        log(f"Purged bot-page cache: {os.path.relpath(fp)}", "WARN")
                except Exception:
                    pass
        if purged:
            log(f"Startup bot-cache purge: removed {purged} poisoned file(s)", "INFO")

    threading.Thread(target=_purge_bot_cache, daemon=True, name="bot-cache-purge").start()

    if SCAN_PATHS:
        threading.Thread(target=_run_path_scanner, daemon=True, name="path-scanner").start()

    if CRAWL:
        def _bg_crawl() -> None:
            enqueue(SITE_URL)
            crawl_parallel()
            s = stats.snapshot()
            with _cdn_port_lock:
                cdn_map = dict(_cdn_host_port)
            log(f"Crawl done — {s['crawled']} fetched · {s['saved']} cached · "
                f"{s['cdn_fetched']} cdn · {s['captured']} captured · "
                f"{s['revealed']} revealed · {s['conn_errors']} unreachable · "
                f"{s['http_errors']} HTTP errors")
            for host, port in cdn_map.items():
                log(f"  CDN {host} → http://localhost:{port}", "CDN")
        threading.Thread(target=_bg_crawl, daemon=True, name="crawl-main").start()
        log("Crawler started in background.")

    log(f"Listening on http://{HOST}:{PORT}")

    if HOOK_GUI:
        # On Linux/X11 Tkinter MUST run on the main thread.
        # Flask moves to a daemon thread so the main thread is free for the GUI.
        def _flask_thread():
            app.run(host=HOST, port=PORT, threaded=True, debug=False, use_reloader=False, request_handler=_S2LWSGIRequestHandler)
        threading.Thread(target=_flask_thread, daemon=True, name="flask").start()
        _launch_hook_gui()   # blocks main thread — GUI event loop runs here
    else:
        app.run(host=HOST, port=PORT, threaded=True, debug=False, use_reloader=False, request_handler=_S2LWSGIRequestHandler)
