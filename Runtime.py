"""Shared process state, compatibility exports and startup orchestration."""
#!/usr/bin/env python3
# ── Silence multiprocessing.resource_tracker "leaked semaphore" warning ──
# Some indirect dependency (cffi/curl_cffi/cloudscraper/etc.) creates a named
# POSIX semaphore through the multiprocessing module without releasing it
# before our hard os._exit(0) shutdown in the SIGINT handler. The OS reaps
# the semaphore anyway; the warning is purely cosmetic.
#
# The warning is emitted by the resource_tracker *subprocess*, not this
# process, so an in-process warnings.filterwarnings() cannot suppress it.
# PYTHONWARNINGS is read by EVERY Python interpreter at startup, including
# the resource_tracker child, which inherits this env var via fork+exec.
# Setting it here, BEFORE any imports that might trigger multiprocessing,
# propagates the filter to all child processes.
import os as _os
_s2l_existing_warnings = _os.environ.get("PYTHONWARNINGS", "")
_s2l_sem_filter = "ignore::UserWarning:multiprocessing.resource_tracker"
if _s2l_sem_filter not in _s2l_existing_warnings:
    _os.environ["PYTHONWARNINGS"] = (
        (_s2l_existing_warnings + "," if _s2l_existing_warnings else "")
        + _s2l_sem_filter
    )
# ── STDLIB ────────────────────────────────────────────────────────────────────
import os
import ipaddress
import sys
import re
import json
import time
import socket
import queue
import zlib
import gc
import hashlib
import codecs
import mimetypes
import email.utils
from collections import deque
import logging
import threading
import warnings
import base64
import struct
import ssl
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace as _dc_replace
from typing import Callable
from urllib.parse import urljoin, urlparse, urldefrag
# Defensive belt-and-suspenders: if multiprocessing.resource_tracker is ever
# imported into THIS process (e.g. via a library), patch its warnings.warn so
# any future in-process emission of the "leaked semaphore" warning is muted.
try:
    import multiprocessing.resource_tracker as _s2l_rt
    if not getattr(_s2l_rt, "_s2l_warn_patched", False):
        _s2l_rt_orig_warn = _s2l_rt.warnings.warn
        def _s2l_silent_warn(message, *a, **k):
            if "leaked semaphore" in str(message):
                return
            return _s2l_rt_orig_warn(message, *a, **k)
        _s2l_rt.warnings.warn = _s2l_silent_warn
        _s2l_rt._s2l_warn_patched = True
except Exception:
    pass
# ── Third Party ───────────────────────────────────────────────────────────────
import requests
import requests.adapters
import urllib3
import urllib3.util.retry
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from flask import Flask, Response, stream_with_context, request as flask_request, redirect
from werkzeug.serving import WSGIRequestHandler, ThreadedWSGIServer, make_server
import colorama
from colorama import Fore, Style
try:
    import brotli as _brotli
    _BROTLI_OK = True
except ImportError:
    _BROTLI_OK = False
# cloudscraper: pure-Python Cloudflare bypass, used as the fallback tier when
# curl_cffi isn't available or fails. ai-cloudscraper is the maintained fork
# (the original cloudscraper is unmaintained since 2023 — last release 1.2.71,
# 2023-04-25). WHEEL REALITY (verified against ai-cloudscraper 3.8.4): the
# fork installs its module under the SAME name `cloudscraper`, so the first
# import below is a future-proofing no-op today — the second import is what
# actually binds, and it binds the fork's module when ai-cloudscraper is
# installed, the original's when only cloudscraper is. Guarded so a missing
# import doesn't crash startup — the session factory falls through to plain
# requests.
try:
    import ai_cloudscraper as cloudscraper
    _CLOUDSCRAPER_OK = True
except ImportError:
    try:
        import cloudscraper
        _CLOUDSCRAPER_OK = True
    except ImportError:
        _CLOUDSCRAPER_OK = False
# curl_cffi: real Chrome TLS fingerprinting — much more effective vs Cloudflare
# than cloudscraper (which uses Python ssl and has a detectable JA3 fingerprint).
# Install with: pip install curl-cffi --break-system-packages (not recommended on linux, use venv.)
try:
    from curl_cffi import requests as _cffi_requests
    from curl_cffi.const import CurlWsFlag as _CurlWsFlag
    _CURL_CFFI_OK = True
except ImportError:
    _cffi_requests = None
    _CurlWsFlag    = None
    _CURL_CFFI_OK  = False

# WAFW00F is optional; without it, proxying continues with an explicit status.
try:
    from wafw00f.main import WAFW00F as _WAFW00F
except ImportError:
    _WAFW00F = None

# tqdm: progress bar library used by SCAN_PATHS to show scan progress
# in-place (one updating line) instead of spamming a new log line per
# batch of paths. Optional — when missing, the scanner falls back to
# its old "log every SCANS_PER_SECOND paths" behavior so the script
# still works in minimal environments. Install with: pip install tqdm
try:
    from tqdm import tqdm as _tqdm
    _TQDM_OK = True
except ImportError:
    _tqdm = None
    _TQDM_OK = False

# Module-level reference to the active SCAN_PATHS tqdm bar, if any.
# When non-None, log() routes its output through tqdm.write() so log lines
# from ANY thread (Flask, crawler, CDN servers, keepalive, etc.) appear
# ABOVE the bar instead of corrupting it mid-line. Set by _run_path_scanner
# when it opens a bar, cleared to None when the bar closes.
_active_scan_pbar = None

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

sys.setrecursionlimit(20_000)
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RAINBOW_LOGS = False   # early default so log() works before CONFIG
_qt_shutdown_requested = threading.Event()
_s2l_qt_message_handler = None
_s2l_qt_previous_message_handler = None

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
    "WS":    Fore.BLUE    + Style.BRIGHT,    # WebSocket events
    "SSE":   Fore.MAGENTA,                    # SSE events
    "TUNNEL":Fore.MAGENTA + Style.BRIGHT,    # TCP/UDP tunnel
    # 24-bit truecolor escape (#772953 → RGB 119,41,83). colorama's Fore has no
    # arbitrary-HEX helper, so we emit the SGR sequence directly; every modern
    # terminal (and the Win10+ console) supports 24-bit color, and the existing
    # Style.RESET_ALL in log() still clears it at the end of the line.
    "CAPTURE": "\x1b[38;2;119;41;83m",       # HTTP capture written to disk
}
_CACHE_HIT_MARKER = f"{_LEVEL_COLOR['←']}[←]{Style.RESET_ALL}"

# Real lolcat detection — shutil.which() resolves through $PATH, so this works
# identically whether lolcat was installed via pacman (Arch), apt (Debian/
# Ubuntu), the ruby gem, pip, cargo, or a Termux package: whatever the shell
# would run for `lolcat` is exactly what this finds, no distro-specific path
# guessing required.
_LOLCAT_BIN: str | None       = shutil.which("lolcat")
_lolcat_mode: list | bool | None = None   # None=not probed, False=unusable, list=working argv tail
_lolcat_probe_lock            = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
from .Config import (
    SITE,
    HOST,
    PORT,
    DEVICE,
    TIMEOUT_READ,
    TIMEOUT_CONN,
    TIMEOUT_STREAM_READ,
    WORKERS,
    MAX_FNAME,
    SAVE_BATCH,
    SAVE_INTERVAL,
    CRAWL_DEPTH,
    SCAN_LIMIT,
    RETRIES,
    BACKOFF,
    CRAWL,
    OFFLINE,
    PREMAP_CDN,
    SAVE_ERRORS,
    DUMP_ALL,
    PROXY_CDN,
    CACHE_CDN,
    VERSION,
    MULTIPORT,
    HOOK_GUI,
    RAINBOW_LOGS,
    SHOW_HIDDEN,
    SCAN_PATHS,
    SCANS_PER_SECOND,
    WORDLISTS_DIR,
    CAPTURE,
    CAPTURE_CDN,
    CAPTURE_BODIES,
    CAPTURE_SKIP_STATIC,
    CAPTURE_WS,
    WEB_CDNVIEWER,
    WEB_CDNVIEWER_IP_WHITELIST,
    WS_PING_INTERVAL,
    WS_PONG_TIMEOUT,
    WS_AUTO_RECONNECT,
    WS_DEFLATE,
    WS_LOG_FRAMES,
    WS_MAX_MSG_BYTES,
    SSE_PROXY,
    SSE_HEARTBEAT,
    TCP_TUNNEL,
    UDP_TUNNEL,
    TUNNEL_TIMEOUT,
    POOL_MAXSIZE,
    POOL_CONNECTIONS,
    DEAD_HOST_TTL,
    STATS_WINDOW,
    GRACEFUL_SHUTDOWN,
    MAX_BODY_BYTES,
    MAX_DECOMPRESSED_BYTES,
    STREAM_CACHE_MAX_BYTES,
    BLOCK_PATHS,
    CDN_BLOCK,
    DUMP_TARGETS,
    DONT_REWRITE,
    parse_size
)

# Byte-size flags: human literals in Config ("512BYTE" / "5MB" / "1GB") —
# parsed to plain ints HERE, at import time, so every consumer (Flask
# MAX_CONTENT_LENGTH, WS reassembly caps, stream thresholds, banner)
# keeps seeing ints. Plain ints in Config also still work.
SCAN_LIMIT             = parse_size(SCAN_LIMIT)
WS_MAX_MSG_BYTES       = parse_size(WS_MAX_MSG_BYTES)
MAX_BODY_BYTES         = parse_size(MAX_BODY_BYTES)
MAX_DECOMPRESSED_BYTES = parse_size(MAX_DECOMPRESSED_BYTES)
STREAM_CACHE_MAX_BYTES = parse_size(STREAM_CACHE_MAX_BYTES)

_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailer", "transfer-encoding",   # RFC 7230 §4.4: "Trailer" (the
    # pre-modular monolith wrote "trailers" — the HTTP/2 frame name — so real
    # Trailer fields were never stripped; fixed in the 9.0 hop-by-hop pass)

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


def _connection_tokens(value) -> frozenset:
    """RFC 7230 §6.1 — header names listed in a Connection field are
    hop-by-hop for THAT message. Callers union the returned names into
    their skip set, so a server declaring ``Connection: x-acme-hop`` gets
    ``x-acme-hop`` stripped without any static list knowing about it —
    the protocol describes itself, nothing to maintain.

    "upgrade" is the ONE name we must keep: the WebSocket handler owns
    it (see the note in _HOP_BY_HOP). Keywords like close/keep-alive map
    to names already in the static list, making them harmless no-ops.
    Accepts str, list or tuple values (curl_cffi sometimes produces
    list-valued header fields).
    """
    if not value:
        return frozenset()
    if isinstance(value, (list, tuple)):
        value = ",".join(str(v) for v in value)
    return frozenset(
        t for t in (tok.strip().lower() for tok in str(value).split(","))
        if t and t != "upgrade"
    )


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

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})

# RSC wire format signatures (Next.js streaming protocol)
_WIRE_FORMAT_SIGNATURES: tuple = (
    b":HL[", b":H[", b":I[", b":E[", b'0:{"',
    b"<turbo-stream", b'hx-swap-oob=',
)

# Extra headers to strip from forwarded requests.
# sec-fetch-* are intentionally NOT stripped — they're needed by CF and Google
# for legitimate bot detection bypass. The _do_upstream function adds them
# from the real browser request.
# sec-ch-ua* (Client Hints) are intentionally KEPT — they help CF fingerprinting.
# x-requested-with is intentionally kept — servers use it as CSRF defense.
_STRIP_FWD_EXTRA = frozenset({
    "purpose",
})

# ──────────────────────────────────────────────────────────────────────────────
# WAFW00F detection
# ──────────────────────────────────────────────────────────────────────────────

_last_platform = {
    "name": "Skipped (offline)" if OFFLINE else "Unknown",
    "ts": 0.0,
}
_last_platform_lock = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# Hook system
# ──────────────────────────────────────────────────────────────────────────────

_REQ_HOOKS:  list[tuple[re.Pattern, frozenset, Callable]] = []
_RESP_HOOKS: list[tuple[re.Pattern, frozenset, Callable]] = []

# ═══════════════════════════════════════════════════════════════════════════════
#  YOUR HOOKS GO HERE
#
#  `pattern` matches against ctx.path ONLY (e.g. "/api/user/me"), never the
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
# @on_response(pattern=r"/api/config")
# def _patch_config(ctx: HookContext) -> None:
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
_gui_drop_last_log: list = [0.0]   # [last_drop_ts] — throttled WARN throttle

# Active GUI hooks  (list of dicts, mutated only on GUI thread)
_gui_hooks: list[dict] = []   # {name, method, status, pattern, body_bytes, enabled: bool}
_gui_hooks_lock = threading.Lock()

# ── WebSocket GUI hooks ──────────────────────────────────────────────────────
# Captured WS messages feed the GUI traffic log; enabled hooks replace the
# payload of matching messages on the fly. The frame pump calls
# _apply_gui_ws_hooks() once per reassembled TEXT/BINARY message in each
# direction, and _gui_ws_push() once for inspection. Both are no-ops when
# HOOK_GUI is off, so the pump stays cheap when the inspector is closed.
#
# INSTANCED WS HOOKS:
#   Each hook carries a `match_mode` so it only fires for the payload the user
#   actually captured (instead of blanket-overriding every message on the same
#   path). The fields are:
#
#     match_mode   : "any" | "exact" | "regex" | "contains"
#         "any"      — override every matching message on the path
#         "exact"    — override ONLY when the incoming payload equals
#                      match_payload byte-for-byte
#         "regex"    — override ONLY when the incoming payload (decoded as
#                      UTF-8 with errors ignored) matches the match_payload
#                      regex
#         "contains" — override ONLY when match_payload appears as a
#                      substring of the incoming payload
#     match_payload: bytes
#         For "exact"/"contains" — raw bytes to compare against.
#         For "regex"            — UTF-8 string compiled as a regex.
#
#   Hooks created from a captured message default to "exact" with that
#   message's payload, so the user's intent ("replace THIS response") is
#   honored precisely instead of poisoning every other response on the same
#   path. Hooks with match_mode="any" still work as before for users who
#   genuinely want a blanket override.
_gui_ws_log_queue: queue.Queue = queue.Queue(maxsize=2000)
_gui_ws_drop_last_log: list = [0.0]
_gui_ws_hooks: list[dict] = []   # {name, direction, opcode, pattern, origin_url,
                                  #  body_bytes, enabled, match_mode, match_payload}
_gui_ws_hooks_lock = threading.Lock()

# Hard cap on how much of a captured body the hook_gui Body Editor will load
# into its QPlainTextEdit. A QPlainTextEdit document scales O(n) for layout and
# memory, so loading a 100 MB response into the editor both freezes the UI
# (multi-second blocking layout pass) and makes the editor's sizeHint explode,
# which in turn forces the surrounding QSplitter to grow the editor pane until
# it swallows the Traffic Log. Capping at 512 KB keeps the editor instant and
# pins the splitter to its configured sizes; the full body is still available
# on disk via the capture files.
_EDITOR_MAX_BYTES = parse_size("512KB")

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

# curl_cffi impersonation target per device — keeps the TLS ClientHello
# fingerprint consistent with the User-Agent we actually send. Sending an
# iPhone/Android UA over a desktop-Chrome TLS fingerprint (or vice versa) is
# exactly the kind of cross-signal mismatch Cloudflare Bot Management looks
# for. Each entry is an ordered fallback list because impersonation target
# names vary between curl_cffi versions/builds — the first one the installed
# build actually recognises wins, and that choice is cached per device so we
# only ever pay for the trial-and-error once per process.
_IMPERSONATE_BY_DEVICE: dict[str, tuple[str, ...]] = {
    "mobile":    ("chrome131_android", "chrome124_android", "chrome123_android", "chrome99_android", "chrome136"),
    "tablet":    ("chrome131_android", "chrome124_android", "chrome123_android", "chrome99_android", "chrome136"),
    "iphone":    ("safari18_0", "safari17_2_ios", "safari17_0", "safari15_5", "chrome136"),
    "ipad":      ("safari18_0", "safari17_2_ios", "safari17_0", "safari15_5", "chrome136"),
    "macintosh": ("chrome136",),
    "desktop":   ("chrome136",),
    "ie11":      ("chrome136",),   # no realistic modern impersonation target — best effort
    "symbian":   ("chrome136",),
    "bot":       ("chrome136",),
}
_cffi_impersonate_cache: dict[str, str] = {}

# Minimal block responses some edges/WAFs return as plain text with no HTML
# wrapper at all ("blocked", "Access Denied", "Error 1020", ...). These are
# short enough to fall under the normal 64-byte floor AND lack <html>, so the
# regular bot-page heuristics never see them — they'd otherwise sail through
# and get served/cached as if they were real content (a blank-looking page
# that just says "blocked").
# Generic WAF/block text signatures. Removed CF-specific error numbers and
# "cloudflare"/"cf-ray" markers — those false-positive on short API responses
# that happen to echo the CF ray id. Removed "rate limit"/"too many requests"
# — those are legitimate API signals, not WAF blocks, and treating them as
# blocks causes a retry cascade that starves the SPA of data.
_RAW_BLOCK_TEXT: tuple[bytes, ...] = (
    b"blocked", b"access denied", b"request blocked",
    b"please verify you are a human",
    b"prove you're not a robot",
)

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
)

# Per-client sessions: each unique browser (identified by IP) gets its own
# session so cookies don't bleed between clients connected to the same S2L.
_CLIENT_SESSIONS:    dict[str, object] = {}
_CLIENT_LAST:        dict[str, float]  = {}
_CLIENT_LOCK  = threading.Lock()
_CLIENT_TTL   = 1800  # 30 min idle before session is evicted

# ──────────────────────────────────────────────────────────────────────────────
# Body decompression  (fixes blank-page bug when upstream sends gzip/br)
# ──────────────────────────────────────────────────────────────────────────────

# ── Dynamic MIME detection ────────────────────────────────────────────────
# We rely on Python's stdlib `mimetypes.guess_type()` (which reads
# /etc/mime.types at import time) supplemented by the actual upstream
# `Content-Type` header captured on the first fetch. No static frozenset
# of "known" content types is maintained — anything not recognized by
# guess_type() falls through to `application/octet-stream`, which is the
# safe binary-safe default for proxying.
#
# Subresource classification (for log suppression) is now derived from
# the content-type itself: anything starting with image/, audio/, video/,
# font/, or known application/wasm/application/octet-stream is treated
# as a subresource. This matches what the old _SUBRESOURCE_CTS covered,
# without maintaining a hardcoded list.

# ──────────────────────────────────────────────────────────────────────────────
# Header helpers
# ──────────────────────────────────────────────────────────────────────────────

# Concrete expose list used for credentialed CORS responses. The CORS spec
# forbids the wildcard "*" for Access-Control-Expose-Headers when the request
# carries credentials (cookies / Authorization) — browsers silently strip the
# headers listed here (Authorization, Content-Encoding, WWW-Authenticate, …)
# from the response, which manifests as opaque "CORS error" failures on cross-
# origin (MULTIPORT CDN port) fetches that read those headers. The concrete
# list echoes what filter_resp() already sets inline; use _apply_cors_headers()
# in any cache-hit / manually-built response path so the same list survives.
_CORS_CONCRETE_EXPOSE = (
    "Content-Type, Content-Length, Content-Range, Content-Disposition, "
    "Date, ETag, Last-Modified, Location, Range, Set-Cookie, "
    "Content-Encoding, Vary, WWW-Authenticate, Authorization"
)

# ──────────────────────────────────────────────────────────────────────────────
# General utils
# ──────────────────────────────────────────────────────────────────────────────

# ── Client-aware proxy addressing ────────────────────────────────────────────
# Rewrites used to hardcode "http://localhost:PORT". That only works on the
# machine running S2L: on a phone/tablet browsing via the LAN IP, every
# rewritten URL pointed at the DEVICE's own localhost → ERR_CONNECTION_REFUSED
# → HTML5 games/apps never loaded on remote devices (and origins/Referer
# checks failed for the same reason). Deriving the base from the request's
# Host header keeps every rewrite reachable from ANY client.

_NETLOC_RE       = re.compile(r"^[A-Za-z0-9._\-]+:\d{1,5}$|^[A-Za-z0-9._\-]+$")
_NETLOC_IPV6_RE  = re.compile(r"^\[[0-9A-Fa-f:.]+\](?::\d{1,5})?$")

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

_s2l_js_lock = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────────────

# Reserved folder, one per host, that holds EVERY cache file whose URL carries
# a query string. The host tree outside it mirrors the real site's paths 1:1;
# query variants live in here so they never mix with real files.
_QUERY_DIR = "__query__"

# Characters written verbatim into a query filename. Everything else is
# percent-encoded. ``.`` is handled separately (see below).
_QS_SAFE_RE = re.compile(r"[A-Za-z0-9_\-=&,+~@]")
_QS_PCT_RE  = re.compile(r"%[0-9A-Fa-f]{2}")

# Hard ceiling for one on-disk name (bytes == chars: names are pure ASCII).
# Most filesystems allow 255; 240 leaves room for the device suffix.
_FS_NAME_MAX = 240

# ──────────────────────────────────────────────────────────────────────────────
# Site init
#
# Computed at the END of this module, after the implementation imports, so the
# expression can be exactly the monolith's (`build_base_url(SITE) or ...`).
# Nothing between here and there reads these values at import time; functions
# defined in between resolve them lazily at call time.
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Hidden path scanner  (SCAN_PATHS)
#
# Three modes, all driven by wordlist FILES under wordlists/:
#   "all"        — every wordlist file found under wordlists/ (recursive).
#                  Each file runs to completion before the next starts.
#   "all-in-dir" — every wordlist file in wordlists/ top-level only (non-recursive).
#   "dir/file"   — one specific wordlist, path relative to wordlists/.
#                  e.g. "common/admin.txt" → wordlists/common/admin.txt
#                       "admin.txt"        → wordlists/admin.txt
#
# Wordlist format: one path per line.  Blank lines and lines starting with '#'
# are ignored.  Leading '/' is stripped automatically.
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Status-code filter for SCAN_PATHS
#
# Two modes, set interactively at scan start (see _prompt_scan_status_filter):
#   _SCAN_BLOCK_STATUSES : set[int]  — codes to SUPPRESS from the log
#   _SCAN_ONLY_STATUS    : int | None — if set, ONLY this code is logged
#
# When _SCAN_ONLY_STATUS is set it wins (block-list is ignored). Both default
# to "nothing filtered" so pressing Enter at the prompt leaves the log unfiltered.
# ──────────────────────────────────────────────────────────────────────────────
_SCAN_BLOCK_STATUSES: set[int] = set()
_SCAN_ONLY_STATUS:    int | None = None

# The status-code universe we present in the filter prompt. These are the
# codes a path scanner is realistically going to encounter — every code here
# is one the user might want to either block or pin as the only one to show.
# Codes NOT in this list still work — the user can type any number — this is
# just the "we have this list of status" display the prompt needs.
_SCAN_KNOWN_STATUSES: tuple[int, ...] = (
    # 2xx — success
    200, 201, 202, 203, 204, 206,
    # 3xx — redirection
    301, 302, 303, 304, 307, 308,
    # 4xx — client errors
    400, 401, 403, 404, 405, 406, 408, 409, 410, 411, 413, 415, 418, 422, 429,
    # 5xx — server errors
    500, 501, 502, 503, 504, 511,
)

# ──────────────────────────────────────────────────────────────────────────────
# Domain policy
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Stats
# ──────────────────────────────────────────────────────────────────────────────

# Process start time — used by the web viewer to show uptime. Set here so it
# reflects when the module finished importing (≈ when the server is about to
# listen), not when main() is entered.
_START_TS = time.time()

# ──────────────────────────────────────────────────────────────────────────────
# Live WAF/platform + per-CDN-host health registries (for the web viewer)
#
# _last_platform: the WAFW00F result from startup, exposed to the viewer.
#
# _cdn_health: per-CDN-host lightweight health record:
#   {host: {"last_seen": epoch, "requests": int, "errors": int,
#           "last_status": int, "last_rtt_ms": int}}
# Updated from the CDN mini-server + ext_asset route on every request so the
# viewer can show a green/red health dot + last-seen relative time + RTT per
# host. Bounded: entries are pruned when a host leaves _cdn_host_port.
# ──────────────────────────────────────────────────────────────────────────────
_cdn_health: dict[str, dict] = {}
_cdn_health_lock = threading.Lock()

# Max age (seconds) a CDN health record may stay without a fresh request before
# it's eligible for pruning. Long enough that a quiet-but-valid CDN host isn't
# dropped, short enough that transient hosts (one-off asset fetches) don't
# accumulate forever in long-running sessions.
_CDN_HEALTH_MAX_AGE = 600   # 10 minutes

_health_pruner_started = False

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

# ──────────────────────────────────────────────────────────────────────────────
# Queues & shared state  (MUST be defined before _save_worker thread starts)
# ──────────────────────────────────────────────────────────────────────────────

visited:        set = set()
# Insertion order companion to `visited` (both mutated under visited_lock).
# The eviction in Jobs._cap_visited() pops from the LEFT — a plain set has
# hash order, so evicting "the oldest half" from the set alone dropped
# ~random URLs and caused duplicate re-fetch storms past _VISITED_MAX.
_visited_order: deque = deque()
_VISITED_MAX = 100000  # cap to prevent unbounded memory growth
# Dedup keys of the save worker: (url, device-variant) tuples of bodies
# already batched/written. NOT paths — the on-disk path of a URL only
# becomes final when the flush resolves the .q collision guard, so a
# path-keyed dedup let same-batch colliding queries clobber each other.
saved_keys:     set = set()
_SAVED_KEYS_MAX = 50000  # cap to prevent unbounded memory growth
# sha1(body) → FIRST on-disk path that received those bytes. Used by the
# save worker to materialize duplicate-content URLs as HARDLINKS instead of
# skipping them (see _save_worker) — zero extra disk, and every fetched URL
# still gets its own cache path (variants + OFFLINE correctness).
content_hashes: dict = {}
_CONTENT_HASHES_MAX = 50000  # cap to prevent unbounded memory growth

visited_lock = threading.Lock()
save_lock    = threading.Lock()
content_lock = threading.Lock()

url_queue = queue.Queue()

# Domains to skip in background CDN pre-fetch (background session lacks user
# cookies/interactive tokens, so any auth- or challenge-gated host always
# fails there). No site is named here on purpose: `_dead_hosts` below learns
# this dynamically the first time a host returns a detected bot/challenge
# page (see _is_bot_page) and skips it for the rest of the run — that covers
# every CAPTCHA/consent-gate vendor generically instead of a fixed list of
# names. Only user-configured CDN_BLOCK entries are excluded up front.
_NO_BG_FETCH_DOMAINS: frozenset = frozenset(CDN_BLOCK)

# Dead-host tracking with TTL pruning. A host marked dead is skipped for
# DEAD_HOST_TTL seconds (default 60s), after which it's retried — this lets
# transient failures (DNS hiccup, rate-limit, CF block) recover without
# permanently blocking a host for the entire run.
_dead_hosts:      dict[str, float] = {}   # netloc → expiry timestamp
_dead_hosts_lock: threading.Lock = threading.Lock()

_crawl_done     = threading.Event()   # set when initial crawl finishes
_cdn_thread_sem = threading.Semaphore(32)  # cap concurrent CDN fetch threads

# Regex for extracting URLs from arbitrary response bodies (DUMP_ALL mode).
# Three alternatives, each with guards tuned to minimise false positives:
#
#  1. Explicit scheme:// — http(s)://, ws(s)://, ftp://, file://. The host
#     char class stops at the first character that can't legally appear in
#     a hostname, then the path/query portion is allowed the full RFC 3986
#     sub-delims + pchar set. ws/wss support is new — modern SPAs route
#     realtime traffic over WebSocket and DUMP_ALL needs to see those URLs
#     so the WS handler can pre-register the upstream host.
#
#  2. Protocol-relative //host/path — only matched right after a context
#     character that genuinely precedes a URL in real code: quotes, parens,
#     `=`, `:`, backtick (template literals), or comma (JSON arrays /
#     multi-attribute HTML). This is how these are actually written
#     (src="//cdn...", url(//fonts...), fetch(`//api...`), ["//a.com","//b.com"]).
#     Without that guard, a bare "// comment" in JS/CSS matches just as easily.
#
#  3. Bare root-relative /path — only matched when NOT a continuation of a
#     longer token (so "foo/bar/baz" inside a string doesn't yield a spurious
#     "/bar/baz"). Requires at least 2 chars to avoid matching "/" alone.
#     The path char class adds @, ~, %, &, ?, =, # so query strings and
#     fragment-bearing links inside JS bundles are caught too.
#
# Trailing punctuation (.,;:!?)]}>'"`\) that the greedy path class hoovers up
# is trimmed post-match by _clean_url_match() — doing it in-regex would require
# variable-length lookbehinds which Python's re doesn't support.
_URL_HOST_CHARS  = rb"[a-zA-Z0-9\-._:]+"          # host + port
_URL_PATH_CHARS  = rb"[a-zA-Z0-9\-._~:/?#@!$&'*+,;=%()"  # path + query
_URL_TRAILING    = frozenset(b".,;:!?)\"'<>]}\\`")

# Schemes we recognize. `wss?` covers ws:// and wss:// (realtime APIs).
# `ftp`/`file` are rare in practice but cost nothing to include.
_URL_SCHEME_PREFIX = rb"(?:https?|wss?|ftp|file)://"

URL_REGEX = re.compile(
    # 1. Explicit scheme:// — extended to ws/wss/ftp/file in addition to http(s)
    _URL_SCHEME_PREFIX + _URL_HOST_CHARS + _URL_PATH_CHARS + rb"]*"
    # 2. Protocol-relative //host/path — backtick and comma added to the
    #    lookbehind so template literals and JSON arrays of CDN URLs are caught.
    rb"|(?<=[\"'(=:` ,])//" + _URL_HOST_CHARS + _URL_PATH_CHARS + rb"]*"
    # 3. Root-relative /path — expanded path char class with @ ~ % & ? = #
    #    so query/fragment-bearing in-page links inside JS bundles match.
    rb"|(?<![a-zA-Z0-9_/])/(?!/)[a-zA-Z0-9_\-/.@~%&?=#]{2,}"
)

# HTML tags that represent external sub-resources worth fetching
_ASSET_TAGS = frozenset({"script", "img", "link", "source", "video", "audio"})

# save_queue must be defined before _save_worker starts
save_queue: queue.Queue = queue.Queue()

# Temp suffix for atomic cache writes (temp + os.replace, see
# Crawler/Crawl/Save.py). The startup sweep and the bot-cache purge skip
# files with this suffix; a crash mid-write leaves at most one inert temp
# per path, which the next write of that path overwrites.
_CACHE_TMP_SUFFIX = ".s2l-tmp"

# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Capture system  (CAPTURE)
# ──────────────────────────────────────────────────────────────────────────────

_capture_queue: queue.Queue = queue.Queue()

# ──────────────────────────────────────────────────────────────────────────────
# WebSocket capture system  (CAPTURE_WS)
#
# Mirrors the HTTP capture pipeline: a bounded queue + a single writer thread
# that persists every reassembled TEXT/BINARY frame (in both directions) as a
# JSON document under site_data/<site>/captures_ws/. Each tunnel gets its own
# subdirectory keyed by tunnel id so messages from independent sockets don't
# interleave in one file. Binary payloads are base64-encoded; text payloads
# are stored as UTF-8 (errors replaced) with an encoding marker, exactly like
# the HTTP capture body encoder.
# ──────────────────────────────────────────────────────────────────────────────

_ws_capture_queue: queue.Queue = queue.Queue(maxsize=5000)

# ──────────────────────────────────────────────────────────────────────────────
# Crawler
# ──────────────────────────────────────────────────────────────────────────────

# Persistent background discovery workers — stay alive AFTER the initial crawl
# so URLs discovered by the proxy handler (link extraction from served pages)
# continue to be crawled. Without this, the first browser navigation to a page
# that wasn't in the initial crawl would serve the page live but never pre-cache
# its sub-resources, forcing the user to reload to get a fully-cached page.
_DISCOVERY_WORKERS = max(2, min(8, WORKERS // 4))
_discovery_started  = threading.Event()

# Bounded thread pool for external asset prefetch — prevents thread exhaustion
# when a page contains hundreds of external links. Each _fetch_external_asset
# call used to spawn its own thread; with large pages this hit the OS thread
# limit ("can't start new thread"). Now we reuse a small pool of workers.
_ext_asset_queue: queue.Queue = queue.Queue(maxsize=20000)
_ext_asset_started = threading.Event()

# ──────────────────────────────────────────────────────────────────────────────
# Streaming helper  (large binary responses — video, WASM, big downloads)
# ──────────────────────────────────────────────────────────────────────────────

# Minimum Content-Length (bytes) that triggers streaming even for non-binary CTs.
# Anything smaller is buffered so hooks and URL rewriting still apply.
_STREAM_MIN_BYTES = parse_size("5MB")  # below this, buffer instead of stream

# Content-Type prefixes that should always stream (audio/video/wasm/binary
# archives). Derived dynamically from the Content-Type header instead of a
# hardcoded frozenset — no maintenance burden when a new format appears.
_STREAM_PREFIXES = ("video/", "audio/", "application/wasm",
                    "application/zip", "application/x-tar",
                    "application/gzip", "application/x-bzip2",
                    "application/x-7z-compressed", "application/x-rar")

# Maximum raw (compressed) body size we'll accumulate in RAM for disk caching
# during streaming. Bodies larger than this are streamed to the browser but
# NOT cached to disk (avoids OOM on huge video/download streams).
# Defined publicly as STREAM_CACHE_MAX_BYTES in the Layer 2 CONFIG block above;
# this alias keeps the internal references stable without forcing a global rename.
_STREAM_CACHE_MAX_BYTES = STREAM_CACHE_MAX_BYTES

# ──────────────────────────────────────────────────────────────────────────────
# WebSocket proxy  (real-time messaging apps, gateways with subprotocols)
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# WebSocket v2 — robust extensions (RFC 6455 + RFC 7692 permessage-deflate)
#
# Additive layer on top of the existing _ws_read_frame / _ws_make_frame
# primitives. Provides:
#   • Fast C-level frame masking/unmasking (10-50× faster than per-byte Python)
#   • permessage-deflate negotiation + compression/decompression
#   • Ping/pong keepalive thread with configurable interval + pong timeout
#   • Auto-reconnect: transparently re-establish dropped upstream WS connections
#   • Frame logging (verbose mode for debugging)
#   • Message hooks: on_ws_message(direction, opcode, payload) per tunnel
# ──────────────────────────────────────────────────────────────────────────────

# WebSocket opcodes (RFC 6455 §5.2)
_WS_OPCODE_CONTINUATION = 0x0
_WS_OPCODE_TEXT         = 0x1
_WS_OPCODE_BINARY       = 0x2
_WS_OPCODE_CLOSE        = 0x8
_WS_OPCODE_PING         = 0x9
_WS_OPCODE_PONG         = 0xA

# Display names for the GUI traffic log + hook log lines.
_WS_OP_NAMES: dict[int, str] = {
    _WS_OPCODE_CONTINUATION: "cont",
    _WS_OPCODE_TEXT:         "text",
    _WS_OPCODE_BINARY:       "bin",
    _WS_OPCODE_CLOSE:        "close",
    _WS_OPCODE_PING:         "ping",
    _WS_OPCODE_PONG:         "pong",
}

# Per-message-deflate window bits — 15 is the RFC default and matches what
# every browser negotiates. Going lower saves a little memory at the cost of
# worse compression; going higher is non-standard and most servers reject it.
_WS_PMDE_WINDOW_BITS = 15
_WS_PMDE_MEM_LEVEL   = 8

# zstandard: used by decompress_body() for HTTP responses with
# Content-Encoding: zstd. Imported here (top-level) so it's available
# everywhere — the decompress_body and streaming paths both reference it.
try:
    import zstandard as _zstd
    _ZSTD_OK = True
except ImportError:
    _ZSTD_OK = False

# Per-tunnel deflate contexts. We keep separate inflate/deflate objects
# per tunnel because RFC 7692 mandates that context persists across messages
# within the same connection. Keyed by id(tunnel) so cleanup is automatic
# when the tunnel object is GC'd.
_ws_inflate_ctx:  dict = {}
_ws_deflate_ctx:  dict = {}
_ws_ctx_lock = threading.Lock()

# Message hook registry — called for every WS message in either direction.
# Signature: fn(direction: str, opcode: int, payload: bytes, tunnel_id: int)
# direction is "in" (browser→server) or "out" (server→browser)
_WS_MSG_HOOKS: list[Callable] = []

# NOTE: the old v1 `_pump_ws_frames` has been removed. The WS pipeline now
# exclusively uses `_pump_ws_frames_v2` (above) which is a strict superset —
# same frame relay, plus optional keepalive, permessage-deflate, frame
# logging, GUI hooks, and capture. When all those features are disabled
# (the defaults), v2 behaves identically to what v1 did, so there is no
# functional regression — only one code path to maintain.

# ──────────────────────────────────────────────────────────────────────────────
# Server-Sent Events proxy (text/event-stream)
#
# SSE is unidirectional (server→browser) over a long-lived HTTP connection.
# The standard _stream_resp path works but has two problems for SSE:
#   1. Werkzeug's iter_content chunks arbitrarily across event boundaries,
#      so a browser's EventSource may receive half an event per chunk and
#      block waiting for the rest. We re-buffer to whole-event boundaries.
#   2. Many SSE endpoints go quiet for minutes between events; intermediate
#      proxies/CDNs may close idle connections. (Keepalive injection is
#      currently a pass-through — the stream is forwarded unchanged.)
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Raw TCP/UDP tunneling via WebSocket transport
#
# Browsers cannot open raw TCP/UDP sockets — the closest they get is
# WebSocket. So we expose a WebSocket endpoint that transparently bridges
# to a raw TCP (or UDP) connection to the real upstream host:port.
#
# Route:  /__s2l_tcp__/<host:port>          (TCP)
# Route:  /__s2l_udp__/<host:port>          (UDP)
#
# The browser opens:  new WebSocket("ws://localhost:PORT/__s2l_tcp__/server.example.com:43594")
# Each WS message (binary or text) is forwarded as a raw TCP segment.
# Each TCP segment received is framed back as a WS binary message.
#
# This is enough for most browser apps / custom-protocol apps that need
# a persistent socket. It does NOT support UDP multicast or broadcast.
# ──────────────────────────────────────────────────────────────────────────────

# Active tunnels — for stats and graceful shutdown
_active_tunnels: dict = {}
_active_tunnels_lock = threading.Lock()

# ──────────────────────────────────────────────────────────────────────────────
# Flask proxy app
# ──────────────────────────────────────────────────────────────────────────────

# Bounded thread pool for ALL WSGI request processing (main app + every CDN
# mini-server). Replaces Werkzeug's default ThreadingMixIn which creates a
# new OS thread per request — with 60 crawl workers + 10+ CDN servers + browser
# requests, that exceeded the OS thread limit ("can't start new thread").
# The pool caps total concurrent request threads at MAX_HTTP_THREADS; excess
# requests queue instead of crashing.
MAX_HTTP_THREADS = 128
_http_pool = ThreadPoolExecutor(max_workers=MAX_HTTP_THREADS, thread_name_prefix="http")
_link_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="link-ext")

# OOM FIX: no MAX_CONTENT_LENGTH meant a multi-hundred-MB upload (or a few
# concurrent ones — the server is a thread pool) was fully buffered in RAM
# via get_data(). Bound it; raise the config for unusual legit upload sizes.

_BODY_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
_SAFE_METHODS = frozenset({"GET", "HEAD"})
_ALL_METHODS  = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]

# Concrete allow-list used for credentialed CORS preflights. The CORS spec
# forbids the wildcard "*" for Access-Control-Allow-Headers /
# Access-Control-Expose-Headers when the request carries credentials
# (cookies / Authorization). Browsers silently reject credentialed preflights
# that answer with "*", which manifests as opaque "CORS error" failures on
# cross-origin (MULTIPORT CDN port) fetches with credentials:"include".
# Echoing the client's Access-Control-Request-Headers is allowed, but some
# browsers reject unknown tokens — so we merge it with a safe baseline.
_CORS_BASE_ALLOW_HEADERS = (
    "Accept, Authorization, Content-Type, X-Requested-With, X-CSRF-Token, "
    "X-CSRFToken, X-XSRF-Token, Origin, Referer, User-Agent, DNT, "
    "Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site, Sec-Fetch-User, "
    "Accept-Language, Accept-Encoding, Range, If-Range, Cache-Control, Pragma"
)

_SW_PATHS = frozenset({
    "/service-worker.js", "/sw.js", "/serviceworker.js",
})

# Minimal no-op service worker. Does NOT delete caches — SPAs and real-time
# apps rely on their SW caches for app-shell/bundle loading. Wiping them on
# every activate forces a full re-fetch of every chunk, which on flaky networks
# hangs the SPA init indefinitely (blank page). The no-op SW just claims clients
# and passes fetch events through without interception, so the SPA's own fetch
# patches (injected by _inject_sw_clear) handle URL rewriting.
#
# The SW also patches self.importScripts so that relative URLs (e.g.
# "./next/static/chunks/5376.js") are resolved against the proxy origin
# (http://localhost:PORT/) instead of the SW's own scope URL. Without this,
# webpack chunk loading inside a SW throws "The URL '...' is invalid" because
# the SW scope is http://localhost:PORT/sw.js and relative URLs resolve
# against /sw.js — which works for /next/... but NOT for ./next/... when the
# SW was registered from a sub-path. The patch normalizes all relative URLs
# to absolute proxy-relative URLs before calling the original importScripts.
_SW_NOOP = (
    b"/* S2L no-op SW: claims clients, does not intercept fetch or delete caches */\n"
    b"self.addEventListener('install', e => e.waitUntil(self.skipWaiting()));\n"
    b"self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));\n"
    b"self.addEventListener('fetch', () => {});\n"  # don't intercept — pass through
    # Patch importScripts to resolve relative URLs against the proxy origin.
    b"try{\n"
    b"  var _origIS=self.importScripts;\n"
    b"  if(typeof _origIS==='function'){\n"
    b"    var _px=self.location.origin;\n"
    b"    self.importScripts=function(){\n"
    b"      var args=Array.prototype.slice.call(arguments).map(function(u){\n"
    b"        if(typeof u!=='string')return u;\n"
    b"        if(/^(?:https?|wss?|blob|data|file|ftp):/i.test(u))return u;\n"
    b"        try{return new URL(u,_px+'/').href;}catch(e){\n"
    b"          var cu=u.replace(/^(?:\\.\\.?\\/)+/,'');\n"
    b"          return _px+'/'+cu;\n"
    b"        }\n"
    b"      });\n"
    b"      return _origIS.apply(self,args);\n"
    b"    };\n"
    b"  }\n"
    b"}catch(e){}\n"
)

# ── /__s2l_ext__/<host>/<path>  —  CDN fallback (MULTIPORT=False) ─────

# ──────────────────────────────────────────────────────────────────────────────
# Web viewer  (/__s2l_viewer__)
#
# Replaces the old terminal "press 1" MULTIPORT viewer. A self-contained HTML
# page that polls /__s2l_viewer_data__ every 50 ms (vs the terminal viewer's
# 2 s) and re-renders the live CDN host→port table with surgical DOM diffs so
# the page never freezes, even with hundreds of hosts. Polling uses recursive
# setTimeout (NOT setInterval) so a slow fetch can never stack requests; the
# next poll is only scheduled after the previous render completes.
#
# Both viewer routes handle OPTIONS explicitly so a cross-origin preflight
# (e.g. an embedding dashboard on a different origin) gets a proper CORS
# response with the same concrete Allow-Headers list used everywhere else.
# ──────────────────────────────────────────────────────────────────────────────

_S2L_VIEWER_REFRESH_MS = 50

# ── Reset-stats endpoint ─────────────────────────────────────────────────────
# Zeros every counter + clears the rolling-window event log so a developer
# can start a clean benchmark window from the viewer's "Reset" button without
# restarting the server. POST-only (mutating action); OPTIONS for CORS.

# ── Main viewer data + page ──────────────────────────────────────────────────

# ── /__s2l_ws_ext__/<host:port>/<path>  —  WebSocket proxy to arbitrary external hosts ──
# ── /__s2l_tcp__/<host:port>             —  raw TCP tunnel via WebSocket transport ──
# ── /__s2l_udp__/<host:port>             —  raw UDP tunnel via WebSocket transport ──
#
# The S2L JS injector rewrites:
#     new WebSocket("wss://ws.example.com:8443/ws?token=xxx")
# to:
#     new WebSocket("ws://localhost:PORT/__s2l_ws_ext__/ws.example.com:8443/ws?token=xxx")
#
# A real WS upgrade to any of these three paths is intercepted and fully
# handled at the socket layer by _S2LWSGIRequestHandler._handle_ws_direct()
# / _handle_tunnel_direct() BEFORE Flask ever runs — see that class. These
# three Flask views only ever get called when the request did NOT carry a
# WS Upgrade header (e.g. someone opened the URL directly in a browser
# tab), so their entire job is to explain that.

# ── Main proxy ────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Startup banner
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────


# Assemble implementations after all shared state has been initialized.
# These explicit re-exports also preserve the historical `import s2l` API.
from .Core.Cache import (
    _safe_seg,
    _encode_query_for_filename,
    _beautify_query_name,
    _cap_query_fname,
    _query_sidecar_path,
    _query_sidecar_owner,
    _write_query_sidecar,
    _purge_cached_file,
    _sweep_legacy_cache,
    local_path,
)
from .Capture.HTTP import (
    _CaptureRecord,
    _capture_path,
    _encode_body,
    _capture_worker,
    _maybe_capture,
)
from .Capture.WebSocket import (
    _WSCaptureRecord,
    _ws_capture_dir,
    _ws_capture_worker,
    _maybe_capture_ws,
)
from .CDN.Registry import (
    _real_origin_for,
    _register_cdn_host,
    _preregister_cdn_hosts_from_disk,
    _proxy_target,
)
from .CDN.Server import (
    _start_cdn_server,
)
from .CLI.Banner import (
    _banner,
)
from .Core.Compression import (
    decompress_body,
    _decompress_body_impl,
)
from .Crawler.Crawl import (
    _mark_host_dead,
    _is_host_dead,
    _clean_url_match,
    _fetch_external_asset,
    _save_worker,
    enqueue,
    _crawl,
    _crawl_worker,
    _start_ext_asset_workers,
    _ext_asset_worker,
    _discovery_worker,
    _start_discovery_workers,
    _extract_links_async,
    crawl_parallel,
)
from .Hooks.GUI import (
    _launch_hook_gui,
)
from .HTTP.Headers import (
    filter_fwd,
    _all_set_cookies,
    _resp_headers_dict,
    _flatten_cookiejar,
    _needs_isolation,
    _cors_origin_pair,
    _pna_allow_header,
    _ci_header_keys,
    _ci_get_header,
    _ci_remove_header,
    _apply_corp_header,
    _apply_cors_headers,
    _apply_security_headers,
    _is_worker_request,
    _is_document_request,
    _is_document_ct,
    _coep_value,
    _apply_isolation_headers,
    filter_resp,
    _finalize_security_headers,
    inject_csrf_headers,
    rewrite_origin,
)
from .Hooks.Hooks import (
    HookContext,
    _norm_methods,
    on_request,
    on_response,
    _run_hooks,
    _gui_ws_push,
    _apply_gui_ws_hooks,
    _gui_push,
    _gui_push_raw,
    _origin_matches,
    _apply_gui_hooks,
    _make_hook_store,
    _editor_preview,
)
from .Core.Logging import (
    _lolcat_internal,
    _probe_lolcat,
    _lolcat_real,
    log,
)
from .Core.MIME import (
    guess_mime,
    _ensure_wasm_content_type,
    _ct_base,
    _resolve_mime_with_sniff,
    is_static_asset,
    is_subresource,
)
from .Proxy.Upstream import (
    _main_security_headers,
    _normalize_request_origin,
    _cors_allow_headers,
    _upstream_url,
    _build_ctx,
    _is_sw_path,
    _do_upstream,
    _make_flask_resp,
)
from .Proxy.Cache import (
    _parse_range_header,
    _disk_validators,
    _conditional_304,
    _apply_freshness,
    _serve_range_from_disk,
    _serve_cached,
    _cached_response,
)
from .Proxy.Assets import (
    ext_asset,
)
from .Proxy.Routes import (
    proxy,
)
from .Proxy.TunnelRoutes import (
    ws_ext,
    tcp_ext,
    udp_ext,
)
from .Rewriting.Injector import (
    _build_s2l_injector,
)
from .Rewriting.HTML import (
    _reveal_hidden,
    _inject_sw_clear,
    _is_dont_rewrite_host,
    _rewrite_ext_urls,
)
from .Rewriting.JSON import (
    _rewrite_json_urls,
)
from .Crawler.Scanner import (
    _load_wordlist,
    _resolve_wordlists,
    _scan_paths_summary,
    _parse_status_csv,
    _prompt_scan_status_filter,
    _run_path_scanner,
)
from .Network.Server import (
    _PooledWSGIServer,
    _S2LWSGIRequestHandler,
)
from .HTTP.Sessions import (
    _sanitize_ua,
    _detect_device_from_ua,
    _effective_device,
    _device_variant,
    _next_cf_config,
    _make_cffi_session,
    _make_session,
    _make_cloudscraper_session,
    _make_requests_session,
    _is_cf_block,
    _is_raw_block_text,
    _is_bot_page,
    _get_proxy_session,
    _client_id,
    _get_client_session,
    _short_exc,
)
from .Core.Stats import (
    _Stats,
    record_cdn_health,
    snapshot_cdn_health,
    prune_cdn_health,
    _start_health_pruner,
)
from .HTTP.Streaming import (
    _should_stream,
    _stream_resp,
    _is_sse_response,
    _sse_iter,
    _sse_stream_generator,
    _stream_sse_resp,
)
from .Network.Tunnels import (
    _pump_tcp_over_ws,
)
from .Core.URL import (
    _raw_path_after,
    _is_ip_literal,
    _is_ipv6_literal,
    _is_wire_payload,
    _is_valid_html,
    set_last_platform,
    get_last_platform,
    detect_platform,
    _request_host_header,
    _request_hostname,
    _proxy_base,
    rewrite_abs_urls,
    strip_fragment,
    normalize_url,
    resolve_ip,
    url_depth,
    is_html,
    _fmt_size,
    _fmt_host,
    _fmt_path_qs,
    log_req,
    build_base_url,
    is_allowed_domain,
    is_external_domain,
)
from .Viewers.Common import (
    _viewer_cors_headers,
    _viewer_disabled_response,
    _viewer_ip_denied_response,
    _viewer_ip_allowed,
)
from .Viewers.Hosts import (
    _s2l_hp_endpoint,
)
from .Viewers.Reset import (
    _s2l_reset_stats_endpoint,
)
from .Viewers.Data import (
    _s2l_viewer_data_endpoint,
)
from .Viewers.CDN import (
    _s2l_viewer_endpoint,
)
from .WebSockets.Protocol import (
    _ws_unmask,
    _ws_deflate_init,
    _ws_deflate_msg,
    _ws_inflate_msg,
    _ws_deflate_cleanup,
    on_ws_message,
    _run_ws_msg_hooks,
    _ws_handshake_accept,
    _recv_exact,
    _ws_read_frame,
    _ws_make_frame,
)
from .WebSockets.Tunnel import (
    _WSTunnel,
    _ws_keepalive_loop,
    _pump_ws_frames_v2,
)
from .WebSockets.Upstream import (
    _RawSocketUpstreamWS,
    _get_session_for_ws,
    _CffiUpstreamWS,
    _ws_connect_upstream,
)
from .Application import create_app

# ── Site init (same expression and order as the monolith) ────────────────────
# build_base_url() probes the upstream and logs the resolution, exactly like
# the monolith does at import time. It is safe here because every helper it
# needs (_make_session, detect_platform, resolve_ip, log, ...) is imported
# above. Nothing earlier in this module reads these values at import time.
SITE_URL  = build_base_url(SITE) or f"http://{SITE}"
MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.removeprefix("www.").replace(".", "_")
SRC_FOLDER  = os.path.join("site_src",  SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)

stats = _Stats()

# Flask app: created and configured at module scope exactly like the monolith,
# then handed to create_app() so callbacks and routes are registered on it.
app = Flask(__name__, static_folder=None)
app.config["MAX_CONTENT_LENGTH"] = MAX_BODY_BYTES
create_app(app)


def main() -> None:
    """Initialize the target and workers, then run the proxy."""
    global CACHE_CDN
    os.makedirs(SRC_FOLDER, exist_ok=True)
    os.makedirs(DATA_FOLDER, exist_ok=True)
    threading.Thread(target=_save_worker, daemon=True, name="save-worker").start()
    import signal

    # Validate flag combinations
    if CACHE_CDN and not PROXY_CDN:
        log("CACHE_CDN=True requires PROXY_CDN=True — disabling CACHE_CDN", "WARN")
        CACHE_CDN = False
    if not PROXY_CDN and MULTIPORT:
        log("MULTIPORT=True has no effect without PROXY_CDN=True", "WARN")
    def _sigint_handler(_sig, _frame):
        _qt_shutdown_requested.set()
        # Clear the ^C that the terminal echoes on Ctrl+C — write \r + spaces
        # to overwrite it, then move cursor to start of line.
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
        print(f"{Fore.YELLOW}{Style.BRIGHT}Shutting down...{Style.RESET_ALL}")
        if GRACEFUL_SHUTDOWN:
            # Drain in-flight requests: wait for save_queue and capture_queue
            # to flush so no cached data is lost. Daemon threads (Flask, CDN
            # servers) will be killed by os._exit after the drain.
            try:
                log("Draining save_queue...", "INFO")
                save_queue.join()
            except Exception:
                pass
            try:
                log("Draining capture queue...", "INFO")
                _capture_queue.join()
            except Exception:
                pass
            try:
                if CAPTURE_WS:
                    log("Draining WS capture queue...", "INFO")
                    _ws_capture_queue.join()
            except Exception:
                pass
            log("Drain complete.", "INFO")
        print(f"{Fore.YELLOW}{Style.BRIGHT}Script finished by user command{Style.RESET_ALL}")
        # Use os._exit instead of sys.exit to avoid the "[1]+ Killed" shell
        # message that sys.exit produces when called from a signal handler.
        # os._exit kills all daemon threads immediately without running
        # atexit handlers (which is what we want — Flask/CDN servers are
        # daemon threads).
        os._exit(0)

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

    if CAPTURE_WS:
        os.makedirs(os.path.join(DATA_FOLDER, "captures_ws"), exist_ok=True)
        threading.Thread(target=_ws_capture_worker, daemon=True, name="ws-capture-worker").start()

    _banner()

    # Purge any bot/CAPTCHA pages that slipped into cache in previous runs.
    # Skip this in OFFLINE mode — purging destroys irreplaceable cache entries
    # when there's no upstream to re-fetch them from.
    def _purge_bot_cache() -> None:
        if OFFLINE:
            log("Skipping bot-cache purge in OFFLINE mode (no upstream to re-fetch)", "INFO")
            return
        purged = 0
        for root, _dirs, files in os.walk(SRC_FOLDER):
            for fn in files:
                if fn.endswith(_CACHE_TMP_SUFFIX):
                    continue   # atomic-write temp (save worker, in flight)
                # Scan ALL files, not just .html — bot pages can be cached
                # under any extension when the URL had no extension (collapsed
                # to /index.html) but the real CT was JSON/text/etc.
                fp = os.path.join(root, fn)
                try:
                    with open(fp, "rb") as f:
                        data = f.read(8192)
                    if _is_bot_page(data):
                        _purge_cached_file(fp)
                        purged += 1
                        log(f"Purged bot-page cache: {os.path.relpath(fp)}", "WARN")
                except Exception:
                    pass
        if purged:
            log(f"Startup bot-cache purge: removed {purged} poisoned file(s)", "INFO")

    threading.Thread(target=_purge_bot_cache, daemon=True, name="bot-cache-purge").start()

    # One-shot sweep of legacy-layout cache files (pre-9.1 / 9.1a / 09/10
    # naming). The migration bridges are gone — those files can never be
    # served again, so they are deleted and the reclaimed space logged.
    # Safe in OFFLINE too: the sweep only removes files NO URL can resolve
    # to (liveness proven via local_path), unlike the bot purge above.
    # Unconditional (9.0 contract) and backgrounded — a big mirror must not
    # delay the first serve.
    threading.Thread(target=_sweep_legacy_cache, daemon=True,
                     name="legacy-cache-sweep").start()

    if SCAN_PATHS and not OFFLINE:
        # Run the interactive status-filter prompt in the MAIN thread BEFORE
        # starting the scanner daemon so input() reads whole lines (not single
        # chars) and the filter is set before any scan happens.
        try:
            if sys.stdin.isatty():
                _prompt_scan_status_filter()
            else:
                log("SCAN_PATHS: stdin is not a TTY — skipping status filter prompt "
                    "(all status codes will be logged)", "INFO")
        except Exception as _e:
            log(f"SCAN_PATHS: status filter prompt failed ({_e}) — proceeding unfiltered", "WARN")
        threading.Thread(target=_run_path_scanner, daemon=True, name="path-scanner").start()
    elif SCAN_PATHS and OFFLINE:
        log("SCAN_PATHS ignored in OFFLINE mode — no upstream available", "WARN")

    # Register cached CDN hosts before offline serving or crawler startup so
    # MULTIPORT rewrites point at live destinations instead of dead ports.
    if (OFFLINE or PREMAP_CDN) and PROXY_CDN and MULTIPORT:
        _n_pre = _preregister_cdn_hosts_from_disk()
        if _n_pre:
            _premap_mode = "OFFLINE" if OFFLINE else "PREMAP_CDN"
            log(f"{_premap_mode}: pre-registered {_n_pre} CDN host(s) from disk cache", "INFO")
        if OFFLINE:
            try:
                _cached_count = sum(len(files) for _, _, files in os.walk(SRC_FOLDER))
            except Exception:
                _cached_count = 0
            if _cached_count == 0:
                log(f"OFFLINE mode enabled but {SRC_FOLDER} has 0 cached files — every request will 404.", "ERROR")
            else:
                log(f"OFFLINE mode serving {_cached_count} cached file(s) from {SRC_FOLDER}", "INFO")

    if CRAWL and not OFFLINE:
        def _bg_crawl() -> None:
            enqueue(SITE_URL)
            crawl_parallel()
            s = stats.snapshot()
            with _cdn_port_lock:
                cdn_map = dict(_cdn_host_port)
            log(f"Crawl done — {s['crawled']} fetched · {s['saved']} cached · "
                f"{s['cdn_fetched']} cdn · {s['captured']} captured · "
                f"{s['ws_captured']} ws-captured · "
                f"{s['revealed']} revealed · {s['conn_errors']} unreachable · "
                f"{s['http_errors']} HTTP errors")
            for host, port in cdn_map.items():
                log(f"  CDN {host} → http://localhost:{port}", "CDN")
        threading.Thread(target=_bg_crawl, daemon=True, name="crawl-main").start()
        log("Crawler started in background.")
    else:
        if CRAWL and OFFLINE:
            log("CRAWL ignored in OFFLINE mode — no upstream available", "WARN")
        # No bulk crawl will run (CRAWL=False, or OFFLINE where crawling is
        # impossible) → nothing will ever drain url_queue. Mark the crawl
        # done so enqueue() no-ops instead of accumulating one queue entry
        # per proxied response for the whole process lifetime (unbounded
        # memory growth). Procedural mode stays "proxy-on-demand only".
        _crawl_done.set()

    # NOTE (sentinel contract): in bulk mode the persistent discovery
    # workers are started by crawl_parallel() AFTER the initial crawl's
    # None sentinels (see Jobs.py). Starting them HERE would let them eat
    # those sentinels — every sentinel consumed by a discovery worker
    # permanently kills that worker (no restart) while a crawl worker
    # blocks on url_queue.get() forever.

    # External asset prefetch pool — always start (used by proxy in any mode
    # except OFFLINE). Bounded pool prevents thread exhaustion.
    if not OFFLINE:
        _start_ext_asset_workers()

    # Periodic prune of stale CDN health records (keeps _cdn_health bounded).
    _start_health_pruner()

    log(f"Listening on http://{HOST}:{PORT}", "INFO")
    if PROXY_CDN and MULTIPORT:
        if WEB_CDNVIEWER:
            log(f"MULTIPORT web viewer → http://localhost:{PORT}/__s2l_viewer__ (50 ms live refresh)", "INFO")

    if HOOK_GUI:
        # On Linux/X11 PyQt6 MUST run on the main thread (Qt's event loop
        # dispatches via the main thread; creating QApplication off-main
        # is unreliable across Wayland/X11 compositors).
        # Flask moves to a daemon thread so the main thread is free for the GUI.
        def _flask_thread():
            srv = make_server(HOST, PORT, app, threaded=True,
                              request_handler=_S2LWSGIRequestHandler)
            srv.__class__ = _PooledWSGIServer   # swap to use pool
            srv.serve_forever()
        threading.Thread(target=_flask_thread, daemon=True, name="flask").start()
        _launch_hook_gui()   # blocks main thread — GUI event loop runs here
    else:
        srv = make_server(HOST, PORT, app, threaded=True,
                          request_handler=_S2LWSGIRequestHandler)
        srv.__class__ = _PooledWSGIServer   # swap to use pool
        srv.serve_forever()
