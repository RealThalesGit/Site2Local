#!/usr/bin/env python3
# ── Silence multiprocessing.resource_tracker "leaked semaphore" warning ──
# The warning is emitted by the resource_tracker *subprocess* (not this one),
# so warnings.filterwarnings() can't suppress it. PYTHONWARNINGS is read by
# every Python interpreter at startup, including the resource_tracker child,
# which inherits this env var via fork+exec. Setting it BEFORE any imports
# that might trigger multiprocessing propagates the filter to all children.
import os as _os
_s2l_existing_warnings = _os.environ.get("PYTHONWARNINGS", "")
_s2l_sem_filter = "ignore::UserWarning:multiprocessing.resource_tracker"
if _s2l_sem_filter not in _s2l_existing_warnings:
    _os.environ["PYTHONWARNINGS"] = (
        (_s2l_existing_warnings + "," if _s2l_existing_warnings else "")
        + _s2l_sem_filter
    )
import os, sys, re, json, time, socket, queue, zlib, gc
import hashlib, mimetypes, logging, threading, warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
# Belt-and-suspenders: also patch warnings.warn in case resource_tracker is
# imported into THIS process via a library.
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
# Supplement system mime.types with modern types some distros ship without.
for _ext, _mt in (
    (".mjs",       "application/javascript"),
    (".cjs",       "application/javascript"),
    (".jsx",       "application/javascript"),
    (".tsx",       "application/javascript"),
    (".vue",       "application/javascript"),
    (".svelte",    "application/javascript"),
    (".webmanifest", "application/manifest+json"),
    (".wasm",      "application/wasm"),
    (".avif",      "image/avif"),
    (".webp",      "image/webp"),
    (".heic",      "image/heic"),
    (".heif",      "image/heif"),
    (".apng",      "image/apng"),
    (".opus",      "audio/ogg"),
    (".oga",       "audio/ogg"),
    (".ogv",       "video/ogg"),
    (".m4a",       "audio/mp4"),
    (".m4v",       "video/mp4"),
    (".webm",      "video/webm"),
    (".woff2",     "font/woff2"),
    (".woff",      "font/woff"),
    (".ttf",       "font/ttf"),
    (".otf",       "font/otf"),
    (".map",       "application/json"),
    (".json5",     "application/json"),
    (".csv",       "text/csv"),
    (".md",        "text/markdown"),
    (".svg",       "image/svg+xml"),
    (".ics",       "text/calendar"),
    (".epub",      "application/epub+zip"),
    (".m3u8",      "application/vnd.apple.mpegurl"),
    (".ts",        "video/mp2t"),
):
    try:
        mimetypes.add_type(_mt, _ext)
    except Exception:
        pass
import subprocess, tempfile, datetime
from dataclasses import dataclass, field, replace as _dc_replace
from typing import Callable
import base64
import struct
import ssl
from urllib.parse import urljoin, urlparse, urldefrag
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
# cloudscraper: pure-Python Cloudflare bypass, fallback tier when curl_cffi
# isn't available. ai-cloudscraper is the maintained fork (original is
# unmaintained since 2023). Guarded so missing import doesn't crash startup.
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
# than cloudscraper. Install: pip install curl-cffi
try:
    from curl_cffi import requests as _cffi_requests
    from curl_cffi.const import CurlWsFlag as _CurlWsFlag
    _CURL_CFFI_OK = True
except ImportError:
    _cffi_requests = None
    _CurlWsFlag    = None
    _CURL_CFFI_OK  = False

# tqdm: optional progress bar for SCAN_PATHS. Falls back to periodic log lines
# when missing. Install: pip install tqdm
try:
    from tqdm import tqdm as _tqdm
    _TQDM_OK = True
except ImportError:
    _tqdm = None
    _TQDM_OK = False

# Active SCAN_PATHS tqdm bar, or None. When set, log() routes output through
# tqdm.write() so log lines from any thread appear ABOVE the bar instead of
# corrupting it mid-line.
_active_scan_pbar = None

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

_ANSI_ESC       = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_ANSI_STRIP     = re.compile(r'\x1b\[(\d+)(;\d+)?(;\d+)?[m|K]')
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
}

# ──────────────────────────────────────────────────────────────────────────────
# Rainbow engine — native pure-Python sine-wave gradient (no external binary)
#
# Replicates and extends the lolcat algorithm: RGB computed from sine waves
# offset by 2π/3, mapped to 256-color ANSI (38;5;N) for a smooth gradient.
# Pure Python, no subprocess overhead, no external dependency. Thread-safe.
# ──────────────────────────────────────────────────────────────────────────────

import math as _math

# Pre-computed color cache: RGB tuple → ANSI 256-color escape string.
# The lolcat algorithm quantizes RGB to a 6×6×6 cube (216 colors) plus 24
# grays, so there are only ~240 distinct possible outputs — caching them
# avoids redundant math + string formatting on every character.
_RGB_TO_ANSI: dict[tuple[int, int, int], str] = {}

def _rgb_to_ansi256(r: float, g: float, b: float) -> str:
    """Quantize RGB (0-255 float) to the nearest 256-color ANSI code and
    return the escape sequence. Results are cached because the lolcat
    algorithm produces a limited set of distinct colors."""
    # Quantize to 0-5 per channel for the 6x6x6 cube.
    ri, gi, bi = int(r), int(g), int(b)
    key = (ri, gi, bi)
    cached = _RGB_TO_ANSI.get(key)
    if cached is not None:
        return cached
    # Gray detection (replicates lolcat's logic).
    sep = 2.5
    gray = False
    while sep <= 256:
        if ri < sep or gi < sep or bi < sep:
            gray = ri < sep and gi < sep and bi < sep
            break
        sep += 42.5
    if gray:
        color = 232 + int((ri + gi + bi) / 33.0)
    else:
        color = 16 + (int(6 * ri / 256) * 36
                      + int(6 * gi / 256) * 6
                      + int(6 * bi / 256))
    esc = f"\x1b[38;5;{color}m"
    if len(_RGB_TO_ANSI) < 512:  # bounded cache
        _RGB_TO_ANSI[key] = esc
    return esc

# Pre-compute the sine-wave RGB table for one full rainbow cycle at high
# resolution. The lolcat formula is sin(freq * pos) where pos = i / spread.
# One full cycle = period 2π/freq. We sample the cycle into N entries; at
# runtime each character advances `pos` by 1/spread, and we map `pos` into
# a table index. This turns per-character trig calls into array lookups.
_RAINBOW_TABLE_SIZE = 1024
_RAINBOW_TABLE: list[str] = []
_RAINBOW_PERIOD: float = 0.0  # 2π/freq — one full rainbow cycle in `pos` units

def _build_rainbow_table(freq: float) -> None:
    """Pre-compute one full rainbow cycle (period 2π/freq) into the table."""
    global _RAINBOW_PERIOD
    _RAINBOW_PERIOD = (2 * _math.pi) / freq
    _RAINBOW_TABLE.clear()
    two_pi_over_3 = 2 * _math.pi / 3
    four_pi_over_3 = 4 * _math.pi / 3
    period = _RAINBOW_PERIOD
    n = _RAINBOW_TABLE_SIZE
    for i in range(n):
        # Map table index i → position in [0, period)
        pos = (i / n) * period
        r = _math.sin(freq * pos) * 127 + 128
        g = _math.sin(freq * pos + two_pi_over_3) * 127 + 128
        b = _math.sin(freq * pos + four_pi_over_3) * 127 + 128
        _RAINBOW_TABLE.append(_rgb_to_ansi256(r, g, b))

# Build with default lolcat freq=0.1 (matches real lolcat's default).
_build_rainbow_table(0.1)

# Reset ANSI.
_ANSI_RESET = "\x1b[0m"


class _RainbowEngine:
    """Native rainbow engine — sine-wave gradient with continuous flow across
    consecutive calls (like piping a stream through lolcat). Thread-safe via
    a shared lock; the gradient position is shared so log lines from multiple
    threads still form a cohesive rainbow.
    """
    __slots__ = ("freq", "spread", "seed", "_pos")

    def __init__(self, freq: float = 0.1, spread: float = 3.0, seed: int = 0):
        self.freq   = freq
        self.spread = spread
        self.seed   = seed
        self._pos   = float(seed) if seed else 0.0

    def colorize(self, text: str) -> str:
        """Rainbow-color text. The gradient flows continuously across calls
        (consecutive log lines form one rainbow stream). Existing ANSI codes
        in the input are stripped first so the rainbow overrides uniformly.
        Every character (including spaces) gets a color code and advances
        the gradient, matching lolcat's behavior exactly."""
        plain = _ANSI_STRIP.sub("", text)
        if not plain:
            return text
        out: list[str] = []
        append = out.append
        table = _RAINBOW_TABLE
        n = _RAINBOW_TABLE_SIZE
        period = _RAINBOW_PERIOD
        pos = self._pos
        # Each character advances `pos` by 1/spread (lolcat convention).
        advance = 1.0 / self.spread
        # Scale: table_index = (pos / period) * n, wrapped via modulo.
        scale = n / period
        for ch in plain:
            idx = int(pos * scale) % n
            append(table[idx])
            append(ch)
            pos += advance
        self._pos = pos
        append(_ANSI_RESET)
        return "".join(out)


# Singleton engine — shared across all rainbow calls so the gradient flows
# continuously from one log line to the next, exactly like `... | lolcat`.
_rainbow = _RainbowEngine(freq=0.1, spread=3.0, seed=0)
_rainbow_lock = threading.Lock()


def _rainbow_text(text: str) -> str:
    """Apply rainbow coloring (native sine-wave gradient, 256-color ANSI) to
    any text when RAINBOW_LOGS is True. Existing ANSI codes are stripped first
    so the rainbow overrides per-level colors uniformly. The gradient position
    is continuous across calls — consecutive log lines flow into each other,
    exactly like piping output through `lolcat`."""
    if not RAINBOW_LOGS:
        return text
    with _rainbow_lock:
        return _rainbow.colorize(text)


def _rainbow_print(*args, sep: str = " ", end: str = "\n",
                   file=None, flush: bool = False) -> None:
    """Drop-in replacement for print() that rainbow-colors the output when
    RAINBOW_LOGS is True. Falls back to plain print() off."""
    text = sep.join(str(a) for a in args)
    if RAINBOW_LOGS:
        text = _rainbow_text(text)
    if file is None:
        file = sys.stdout
    print(text, end=end, file=file, flush=flush)


def log(msg: str, level: str = "INFO") -> None:
    ts    = time.strftime("%H:%M:%S")
    color = _LEVEL_COLOR.get(level, Fore.WHITE)
    tag   = f"{color}[{level}]{Style.RESET_ALL}"
    line  = f"{Style.DIM}{ts}{Style.RESET_ALL} {tag} {msg}"
    with _log_lock:
        if RAINBOW_LOGS:
            line = _rainbow_text(line)
        # Route through tqdm.write() when a progress bar is active so log
        # lines from any thread appear ABOVE the bar instead of corrupting it.
        if _TQDM_OK and _active_scan_pbar is not None:
            _tqdm.write(line, file=sys.stderr)
        else:
            print(line)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────
SITE = "example.com" # target domain or URL
HOST = "0.0.0.0" # listen address
PORT = 8080 # listen port
DEVICE = "auto"  # UA profile: auto|desktop|mobile|tablet|macintosh|iphone|ipad|ie11|symbian|bot — auto mirrors the requesting browser's own User-Agent
TIMEOUT_READ = 12 # upstream read timeout (s)
TIMEOUT_CONN = 6 # upstream connect timeout (s)
TIMEOUT_STREAM_READ = 120 # streaming read timeout (s) — longer for video/audio
WORKERS = 60 # crawler thread-pool size
MAX_FNAME = 180 # max on-disk path-segment length
SAVE_BATCH = 32 # files per save-flush
SAVE_INTERVAL = 0.15 # max seconds between save flushes
CRAWL_DEPTH = 7 # max URL path depth to follow
SCAN_LIMIT = 256 * 1024 # body bytes scanned in DUMP_ALL mode
RETRIES = 2 # retry count on 5xx / timeout
BACKOFF = 0.4 # exponential backoff base (s)
CRAWL = True # crawl at startup; False = proxy-on-demand only
SKIP_CRAWL_CACHE = True # True = CRAWL is resumable (skip files already on disk).
# False = re-download everything on every start.
OFFLINE = False # never hit upstream — serve disk only
SAVE_ERRORS = False # cache 4xx/5xx responses
DUMP_ALL = False # extract + crawl every URL found in any response body
PROXY_CDN = True # proxy external CDN/third-party assets
CACHE_CDN = True # cache CDN assets to disk (False = live-proxy, no disk)
MULTIPORT = True # each CDN host gets a dedicated port (False = /__s2l_ext__/)
HOOK_GUI = False # Tkinter traffic inspector + live hook editor
RAINBOW_LOGS = False # Native rainbow output for ALL terminal output and also its not using lolcat bin anymore
# (banner, logs, scan paths, viewer, shutdown).
# Pure-Python sine-wave gradient (256-color ANSI),
# no external binary needed. Continuous gradient
# flows across log lines like `... | lolcat`.
SHOW_HIDDEN = False # un-hide display:none / disabled elements in HTML
SCAN_PATHS = False # hidden-path scanner: False | "all" | "all-in-dir" | "<dir>/<file>"
#   "all"        — every wordlist under wordlists/ (recursive)
#   "all-in-dir" — every wordlist in wordlists/ top-level only
#   "dir/file"   — one specific wordlist (relative to wordlists/)
SCANS_PER_SECOND = 1000 # SCAN_PATHS rate limit (probes/sec across all worker
# threads). ~15% of this (clamped [4,24]) is the worker
# thread count. Also drives the no-tqdm progress refresh
# cadence. 0 or negative disables the rate limit.
CAPTURE = False # record every request+response as JSON
CAPTURE_CDN = False # include CDN responses in captures
CAPTURE_BODIES = False # include request bodies in captures
CAPTURE_SKIP_STATIC = True # skip images/fonts/JS/CSS from captures
# NOTE: COOP/COEP used to be a manual global toggle. It's now PASSIVE (see
# filter_resp): the origin's own Cross-Origin-Opener/Embedder-Policy headers
# are simply no longer stripped, and a content-sniff heuristic adds safe
# defaults on top-level HTML that looks like it needs isolation (SharedArrayBuffer
# / WASM threads / Worker) but wasn't sent any — no flag to remember to flip.
FIREFOX_PROXY = False # MITM forward proxy for the actual browser (root CA + CONNECT tunnel)
FIREFOX_PROXY_PORT = 8443 # port to point Firefox's manual proxy config at
# ──────────────────────────────────────────────────────────────────────────────
# LAYER 2 CONFIG
# ──────────────────────────────────────────────────────────────────────────────
WS_PING_INTERVAL = 0 # seconds between WS keepalive pings (0 = off)
WS_PONG_TIMEOUT = 30 # seconds to wait for pong before declaring dead
WS_AUTO_RECONNECT = True # transparently re-establish dropped WS upstreams
WS_DEFLATE = False # negotiate permessage-deflate (RFC 7692)
WS_LOG_FRAMES = False # log every WS frame (verbose; for debugging)
WS_MAX_MSG_BYTES = 8 * 1024 * 1024   # hard cap on reassembled WS message size
SSE_PROXY = True # proxy Server-Sent Events streams (text/event-stream)
SSE_HEARTBEAT = 15 # seconds between injected SSE keepalives (0 = off)
TCP_TUNNEL = False # /__s2l_tcp__/<host:port> raw TCP tunnel route
UDP_TUNNEL = False # /__s2l_udp__/<host:port> raw UDP tunnel route
TUNNEL_TIMEOUT = 300 # idle timeout (s) for raw TCP/UDP tunnels
POOL_MAXSIZE = 64 # per-session connection pool size
POOL_CONNECTIONS = 32# per-session connection pool slots
DEAD_HOST_TTL = 60 # seconds to remember a dead upstream host
GRACEFUL_SHUTDOWN = True # drain in-flight requests on SIGTERM

# ──────────────────────────────────────────────────────────────────────────────
# Filters / constants
# ──────────────────────────────────────────────────────────────────────────────

BLOCK_PATHS = ()   # user-tunable: paths to block with 403 (e.g. vendor challenge URLs)
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

    Object-key storage services are the textbook case: an object key that legitimately
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

def _is_ip_literal(host: str) -> bool:
    """Return True if `host` is a literal IP address (v4 or v6), not a DNS name.

    Used to decide whether to send SNI in TLS handshakes: RFC 6066 says SNI
    is for DNS names only, and many servers/WAFs reject connections that
    send an IP literal as the SNI hostname.
    """
    if not host:
        return False
    h = host.strip("[]")
    # IPv6 contains ':', IPv4 contains '.', DNS names contain only [a-zA-Z0-9._-]
    if ":" in h:
        return True  # IPv6 literal
    if h.replace(".", "").isdigit() and h.count(".") == 3:
        return True  # IPv4 literal
    # Pure numeric (rare, but e.g. "1234" as a hostname) — treat as IP
    return h.isdigit()

def _is_ipv6_literal(host: str) -> bool:
    """Return True only if `host` is an IPv6 literal (not IPv4, not DNS name).

    curl_cffi has a known bug with IPv6 literals in ws_connect (TLS handshake
    fails with "invalid library"). IPv4 literals work fine — this function is
    used to skip curl_cffi ONLY for IPv6, so IPv4 hosts still benefit from
    curl_cffi's browser TLS fingerprinting.
    """
    if not host:
        return False
    h = host.strip("[]")
    return ":" in h

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
        log(f"req hook [{','.join(sorted(mset))}] {pattern} → {fn.__name__}()", "HOOK")
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
    # Iterate over a COPY — a hook may register new hooks at runtime, which
    # would mutate the list mid-iteration (RuntimeError: list changed size
    # during iteration). The GUI hooks already do this; apply the same
    # protection here.
    for pat, mset, fn in list(hooks):
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
#   Earlier versions of _apply_gui_ws_hooks blindly replaced EVERY message
#   whose (direction, opcode, origin, path) matched a hook — so if a server
#   sent different responses on the same WS path (the normal case for any
#   realtime API: chat messages, ticker updates, presence pings, etc.), the
#   hook overrode ALL of them with the same canned body. The fix introduces
#   two new hook fields:
#
#     match_mode   : "any" | "exact" | "regex" | "contains"
#         "any"      — override every matching message (legacy behavior)
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
_gui_ws_hooks: list[dict] = []   # {name, direction, opcode, pattern, origin_url,
                                  #  body_bytes, enabled, match_mode, match_payload}
_gui_ws_hooks_lock = threading.Lock()

def _gui_ws_push(direction: str, opcode: int, payload: bytes,
                 ws_url: str, tunnel_id: int) -> None:
    """Push a captured WS message to the GUI traffic log (after hook apply)."""
    if not HOOK_GUI:
        return
    try:
        parsed = urlparse(ws_url)
        _gui_ws_log_queue.put_nowait({
            "ts":        time.strftime("%H:%M:%S"),
            "direction": direction,        # "in" (browser→srv) | "out" (srv→browser)
            "opcode":    opcode,
            "op_name":   _WS_OP_NAMES.get(opcode, f"?{opcode}"),
            "ws_url":    ws_url,
            "origin":    parsed.netloc or "",
            "path":      parsed.path or "/",
            "size":      len(payload),
            "payload":   payload,
            "tunnel_id": tunnel_id,
        })
    except queue.Full:
        pass

def _apply_gui_ws_hooks(direction: str, opcode: int, payload: bytes,
                        ws_url: str, tunnel_id: int) -> bytes:
    """Apply the first matching enabled GUI WS hook. Returns the (possibly
    replaced) payload. Thread-safe.

    Filters: direction, opcode, origin_url, path pattern. First match wins.

    INSTANCED HOOKS:
      Each hook carries a `match_mode` that decides whether the hook should
      fire for THIS specific payload. Without this check, a hook created from
      one captured message would override EVERY subsequent message on the
      same path — the user's actual intent ("replace this one response") was
      being violated, polluting unrelated messages on the same WebSocket.

      match_mode == "any"      → fire on every (direction, opcode, origin, path) match
      match_mode == "exact"    → fire only when payload == match_payload (byte-for-byte)
      match_mode == "regex"    → fire only when payload (UTF-8 decoded) matches match_payload regex
      match_mode == "contains" → fire only when match_payload is a substring of payload
    """
    if not HOOK_GUI:
        return payload
    with _gui_ws_hooks_lock:
        hooks = list(_gui_ws_hooks)
    _path = urlparse(ws_url).path or "/"
    _host = urlparse(ws_url).netloc.lower().split(":", 1)[0]
    if _host.startswith("www."):
        _host = _host[4:]
    for h in hooks:
        if not h["enabled"]:
            continue
        hd = (h.get("direction") or "*").strip().lower()
        if hd not in ("*", "any", "both", "") and hd != direction:
            continue
        try:
            ho = int(h.get("opcode", 0) or 0)
        except (ValueError, TypeError):
            ho = 0
        if ho and ho != opcode:
            continue
        ho_url = (h.get("origin_url") or "*").strip().lower()
        if ho_url not in ("", "*", "any"):
            if ho_url in ("target", "main"):
                if _host != MAIN_HOST.lower():
                    continue
            elif not (_host == ho_url or _host.endswith("." + ho_url)):
                continue
        try:
            if not re.search(h["pattern"], _path, re.IGNORECASE):
                continue
        except re.error:
            continue
        # ── Instanced match check: does THIS payload match the hook's
        # captured target? If not, skip — don't blanket-override every
        # message on the same path.
        mode = (h.get("match_mode") or "any").strip().lower()
        if mode not in ("any", "", "exact", "regex", "contains"):
            mode = "any"
        if mode != "any":
            mp = h.get("match_payload") or b""
            if not mp:
                # No match_payload set → fall back to "any" so legacy hooks
                # without a payload don't silently no-op.
                pass
            elif mode == "exact":
                if payload != mp:
                    continue
            elif mode == "contains":
                if mp not in payload:
                    continue
            elif mode == "regex":
                try:
                    pat = re.compile(mp.decode("utf-8", "ignore"), re.DOTALL)
                    if not pat.search(payload.decode("utf-8", "ignore")):
                        continue
                except (re.error, UnicodeDecodeError):
                    continue
        new_payload = h["body_bytes"]
        log(f"ws hook '{h['name']}' matched {direction} {ws_url} "
            f"op={_WS_OP_NAMES.get(opcode, opcode)} mode={mode} "
            f"{_fmt_size(len(new_payload))}", "HOOK")
        return new_payload
    return payload

# ── FIREFOX_PROXY request log + hooks ───────────────────────────────────────
# Mirrors _gui_log_queue above, but fed by the MITM forward proxy's live
# browser traffic (any site) instead of the reverse proxy's own fetches for
# MAIN_HOST. Defined at module level (not inside the GUI) because
# the MITM connection handler runs in background threads regardless of
# whether the GUI happens to be open.
_gui_fwd_log_queue: queue.Queue = queue.Queue(maxsize=2000)
_gui_fwd_req_hooks: list[dict]  = []   # {name, method, pattern, body_bytes, enabled}
_gui_fwd_req_hooks_lock = threading.Lock()

def _gui_fwd_push(tag: str, method: str, host: str, path: str, status,
                   req_headers: dict, body: bytes) -> None:
    if not HOOK_GUI:
        return
    try:
        _gui_fwd_log_queue.put_nowait({
            "ts":     time.strftime("%H:%M:%S"),
            "method": method,
            "path":   f"[{tag}] {host}{path}",
            "status": status,
            "ct":     req_headers.get("Content-Type", req_headers.get("content-type", "")),
            "size":   len(body or b""),
            "body":   body if body else b"(empty request body)",
            "req_headers": req_headers,
        })
    except queue.Full:
        pass

def _apply_fwd_req_hooks(method: str, path: str, headers: dict, body: bytes) -> bytes:
    """Apply the first matching enabled Firefox-Proxy request hook.

    Overrides the body of a REAL, live browser request before it's forwarded
    upstream (passthru case) — fed by the MITM forward proxy's own traffic
    (any site), not the reverse proxy's fetches for MAIN_HOST.
    """
    with _gui_fwd_req_hooks_lock:
        hooks = list(_gui_fwd_req_hooks)
    for h in hooks:
        if not h["enabled"]:
            continue
        mf = h["method"].strip().upper()
        if mf not in ("", "*") and method.upper() != mf:
            continue
        try:
            if not re.search(h["pattern"], path, re.IGNORECASE):
                continue
        except re.error:
            continue
        new_body = h["body_bytes"]
        headers["Content-Length"] = str(len(new_body))
        log(f"fwd req hook '{h['name']}' matched {method} {path}", "HOOK")
        return new_body
    return body

def _gui_push(ctx: HookContext) -> None:
    """Push a proxied request/response to the GUI traffic log with body."""
    if not HOOK_GUI:
        return

    body = ctx.resp_body if ctx.resp_body is not None else b""

    # Show a readable placeholder in the Body Editor for truly empty responses.
    # Distinguish between legitimately-empty statuses (204/304/3xx) and
    # suspicious empties (200 with no body) so the user can tell at a glance
    # whether the emptiness is expected or a bug.
    display_body = body
    if len(body) == 0:
        sc = ctx.resp_status
        ct = (ctx.resp_ct or "").lower()
        if sc == 204:
            display_body = "(204 No Content - response has no body by definition)".encode()
        elif sc == 304:
            display_body = "(304 Not Modified - served from browser cache, no body)".encode()
        elif sc == 206:
            display_body = "(206 Partial Content - empty body, range request may have produced no bytes)".encode()
        elif sc in (301, 302, 303, 307, 308):
            display_body = "(redirect - no body, follow Location header)".encode()
        elif "json" in ct:
            display_body = b"(empty JSON response -- upstream returned 200 with no body)"
        elif "html" in ct:
            display_body = b"(empty HTML response -- upstream returned 200 with no body)"
        else:
            display_body = b"(empty response)"

    try:
        # Extract origin host from ctx.url so the hook editor can auto-populate
        # the "Origin URL" field and hooks can match by domain.
        _origin = ""
        try:
            _origin = urlparse(ctx.url).netloc or ""
        except Exception:
            pass
        _gui_log_queue.put_nowait({
            "ts":       time.strftime("%H:%M:%S"),
            "method":   ctx.method,
            "path":     ctx.path,
            "query":    ctx.query,
            "status":   ctx.resp_status,
            "ct":       ctx.resp_ct.split(";")[0].strip() if ctx.resp_ct else "",
            "web_type": "None",            # main proxy requests have no CDN tag — show "None"
            "size":     len(body),         # real size (0 = correct)
            "body":     display_body,      # what the editor shows
            "origin":   _origin,
        })
    except queue.Full:
        # Throttle: only log the first drop in a 5-second window, otherwise
        # a busy site floods the console with hundreds of identical WARNs.
        now = time.time()
        if now - _gui_drop_last_log[0] > 5.0:
            log(f"GUI queue full — dropping entries ({ctx.method} {ctx.path} …)", "WARN")
            _gui_drop_last_log[0] = now

def _gui_push_raw(method: str, path: str, status: int, ct: str, body: bytes,
                  display_tag: str = "", origin: str = "",
                  _skip_log: bool = False) -> None:
    """Lightweight push for CDN mini-server / ext_asset routes.

    Also logs a matching line to the terminal so the GUI traffic log and the
    terminal log stay in sync. Callers that already log to terminal themselves
    (e.g. log_req) set _skip_log=True to avoid double-logging.

    display_tag is shown as a prefix in the Content-Type column (e.g. "[cdn:8081]")
    so the path column always contains the bare path that hook patterns match against.
    Previously the tag was embedded in `path` itself, which meant the GUI traffic log
    showed e.g. "[cdn:8081] /generate_204" and users trying to write a hook pattern
    from it would include "[cdn:8081]" — causing their patterns to never match because
    _apply_gui_hooks (and _run_hooks) test against ctx.path which is always the bare path.

    origin is the upstream host this request was served from (e.g. "cdn.example.com").
    It's stored in the traffic log entry so the hook editor can auto-populate the
    "Origin URL" field, and so hooks can match by domain (not just path).
    """
    # Sync: log to terminal even when HOOK_GUI is off, so the terminal always
    # shows the same set of entries the GUI would show.
    if body is None:
        body = b""
    elif not isinstance(body, bytes):
        try:
            body = str(body).encode("utf-8")
        except Exception:
            body = b""

    # Build a meaningful display body for the GUI editor.
    if body:
        display_body = body
    elif status == 204:
        display_body = "(204 No Content - response has no body by definition)".encode()
    elif status == 304:
        display_body = "(304 Not Modified - served from browser cache, no body)".encode()
    elif status == 206:
        display_body = "(206 Partial Content - empty body, range request may have produced no bytes)".encode()
    elif status in (301, 302, 303, 307, 308):
        display_body = "(redirect - no body, follow Location header)".encode()
    else:
        display_body = b"(empty response)"
    ct_clean = ct.split(";")[0].strip() if ct else ""
    web_type = display_tag or "None"

    # Sync: emit a terminal line for this entry (unless the caller already did).
    if not _skip_log:
        _tag = display_tag.strip("[]") if display_tag else ""
        _prefix = f"[{_tag}] " if _tag else ""
        _host = origin or ""
        _log_level = "CDN" if _tag and ("cdn" in _tag.lower() or "ext" in _tag.lower()) else "→"
        log(f"{_prefix}{method} {status} {_host}{path} {_fmt_size(len(body))}", _log_level)

    if not HOOK_GUI:
        return

    try:
        _gui_log_queue.put_nowait({
            "ts":       time.strftime("%H:%M:%S"),
            "method":   method,
            "path":     path,
            "query":    "",
            "status":   status,
            "ct":       ct_clean,
            "web_type": web_type,
            "size":     len(body),
            "body":     display_body,
            "origin":   origin,
        })
    except queue.Full:
        pass

def _origin_matches(hook_origin: str, ctx: HookContext) -> bool:
    """Check if a hook's origin filter matches the request's actual origin.

    hook_origin semantics:
      - "" / "*" / "any"  → matches ANY origin (the target site AND all CDNs)
      - "target" / "main" → matches only MAIN_HOST (the target site)
      - a hostname        → matches if the request's netloc ends with it
                            (e.g. "cdn.example.com" matches that exact host;
                             "example.com" also matches "cdn.example.com")
    This lets users write hooks that target a specific CDN without affecting
    the main site, or vice-versa.
    """
    ho = (hook_origin or "").strip().lower()
    if ho in ("", "*", "any"):
        return True
    try:
        netloc = urlparse(ctx.url).netloc.lower()
    except Exception:
        netloc = ""
    if not netloc:
        return False
    # Strip port
    host = netloc.split(":", 1)[0]
    # Strip www.
    if host.startswith("www."):
        host = host[4:]
    if ho in ("target", "main"):
        return host == MAIN_HOST.lower() or host == ("www." + MAIN_HOST).lower()
    # Hostname suffix match: "example.com" matches "cdn.example.com"
    return host == ho or host.endswith("." + ho)

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
        if hook_status not in (0, ctx.resp_status):
            continue
        # Origin filter — match by domain (target site vs specific CDN)
        if not _origin_matches(h.get("origin_url", ""), ctx):
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
        log(f"hook '{h['name']}' matched {ctx.method} {ctx.path} "
            f"[{ctx.resp_status}] {_fmt_size(len(new_body))}", "HOOK")
        break

def _make_hook_store(subdir: str, extra_binary_fields: tuple = ()):
    """Factory for a hook-list disk-persistence helper.

    Shared by every hook editor (response hooks, request-body-override hooks,
    and the Firefox-Proxy request hooks added below) so the save/delete/load
    logic exists in exactly one, correct place instead of being copy-pasted
    per tab. Fixes two real bugs that existed in the old per-tab copies:
      1. Deletion matched files by `str.startswith(safe_name)`, so deleting a
         hook named "foo" also deleted a hook named "foobar" (same prefix).
         Now the exact body/meta filenames are constructed and removed
         directly — no directory scan, no collision.
      2. The on-disk extension used to be sniffed from body content (.html /
         .json / .txt); editing a hook's body into a different-looking format
         silently orphaned the old file under its previous extension. Body is
         now always stored as `{name}.body` — content sniffing for display
         purposes still happens in the editor, it just no longer decides a
         filename.
    Writes are tmpfile+os.replace (atomic on POSIX), so a crash mid-save can
    never leave a half-written body/meta pair for the loader to trip over.

    `extra_binary_fields` lists hook dict keys OTHER than `body_bytes` that
    also hold raw bytes (e.g. the WS hook's `match_payload`). They're
    base64-encoded into the JSON meta on save and decoded back on load —
    JSON can't serialize bytes natively, and without this the WS hook save
    would silently fail every time `match_payload` was set.
    """
    hooks_dir = os.path.join("site_data", subdir, MAIN_HOST)
    os.makedirs(hooks_dir, exist_ok=True)
    _extra_bin = tuple(extra_binary_fields)

    def _paths(name: str) -> tuple[str, str]:
        safe = re.sub(r"[^\w\-.]", "_", name)
        p = os.path.join(hooks_dir, f"{safe}.body")
        return p, p + ".meta.json"

    def _atomic_write(target: str, data: bytes) -> None:
        fd, tmp = tempfile.mkstemp(dir=hooks_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, target)
        except Exception:
            try: os.remove(tmp)
            except OSError: pass
            raise

    def save(hook: dict) -> None:
        try:
            p, meta_path = _paths(hook["name"])
            meta = {k: v for k, v in hook.items() if k != "body_bytes"}
            # Base64-encode any extra binary fields so they survive JSON
            # serialization. Stored under "<field>_b64" so the loader knows
            # exactly which keys to decode back to bytes.
            for f in _extra_bin:
                v = meta.get(f)
                if isinstance(v, (bytes, bytearray)):
                    meta[f + "_b64"] = base64.b64encode(bytes(v)).decode("ascii")
                    del meta[f]
            _atomic_write(p, hook["body_bytes"])
            _atomic_write(meta_path, json.dumps(meta, indent=2).encode("utf-8"))
        except Exception as e:
            log(f"Hook disk save failed ({subdir}): {e}", "ERROR")

    def delete(name: str) -> None:
        for fp in _paths(name):
            try:
                os.remove(fp)
            except OSError:
                pass

    def load_all(defaults: dict) -> list:
        loaded = []
        try:
            for fn in sorted(os.listdir(hooks_dir)):
                if not fn.endswith(".body.meta.json"):
                    continue
                meta_path = os.path.join(hooks_dir, fn)
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    body_file = meta_path[:-len(".meta.json")]
                    if not os.path.isfile(body_file):
                        continue
                    with open(body_file, "rb") as f:
                        body_bytes = f.read()
                    # Decode any extra binary fields back from base64.
                    for f in _extra_bin:
                        b64_key = f + "_b64"
                        if b64_key in meta:
                            try:
                                meta[f] = base64.b64decode(meta.pop(b64_key))
                            except Exception:
                                meta[f] = b""
                    loaded.append({**defaults, **meta, "body_bytes": body_bytes})
                except Exception as e:
                    log(f"Hook load error {fn} ({subdir}): {e}", "WARN")
        except Exception:
            pass
        return loaded

    return save, delete, load_all

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
        _CYN = "#00d0d0"   # cyan accent for origin column
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
            def _sa(_e): widget.tag_add("sel","1.0","end"); return "break"
            def _sc(_e):
                try:
                    s = widget.get("sel.first","sel.last")
                    root.clipboard_clear(); root.clipboard_append(s)
                except tk.TclError: pass
                return "break"
            def _sv(_e):
                try:
                    t = root.clipboard_get()
                    try: widget.delete("sel.first","sel.last")
                    except tk.TclError: pass
                    widget.insert("insert", t)
                except tk.TclError: pass
                return "break"
            def _sx(_e):
                _sc(_e)
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
        # Give the top area (traffic log + body editor) the lion's share of
        # vertical space. Active hooks only needs ~120px for 3-4 rows.
        v_pane.add(top_area, minsize=360, height=600)
        v_pane.add(bot_area, minsize=100, height=120)

        # Horizontal split inside top_area: left (traffic log) | right (body editor)
        # Give the traffic log MORE width than the body editor — the path column
        # was too narrow to read. 55/45 split instead of the old ~50/50.
        h_pane = tk.PanedWindow(top_area, orient=tk.HORIZONTAL, bg=_BG, sashwidth=5,
                                sashrelief="flat")
        h_pane.pack(fill=tk.BOTH, expand=True)

        left  = tk.Frame(h_pane, bg=_PNL)
        right = tk.Frame(h_pane, bg=_PNL)
        h_pane.add(left,  minsize=520, width=560)
        h_pane.add(right, minsize=300, width=420)

        # ── LEFT: traffic log ─────────────────────────────────────────────────
        _lbl(left, "Traffic Log", fg=_ACC, font=("Consolas", 11, "bold"),
             bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))

        tree_frame = tk.Frame(left, bg=_PNL)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=4)

        cols = ("ts", "method", "origin", "path", "status", "web_type", "ct", "size")
        tree = ttk.Treeview(tree_frame, columns=cols, show="headings", selectmode="browse")
        for c, w, label in (
            ("ts",       58,  "Time"),
            ("method",   50,  "Method"),
            ("origin",   110, "Origin"),
            ("path",     0,   "Path"),      # 0 = stretch (gets the remaining space)
            ("status",   44,  "Status"),
            ("web_type", 72,  "Web Type"),  # [cdn], [ext], [stream], [cdn:8081]
            ("ct",       100, "File Type"), # MIME type only
            ("size",     56,  "Size"),
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
            for iid, d in _row_data.items():
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

        def _show_traffic_search(_event=None):
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

        def _body_find_next(_event=None):
            if not _body_find_positions:
                return "break"
            _body_find_idx[0] = (_body_find_idx[0] + 1) % len(_body_find_positions)
            body_box.see(_body_find_positions[_body_find_idx[0]])
            return "break"

        def _body_find_prev(_event=None):
            if not _body_find_positions:
                return "break"
            _body_find_idx[0] = (_body_find_idx[0] - 1) % len(_body_find_positions)
            body_box.see(_body_find_positions[_body_find_idx[0]])
            return "break"

        def _body_find_close(_event=None):
            body_box.tag_remove("find_hi", "1.0", "end")
            body_find_frame.pack_forget()
            body_box.focus_set()

        body_find_entry.bind("<Return>",       _body_find_next)
        body_find_entry.bind("<Shift-Return>", _body_find_prev)
        body_find_entry.bind("<Escape>",       _body_find_close)
        body_find_entry.bind("<KeyRelease>",   _body_find_apply)

        def _show_body_find(_event=None):
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
        row3 = tk.Frame(save_frame, bg=_PNL); row3.pack(fill=tk.X, pady=2)

        _lbl(row1, "Name:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        name_entry = _entry(row1, width=14)
        name_entry.pack(side=tk.LEFT, padx=(2, 10))

        _lbl(row1, "Method:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        _METHOD_OPTS = ["*", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
        _RH_METHOD_OPTS = _METHOD_OPTS   # shared by the Firefox-Proxy request-hook tab
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

        _lbl(row2, "Pattern url:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        pat_entry = _entry(row2, width=34)
        pat_entry.insert(0, r".*")
        pat_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)

        # Origin URL — which domain/host this hook targets.
        #   * / empty / "any"  → any origin (target site + all CDNs)
        #   "target"           → only the main target site (MAIN_HOST)
        #   a hostname         → that specific host (suffix match: "example.com"
        #                        also matches "cdn.example.com")
        _lbl(row3, "Origin url:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        origin_entry = _entry(row3, width=20)
        origin_entry.insert(0, "*")
        origin_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)
        _lbl(row3, "  (target / * / host)", fg=_DIM, bg=_PNL).pack(side=tk.LEFT)

        save_status = _lbl(right, text="", fg=_DIM, bg=_PNL)
        save_status.pack(anchor="w", padx=8, pady=(0, 2))

        def _on_select(_event):
            sel = tree.selection()
            if not sel:
                return
            iid  = sel[0]
            data = _row_data.get(iid)
            if not data:
                return

            path_qs = data["path"] + (f"?{data['query']}" if data.get("query") else "")
            _info_origin = data.get("origin", "")
            _info_prefix = f"[{_info_origin}]  " if _info_origin else ""
            _info_wt = data.get("web_type", "")
            _info_wt_s = f"  {_info_wt}" if _info_wt else ""
            info_var.set(f"{_info_prefix}{data['method']}  {path_qs}  [{data['status']}]{_info_wt_s}  {data['ct']}")

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
            # Origin: auto-populate from the traffic log entry's origin field.
            # If it's the main target site, use "target"; otherwise use the CDN host.
            _data_origin = data.get("origin", "")
            origin_entry.delete(0, "end")
            if _data_origin:
                origin_entry.insert(0, _data_origin)
            else:
                origin_entry.insert(0, "*")
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
                        "name":        item.get("name", "imported"),
                        "method":      item.get("method", "*"),
                        "status":      int(item.get("status", 200)),
                        "pattern":     item.get("pattern", ".*"),
                        "origin_url":  item.get("origin_url", item.get("origin", "*")),
                        "body_bytes":  item.get("body", "").encode("utf-8"),
                        "enabled":     item.get("enabled", True),
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

        def _on_hf_configure(_e):
            hooks_canvas.configure(scrollregion=hooks_canvas.bbox("all"))
        def _on_canvas_configure(e):
            hooks_canvas.itemconfig(hf_win, width=e.width)
        hooks_frame.bind("<Configure>", _on_hf_configure)
        hooks_canvas.bind("<Configure>", _on_canvas_configure)

        _HOOK_COLS   = ("Name", "Method", "Status", "Origin", "Pattern", "On", "", "")
        _HOOK_WIDTHS = (16, 7, 6, 18, 28, 4, 4, 4)

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
                    row=1, column=0, columnspan=8, sticky="w", padx=4, pady=4)
                return
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                _lbl(hooks_frame, h["name"], bg=_PNL, fg=row_fg,
                     width=16, anchor="w").grid(row=row_idx, column=0, sticky="w", padx=3)
                _lbl(hooks_frame, h["method"], bg=_PNL,
                     fg=_YLW if h["enabled"] else _DIM,
                     width=7, anchor="w").grid(row=row_idx, column=1, sticky="w", padx=3)
                _lbl(hooks_frame, str(h["status"]), bg=_PNL,
                     fg=_GRN if h["enabled"] else _DIM,
                     width=6, anchor="w").grid(row=row_idx, column=2, sticky="w", padx=3)
                _origin_disp = h.get("origin_url", "*") or "*"
                if len(_origin_disp) > 18:
                    _origin_disp = _origin_disp[:17] + "…"
                _lbl(hooks_frame, _origin_disp, bg=_PNL,
                     fg=_CYN if h["enabled"] else _DIM,
                     width=18, anchor="w").grid(row=row_idx, column=3, sticky="w", padx=3)
                _lbl(hooks_frame,
                     h["pattern"][:28] + ("…" if len(h["pattern"]) > 28 else ""),
                     bg=_PNL, fg=_DIM, width=28, anchor="w").grid(row=row_idx, column=4, sticky="w", padx=3)

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
                               width=2).grid(row=row_idx, column=5, padx=3)

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
                        origin_entry.delete(0, "end")
                        origin_entry.insert(0, hook_ref.get("origin_url", "*") or "*")
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
                     fg=_GRN).grid(row=row_idx, column=6, padx=3)

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
                     font=("Consolas", 8)).grid(row=row_idx, column=7, padx=3)

        _render_hook_rows()

        # ── Hook disk persistence — delegates to the shared store factory ──────
        _hook_save_to_disk, _hook_delete_from_disk, _hook_load_all = _make_hook_store("MyHooks")

        def _load_hooks_from_disk() -> None:
            """Load previously saved hooks from site_data/MyHooks/{host}/ at startup."""
            for hook in _hook_load_all({"name": "?", "method": "*", "status": 200,
                                         "pattern": ".*", "origin_url": "*",
                                         "enabled": True}):
                hook["status"] = int(hook.get("status", 200) or 0)
                if "origin_url" not in hook:
                    hook["origin_url"] = "*"
                with _gui_hooks_lock:
                    if hook["name"] not in [h["name"] for h in _gui_hooks]:
                        _gui_hooks.append(hook)
                        log(f"Loaded hook '{hook['name']}' from disk", "HOOK")

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
            origin_str = origin_entry.get().strip() or "*"
            hook = {
                "name":        name,
                "method":      method_var.get(),
                "status":      status,
                "pattern":     pat_str,
                "origin_url":  origin_str,
                "body_bytes":  body_txt.encode("utf-8"),
                "enabled":     True,
            }
            with _gui_hooks_lock:
                _gui_hooks.append(hook)
            _hook_save_to_disk(hook)
            _render_hook_rows()
            save_status.config(
                text=f"Saved '{name}'  [{hook['method']}]  {hook['pattern']}",
                fg=_GRN)
            log(f"GUI hook saved: '{name}' [{hook['method']}] {hook['pattern']}", "HOOK")

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
        # TAB 2: WebSocket Hooks
        # Same shape as Tab 1 (Traffic / Hooks) but for WebSocket messages
        # instead of HTTP responses. The body editor is split in two:
        #   • LEFT  — fully editable hex dump (canonical: offset + hex bytes)
        #   • RIGHT — read-only decoded text (UTF-8, best-effort), live-synced
        # Hooks replace the payload of matching WS messages on the fly; the
        # opcode is preserved so a text hook still ships as text and a binary
        # hook still ships as binary.
        # ══════════════════════════════════════════════════════════════════════
        tab_ws = tk.Frame(nb, bg=_BG)
        nb.add(tab_ws, text="  WebSocket Hooks")

        ws_v_pane = tk.PanedWindow(tab_ws, orient=tk.VERTICAL, bg=_BG,
                                   sashwidth=5, sashrelief="flat")
        ws_v_pane.pack(fill=tk.BOTH, expand=True)
        ws_top = tk.Frame(ws_v_pane, bg=_BG)
        ws_bot = tk.Frame(ws_v_pane, bg=_PNL)
        ws_v_pane.add(ws_top, minsize=360, height=600)
        ws_v_pane.add(ws_bot, minsize=100, height=120)

        # Horizontal split inside ws_top: left (WS log) | right (hook editor)
        ws_h_pane = tk.PanedWindow(ws_top, orient=tk.HORIZONTAL, bg=_BG,
                                   sashwidth=5, sashrelief="flat")
        ws_h_pane.pack(fill=tk.BOTH, expand=True)
        ws_left  = tk.Frame(ws_h_pane, bg=_PNL)
        ws_right = tk.Frame(ws_h_pane, bg=_PNL)
        ws_h_pane.add(ws_left,  minsize=480, width=520)
        ws_h_pane.add(ws_right, minsize=360, width=460)

        # ── LEFT: WS message log ────────────────────────────────────────────
        _lbl(ws_left, "WebSocket Messages", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
        ws_tree_frame = tk.Frame(ws_left, bg=_PNL)
        ws_tree_frame.pack(fill=tk.BOTH, expand=True, padx=4)
        ws_cols = ("ts", "dir", "op", "origin", "path", "size")
        ws_tree = ttk.Treeview(ws_tree_frame, columns=ws_cols, show="headings",
                               selectmode="browse")
        for c, w, label in (
            ("ts",     58, "Time"),
            ("dir",    36, "Dir"),
            ("op",     50, "Op"),
            ("origin", 130, "Origin"),
            ("path",   0,  "Path"),
            ("size",   56, "Size"),
        ):
            ws_tree.heading(c, text=label)
            ws_tree.column(c, width=w, anchor="w", stretch=(c == "path"))
        ws_tree.tag_configure("in",  foreground=_GRN)
        ws_tree.tag_configure("out", foreground=_CYN)
        ws_vsb = ttk.Scrollbar(ws_tree_frame, orient="vertical", command=ws_tree.yview)
        ws_hsb = ttk.Scrollbar(ws_tree_frame, orient="horizontal", command=ws_tree.xview)
        ws_tree.configure(yscrollcommand=ws_vsb.set, xscrollcommand=ws_hsb.set)
        ws_vsb.pack(side=tk.RIGHT, fill=tk.Y)
        ws_hsb.pack(side=tk.BOTTOM, fill=tk.X)
        ws_tree.pack(fill=tk.BOTH, expand=True)

        ws_ctrl = tk.Frame(ws_left, bg=_PNL)
        ws_ctrl.pack(fill=tk.X, padx=8, pady=4)
        ws_auto_scroll = tk.BooleanVar(value=True)
        ws_paused = tk.BooleanVar(value=False)
        tk.Checkbutton(ws_ctrl, text="Auto-scroll", variable=ws_auto_scroll,
                       bg=_PNL, fg=_FG, selectcolor=_BG,
                       activebackground=_PNL).pack(side=tk.LEFT)
        tk.Checkbutton(ws_ctrl, text="Pause", variable=ws_paused,
                       bg=_PNL, fg=_FG, selectcolor=_BG,
                       activebackground=_PNL).pack(side=tk.LEFT, padx=8)
        _ws_row_data: dict[str, dict] = {}
        _WS_MAX_ROWS = 800

        def _ws_clear():
            for i in ws_tree.get_children():
                ws_tree.delete(i)
            _ws_row_data.clear()
        _btn(ws_ctrl, "Clear", _ws_clear).pack(side=tk.LEFT)

        # ── RIGHT: WS hook editor ───────────────────────────────────────────
        _lbl(ws_right, "WebSocket Hook Editor", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
        ws_info_var = tk.StringVar(value="← Select a WS message")
        _lbl(ws_right, textvariable=ws_info_var, fg=_ACC, bg=_PNL).pack(anchor="w", padx=8)

        ws_row1 = tk.Frame(ws_right, bg=_PNL); ws_row1.pack(fill=tk.X, padx=8, pady=2)
        ws_row2 = tk.Frame(ws_right, bg=_PNL); ws_row2.pack(fill=tk.X, padx=8, pady=2)
        ws_row3 = tk.Frame(ws_right, bg=_PNL); ws_row3.pack(fill=tk.X, padx=8, pady=2)

        _lbl(ws_row1, "Name:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        ws_name_entry = _entry(ws_row1, width=14)
        ws_name_entry.pack(side=tk.LEFT, padx=(2, 10))

        _lbl(ws_row1, "Dir:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        ws_dir_var = tk.StringVar(value="*")
        ttk.Combobox(ws_row1, textvariable=ws_dir_var,
                     values=["*", "in", "out"], width=5, state="readonly",
                     font=("Consolas", 10)).pack(side=tk.LEFT, padx=(2, 10))

        _lbl(ws_row1, "Op:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        ws_op_var = tk.StringVar(value="*")
        ttk.Combobox(ws_row1, textvariable=ws_op_var,
                     values=["*", "text", "bin"], width=6, state="readonly",
                     font=("Consolas", 10)).pack(side=tk.LEFT)

        _lbl(ws_row2, "Pattern url:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        ws_pat_entry = _entry(ws_row2, width=34)
        ws_pat_entry.insert(0, r".*")
        ws_pat_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)

        _lbl(ws_row3, "Origin url:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
        ws_origin_entry = _entry(ws_row3, width=20)
        ws_origin_entry.insert(0, "*")
        ws_origin_entry.pack(side=tk.LEFT, padx=(2, 0), fill=tk.X, expand=True)
        _lbl(ws_row3, "  (target / * / host)", fg=_DIM, bg=_PNL).pack(side=tk.LEFT)

        # ── Match mode (internal — no GUI combobox) ──────────────────────
        # The match mode is now determined AUTOMATICALLY from whether a
        # message has been captured:
        #   - match_payload is set (user clicked a row) → "exact"
        #     The hook fires ONLY when the incoming payload equals the
        #     captured one byte-for-byte.
        #   - match_payload is empty (no row clicked) → "any"
        #     The hook fires on EVERY message matching the path/dir/op
        #     filters above.
        # The user no longer needs to choose — the WHEN panel makes it
        # obvious what will trigger the hook.
        ws_match_mode_var = tk.StringVar(value="any")
        # Hidden payload holder — populated when a row is selected, sent to
        # the hook store on Save. Storing as bytes (not as a StringVar) so
        # binary payloads survive intact.
        ws_match_payload_holder: list[bytes] = [b""]

        # ── Bottom rows (packed FIRST at bottom so they're always visible) ──
        # CRITICAL: these must be packed with side=BOTTOM BEFORE the
        # expandable when→then pane, otherwise the pane eats all vertical
        # space and pushes the Save button off-screen.
        ws_btn_row = tk.Frame(ws_right, bg=_PNL)
        ws_btn_row.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(0, 4))
        ws_save_status = _lbl(ws_right, text="", fg=_DIM, bg=_PNL)
        ws_save_status.pack(side=tk.BOTTOM, anchor="w", padx=8, pady=(0, 2))

        # ── When → Then layout ─────────────────────────────────────────────
        # The editor is split into two side-by-side panels that make the
        # hook's behavior self-explanatory:
        #
        #   LEFT  (WHEN): "If the site sends or tries to send:"
        #      Read-only text preview of match_payload — the exact content
        #      that triggers this hook. Auto-populated when you click a
        #      message in the traffic log. Shows "(any message on this
        #      path)" when match_mode is "any" (no specific trigger).
        #
        #   RIGHT (THEN): "Immediately changes to my hook before it gets
        #                  actually sent:"
        #      Editable hex editor (top) + live decoded text preview
        #      (bottom). This is body_bytes — what the hook replaces the
        #      original message with.
        #
        # The arrow between the panels (►) reinforces the data flow:
        # captured message → your replacement.
        ws_when_then_pane = tk.PanedWindow(ws_right, orient=tk.HORIZONTAL,
                                           bg=_BG, sashwidth=5, sashrelief="flat")
        ws_when_then_pane.pack(fill=tk.BOTH, expand=True, padx=8, pady=(6, 4))

        # ── LEFT: WHEN panel (match condition — read-only) ─────────────
        ws_when_frame = tk.Frame(ws_when_then_pane, bg=_PNL)
        ws_when_then_pane.add(ws_when_frame, minsize=260, width=300)

        _lbl(ws_when_frame, "If the site sends or tries to send:",
             fg=_RED, bg=_PNL,
             font=("Consolas", 9, "bold")).pack(anchor="w", padx=4, pady=(2, 0))
        _lbl(ws_when_frame, "(read-only — auto-filled when you click a message)",
             fg=_DIM, bg=_PNL, font=("Consolas", 8)).pack(anchor="w", padx=4)

        ws_when_text = scrolledtext.ScrolledText(
            ws_when_frame, bg="#0a0505", fg=_YLW, insertbackground=_ACC,
            font=("Consolas", 10), relief="flat", wrap="word")
        ws_when_text.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)
        ws_when_text.config(state="disabled")
        _bind_editor_keys(ws_when_text)

        # ── RIGHT: THEN panel (replacement — editable hex + preview) ────
        ws_then_frame = tk.Frame(ws_when_then_pane, bg=_PNL)
        ws_when_then_pane.add(ws_then_frame, minsize=320, width=380)

        _lbl(ws_then_frame, "Immediately changes to my hook before it gets actually sent:",
             fg=_GRN, bg=_PNL,
             font=("Consolas", 9, "bold")).pack(anchor="w", padx=4, pady=(2, 0))

        # THEN panel internally split: hex (editable, top) + text (preview, bottom)
        ws_then_split = tk.PanedWindow(ws_then_frame, orient=tk.VERTICAL,
                                       bg=_BG, sashwidth=4, sashrelief="flat")
        ws_then_split.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)

        ws_hex_frame = tk.Frame(ws_then_split, bg=_PNL)
        ws_txt_frame = tk.Frame(ws_then_split, bg=_PNL)
        ws_then_split.add(ws_hex_frame, minsize=120)
        ws_then_split.add(ws_txt_frame, minsize=80)

        _lbl(ws_hex_frame, "Hex  (editable)", fg=_YLW, bg=_PNL,
             font=("Consolas", 9, "bold")).pack(anchor="w", padx=4, pady=(2, 0))
        ws_hex_box = scrolledtext.ScrolledText(
            ws_hex_frame, bg="#050505", fg=_GRN, insertbackground=_ACC,
            font=("Consolas", 10), relief="flat", undo=True, wrap="none")
        ws_hex_box.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)
        _bind_editor_keys(ws_hex_box)

        _lbl(ws_txt_frame, "Text  (live preview)", fg=_DIM, bg=_PNL,
             font=("Consolas", 9, "bold")).pack(anchor="w", padx=4, pady=(2, 0))
        ws_txt_box = scrolledtext.ScrolledText(
            ws_txt_frame, bg="#080808", fg=_FG, insertbackground=_ACC,
            font=("Consolas", 10), relief="flat", wrap="none")
        ws_txt_box.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)
        ws_txt_box.config(state="disabled")
        _bind_editor_keys(ws_txt_box)

        # ── Hex / text helpers + live sync ──────────────────────────────────
        def _bytes_to_hexdump(data: bytes) -> str:
            """Canonical hex dump: '00000000  48 54 54 50 ...' (16 bytes/line).
            Empty input → empty string (the editor shows a placeholder hint)."""
            if not data:
                return ""
            lines = []
            for off in range(0, len(data), 16):
                chunk = data[off:off + 16]
                hex_part = " ".join(f"{b:02x}" for b in chunk)
                # Wider gap after the 8th byte for readability.
                if len(chunk) > 8:
                    hex_part = hex_part[:24] + " " + hex_part[24:]
                lines.append(f"{off:08x}  {hex_part}")
            return "\n".join(lines)

        def _hexdump_to_bytes(text: str) -> bytes | None:
            """Parse a hex dump back to bytes. Returns None on invalid input.

            Strips an optional 8-hex-digit offset prefix per line; every
            whitespace-separated token after that MUST be valid hex with an
            even number of digits. Anything else → None (caller shows error).
            """
            out = bytearray()
            for line in text.splitlines():
                stripped = re.sub(r"^[0-9a-fA-F]{8}\s+", "", line).strip()
                if not stripped:
                    continue
                for tok in stripped.split():
                    if not re.fullmatch(r"[0-9a-fA-F]+", tok):
                        return None
                    if len(tok) % 2 != 0:
                        return None
                    out.extend(bytes.fromhex(tok))
            return bytes(out)

        def _ws_sync_text_from_hex(*_):
            """Live-update the read-only text pane from the hex pane."""
            raw = ws_hex_box.get("1.0", "end-1c")
            parsed = _hexdump_to_bytes(raw)
            if parsed is None:
                ws_save_status.config(text="Hex invalid — odd digits or non-hex char.", fg=_RED)
                return
            decoded = parsed.decode("utf-8", errors="replace")
            # Replace non-printable control chars (except common whitespace)
            display = "".join(
                c if (c.isprintable() or c in "\n\r\t") else "·"
                for c in decoded
            )
            ws_txt_box.config(state="normal")
            ws_txt_box.delete("1.0", "end")
            ws_txt_box.insert("1.0", display)
            ws_txt_box.config(state="disabled")
            ws_save_status.config(
                text=f"Replacement OK — {len(parsed)} byte(s). Preview below the hex editor.",
                fg=_DIM)

        ws_hex_box.bind("<KeyRelease>", _ws_sync_text_from_hex)
        # Also re-sync on paste / cut (Tk doesn't always fire KeyRelease for those)
        for _seq in ("<<Paste>>", "<<Cut>>", "<<PasteSelection>>"):
            ws_hex_box.bind(_seq, lambda e: (_ws_sync_text_from_hex(), "break")[1])

        # ── WHEN panel updater ───────────────────────────────────────────
        # Refreshes the left "If the site sends..." panel to show the
        # current match_payload (decoded as text) or a placeholder when
        # match_mode is "any" (no specific trigger). Called on row select,
        # on match_mode change, and on hook edit.
        def _ws_update_when_panel(*_):
            mode = ws_match_mode_var.get().strip().lower() or "any"
            mp = ws_match_payload_holder[0]
            ws_when_text.config(state="normal")
            ws_when_text.delete("1.0", "end")
            if mode == "any" or not mp:
                # "any" = no specific trigger → show placeholder.
                # The hook will fire on EVERY message matching the
                # path/dir/op filters above.
                ws_when_text.insert("1.0",
                    "(any message matching the path/dir/op filters above)\n\n"
                    "Every message on this WebSocket path will be replaced.\n\n"
                    "Click a message in the traffic log on the left to\n"
                    "target a specific payload — only that exact content\n"
                    "will trigger the hook.")
                ws_when_text.config(state="disabled", fg=_DIM)
            else:
                # exact / contains / regex — show the decoded match_payload.
                # For legacy hooks saved with contains/regex modes, we still
                # display the payload (or pattern) so the user can see what
                # the trigger is.
                if mode == "regex":
                    ws_when_text.insert("1.0",
                        f"[regex pattern — legacy hook]\n"
                        f"{mp.decode('utf-8', 'replace')}\n\n"
                        f"Incoming payloads matching this regex will be replaced.")
                else:
                    decoded = mp.decode("utf-8", errors="replace")
                    display = "".join(
                        c if (c.isprintable() or c in "\n\r\t") else "·"
                        for c in decoded
                    )
                    ws_when_text.insert("1.0", display)
                ws_when_text.config(state="disabled", fg=_YLW)

        # trace_add fires when match_mode_var is set programmatically
        # (on row select, on hook edit). No combobox needed — the mode
        # is auto-determined from whether a payload was captured.
        ws_match_mode_var.trace_add("write", _ws_update_when_panel)
        # Initial render
        _ws_update_when_panel()

        def _ws_on_select(_event):
            sel = ws_tree.selection()
            if not sel:
                return
            data = _ws_row_data.get(sel[0])
            if not data:
                return
            payload = data.get("payload") or b""
            _dir_raw = data.get("direction", "?")
            _dir_disp = "[->] IN" if _dir_raw == "in" else ("[<-] OUT" if _dir_raw == "out" else _dir_raw.upper())
            _op_disp = (data.get("op_name") or "?").upper()
            ws_info_var.set(
                f"{_dir_disp}  {_op_disp}  {data['ws_url']}  "
                f"[{_fmt_size(len(payload))}]")
            # Load the payload into the THEN panel's hex editor (the
            # replacement starts as a copy of the original — user edits
            # from there). Re-rendered canonically.
            ws_hex_box.config(state="normal")
            ws_hex_box.delete("1.0", "end")
            ws_hex_box.insert("1.0", _bytes_to_hexdump(payload))
            _ws_sync_text_from_hex()
            # Auto-populate hook editor fields from selected message
            ws_dir_var.set(_dir_raw if _dir_raw in ("in", "out") else "*")
            ws_op_var.set({1: "text", 2: "bin"}.get(data.get("opcode", 0), "*"))
            ws_pat_entry.delete(0, "end")
            try:
                ws_pat_entry.insert(0, re.escape(urlparse(data["ws_url"]).path or "/"))
            except Exception:
                ws_pat_entry.insert(0, r".*")
            ws_origin_entry.delete(0, "end")
            ws_origin_entry.insert(0, data.get("origin", "") or "*")
            if not ws_name_entry.get().strip():
                ws_name_entry.delete(0, "end")
                ws_name_entry.insert(0, f"ws_{_dir_raw}_{_op_disp.lower()}")
            # Capture THIS message's payload as the WHEN trigger and
            # default to "exact" — clicking a row means "replace THIS
            # specific message". User can switch to any/contains/regex
            # via the combobox if they want a broader match.
            ws_match_payload_holder[0] = bytes(payload)
            ws_match_mode_var.set("exact")   # triggers _ws_update_when_panel
        ws_tree.bind("<<TreeviewSelect>>", _ws_on_select)

        # ── Disk persistence + Save / Test buttons ──────────────────────────
        _ws_hook_save_to_disk, _ws_hook_delete_from_disk, _ws_hook_load_all = \
            _make_hook_store("MyWSHooks", extra_binary_fields=("match_payload",))

        def _ws_save_hook():
            name = ws_name_entry.get().strip()
            if not name:
                messagebox.showwarning("S2L", "Hook name is required.")
                return
            raw_hex = ws_hex_box.get("1.0", "end-1c")
            body_bytes = _hexdump_to_bytes(raw_hex)
            if body_bytes is None:
                messagebox.showerror("S2L",
                    "Invalid hex — fix the highlighted error before saving.")
                return
            if not body_bytes:
                messagebox.showwarning("S2L",
                    "Hook payload is empty — nothing to inject.")
                return
            pat_str = ws_pat_entry.get().strip() or r".*"
            try:
                re.compile(pat_str)
            except re.error as e:
                messagebox.showerror("S2L", f"Invalid pattern: {e}")
                return
            op_map = {"text": 1, "bin": 2}
            opcode = op_map.get(ws_op_var.get(), 0)
            # Match mode is AUTO-DETERMINED: if a payload was captured
            # (user clicked a message in the traffic log), use "exact" —
            # the hook fires ONLY on that specific payload. If no payload
            # was captured, use "any" — the hook fires on every message
            # matching the path/dir/op filters.
            match_payload = ws_match_payload_holder[0]
            if match_payload:
                match_mode = "exact"
            else:
                match_mode = "any"
                match_payload = b""
            hook = {
                "name":           name,
                "direction":      ws_dir_var.get(),
                "opcode":         opcode,
                "pattern":        pat_str,
                "origin_url":     ws_origin_entry.get().strip() or "*",
                "body_bytes":     body_bytes,
                "enabled":        True,
                "match_mode":     match_mode,
                "match_payload":  match_payload,
            }
            with _gui_ws_hooks_lock:
                existing = next((h for h in _gui_ws_hooks if h["name"] == name), None)
                if existing is not None:
                    if not messagebox.askyesno("S2L",
                            f"A WS hook named '{name}' already exists.\nReplace it?"):
                        return
                    _gui_ws_hooks[:] = [h for h in _gui_ws_hooks if h["name"] != name]
                _gui_ws_hooks.append(hook)
            _ws_hook_save_to_disk(hook)
            _render_ws_hook_rows()
            _trigger_desc = ("exact payload" if match_mode == "exact"
                             else "any message on path")
            ws_save_status.config(
                text=f"Saved '{name}'  [dir={hook['direction']} op={ws_op_var.get()} "
                     f"trigger={_trigger_desc}]  {hook['pattern']}",
                fg=_GRN)
            log(f"GUI WS hook saved: '{name}' [dir={hook['direction']} op={ws_op_var.get()} "
                f"trigger={_trigger_desc}] {hook['pattern']}", "HOOK")

        def _ws_test_hook():
            pat_str = ws_pat_entry.get().strip()
            test_path = simpledialog.askstring("Test WS Hook",
                "Enter a WS path to test (e.g. /ws/chat):", parent=root)
            if test_path is None:
                return
            try:
                matched = bool(re.search(pat_str, test_path, re.IGNORECASE))
            except re.error as e:
                messagebox.showerror("S2L", f"Pattern error: {e}")
                return
            ws_save_status.config(
                text=f"{'MATCH' if matched else 'NO MATCH'}  pattern against '{test_path}'",
                fg=_GRN if matched else _RED)

        # Populate the button row (frame was created earlier and packed at
        # BOTTOM — we just add the buttons now that the callbacks exist).
        _btn(ws_btn_row, "Save WS Hook", _ws_save_hook,
             font=("Consolas", 10, "bold")).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 4))
        _btn(ws_btn_row, "Test Pattern", _ws_test_hook,
             font=("Consolas", 9)).pack(side=tk.LEFT)

        # ── BOTTOM: Active WS Hooks ─────────────────────────────────────────
        _lbl(ws_bot, "Active WebSocket Hooks", fg=_ACC,
             font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(4, 2))
        ws_hooks_scroll = tk.Frame(ws_bot, bg=_PNL)
        ws_hooks_scroll.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))
        ws_h_sb = ttk.Scrollbar(ws_hooks_scroll, orient="vertical")
        ws_h_sb.pack(side=tk.RIGHT, fill=tk.Y)
        ws_hooks_canvas = tk.Canvas(ws_hooks_scroll, bg=_PNL, bd=0,
                                    highlightthickness=0, yscrollcommand=ws_h_sb.set)
        ws_hooks_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ws_h_sb.config(command=ws_hooks_canvas.yview)
        ws_hooks_frame = tk.Frame(ws_hooks_canvas, bg=_PNL)
        ws_h_win = ws_hooks_canvas.create_window((0, 0), window=ws_hooks_frame, anchor="nw")

        def _on_ws_hf_configure(_e):
            ws_hooks_canvas.configure(scrollregion=ws_hooks_canvas.bbox("all"))
        def _on_ws_canvas_configure(e):
            ws_hooks_canvas.itemconfig(ws_h_win, width=e.width)
        ws_hooks_frame.bind("<Configure>", _on_ws_hf_configure)
        ws_hooks_canvas.bind("<Configure>", _on_ws_canvas_configure)

        _WS_HOOK_COLS   = ("Name", "Dir", "Op", "Origin", "Pattern", "On", "", "")
        _WS_HOOK_WIDTHS = (12, 5, 5, 14, 22, 4, 4, 4)

        def _render_ws_hook_rows():
            for w in ws_hooks_frame.winfo_children():
                w.destroy()
            for col_idx, (col_name, col_w) in enumerate(zip(_WS_HOOK_COLS, _WS_HOOK_WIDTHS)):
                _lbl(ws_hooks_frame, col_name, fg=_ACC, bg=_PNL,
                     font=("Consolas", 9, "bold"),
                     width=col_w, anchor="w").grid(row=0, column=col_idx, sticky="w", padx=3, pady=2)
            with _gui_ws_hooks_lock:
                hooks_copy = list(_gui_ws_hooks)
            if not hooks_copy:
                _lbl(ws_hooks_frame, "No WS hooks saved yet.", fg=_DIM, bg=_PNL).grid(
                    row=1, column=0, columnspan=8, sticky="w", padx=4, pady=4)
                return
            op_name_map = {1: "TEXT", 2: "BIN", 0: "*"}
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                _lbl(ws_hooks_frame, h["name"], bg=_PNL, fg=row_fg,
                     width=12, anchor="w").grid(row=row_idx, column=0, sticky="w", padx=3)
                _lbl(ws_hooks_frame, h.get("direction", "*") or "*", bg=_PNL,
                     fg=_YLW if h["enabled"] else _DIM,
                     width=5, anchor="w").grid(row=row_idx, column=1, sticky="w", padx=3)
                _lbl(ws_hooks_frame, op_name_map.get(h.get("opcode", 0), "*"), bg=_PNL,
                     fg=_GRN if h["enabled"] else _DIM,
                     width=5, anchor="w").grid(row=row_idx, column=2, sticky="w", padx=3)
                _origin_disp = h.get("origin_url", "*") or "*"
                if len(_origin_disp) > 14:
                    _origin_disp = _origin_disp[:13] + "…"
                _lbl(ws_hooks_frame, _origin_disp, bg=_PNL,
                     fg=_CYN if h["enabled"] else _DIM,
                     width=14, anchor="w").grid(row=row_idx, column=3, sticky="w", padx=3)
                _pat_disp = h["pattern"][:22] + ("…" if len(h["pattern"]) > 22 else "")
                _lbl(ws_hooks_frame, _pat_disp, bg=_PNL, fg=_DIM,
                     width=22, anchor="w").grid(row=row_idx, column=4, sticky="w", padx=3)
                # Enabled toggle
                bv = tk.BooleanVar(value=h["enabled"])
                def _make_ws_toggle(hook_ref, var):
                    def _toggle():
                        with _gui_ws_hooks_lock:
                            hook_ref["enabled"] = var.get()
                        _ws_hook_save_to_disk(hook_ref)
                        _render_ws_hook_rows()
                    return _toggle
                tk.Checkbutton(ws_hooks_frame, variable=bv, command=_make_ws_toggle(h, bv),
                               bg=_PNL, fg=_FG, selectcolor=_BG,
                               activebackground=_PNL,
                               width=2).grid(row=row_idx, column=5, padx=3)
                # Edit — load hook back into the editor
                def _make_ws_edit(hook_ref):
                    def _edit():
                        ws_name_entry.delete(0, "end")
                        ws_name_entry.insert(0, hook_ref["name"])
                        ws_dir_var.set(hook_ref.get("direction", "*") or "*")
                        op_val = hook_ref.get("opcode", 0)
                        ws_op_var.set({1: "text", 2: "bin"}.get(op_val, "*"))  # combobox values are lowercase
                        ws_pat_entry.delete(0, "end")
                        ws_pat_entry.insert(0, hook_ref["pattern"])
                        ws_origin_entry.delete(0, "end")
                        ws_origin_entry.insert(0, hook_ref.get("origin_url", "*") or "*")
                        ws_hex_box.config(state="normal")
                        ws_hex_box.delete("1.0", "end")
                        ws_hex_box.insert("1.0", _bytes_to_hexdump(hook_ref["body_bytes"]))
                        _ws_sync_text_from_hex()
                        # Restore instanced-match fields. IMPORTANT: set the
                        # payload holder BEFORE the mode var, because setting
                        # the mode var triggers _ws_update_when_panel (via the
                        # trace_add binding) which reads the holder to render
                        # the WHEN panel. Wrong order = panel renders empty.
                        ws_match_payload_holder[0] = hook_ref.get("match_payload", b"") or b""
                        _mm = hook_ref.get("match_mode", "any") or "any"
                        ws_match_mode_var.set(_mm)   # triggers _ws_update_when_panel
                        ws_save_status.config(text=f"Editing '{hook_ref['name']}'", fg=_YLW)
                    return _edit
                _btn(ws_hooks_frame, "Ed", _make_ws_edit(h),
                     font=("Consolas", 8), bg="#051a05",
                     fg=_GRN).grid(row=row_idx, column=6, padx=3)
                # Delete
                def _make_ws_del(hook_name):
                    def _del():
                        with _gui_ws_hooks_lock:
                            _gui_ws_hooks[:] = [x for x in _gui_ws_hooks if x["name"] != hook_name]
                        _ws_hook_delete_from_disk(hook_name)
                        _render_ws_hook_rows()
                        ws_save_status.config(text=f"Deleted '{hook_name}'.", fg=_YLW)
                    return _del
                _btn(ws_hooks_frame, "X", _make_ws_del(h["name"]),
                     bg="#1a0505", fg=_RED,
                     font=("Consolas", 8)).grid(row=row_idx, column=7, padx=3)

        _render_ws_hook_rows()

        def _load_ws_hooks_from_disk() -> None:
            """Load previously saved WS hooks from site_data/MyWSHooks/{host}/.

            Defaults include the new instanced-match fields so legacy hooks
            saved before this feature existed still load — they get
            match_mode="any" and an empty match_payload, which preserves the
            old "override every message on this path" behavior exactly.
            """
            for hook in _ws_hook_load_all({"name": "?", "direction": "*",
                                            "opcode": 0, "pattern": ".*",
                                            "origin_url": "*", "enabled": True,
                                            "match_mode": "any",
                                            "match_payload": b""}):
                # Backfill any missing instanced fields on legacy entries
                hook.setdefault("match_mode", "any")
                hook.setdefault("match_payload", b"")
                with _gui_ws_hooks_lock:
                    if hook["name"] not in [h["name"] for h in _gui_ws_hooks]:
                        _gui_ws_hooks.append(hook)
                        log(f"Loaded WS hook '{hook['name']}' "
                            f"(match={hook.get('match_mode', 'any')}) from disk", "HOOK")

        root.after(100, lambda: (_load_ws_hooks_from_disk(), _render_ws_hook_rows()))

        # ── WS log queue poller ─────────────────────────────────────────────
        def _ws_poll():
            if not ws_paused.get():
                count = 0
                while count < 60:
                    try:
                        entry = _gui_ws_log_queue.get_nowait()
                    except queue.Empty:
                        break
                    sz = entry["size"]
                    if sz >= 1024 * 1024:
                        sz_s = f"{sz / 1024 / 1024:.1f}MB"
                    elif sz >= 1024:
                        sz_s = f"{sz // 1024}KB"
                    elif sz > 0:
                        sz_s = f"{sz}B"
                    else:
                        sz_s = "—"
                    _dir = entry["direction"]
                    dir_disp = "[->] IN" if _dir == "in" else ("[<-] OUT" if _dir == "out" else _dir)
                    _op_upper = (entry.get("op_name") or "").upper()
                    iid = ws_tree.insert("", "end", values=(
                        entry["ts"], dir_disp, _op_upper,
                        entry["origin"], entry["path"], sz_s,
                    ), tags=(entry["direction"],))
                    _ws_row_data[iid] = entry
                    if len(ws_tree.get_children()) > _WS_MAX_ROWS:
                        old = ws_tree.get_children()[0]
                        _ws_row_data.pop(old, None)
                        ws_tree.delete(old)
                    if ws_auto_scroll.get():
                        ws_tree.see(iid)
                    count += 1
            root.after(150, _ws_poll)
        root.after(150, _ws_poll)

        # ══════════════════════════════════════════════════════════════════
        # TAB 3: Firefox Proxy  (only shown when FIREFOX_PROXY is enabled)
        # Live log of every request the MITM forward proxy has seen — real
        # browser traffic, any site, not just MAIN_HOST — on the left, with
        # a hook editor for overriding matching request bodies before
        # they're forwarded. Same shape as Tab 1, mirrored for requests.
        # ══════════════════════════════════════════════════════════════════
        if FIREFOX_PROXY:
            tab_fwd = tk.Frame(nb, bg=_BG)
            nb.add(tab_fwd, text="  Firefox Proxy")

            fwd_v = tk.PanedWindow(tab_fwd, orient=tk.VERTICAL, bg=_BG, sashwidth=5)
            fwd_v.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
            fwd_top_area = tk.Frame(fwd_v, bg=_BG)
            fwd_bot_area = tk.Frame(fwd_v, bg=_PNL)
            fwd_v.add(fwd_top_area, minsize=280)
            fwd_v.add(fwd_bot_area, minsize=160)

            fwd_h = tk.PanedWindow(fwd_top_area, orient=tk.HORIZONTAL, bg=_BG, sashwidth=5)
            fwd_h.pack(fill=tk.BOTH, expand=True)
            fwd_left  = tk.Frame(fwd_h, bg=_PNL)
            fwd_right = tk.Frame(fwd_h, bg=_PNL)
            fwd_h.add(fwd_left,  minsize=460)
            fwd_h.add(fwd_right, minsize=340)

            _lbl(fwd_left, "Request Log  (live browser traffic via MITM)", fg=_ACC,
                 font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
            fwd_tree_frame = tk.Frame(fwd_left, bg=_PNL)
            fwd_tree_frame.pack(fill=tk.BOTH, expand=True, padx=4)
            fwd_tree = ttk.Treeview(fwd_tree_frame, columns=("ts", "method", "path", "status", "ct", "size"),
                                     show="headings", selectmode="browse")
            for c, w, label in (("ts", 70, "Time"), ("method", 60, "Method"), ("path", 0, "Host + Path"),
                                 ("status", 60, "Status"), ("ct", 130, "Content-Type"), ("size", 68, "Size")):
                fwd_tree.heading(c, text=label)
                fwd_tree.column(c, width=w, anchor="w", stretch=(c == "path"))
            fwd_tree.tag_configure("GET", foreground=_GRN)
            fwd_tree.tag_configure("POST", foreground=_YLW)
            fwd_tree.tag_configure("err", foreground=_RED)
            fwd_vsb = ttk.Scrollbar(fwd_tree_frame, orient="vertical", command=fwd_tree.yview)
            fwd_tree.configure(yscrollcommand=fwd_vsb.set)
            fwd_vsb.pack(side=tk.RIGHT, fill=tk.Y)
            fwd_tree.pack(fill=tk.BOTH, expand=True)
            _fwd_row_data: dict = {}

            _lbl(fwd_right, "Request Body / Headers", fg=_ACC,
                 font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(6, 2))
            fwd_info_var = tk.StringVar(value="← Select a request")
            _lbl(fwd_right, textvariable=fwd_info_var, fg=_ACC, bg=_PNL).pack(anchor="w", padx=8)
            fwd_body_box = _text_box(fwd_right)
            fwd_body_box.pack(fill=tk.BOTH, expand=True, padx=8, pady=4)
            _bind_editor_keys(fwd_body_box)

            def _fwd_on_select(_event):
                sel = fwd_tree.selection()
                if not sel:
                    return
                data = _fwd_row_data.get(sel[0])
                if not data:
                    return
                fwd_info_var.set(f"{data['method']}  {data['path']}  [{data['status']}]")
                fwd_body_box.config(state="normal")
                fwd_body_box.delete("1.0", "end")
                hdrs = data.get("req_headers") or {}
                hdr_txt = "\n".join(f"{k}: {v}" for k, v in hdrs.items())
                body_raw = data.get("body") or b""
                try:
                    body_txt = body_raw.decode("utf-8", errors="replace")
                except Exception:
                    body_txt = body_raw.hex()
                fwd_body_box.insert("1.0", hdr_txt + ("\n\n" if hdr_txt else "") + body_txt)
                fwd_name_entry.delete(0, "end")
                fwd_pat_entry.delete(0, "end")
                fwd_pat_entry.insert(0, re.escape(data["path"].split("] ", 1)[-1]))
                fwd_method_var.set(data["method"] if data["method"] in _RH_METHOD_OPTS else "*")
            fwd_tree.bind("<<TreeviewSelect>>", _fwd_on_select)

            _lbl(fwd_right, "New Request Hook (overrides the body of matching live requests):",
                 fg=_ACC, bg=_PNL, wraplength=380, justify="left").pack(anchor="w", padx=8, pady=(6, 2))
            fwd_row1 = tk.Frame(fwd_right, bg=_PNL); fwd_row1.pack(fill=tk.X, padx=8, pady=2)
            _lbl(fwd_row1, "Name:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
            fwd_name_entry = _entry(fwd_row1, width=12)
            fwd_name_entry.pack(side=tk.LEFT, padx=(2, 10))
            _lbl(fwd_row1, "Method:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
            fwd_method_var = tk.StringVar(value="*")
            ttk.Combobox(fwd_row1, textvariable=fwd_method_var, values=_RH_METHOD_OPTS,
                         width=7, state="readonly", font=("Consolas", 10)).pack(side=tk.LEFT)
            fwd_row2 = tk.Frame(fwd_right, bg=_PNL); fwd_row2.pack(fill=tk.X, padx=8, pady=2)
            _lbl(fwd_row2, "Path pattern:", fg=_ACC, bg=_PNL).pack(side=tk.LEFT)
            fwd_pat_entry = _entry(fwd_row2, width=30)
            fwd_pat_entry.insert(0, r".*")
            fwd_pat_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(2, 0))
            fwd_save_status = _lbl(fwd_right, text="", fg=_DIM, bg=_PNL)
            fwd_save_status.pack(anchor="w", padx=8, pady=(0, 2))

            _fwd_hook_save_to_disk, _fwd_hook_delete_from_disk, _fwd_hook_load_all = \
                _make_hook_store("MyFwdHooks")

            def _render_fwd_hook_rows():
                for w in fwd_hooks_frame.winfo_children():
                    w.destroy()
                for col_idx, col_name in enumerate(("Name", "Method", "Pattern", "On", "")):
                    _lbl(fwd_hooks_frame, col_name, fg=_ACC, bg=_PNL,
                         font=("Consolas", 9, "bold")).grid(row=0, column=col_idx, sticky="w", padx=6)
                with _gui_fwd_req_hooks_lock:
                    hooks_copy = list(_gui_fwd_req_hooks)
                if not hooks_copy:
                    _lbl(fwd_hooks_frame, "No hooks saved yet.", fg=_DIM, bg=_PNL).grid(
                        row=1, column=0, columnspan=5, sticky="w", padx=4, pady=4)
                    return
                for row_idx, h in enumerate(hooks_copy, start=1):
                    fg = _FG if h["enabled"] else _DIM
                    _lbl(fwd_hooks_frame, h["name"], bg=_PNL, fg=fg).grid(row=row_idx, column=0, sticky="w", padx=6)
                    _lbl(fwd_hooks_frame, h["method"], bg=_PNL, fg=fg).grid(row=row_idx, column=1, sticky="w", padx=6)
                    _lbl(fwd_hooks_frame, h["pattern"][:36], bg=_PNL, fg=_DIM).grid(row=row_idx, column=2, sticky="w", padx=6)
                    bv = tk.BooleanVar(value=h["enabled"])
                    def _mk_toggle(hook_ref=h, var=bv):
                        def _t():
                            with _gui_fwd_req_hooks_lock:
                                hook_ref["enabled"] = var.get()
                            _fwd_hook_save_to_disk(hook_ref)
                            _render_fwd_hook_rows()
                        return _t
                    tk.Checkbutton(fwd_hooks_frame, variable=bv, command=_mk_toggle(),
                                   bg=_PNL, selectcolor=_BG).grid(row=row_idx, column=3, padx=3)
                    def _mk_del(name=h["name"]):
                        def _d():
                            with _gui_fwd_req_hooks_lock:
                                _gui_fwd_req_hooks[:] = [x for x in _gui_fwd_req_hooks if x["name"] != name]
                            _fwd_hook_delete_from_disk(name)
                            _render_fwd_hook_rows()
                        return _d
                    _btn(fwd_hooks_frame, "X", _mk_del(), bg="#1a0505", fg=_RED,
                         font=("Consolas", 8)).grid(row=row_idx, column=4, padx=3)

            def _fwd_save_hook():
                name = fwd_name_entry.get().strip()
                if not name:
                    messagebox.showwarning("S2L", "Hook name is required.")
                    return
                body_full = fwd_body_box.get("1.0", "end-1c")
                body_only = body_full.split("\n\n", 1)[-1] if "\n\n" in body_full else body_full
                pat = fwd_pat_entry.get().strip() or r".*"
                try:
                    re.compile(pat)
                except re.error as e:
                    messagebox.showerror("S2L", f"Invalid pattern: {e}")
                    return
                hook = {"name": name, "method": fwd_method_var.get(), "pattern": pat,
                        "body_bytes": body_only.encode("utf-8"), "enabled": True}
                with _gui_fwd_req_hooks_lock:
                    _gui_fwd_req_hooks[:] = [h for h in _gui_fwd_req_hooks if h["name"] != name] + [hook]
                _fwd_hook_save_to_disk(hook)
                _render_fwd_hook_rows()
                fwd_save_status.config(text=f"Saved '{name}'", fg=_GRN)
                log(f"GUI fwd request hook saved: '{name}' [{hook['method']}] {hook['pattern']}", "HOOK")

            fwd_btn_row = tk.Frame(fwd_right, bg=_PNL)
            fwd_btn_row.pack(fill=tk.X, padx=8, pady=(0, 6))
            _btn(fwd_btn_row, "Save Request Hook", _fwd_save_hook,
                 font=("Consolas", 10, "bold")).pack(side=tk.LEFT, fill=tk.X, expand=True)

            _lbl(fwd_bot_area, "Active Firefox-Proxy Request Hooks", fg=_ACC,
                 font=("Consolas", 11, "bold"), bg=_PNL).pack(anchor="w", padx=8, pady=(4, 2))
            fwd_hooks_frame = tk.Frame(fwd_bot_area, bg=_PNL)
            fwd_hooks_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 4))

            def _load_fwd_hooks_from_disk():
                for hook in _fwd_hook_load_all({"name": "?", "method": "*", "pattern": ".*", "enabled": True}):
                    with _gui_fwd_req_hooks_lock:
                        if hook["name"] not in [h["name"] for h in _gui_fwd_req_hooks]:
                            _gui_fwd_req_hooks.append(hook)

            root.after(100, lambda: (_load_fwd_hooks_from_disk(), _render_fwd_hook_rows()))

            def _fwd_poll():
                count = 0
                while count < 60:
                    try:
                        entry = _gui_fwd_log_queue.get_nowait()
                    except queue.Empty:
                        break
                    sz = entry["size"]
                    sz_s = f"{sz/1024:.1f}KB" if sz >= 1024 else (f"{sz}B" if sz else "—")
                    tagn = "err" if isinstance(entry["status"], int) and entry["status"] >= 400 else entry["method"]
                    iid = fwd_tree.insert("", "end", values=(
                        entry["ts"], entry["method"], entry["path"], entry["status"],
                        entry["ct"], sz_s), tags=(tagn,))
                    _fwd_row_data[iid] = entry
                    if len(fwd_tree.get_children()) > _MAX_ROWS:
                        old = fwd_tree.get_children()[0]
                        _fwd_row_data.pop(old, None)
                        fwd_tree.delete(old)
                    fwd_tree.see(iid)
                    count += 1
                root.after(150, _fwd_poll)
            root.after(150, _fwd_poll)

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
                    _origin_val = entry.get("origin", "") or ""
                    _web_type_val = entry.get("web_type", "") or "None"
                    iid  = tree.insert("", "end", values=(
                        entry["ts"], method, _origin_val, entry["path"],
                        status, _web_type_val, entry["ct"], sz_s,
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

def _make_cffi_session(device: str, base_headers: dict):
    """Build a curl_cffi Session, trying impersonation targets in order until
    one is accepted by the installed curl_cffi build. Raises the last error
    only if every candidate (including the chrome136 catch-all) fails."""
    cached     = _cffi_impersonate_cache.get(device)
    candidates = (cached,) + _IMPERSONATE_BY_DEVICE.get(device, ("chrome136",)) if cached \
                 else _IMPERSONATE_BY_DEVICE.get(device, ("chrome136",))
    last_exc: Exception | None = None
    for target in candidates:
        try:
            s = _cffi_requests.Session(impersonate=target, verify=False)
            s.headers.update(base_headers)
            _cffi_impersonate_cache[device] = target
            return s
        except Exception as e:
            last_exc = e
            continue
    raise last_exc or RuntimeError("no usable curl_cffi impersonation target")

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
            return _make_cffi_session(d, base_headers)
        except Exception as e:
            log(f"curl_cffi init failed, falling back: {e}", "WARN")

    cfg = _next_cf_config(mobile)
    if _CLOUDSCRAPER_OK:
        try:
            s = cloudscraper.create_scraper(browser=cfg, delay=0)
            # MUST override UA after create_scraper — cloudscraper injects old UAs
            # (often Chrome 80-83) that fail browser version checks on modern sites.
            s.headers.update(base_headers)
            s.keep_alive = True
            s.verify     = False
            adapter = requests.adapters.HTTPAdapter(
                max_retries      = _RETRY_POLICY,
                pool_connections = POOL_CONNECTIONS,
                pool_maxsize     = POOL_MAXSIZE,
                pool_block       = False,
            )
            s.mount("https://", adapter)
            s.mount("http://",  adapter)
            return s
        except Exception as e:
            log(f"cloudscraper init failed, falling back to requests: {e}", "WARN")
    else:
        log("ai-cloudscraper not installed (pip install ai-cloudscraper) "
            "— falling back to plain requests", "WARN")

    s = requests.Session()
    s.headers.update(base_headers)
    s.verify = False
    adapter = requests.adapters.HTTPAdapter(
        max_retries      = _RETRY_POLICY,
        pool_connections = POOL_CONNECTIONS,
        pool_maxsize     = POOL_MAXSIZE,
        pool_block       = False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

def _make_cloudscraper_session(device: str | None = None) -> object:
    """Build a cloudscraper session directly (bypassing curl_cffi).

    Used as a fallback when curl_cffi fails on specific networks/hosts.
    """
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
    cfg = _next_cf_config(mobile)
    if not _CLOUDSCRAPER_OK:
        raise RuntimeError("ai-cloudscraper not installed — pip install ai-cloudscraper")
    s = cloudscraper.create_scraper(browser=cfg, delay=0)
    s.headers.update(base_headers)
    s.keep_alive = True
    s.verify     = False
    adapter = requests.adapters.HTTPAdapter(
        max_retries=_RETRY_POLICY,
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
        pool_block=False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

def _make_requests_session(device: str | None = None) -> object:
    """Build a plain requests session (last-resort fallback, no CF bypass)."""
    d = device or _effective_device()
    ua = _sanitize_ua(UA_PROFILES.get(d, UA_PROFILES["macintosh"]))
    ch = _SEC_CH_UA.get(d, _SEC_CH_UA.get("desktop", {}))
    base_headers = {
        "User-Agent":                ua,
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br" if _BROTLI_OK else "gzip, deflate",
        "DNT":                       "1",
        "Upgrade-Insecure-Requests": "1",
        **ch,
    }
    s = requests.Session()
    s.headers.update(base_headers)
    s.verify = False
    adapter = requests.adapters.HTTPAdapter(
        max_retries=_RETRY_POLICY,
        pool_connections=POOL_CONNECTIONS,
        pool_maxsize=POOL_MAXSIZE,
        pool_block=False,
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
        reported against some CDNs). A status-only check misses these entirely.
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

    # Modern Turnstile + "Just a moment" interstitials.
    # These return 200 with a JS challenge body that doesn't always carry
    # the classic jschl_vc tokens — the CF team rotates signatures frequently.
    if status in (200, 403, 503):
        if (b"just a moment" in head
                or b"cf-turnstile" in head
                or b"cf_turnstile" in head
                or b"turnstile.min.js" in head
                or b"challenges.cloudflare.com/turnstile" in head
                or (b"checking if the site connection is secure" in head)
                or (b"enable javascript and cookies" in head)
                or (b"cf-please-wait" in head)):
            return True

    # Generic WAF blocks — "access denied", "blocked",
    # "request blocked" returned as HTML with a 403/503. These were slipping
    # through because _is_raw_block_text only catches SHORT non-HTML bodies.
    if status in (403, 503) and b"<html" in head:
        if (b"access denied" in head
                or b"request blocked" in head
                or b"blocked by" in head
                or b"security check" in head
                or b"verify you are human" in head):
            return True

    return False

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

def _is_raw_block_text(body: bytes, status: int) -> bool:
    # Never treat 429 as a WAF block — it's a legitimate rate-limit signal.
    # Treating it as a block causes a retry that doubles the request rate,
    # pushing the API into a sustained 429 loop.
    if status not in (200, 403, 503):
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
)

def _is_bot_page(body: bytes, status: int = 200, path: str = "") -> bool:
    """Return True if the body is a bot-detection / CAPTCHA page.

    Only fires on HTML responses — JSON API 403s are never bot pages.
    Path-exempt prefixes (internal API routes, /api/, etc.) are skipped.
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
_CLIENT_TTL   = 1800  # 30 min idle before session is evicted

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
        # Evict stale sessions to avoid memory growth. Close them properly —
        # each session holds a connection pool (up to POOL_MAXSIZE=64 conns)
        # and leaking them accumulates file descriptors over time.
        stale = [k for k, t in _CLIENT_LAST.items() if now - t > _CLIENT_TTL]
    for k in stale:
        old_sess = None
        with _CLIENT_LOCK:
            old_sess = _CLIENT_SESSIONS.pop(k, None)
            _CLIENT_LAST.pop(k, None)
        if old_sess is not None:
            try: old_sess.close()
            except Exception: pass
    with _CLIENT_LOCK:
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
            # Use _looks_json_or_text to avoid false-positives
            # on brotli-compressed data that has many high-bit bytes.
            if _looks_json_or_text(data):
                return data   # curl_cffi already decoded it
            if _BROTLI_OK:
                # FIXED: try the one-shot decompress first; if it fails (some
                # servers send brotli streams with trailing junk that confuses
                # the one-shot API), fall back to the streaming decompressor
                # which is more tolerant. We saw "Empty body (decompression
                # failure)" warnings on certain API endpoints where curl_cffi returned a brotli
                # body it couldn't auto-decompress; our one-shot _brotli.decompress
                # also failed silently and we returned the raw compressed bytes
                # as if they were the decoded body — which downstream code
                # correctly flagged as "len(body) == 0 but Content-Length > 0".
                try:
                    return _brotli.decompress(data)
                except Exception:
                    pass
                try:
                    # Streaming decompressor tolerates trailing junk and partial streams
                    d = _brotli.Decompressor()
                    out = d.process(data) + d.finish()
                    if out:
                        return out
                except Exception:
                    pass
                # Both failed — data may already be decoded (curl_cffi) but
                # didn't pass _looks_json_or_text. Return as-is; the caller's
                # empty-body recovery will re-fetch with Accept-Encoding: identity.
                return data
            log("brotli response received but 'brotli' library not installed "
                "(pip install brotli --break-system-packages) — body may be garbled", "WARN")
            return data

        if enc == "zstd":
            if _looks_json_or_text(data):
                return data
            if _ZSTD_OK:
                # FIXED: try one-shot first, then streaming. ZstdDecompressor.decompress
                # requires the frame to declare its size; some servers send
                # streaming frames without that, causing decompress() to raise
                # "could not determine content size in frame header". The
                # streaming_reader() API handles both cases.
                try:
                    return _zstd.ZstdDecompressor().decompress(data)
                except Exception:
                    pass
                try:
                    dctx = _zstd.ZstdDecompressor()
                    out = b"".join(dctx.stream_reader(data))
                    if out:
                        return out
                except Exception:
                    pass
            else:
                log("zstd response but 'zstandard' not installed "
                    "(pip install zstandard --break-system-packages)", "WARN")
            return data

    except Exception:
        pass

    return data

def guess_mime(path: str) -> str:
    """Guess a MIME type from a path extension.

    Uses the stdlib mimetypes module (dynamic — reads /etc/mime.types at
    import, supplemented by the add_type() calls at the top of this file).
    Falls back to application/octet-stream when the extension is unknown or
    absent, which is the safest default for binary-safe proxying.
    """
    if not path:
        return "application/octet-stream"
    # Strip query string / fragment if present (can happen when guess_mime
    # is called on a URL path instead of a local file path).
    path = path.split("?", 1)[0].split("#", 1)[0]
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

def _all_set_cookies(response) -> list:
    """Return every Set-Cookie value on `response`, uncorrupted.

    dict(response.headers) / response.headers.items() silently comma-join
    repeated header names into a single string — which is actively wrong
    for Set-Cookie specifically: a cookie's own Expires=<day>, <date>
    attribute already contains a comma, so a comma-joined multi-cookie
    value is ambiguous to split back apart. A login response that sets 3
    real cookies (session token, CSRF token, a challenge/verification
    token — hCaptcha and friends all set their own) arrives at the browser
    as one mangled value; the browser then drops some of them or mis-splits
    it, which shows up as exactly what it looks like: a verification widget
    stuck in a loop because its own token cookie never made it through,
    "duplicate cookie" console warnings on refresh, and login/session/
    WebSocket-auth state that depends on any of the dropped cookies quietly
    breaking. curl_cffi's Headers exposes .get_list(); urllib3/requests'
    raw header dict exposes .getlist(). Try both; only fall back to the
    single (lossy) value if neither multi-value accessor is available.
    """
    h = getattr(response, "headers", None)
    for meth_name in ("get_list", "getlist"):
        meth = getattr(h, meth_name, None)
        if callable(meth):
            try:
                vals = [v for v in meth("Set-Cookie") if v]
                if vals:
                    return vals
            except Exception:
                pass
    raw_headers = getattr(getattr(response, "raw", None), "headers", None)
    for meth_name in ("getlist", "get_all"):
        meth = getattr(raw_headers, meth_name, None)
        if callable(meth):
            try:
                vals = list(meth("Set-Cookie"))
                if vals:
                    return vals
            except Exception:
                pass
    try:
        single = response.headers.get("Set-Cookie")
    except Exception:
        single = None
    return [single] if single else []

def _resp_headers_dict(response) -> dict:
    """dict(response.headers), with Set-Cookie repaired to a real list
    instead of the comma-mangled single string dict() would silently give.
    Use this everywhere a fetched response's headers feed into filter_resp
    (which already knows how to handle a list of cookies correctly) —
    plain dict(response.headers) must never be used for that purpose."""
    h = dict(response.headers)
    # dict(response.headers) can hand back "set-cookie" in whatever casing
    # that particular header-object implementation uses (curl_cffi's Headers
    # always lower-cases keys) — Python dict keys are case-sensitive even
    # though HTTP header names aren't, so just adding h["Set-Cookie"] = [...]
    # below would create a SECOND key sitting right next to the original
    # mangled one instead of replacing it, and Werkzeug would then send BOTH:
    # the one mangled comma-joined header AND the correctly-split ones,
    # duplicating every single cookie on the wire. Remove any casing of the
    # key first so there is exactly one Set-Cookie entry to work with.
    for k in list(h.keys()):
        if k.lower() == "set-cookie":
            del h[k]
    cookies = _all_set_cookies(response)
    if len(cookies) > 1:
        h["Set-Cookie"] = cookies
    elif len(cookies) == 1:
        h["Set-Cookie"] = cookies[0]
    return h

def _flatten_cookiejar(cookies_obj, prefer_host: str = "") -> dict:
    """Flatten a requests-style cookie jar into a plain name→value dict for
    use as the `cookies=` kwarg on an outgoing request.

    dict(session.cookies) / session.cookies.get(name) look completely safe
    but are landmines here: requests' RequestsCookieJar (used by cloudscraper
    and plain requests.Session) raises CookieConflictError, and curl_cffi's
    Cookies raises CookieConflict, the instant the jar holds two cookies
    with the same name under two different domains. Sessions here are
    per-client and live for the whole run (see _get_client_session), so
    this isn't an edge case — a mirrored page whose CDN/analytics/embeds
    each set their own "session", "_ga", "__cf_bm", etc. hits it on the
    second or third distinct host, and it used to take the whole request
    down with it (or, where it was wrapped in a bare except, silently
    produce a wrong response instead).

    We walk the jar's raw entries ourselves instead of the dict-like
    accessors, so a name collision just picks a value instead of raising:
    prefer whichever cookie's domain actually matches `prefer_host` (the
    upstream host we're about to call), otherwise keep the last one seen.
    """
    if not cookies_obj:
        return {}
    if isinstance(cookies_obj, dict):
        return dict(cookies_obj)
    host = (prefer_host or "").lower()
    chosen_domain: dict[str, str] = {}
    out: dict[str, str] = {}
    try:
        jar = getattr(cookies_obj, "jar", cookies_obj)  # curl_cffi wraps a .jar; requests IS the jar
        for cookie in jar:
            name = getattr(cookie, "name", None)
            if not name:
                continue
            domain = (getattr(cookie, "domain", "") or "").lstrip(".").lower()
            if name in out and host:
                prev = chosen_domain.get(name, "")
                if prev and host.endswith(prev) and not (domain and host.endswith(domain)):
                    continue   # keep the previous, more domain-specific match
            out[name] = getattr(cookie, "value", "") or ""
            chosen_domain[name] = domain
        return out
    except Exception as e:
        log(f"cookie jar flatten failed, falling back to get_dict(): {_short_exc(e)}", "DEBUG")
        for attr in ("get_dict", "items"):
            fn = getattr(cookies_obj, attr, None)
            if not fn:
                continue
            try:
                return dict(fn())
            except Exception:
                continue
        return {}

def _needs_isolation(body: bytes) -> bool:
    """Heuristic: does this HTML look like it needs Cross-Origin isolation?

    Sites that use SharedArrayBuffer / WASM threads (eaglercraft-style clients
    are the canonical case) need COOP+COEP on the top-level document or the
    browser silently refuses to hand them a working SharedArrayBuffer — the
    app initializes just far enough to paint its background/canvas and then
    hangs forever waiting on a threading primitive that was never granted.
    A real, unproxied deployment of such a site sends these headers itself
    (from its own server config); scan for the tell-tale APIs so we can
    reproduce that even when the origin's own header choice didn't survive
    being fetched through us.
    """
    if not body:
        return False
    head = body[:65536]
    has_sab = b"SharedArrayBuffer" in head
    has_atomics = b"Atomics.wait" in head
    has_worker = b"new Worker(" in head and b"postMessage" in head
    # A bare .wasm reference is NOT a strong enough signal — many sites load
    # WASM without needing threads/cross-origin isolation. Requiring COEP for
    # them blocks all cross-origin sub-resources (analytics, fonts, ads) that
    # don't send CORP headers. Only trigger if WASM co-occurs with a threading
    # primitive, or if SAB/Atomics/Worker+postMessage is present on its own.
    has_wasm = b".wasm" in head
    return (has_sab or has_atomics
            or (has_worker and (has_wasm or has_sab)))

def filter_resp(headers: dict, body: bytes = b"", is_top_level_html: bool = False) -> dict:
    # Strip security headers that block our local proxy, plus hop-by-hop headers.
    # NOTE: Cross-Origin-Opener-Policy / Cross-Origin-Embedder-Policy are
    # deliberately NOT in this skip-set — they're PASSIVE now (see below).
    # The origin's own COOP/COEP choice flows through untouched, and we only
    # ADD safe defaults on top-level HTML that looks like it needs isolation
    # but wasn't sent any. No flag to remember to flip per site.
    skip = _HOP_BY_HOP | {
        "content-encoding",
        "content-security-policy", "content-security-policy-report-only",
        "x-frame-options", "strict-transport-security", "x-content-type-options",
        "cross-origin-resource-policy", "permissions-policy",
        "nel", "report-to", "reporting-endpoints",
        # Additional headers that block rendering or reveal proxy
        "x-permitted-cross-domain-policies",  # Adobe Flash/PDF cross-domain
        "x-download-options",                 # IE download behavior
        "x-dns-prefetch-control",            # DNS prefetch control
        "expect-ct",                          # Certificate Transparency
        # Strip upstream CORS — we set our own wildcard below
        "access-control-allow-origin",
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-expose-headers",
        "access-control-max-age",
    }
    out = {k: v for k, v in headers.items() if k.lower() not in skip}
    # CORS: if the browser sent an Origin header, echo it back specifically
    # (not "*") so credentialed cross-origin requests work. With MULTIPORT,
    # CDN assets are served on different ports — a fetch() from the main page
    # to a CDN port is cross-origin and needs Access-Control-Allow-Credentials
    # if the SPA sends credentials:"include". Wildcard "*" blocks credentialed
    # requests, so we only use it when no Origin header is present.
    _req_origin = ""
    try:
        _req_origin = flask_request.headers.get("Origin", "") if flask_request else ""
    except RuntimeError:
        pass
    if _req_origin:
        out["Access-Control-Allow-Origin"] = _req_origin
        out["Access-Control-Allow-Credentials"] = "true"
        out["Vary"] = (out.get("Vary", "") + ", Origin").lstrip(", ")
        out["Access-Control-Expose-Headers"] = "*"
    else:
        out["Access-Control-Allow-Origin"]  = "*"
        out["Access-Control-Expose-Headers"] = "*"
    # Cross-Origin-Resource-Policy: cross-origin — required so that resources served
    # through S2L (images, scripts, fonts from CDN) can be consumed by cross-origin HTML
    # pages also going through S2L (cross-origin iframes, etc.). Without this, when COEP is
    # active any resource without CORP is blocked by the browser.
    out["Cross-Origin-Resource-Policy"] = "cross-origin"
    # COOP/COEP are now PASSIVE: whatever the origin itself decided (kept above,
    # since they're no longer in `skip`) just flows through untouched — no
    # manual flag to remember to flip per site. The one thing we DO add
    # proactively is a safety net for the top-level HTML document: if the
    # origin needed isolation but didn't send the headers (common when the
    # isolation was actually coming from whatever host originally served the
    # page, e.g. a static host's own server config, rather than the app
    # itself), auto-apply the least-disruptive pair only there.
    has_coop = any(k.lower() == "cross-origin-opener-policy"   for k in out)
    has_coep = any(k.lower() == "cross-origin-embedder-policy" for k in out)
    if is_top_level_html and not (has_coop or has_coep) and _needs_isolation(body):
        out["Cross-Origin-Opener-Policy"]   = "same-origin"
        out["Cross-Origin-Embedder-Policy"] = "credentialless"
    if "Set-Cookie" in out:
        # Determine if the request came in over HTTPS or from localhost.
        # Secure cookies require HTTPS except on localhost (per browser specs).
        # For non-localhost HTTP access, forcing Secure silently drops all cookies.
        _req_scheme = flask_request.scheme if flask_request else "http"
        _req_host = flask_request.host if flask_request else "localhost"
        _is_localhost = (_req_scheme == "http"
                         and (_req_host.startswith("localhost")
                              or _req_host.startswith("127.0.0.1")
                              or _req_host.startswith("::1")))
        _can_secure = (_req_scheme == "https") or _is_localhost
        def _rewrite_cookie(c: str) -> str:
            # Set SameSite=None + Secure so cookies work cross-origin (CDN
            # sub-iframe on a different port needs this). Without SameSite=None,
            # browsers block cookies on cross-origin requests.
            c = re.sub(r";\s*SameSite=[^;]+", "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Secure\b",        "", c, flags=re.IGNORECASE)
            c = re.sub(r";\s*Partitioned\b", "", c, flags=re.IGNORECASE)
            c = c.rstrip("; ").rstrip()
            if _can_secure:
                c += "; SameSite=None; Secure"
            else:
                # Non-localhost HTTP: SameSite=Lax works without Secure.
                # SameSite=None would be rejected without Secure.
                c += "; SameSite=Lax"
            # Strip Domain= entirely rather than rewriting it — a Domain that
            # doesn't match the host the browser is actually on gets the WHOLE
            # cookie silently rejected. Dropping it makes the cookie host-only,
            # which is always valid for whatever host is currently in the
            # address bar.
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

    v4 HARDENING: Also strips any header value that still contains localhost /
    127.0.0.1 / 0.0.0.0 / the proxy port — some sites read these to detect
    proxy/MITM usage and trigger CAPTCHAs ("verify you are human"). We make
    a final sweep AFTER the standard rewrite to catch anything that slipped
    through (e.g. a Referer built from window.location that the injector missed).
    """
    proxy_markers = (
        f"localhost:{PORT}", f"127.0.0.1:{PORT}", f"0.0.0.0:{PORT}",
        "localhost:8080", "127.0.0.1:8080",  # common defaults
        "://localhost", "://127.0.0.1", "://0.0.0.0",
    )
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
        elif kl in ("x-forwarded-for", "x-forwarded-host", "x-forwarded-proto",
                     "x-real-ip", "via", "forwarded"):
            # Strip proxy-revealing headers entirely — their presence is
            # itself a fingerprint that some bot-detection systems key on.
            del fwd[key]
        # Final sweep — replace any remaining localhost reference in any
        # header value. This catches edge cases like a custom Referer built
        # from JS that read window.location before the injector patched it.
        val = fwd.get(key)
        if isinstance(val, str):
            for marker in proxy_markers:
                if marker in val:
                    fwd[key] = val.replace(marker, origin_base)
                    val = fwd[key]
                    break

def rewrite_abs_urls(html: bytes) -> bytes:
    """Rewrite absolute MAIN_HOST URLs inside HTML *attribute values* → proxy-relative.

    Only targets attribute-value contexts (href=, src=, action=, data-href=, etc.)
    so that inline JSON/JS (hydration payloads like __NEXT_DATA__, etc.) is NOT modified.
    Touching bare JSON strings breaks SPA frameworks because their JS uses the
    full URLs for API calls and dynamic manifests.

    Before: href="https://example.com/watch?v=abc"
    After:  href="/watch?v=abc"

    JSON (untouched): "url":"https://example.com/watch?v=abc"
    """
    if not html or not MAIN_HOST:
        return html
    # Match only when preceded by an HTML attribute value opener (=" or =')
    # This excludes bare JSON strings and JS string literals outside attributes.
    # The trailing negative lookahead requires the host to actually END there
    # (next char is /, ", ', :, ?, # or nothing) — without it, a bare prefix
    # match would also strip a longer domain sharing the same prefix, or any other domain
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

def _fmt_host(host: str) -> str:
    """Normalize a hostname for display: strip www. prefix and port.
    Returns 'host' (no scheme, no port) for consistent log lines.
    Examples: 'www.example.com:443' → 'example.com'
              'cdn.example.com' → 'cdn.example.com'
    """
    if not host:
        return ""
    # Strip user:pass@ if present
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    # Strip port
    if ":" in host:
        host = host.split(":", 1)[0]
    # Strip www. prefix
    if host.startswith("www."):
        host = host[4:]
    return host

def log_req(method: str, status: int, host: str, path: str, size: int, tag: str = "") -> None:
    """Structured one-line request log + GUI traffic-log entry."""
    prefix = f"[{tag}] " if tag else ""
    _h = _fmt_host(host)
    log(f"{prefix}{method} {status} {_h}{path} {_fmt_size(size)}", "→")
    _gui_push_raw(method, path, status, "", b"",
                  display_tag=f"[{tag.upper()}]" if tag else "",
                  origin=_fmt_host(host),
                  _skip_log=True)   # log_req already logged to terminal above

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
        # Standalone named function so .toString() can be re-injected inside
        # Workers. proxyPort is always the TRUE main port (MP), never the
        # current document's location.port — CDN sub-iframes run on :8087 etc.,
        # but /__s2l_ext__/ only exists on the main app.
        'function __s2l_core(M,HP,EXT,proxyHost,proxyPort){'
          'var PX="http://"+proxyHost+":"+proxyPort;'
          'function rw(u){'
          # Protocol-relative URLs (//cdn.host/path) → prepend https: BEFORE the
          # guard check, or they'd bypass the proxy and hit the real CDN.
          'if(typeof u==="string"&&u.length>2&&u[0]==="/"&&u[1]==="/")u="https:"+u;'
          'if(!u||typeof u!="string"||u[0]=="/"||u.indexOf("://")<0)return u;'
          'try{var p=new URL(u,"http://"+proxyHost+"/");'
          'if(p.hostname===proxyHost)return u;'
          # WS URLs must be handled BEFORE the MAIN_HOST check, or wss://MAIN_HOST
          # gets rewritten to http://localhost:PORT (invalid WS URL → SyntaxError).
          'if(p.protocol==="wss:"||p.protocol==="ws:"){'
            'var wh=p.host;'
            'var cdnp=HP[p.hostname]||HP[p.host];'
            'if(p.host===M||p.host==="www."+M)return "ws://"+proxyHost+":"+proxyPort+p.pathname+(p.search||"")+(p.hash||"");'
            'if(cdnp)return "ws://"+proxyHost+":"+cdnp+p.pathname+(p.search||"")+(p.hash||"");'
            'return "ws://"+proxyHost+":"+proxyPort+"/__s2l_ws_ext__/"+wh+p.pathname+(p.search||"")+(p.hash||"");'
          '}'
          'if(p.host===M||p.host==="www."+M)return PX+p.pathname+(p.search||"")+(p.hash||"");'
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
              # Accept string / URL / Location inputs + options-object form
              # ({protocols: ...}) that some modern libraries use.
              'var ru=url;'
              'if(typeof url==="string")ru=rw(url);'
              'else if(url&&url.href)ru=rw(url.href);'
              'else if(url&&typeof url.toString==="function"){try{ru=rw(url.toString());}catch(e){}}'
              'if(protos&&typeof protos==="object"&&!Array.isArray(protos)&&protos.protocols)protos=protos.protocols;'
              'return protos?new _WS(ru,protos):new _WS(ru);'
            '}'
            'S2LWebSocket.prototype=_WS.prototype;'
            '["CONNECTING","OPEN","CLOSING","CLOSED"].forEach(function(k){S2LWebSocket[k]=_WS[k];});'
            'self.WebSocket=S2LWebSocket;'
          '}}catch(e){}'
          # Nested workers: rewrite the script URL only (no full prelude
          # re-injection — would cause unbounded blob-wrapping recursion).
          'try{var _W2=self.Worker;if(_W2){'
            'self.Worker=function(u,o){return new _W2(typeof u==="string"?rw(u):u,o);};'
            'self.Worker.prototype=_W2.prototype;'
          '}}catch(e){}'
          # importScripts() inside workers: schemeless args (webpack chunks)
          # were resolved against the blob: URL → "invalid URL". Resolve them
          # against PX instead; schemed args go through rw() as usual.
          # v8.1 fix: the generic /^[a-z][a-z0-9+.\-]*:/ regex matched
          # "localhost:8080/..." as if "localhost:" were a URL scheme, sending
          # a schemeless-but-ported string through rw() which then mis-parsed
          # it as a host → produced "/__s2l_ext__/localhost:8080/__s2l_ext__/..."
          # (double-rewrite). Whitelist the real URL schemes instead.
          'try{var _is=self.importScripts;if(typeof _is==="function"){'
            'self.importScripts=function(){'
              'var args=Array.prototype.slice.call(arguments).map(function(u){'
                'if(typeof u!=="string")return u;'
                'if(/^(?:https?|wss?|blob|data|file|ftp):/i.test(u))return rw(u);'
                'try{return new URL(u,PX+"/").href;}catch(e){return PX+u;}'
              '});'
              'return _is.apply(self,args);'
            '};'
          '}}catch(e){}'
          'return rw;'
        '}'
        'var rw=__s2l_core(M,HP,EXT,location.hostname,MP);'
        # ── location.protocol / origin override ────────────────────────────────
        # Forces location.protocol="https:" so SPAs building URLs like
        # `"wss://"+location.host` or `location.protocol+"//"+host` don't produce
        # https://localhost:8080 → ERR_SSL_PROTOCOL_ERROR. The rw() wrappers
        # downgrade wss→ws and https→http internally. Side effect: sites that
        # check location.protocol to decide ws vs wss will pick wss — the proxy
        # then refuses upstream and caches the refusal (5 min) so retries are
        # silent. try/catch: location.protocol is non-configurable in some engines.
        'try{'
          'var _locP2=Object.getPrototypeOf(location);'
          'Object.defineProperty(_locP2,"protocol",{'
            'configurable:true,get:function(){return "https:";}'
          '});'
          'Object.defineProperty(_locP2,"origin",{'
            'configurable:true,get:function(){'
              'return "https://"+location.host;'
            '}'
          '});'
        '}catch(e){}'
        # ── location.href / .assign() / .replace() / window.open() patch ───────
        # Catch raw navigations (location.href=..., .assign(), .replace(),
        # window.open()) — none of the fetch/XHR/src patches above intercept
        # these, so without rewriting the browser would connect to the REAL
        # host directly. All are configurable accessors/methods in major engines.
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
        # SDKs do postMessage(msg, 'https://real-origin') — browser drops it
        # because the receiver is localhost, never the expected origin; the SDK's
        # fallback then navigates to the real host → "connection refused". Fix:
        # relax any specific (non-"*") targetOrigin to "*". Safe locally because
        # no third untrusted party can intercept a wildcard message. Applies in
        # every realm, both directions, for any SDK.
        'try{'
          'var _pmD=Object.getOwnPropertyDescriptor(Window.prototype,"postMessage");'
          'if(!_pmD){'
            # Fallback: wrap window.postMessage directly
            'var _pm0=window.postMessage;'
            'window.postMessage=function(m,t,tr){'
              'if(typeof t==="string"&&t!=="*")t="*";'
              'return _pm0.apply(this,[m,t,tr]);'
            '};'
          '} else if(typeof _pmD.value==="function"){'
            'var _pm1=_pmD.value;'
            'Object.defineProperty(Window.prototype,"postMessage",{'
              'configurable:true,writable:true,enumerable:true,'
              'value:function(m,t,tr){'
                'if(typeof t==="string"&&t!=="*")t="*";'
                'return _pm1.apply(this,[m,t,tr]);'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── MessageEvent.prototype.origin spoof ────────────────────────────────
        # After relaxing targetOrigin, the SDK still checks event.origin !==
        # 'https://expected-origin' and ignores the message. Patch the getter so
        # localhost:MP → "https://"+MAIN_HOST, localhost:CDN_PORT → that CDN's
        # real domain. Must NOT collapse every localhost to MAIN_HOST: app↔embed
        # handshakes verify each other's CDN origin, and a mismatch makes some
        # wrappers re-point the embed iframe at the real remote host.
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
        # These hand a raw HTML string to the native parser, which sets src/href/
        # action attributes during parsing — that step never goes through the
        # .src/.setAttribute property patches (only JS-level access does). The
        # MutationObserver below catches them after parsing.
        #
        # IMPORTANT (v8.1 fix): the previous implementation wrapped the SETTERS
        # themselves, which broke React/Vue/Svelte hydration. SPAs serialize
        # their server-rendered DOM, then hydrate by reading innerHTML /
        # comparing it to the expected virtual DOM. Patching the setter caused
        # subtle differences (URL rewrites that produced different strings than
        # what the framework expected) → hydration mismatch → framework aborts
        # → blank page on modern SPA sites (Discord, Twitter, Next.js apps).
        # The MutationObserver + the .src setter patches already cover >99% of
        # real-world cases. document.write is kept patched because it's rare
        # in modern apps and trivially safe (no hydration concerns).
        'function _rwHtmlStr(html){'
          'try{'
            'return html.replace('
              '/((?:src|href|poster|data-src|data-href|action)\\s*=\\s*)(["\\x27])https?:\\/\\/([a-zA-Z0-9\\-._:]+)((?:(?!\\2)[^<>])*)\\2/gi,'
              'function(m,pre,q,host,tail){return pre+q+rw("https://"+host+tail)+q;}'
            ');'
          '}catch(e){return html;}'
        '}'
        'try{'
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
        # Workers have their own global scope and don't inherit window.fetch —
        # without wrapping, their network calls bypass the proxy and hit the
        # real host (hung because hosts file points it at us). Worse, WASM-thread
        # apps that Atomics.wait() on a worker freeze the whole tab. Fix: inject
        # __s2l_core as a prelude via a same-origin blob: URL wrapping importScripts().
        '[["Worker","__s2l__worker_orig"],["SharedWorker","__s2l__sharedworker_orig"]].forEach(function(pair){'
          'var Ctor=window[pair[0]];if(!Ctor)return;'
          'function Wrapped(scriptURL,opts){'
            'var abs;try{abs=new URL(scriptURL,location.href).href;}catch(e){abs=scriptURL;}'
            'var ru=typeof abs==="string"?rw(abs):abs;'
            # Only prefix PX when ru has NO URL scheme (true bare/relative).
            # The old `ru.indexOf("http")!==0` check wrongly prefixed blob:/data:
            # URLs → "http://localhost:8080blob:http://..." → invalid → every
            # WASM/audio worker failed to start.
            # v8.1: whitelist real URL schemes — same fix as importScripts.
            'if(typeof ru==="string"&&!/^(?:https?|wss?|blob|data|file|ftp):/i.test(ru))ru=PX+ru;'
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
        # Catches dynamically-created elements whose .src is set via JS before
        # insertion in the DOM (MutationObserver misses them — not yet in DOM).
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
        # Some frameworks use setAttribute instead of the .src property.
        'try{'
          'var _ifSA=HTMLIFrameElement.prototype.setAttribute;'
          'HTMLIFrameElement.prototype.setAttribute=function(name,value){'
            'if(name&&name.toLowerCase()==="src"&&typeof value==="string"&&value.indexOf("http")===0)value=rw(value);'
            'return _ifSA.call(this,name,value);'
          '};'
        '}catch(e){}'
        # ── CSS backgroundImage + cssText setter patch ────────────────────────
        # Catches: element.style.backgroundImage = "url(https://cdn.example.com/bg.png)"
        'try{'
          'var _cssRw=function(v){return typeof v==="string"?v.replace(/url\\((["\']?)(https?:\\/\\/[^)"\'\\s<>]+)\\1\\)/gi,function(m,q,u){var r=rw(u);return r!==u?"url("+q+r+q+")":m;}):v;};'
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
        # try/catch: document.documentElement can be null in sandboxed/srcdoc
        # iframes — .observe(null) would throw and halt all subsequent patches.
        'try{'
        'var _A=["src","href","poster","data-src","action"];'
        'function _rn(n){if(!n||n.nodeType!==1)return;'
        '_A.forEach(function(a){var v=n.getAttribute(a);if(v&&v.indexOf("http")===0){var r=rw(v);if(r!==v)n.setAttribute(a,r);}});'
        'var ss=n.getAttribute("srcset");'
        'if(ss){var rs=ss.split(/,\\s+/).map(function(e){var b=e.trim().split(/ +/);if(b[0]&&b[0].indexOf("http")===0){var r=rw(b[0]);if(r!==b[0])b[0]=r;}return b.join(" ");}).join(", ");if(rs!==ss)n.setAttribute("srcset",rs);}}'
        'function _ra(r){try{var e=r.querySelectorAll("[src],[href],[srcset],[poster],[data-src],[action]");for(var i=0;i<e.length;i++)_rn(e[i]);}catch(x){}}'
        # attributeFilter catches src/href/poster changes via setAttribute OR
        # reflected property setters (a.href=..., video.poster=...) we didn't
        # patch individually. rw() is idempotent on proxied/relative URLs, so
        # re-triggering this observer is a harmless no-op.
        'new MutationObserver(function(ms){ms.forEach(function(m){'
          'if(m.type==="attributes"){_rn(m.target);}'
          'else{m.addedNodes.forEach(function(n){_rn(n);_ra(n);});}'
        '});}).observe(document.documentElement,{'
          'childList:true,subtree:true,'
          'attributes:true,attributeFilter:["src","href","poster","data-src","srcset","action"]'
        '});'
        'if(document.readyState!=="loading")_ra(document);else document.addEventListener("DOMContentLoaded",function(){_ra(document);});'
        '}catch(e){}'
        # ── HTMLFormElement.prototype.submit() patch ────────────────────────────
        # form.submit() from JS bypasses click-driven navigation and the action
        # attribute may have changed since the scanner last ran.
        'try{'
          'var _formSubmit=HTMLFormElement.prototype.submit;'
          'HTMLFormElement.prototype.submit=function(){'
            'var a=this.getAttribute("action");'
            'if(a&&a.indexOf("http")===0){var r=rw(a);if(r!==a)this.setAttribute("action",r);}'
            'return _formSubmit.call(this);'
          '};'
        '}catch(e){}'
        # ── iframe.srcdoc patch ─────────────────────────────────────────────────
        # srcdoc embeds a full HTML document as a literal string — never passes
        # through server-side HTML rewriting and srcdoc isn't a URL attribute.
        # Rewrite any src/href/poster/data-src URLs inside the string before
        # assignment so the parsed iframe already points at the proxy.
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
        # ── Service worker + cache handling ─────────────────────────────────
        # Do NOT blanket-unregister SWs or nuke caches — SPAs rely on their SW
        # for JS bundle caching and wiping hangs SPA init (blank page). The
        # fetch/XHR/WS patches already handle URL rewriting. Only unregister SWs
        # whose scope is NOT the current origin (stale from a previous target).
        'try{'
          'if(navigator.serviceWorker){'
            'navigator.serviceWorker.getRegistrations().then(function(r){'
              'r.forEach(function(s){'
                'try{'
                  'var sw=new URL(s.scope);'
                  'if(sw.hostname!==location.hostname){s.unregister();}'
                '}catch(e){}'
              '});'
            '}).catch(function(){});'
          '}'
        '}catch(e){}'
        # ── Live HP refresh — picks up CDN hosts registered after page load ───
        # Polls PX+/__s2l_hp every 2s (then 10s after 30 ticks) so CDN hosts
        # registered AFTER the initial page serve are picked up without a reload.
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
    # The regex matches id="__s2l__" anywhere in the <script> open tag (not just
    # as the first attribute), so <script type="..." id="__s2l__"> is also caught.
    html_bytes = re.sub(
        rb'<script\b[^>]*\bid=["\']__s2l__["\'][^>]*>.*?</script\s*>',
        b'', html_bytes,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Insert the injector AFTER <meta charset> / <meta http-equiv content-type>
    # if present in the first 2KB of <head>. Per HTML spec, the charset
    # declaration must appear within the first 1024 bytes — inserting a ~5KB
    # script before it pushes the charset past the sniff window, causing
    # browsers to fall back to a default charset (Latin-1/Windows-1252 in some
    # WebViews). This mojibake's non-ASCII chars in inline JS → syntax error
    # → SPA init halts → blank page.
    m_meta = re.search(
        rb'<meta[^>]+(?:charset|http-equiv\s*=\s*["\']?content-type)["\']?[^>]*>',
        html_bytes, re.IGNORECASE)
    if m_meta and m_meta.start() < 2048:
        idx = m_meta.end()
        return html_bytes[:idx] + script + html_bytes[idx:]

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
    # Sanitize unsafe chars BEFORE length check. Without this, characters like
    # <, >, !, spaces, newlines pass through and contaminate file/directory
    # names — a URL path containing HTML (e.g. from a malformed API response)
    # would produce cache paths like "vue_app_<!DOCTYPE html>..." which breaks
    # on Windows and creates unusable cache entries on Linux.
    name = re.sub(r"[^\w.\-]", "_", name)
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
    # Include a query-string hash so URLs with different params (e.g. search
    # queries, paginated APIs) are cached as separate files — prevents stale search results.
    if p.query:
        qs_hash = hashlib.sha1(p.query.encode()).hexdigest()[:10]
        last = parts[-1] if parts else "index.html"
        base, ext = os.path.splitext(last)
        ext = ext or ".html"
        parts[-1] = f"{base}_q{qs_hash}{ext}"
    # Strip the port and only the "www." PREFIX (not all occurrences) from netloc.
    # Using .replace("www.", "") would mangle hosts like "mywww.example.com".
    # Stripping the port avoids ":" in directory names (invalid on Windows).
    host_dir = p.netloc.split("@")[-1].split(":")[0]   # strip user:pass@ and :port
    host_dir = host_dir.removeprefix("www.") if host_dir.startswith("www.") else host_dir
    return os.path.join(SRC_FOLDER, host_dir, *parts)

# ──────────────────────────────────────────────────────────────────────────────
# Site init
# ──────────────────────────────────────────────────────────────────────────────

def build_base_url(raw: str) -> str | None:
    """Probe https:// then http:// to find which scheme `raw` actually serves.

    In OFFLINE mode, skip the probe entirely — there's no upstream to test.
    Default to https:// (the common case) and return immediately without any
    network call.

    verify=False here is not optional cosmetic parity with the rest of the
    file: curl_cffi's Session defaults to real certificate verification, and
    this probe used to be the one outbound call that could fail silently on a
    target with a self-signed cert, a hostname mismatch, or a CA bundle
    libcurl can't find (common on Termux). The session itself is built with
    verify=False too; this is belt-and-suspenders.
    """
    if OFFLINE:
        # No upstream — don't probe. Default to https:// (the common case),
        # which is what the probe would have picked anyway.
        _offline_url = f"https://{raw}"
        log(f"OFFLINE: skipping upstream probe for {raw} → {_offline_url}", "INFO")
        return _offline_url

    s = _make_session()
    try:
        for scheme in ("https://", "http://"):
            try:
                r = s.get(scheme + raw, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
                if r.status_code < 500:
                    platform = detect_platform(dict(r.headers))
                    log(f"Resolved {raw} → {r.url}  [{platform}]  IP: {resolve_ip(urlparse(r.url).netloc)}")
                    return r.url
            except Exception as e:
                log(f"Probe {scheme+raw}: {_short_exc(e)}", "WARN")
    finally:
        try: s.close()
        except Exception: pass
    return None

SITE_URL  = build_base_url(SITE) or f"http://{SITE}"
MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.removeprefix("www.").replace(".", "_")
SRC_FOLDER  = os.path.join("site_src",  SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)
WORDLISTS_DIR = "wordlists"   # wordlist files for SCAN_PATHS (one path per line)
os.makedirs(SRC_FOLDER,  exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

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

def _load_wordlist(path: str) -> list[str]:
    """Read one wordlist file → list of path strings (de-duplicated, order kept)."""
    out: list[str] = []
    seen: set[str] = set()
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                p = line.lstrip("/")
                if p and p not in seen:
                    seen.add(p)
                    out.append(p)
    except OSError as e:
        log(f"wordlist read error {path}: {_short_exc(e)}", "WARN")
    return out

def _resolve_wordlists() -> list[tuple[str, list[str]]]:
    """Resolve SCAN_PATHS mode → ordered list of (display_name, paths).

    Returns [] when SCAN_PATHS is falsy or no wordlists are found.
    """
    if not SCAN_PATHS or not isinstance(SCAN_PATHS, str):
        return []
    mode = SCAN_PATHS.strip()
    if not mode:
        return []

    files: list[str] = []   # absolute/relative file paths

    if mode == "all":
        # Recursive walk of WORDLISTS_DIR
        if os.path.isdir(WORDLISTS_DIR):
            for root, _dirs, fns in sorted(os.walk(WORDLISTS_DIR)):
                for fn in sorted(fns):
                    fp = os.path.join(root, fn)
                    if os.path.isfile(fp):
                        files.append(fp)
    elif mode == "all-in-dir":
        # Top-level only
        if os.path.isdir(WORDLISTS_DIR):
            for fn in sorted(os.listdir(WORDLISTS_DIR)):
                fp = os.path.join(WORDLISTS_DIR, fn)
                if os.path.isfile(fp):
                    files.append(fp)
    else:
        # Specific file: relative to WORDLISTS_DIR
        fp = os.path.join(WORDLISTS_DIR, mode)
        if os.path.isfile(fp):
            files.append(fp)
        else:
            # Also allow an absolute / cwd-relative path as a convenience
            if os.path.isfile(mode):
                files.append(mode)
            else:
                log(f"SCAN_PATHS wordlist not found: {mode} (looked in {WORDLISTS_DIR}/)", "ERROR")

    result: list[tuple[str, list[str]]] = []
    for fp in files:
        paths = _load_wordlist(fp)
        if paths:
            disp = os.path.relpath(fp, WORDLISTS_DIR) if fp.startswith(WORDLISTS_DIR) else fp
            result.append((disp, paths))
    return result

def _scan_paths_summary() -> tuple[int, int]:
    """Return (n_wordlists, n_total_paths) for the banner."""
    wls = _resolve_wordlists()
    return (len(wls), sum(len(p) for _, p in wls))


# ──────────────────────────────────────────────────────────────────────────────
# Status-code filter for SCAN_PATHS
#
# Two modes, set interactively at scan start (see _prompt_scan_status_filter):
#   _SCAN_BLOCK_STATUSES : set[int]  — codes to SUPPRESS from the log
#   _SCAN_ONLY_STATUS    : int | None — if set, ONLY this code is logged
#
# When _SCAN_ONLY_STATUS is set it wins (block-list is ignored). Both default
# to "nothing filtered" so legacy behavior is preserved when the user just
# hits Enter at the prompt.
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

def _parse_status_csv(raw: str) -> tuple[list[int], list[str]]:
    """Parse a comma-separated list of status codes.

    Returns (valid_codes, invalid_tokens). The caller uses invalid_tokens to
    decide whether to re-prompt the user — a single typo should NOT silently
    drop a status from the filter list.
    """
    valid: list[int] = []
    invalid: list[str] = []
    if not raw:
        return valid, invalid
    for tok in raw.replace(";", ",").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            n = int(tok)
            if 100 <= n <= 599:
                valid.append(n)
            else:
                invalid.append(tok)
        except ValueError:
            invalid.append(tok)
    return valid, invalid

def _prompt_scan_status_filter() -> None:
    """Interactive prompt that runs ONCE, in the main thread, before the
    scanner daemon starts. Must run here (not in the daemon) because input()
    conflicts with the MULTIPORT viewer's cbreak mode on stdin."""
    global _SCAN_BLOCK_STATUSES, _SCAN_ONLY_STATUS
    _SCAN_BLOCK_STATUSES = set()
    _SCAN_ONLY_STATUS = None

    _rainbow_print()
    _rainbow_print(f"{Fore.CYAN}{Style.BRIGHT}══ SCAN_PATHS — status filter ══{Style.RESET_ALL}")
    _rainbow_print()
    _rainbow_print(f"{Fore.YELLOW}Filter HTTP status codes before scanning? "
          f"{Fore.WHITE}(this can prevent log pollution, you can also ignore this by "
          f"just pressing enter with no stuff typed.){Style.RESET_ALL}")
    _rainbow_print()
    status_chips = []
    for sc in _SCAN_KNOWN_STATUSES:
        if sc == 200:
            color = Fore.GREEN
        elif 300 <= sc < 400:
            color = Fore.YELLOW
        elif 400 <= sc < 500:
            color = Fore.RED
        else:
            color = Fore.MAGENTA + Style.BRIGHT
        status_chips.append(f"{color}{sc}{Style.RESET_ALL}")
    line_buf: list[str] = []
    line_len = 0
    _rainbow_print(f"{Fore.WHITE}Available HTTP status codes: {Style.RESET_ALL}")
    for chip in status_chips:
        chip_len = len(_ANSI_ESC.sub("", chip))
        if line_len + chip_len + 2 > 80 and line_buf:
            _rainbow_print("  " + "  ".join(line_buf))
            line_buf = [chip]
            line_len = chip_len
        else:
            line_buf.append(chip)
            line_len += chip_len + 2
    if line_buf:
        _rainbow_print("  " + "  ".join(line_buf))
    _rainbow_print()

    while True:
        try:
            raw = input(_rainbow_text(
                f"{Fore.WHITE}Enter status codes to hide "
                f"{Fore.YELLOW}(e.g. 404,400){Fore.WHITE}\n"
                f"{Fore.CYAN}> {Style.RESET_ALL}")).strip()
        except (EOFError, KeyboardInterrupt):
            _rainbow_print(f"{Fore.YELLOW}No filter selected — proceeding with full logging.{Style.RESET_ALL}")
            return

        if not raw:
            _rainbow_print(f"{Fore.GREEN}OK, no status filter set — all status codes will be logged.{Style.RESET_ALL}")
            return

        valid, invalid = _parse_status_csv(raw)
        if invalid:
            _rainbow_print(f"{Fore.RED}Invalid status code, type a correct one{Style.RESET_ALL} "
                  f"(bad token(s): {', '.join(invalid)})")
            continue

        if not valid:
            _rainbow_print(f"{Fore.RED}Invalid status code, type a correct one{Style.RESET_ALL}")
            continue

        # Edge case: user tried to block EVERY known status — log would be empty.
        if set(valid) >= set(_SCAN_KNOWN_STATUSES):
            _rainbow_print(f"{Fore.RED}You selected all the status code, try again{Style.RESET_ALL} "
                  f"(blocking every code would produce an empty log)")
            continue

        # Edge case: user blocked all but ONE known status — equivalent to
        # "show only this one". Be explicit so the user understands.
        unblocked_known = [s for s in _SCAN_KNOWN_STATUSES if s not in set(valid)]
        if len(unblocked_known) == 1:
            only = unblocked_known[0]
            _SCAN_BLOCK_STATUSES = set(valid)
            _SCAN_ONLY_STATUS = only
            _rainbow_print(f"{Fore.GREEN}You selected to block all status code, you decided to not "
                  f"block {only}, proceeding to scan..{Style.RESET_ALL}")
            return

        _SCAN_BLOCK_STATUSES = set(valid)
        _SCAN_ONLY_STATUS = None
        _rainbow_print(f"{Fore.GREEN}OK, hiding {len(valid)} status code(s) from the log: "
              f"{', '.join(str(s) for s in sorted(valid))}{Style.RESET_ALL}")
        try:
            yn = input(_rainbow_text(
                f"{Fore.YELLOW}Show only one status code? "
                f"{Fore.WHITE}(y/N) {Fore.CYAN}> {Style.RESET_ALL}")).strip().lower()
        except (EOFError, KeyboardInterrupt):
            yn = ""
        if yn in ("y", "yes"):
            while True:
                try:
                    only_raw = input(_rainbow_text(f"{Fore.WHITE}Show only: {Style.RESET_ALL}")).strip()
                except (EOFError, KeyboardInterrupt):
                    _rainbow_print(f"{Fore.YELLOW}No 'only' filter — using block list only.{Style.RESET_ALL}")
                    break
                try:
                    only = int(only_raw)
                except ValueError:
                    _rainbow_print(f"{Fore.RED}Invalid status code, type a correct one{Style.RESET_ALL}")
                    continue
                if not (100 <= only <= 599):
                    _rainbow_print(f"{Fore.RED}Invalid status code, type a correct one{Style.RESET_ALL}")
                    continue
                _SCAN_ONLY_STATUS = only
                # Remove the "only" status from the block set so the two filters
                # aren't contradictory. _should_log_status checks only_status
                # FIRST so functionally this is a no-op, but it keeps the JSON
                # output honest about what's actually being filtered.
                _SCAN_BLOCK_STATUSES.discard(only)
                _rainbow_print(f"{Fore.GREEN}OK, showing only HTTP {only} responses.{Style.RESET_ALL}")
                break
        return


def _run_path_scanner() -> None:
    """Hidden-path scanner. Iterates wordlists, probes each path on MAIN_HOST
    with HEAD (falls back to GET on 405), classifies the result by status
    code, and writes hits to hidden_paths.json.

    Only statuses the user wants to see are logged — everything else is
    silently skipped (still recorded in hidden_paths.json).
    """
    wordlists = _resolve_wordlists()
    if not wordlists:
        log(f"SCAN_PATHS={SCAN_PATHS!r} but no wordlists found in {WORDLISTS_DIR}/ — "
            f"drop .txt files there (one path per line).", "WARN")
        return

    sess   = _make_session()
    found: list = []
    origin = f"{urlparse(SITE_URL).scheme}://{MAIN_HOST}"
    total_paths = sum(len(p) for _, p in wordlists)
    scanned = 0

    # Rate limit caps total probes/sec across ALL worker threads via a
    # sliding-window lock. SCANS_PER_SECOND <= 0 disables the cap.
    _rate_n = max(0, int(SCANS_PER_SECOND)) if SCANS_PER_SECOND else 0
    _rate_enabled = _rate_n > 0
    _rate_window: list[float] = []
    _rate_lock = threading.Lock()
    # Progress refresh interval for the no-tqdm fallback path.
    _progress_every = _rate_n if _rate_n > 0 else 25

    # Concurrency: ~15% of SCANS_PER_SECOND, clamped to [4, 24]. Parallel
    # workers can hit the rate cap even on slow servers (single-thread can't).
    if _rate_enabled:
        _scan_workers = min(max(int(_rate_n * 0.15), 4), 24)
    else:
        _scan_workers = 8

    def _rate_limit_sleep():
        """Thread-safe sliding-window rate limiter. Sleeps outside the lock."""
        if not _rate_enabled:
            return
        while True:
            with _rate_lock:
                now = time.time()
                while _rate_window and (now - _rate_window[0]) >= 1.0:
                    _rate_window.pop(0)
                if len(_rate_window) < _rate_n:
                    _rate_window.append(now)
                    return
                sleep_for = 1.0 - (now - _rate_window[0])
            if sleep_for > 0:
                time.sleep(min(sleep_for, 1.0))

    mode = (SCAN_PATHS or "").strip()
    if mode in ("all", "all-in-dir"):
        log(f"Loading first wordlist...", "SCAN")
    elif mode:
        log(f"Loading wordlist...", "SCAN")

    log(f"Path scanner — {len(wordlists)} wordlists, {total_paths} paths for {MAIN_HOST}", "SCAN")

    # Snapshot filter settings so mid-scan updates can't change behavior.
    block_set = set(_SCAN_BLOCK_STATUSES)
    only_status = _SCAN_ONLY_STATUS

    def _should_log_status(sc: int) -> bool:
        if only_status is not None:
            return sc == only_status
        return sc not in block_set

    global _active_scan_pbar
    for wl_name, paths in wordlists:
        wl_len = len(paths)
        host_disp = _fmt_host(MAIN_HOST)
        wl_short = os.path.basename(wl_name)
        if len(wl_short) > 30:
            wl_short = wl_short[:27] + "..."

        if _TQDM_OK:
            # tqdm + concurrent path. Worker threads probe paths in parallel;
            # the rate limiter caps total throughput at SCANS_PER_SECOND.
            # All log output is routed through tqdm.write() via the
            # _active_scan_pbar global so nothing corrupts the bar.
            _wl_found = [0]
            _wl_notfound = [0]
            _counters_lock = threading.Lock()

            pbar = _tqdm(
                total=wl_len,
                desc=_rainbow_text(f"  {wl_short}"),
                unit="path",
                unit_scale=False,
                dynamic_ncols=True,
                leave=True,
                file=sys.stderr,
                colour="cyan",
                bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] {postfix}",
            )
            _active_scan_pbar = pbar
            try:
                def _probe_one(p, _wl=wl_name, _host=host_disp):
                    _rate_limit_sleep()
                    url = f"{origin}/{p}"
                    try:
                        r = sess.head(url, timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                                      allow_redirects=False, verify=False)
                        if r.status_code == 405:
                            r = sess.get(url, timeout=(TIMEOUT_CONN, min(TIMEOUT_READ, 6)),
                                         allow_redirects=False, verify=False)
                        sc = r.status_code
                        ct = r.headers.get("Content-Type", "")
                        with _counters_lock:
                            if sc not in (404, 410):
                                found.append({"path": p, "url": url, "status": sc,
                                              "wordlist": _wl,
                                              "content_type": ct})
                                _wl_found[0] += 1
                            else:
                                _wl_notfound[0] += 1
                        if _should_log_status(sc):
                            if sc == 200:
                                c = Fore.GREEN + Style.BRIGHT
                                log(f"{c}Found path: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 404:
                                c = Fore.RED + Style.DIM
                                log(f"{c}Path not found: {_host}/{p}{Style.RESET_ALL}", "SCAN")
                            elif sc in (301, 302, 303, 307, 308):
                                c = Fore.YELLOW
                                loc = r.headers.get("Location", "")
                                extra = f" → {loc}" if loc else ""
                                log(f"{c}Redirect: {_host}/{p} [{sc}]{extra}{Style.RESET_ALL}", "SCAN")
                            elif sc == 403:
                                c = Fore.YELLOW + Style.BRIGHT
                                log(f"{c}Forbidden: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 401:
                                c = Fore.YELLOW + Style.BRIGHT
                                log(f"{c}Auth required: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 500:
                                c = Fore.MAGENTA + Style.BRIGHT
                                log(f"{c}Server error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 200 <= sc < 300:
                                c = Fore.GREEN
                                log(f"{c}Found path: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 300 <= sc < 400:
                                c = Fore.YELLOW
                                log(f"{c}Redirect: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 400 <= sc < 500:
                                c = Fore.RED
                                log(f"{c}Client error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            else:
                                c = Fore.MAGENTA + Style.BRIGHT
                                log(f"{c}Server error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    except _CONN_ERRORS:
                        pass
                    except Exception as e:
                        log(f"scan error {url}: {_short_exc(e)}", "WARN")

                with ThreadPoolExecutor(max_workers=_scan_workers) as pool:
                    futures = {pool.submit(_probe_one, p): p for p in paths}
                    for fut in as_completed(futures):
                        try:
                            fut.result()
                        except Exception:
                            pass
                        scanned += 1
                        with _counters_lock:
                            pf, pm = _wl_found[0], _wl_notfound[0]
                        pbar.set_postfix_str(
                            _rainbow_text(
                                f"{Fore.GREEN}found={pf}{Style.RESET_ALL} "
                                f"{Fore.RED}miss={pm}{Style.RESET_ALL}"
                            ),
                            refresh=True,
                        )
                        pbar.update(1)
            finally:
                pbar.close()
                _active_scan_pbar = None
        else:
            # Legacy path (no tqdm): periodic log-line progress refresh.
            log(f"Scanning paths of {wl_name}... [0/{wl_len}]", "SCAN")
            wl_scanned = 0
            for p in paths:
                _rate_limit_sleep()
                url = f"{origin}/{p}"
                try:
                    r = sess.head(url, timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                                  allow_redirects=False, verify=False)
                    if r.status_code == 405:
                        r = sess.get(url, timeout=(TIMEOUT_CONN, min(TIMEOUT_READ, 6)),
                                     allow_redirects=False, verify=False)
                    sc = r.status_code
                    ct = r.headers.get("Content-Type", "")
                    if sc not in (404, 410):
                        found.append({"path": p, "url": url, "status": sc,
                                      "wordlist": wl_name,
                                      "content_type": ct})
                    if not _should_log_status(sc):
                        wl_scanned += 1
                        scanned += 1
                        if wl_scanned % _progress_every == 0 or wl_scanned == wl_len:
                            log(f"Scanning paths of {wl_name}... [{wl_scanned}/{wl_len}]", "SCAN")
                        continue
                    if sc == 200:
                        c = Fore.GREEN + Style.BRIGHT
                        log(f"{c}Found path: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 404:
                        c = Fore.RED + Style.DIM
                        log(f"{c}Path not found: {host_disp}/{p}{Style.RESET_ALL}", "SCAN")
                    elif sc in (301, 302, 303, 307, 308):
                        c = Fore.YELLOW
                        loc = r.headers.get("Location", "")
                        extra = f" → {loc}" if loc else ""
                        log(f"{c}Redirect: {host_disp}/{p} [{sc}]{extra}{Style.RESET_ALL}", "SCAN")
                    elif sc == 403:
                        c = Fore.YELLOW + Style.BRIGHT
                        log(f"{c}Forbidden: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 401:
                        c = Fore.YELLOW + Style.BRIGHT
                        log(f"{c}Auth required: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 500:
                        c = Fore.MAGENTA + Style.BRIGHT
                        log(f"{c}Server error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 200 <= sc < 300:
                        c = Fore.GREEN
                        log(f"{c}Found path: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 300 <= sc < 400:
                        c = Fore.YELLOW
                        log(f"{c}Redirect: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 400 <= sc < 500:
                        c = Fore.RED
                        log(f"{c}Client error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    else:
                        c = Fore.MAGENTA + Style.BRIGHT
                        log(f"{c}Server error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                except _CONN_ERRORS:
                    if (wl_scanned + 1) % _progress_every == 0:
                        log(f"  {Fore.YELLOW}unreachable: {p}{Style.RESET_ALL}", "WARN")
                except Exception as e:
                    log(f"scan error {url}: {_short_exc(e)}", "WARN")
                wl_scanned += 1
                scanned += 1
                if wl_scanned % _progress_every == 0 or wl_scanned == wl_len:
                    log(f"Scanning paths of {wl_name}... [{wl_scanned}/{wl_len}]", "SCAN")

    # ── Persist results ─────────────────────────────────────────────────
    out = os.path.join(DATA_FOLDER, "hidden_paths.json")
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "target": MAIN_HOST,
                       "mode": SCAN_PATHS,
                       "scans_per_second": SCANS_PER_SECOND,
                       "filter": {
                           "blocked": sorted(block_set),
                           "only": only_status,
                       },
                       "wordlists": [wl for wl, _ in wordlists],
                       "results": found}, f, indent=2)
    except Exception as e:
        log(f"scan save failed: {e}", "ERROR")
    log(f"Scan done — {len(found)}/{scanned} found → {out}", "SCAN")

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
    """Running counters for the entire run (crawled, saved, proxied, ...)."""
    __slots__ = ("_lock", "crawled", "saved", "proxied",
                 "conn_errors", "http_errors", "hooks_run", "revealed", "cdn_fetched", "captured",
                 "crawl_cache_hits")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        for s in self.__slots__[1:]:   # skip _lock
            setattr(self, s, 0)

    def inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            setattr(self, key, getattr(self, key) + n)

    def snapshot(self) -> dict:
        with self._lock:
            return {s: getattr(self, s) for s in self.__slots__
                    if s != "_lock"}

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
            if host in (MAIN_HOST, "www." + MAIN_HOST):
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

        # NOTE: no Upgrade:websocket branch here — _S2LWSGIRequestHandler
        # intercepts every WS upgrade at the socket layer before this view
        # ever runs (see its docstring). A request reaching this function
        # is guaranteed to not be a WS upgrade.

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

        # Cache-hit only for GET/HEAD when CACHE_CDN is enabled.
        # In OFFLINE mode, always check cache regardless of CACHE_CDN —
        # the flag governs WRITES, not reads; a previously-cached asset
        # should still be served when offline.
        lp = local_path(cdn_url)
        if method in _SAFE_METHODS and (CACHE_CDN or OFFLINE) and os.path.isfile(lp):
            result = _serve_cached(lp)
            if result is not None:
                data, ct = result
                log(f"CDN {method} HIT {_fmt_host(cdn_host)}/{p} {_fmt_size(len(data))}", "CDN")
                resp = Response(data, content_type=ct)
                resp.headers["Cache-Control"] = "public, max-age=86400"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                resp.headers["Access-Control-Expose-Headers"] = "*"
                resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
                return resp

        if OFFLINE:
            log(f"CDN {method} 404 {_fmt_host(cdn_host)}/{p} (not cached)", "CDN")
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
            # Streaming support: video/audio from CDN hosts (MULTIPORT) must
            # stream, otherwise they buffer in memory and stall after ~30-40s.
            _has_range_c = bool(flask_request.headers.get("Range", ""))
            _do_stream_c = method in _SAFE_METHODS and (
                _has_range_c or _should_stream(guess_mime("/" + p)))
            kw["stream"] = _do_stream_c
            r    = _get_client_session().request(method, cdn_url, **kw)
            # CRITICAL: when stream=True was used, ALWAYS hand off to _stream_resp.
            # Falling through to r.content loses chunks on curl_cffi HTTP/2 for
            # large binary files (.tar bundles, .wasm, video, etc), producing
            # "0.0B" bodies even though the server returned 200 with real content.
            if _do_stream_c:
                return _stream_resp(r, method, cdn_url)
            body = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            ct   = r.headers.get("Content-Type", "application/octet-stream")
            # Empty-body recovery: curl_cffi's HTTP/2 mode sometimes returns
            # empty content for small images/avatars on CDN hosts. Without
            # this, the browser gets a 200 with 0 bytes — a "phantom" image
            # that loads but displays nothing. Re-fetch with stream=False and
            # identity encoding to recover the real bytes.
            if (not body and r.status_code == 200 and method in _SAFE_METHODS
                    and not getattr(_proxy_local, "in_cdn_refetch", False)):
                _cl_cdn = r.headers.get("Content-Length", "0") or "0"
                try:
                    _cl_cdn_int = int(_cl_cdn)
                except ValueError:
                    _cl_cdn_int = 0
                if _cl_cdn_int > 0:
                    log(f"CDN empty body on {p} — re-fetching with identity encoding", "WARN")
                    try:
                        _proxy_local.in_cdn_refetch = True
                        _retry_hdrs = dict(ctx.req_headers)
                        _retry_hdrs["Accept-Encoding"] = "identity"
                        _retry_r = _get_client_session().request(
                            method, cdn_url, headers=_retry_hdrs,
                            timeout=(TIMEOUT_CONN, TIMEOUT_READ),
                            allow_redirects=True, verify=False, stream=False)
                        _retry_body = _retry_r.content
                        if _retry_body:
                            body = _retry_body
                            r = _retry_r
                            ct = r.headers.get("Content-Type", ct)
                            log(f"CDN re-fetch succeeded: {_fmt_size(len(body))}", "INFO")
                    except Exception as _re:
                        log(f"CDN re-fetch failed: {_short_exc(_re)}", "WARN")
                    finally:
                        _proxy_local.in_cdn_refetch = False
            # Only save on real 200 responses with content — 304 has an empty body and
            # saving it would corrupt/truncate a previously-good cached file.
            if r.status_code == 200 and body and method in _SAFE_METHODS and CACHE_CDN:
                save_queue.put((cdn_url, body, ct))
                stats.inc("cdn_fetched")
            out_headers = filter_resp(_resp_headers_dict(r))
            out_headers["Access-Control-Allow-Origin"]  = "*"
            out_headers["Access-Control-Expose-Headers"] = "*"
            out_headers["Cross-Origin-Resource-Policy"] = "cross-origin"
            if method in _SAFE_METHODS:
                out_headers["Cache-Control"] = "public, max-age=86400"
            # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
            # through the proxy (critical for cross-origin iframes served from CDN hosts).
            _ct_base_c = ct.split(";")[0].strip().lower()
            if r.status_code < 400 and PROXY_CDN:
                if "text/html" in _ct_base_c:
                    body = _rewrite_ext_urls(body)
                    body = rewrite_abs_urls(body)
                    body = _inject_sw_clear(body)
                    out_headers.pop("content-length", None)
                    out_headers.pop("Content-Length", None)
                elif "text/css" in _ct_base_c:
                    body = _rewrite_ext_urls(body)
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

            log(f"CDN {method} {ctx.resp_status} {_fmt_host(cdn_host)}/{p} {_fmt_size(len(ctx.resp_body))} :{port}", "CDN")
            _gui_push_raw(method, f"/{p}", ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                          display_tag=f"[CDN:{port}]", origin=_fmt_host(cdn_host),
                          _skip_log=True)   # line 4702 already logged
            return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
        except Exception as exc:
            log(f"CDN error {cdn_host}/{p}: {_short_exc(exc)}", "WARN")
            return Response(str(exc), status=502)

    def _run():
        _wz = logging.getLogger("werkzeug")
        _wz.setLevel(logging.ERROR)
        try:
            server = make_server(HOST, port, cdn_app, threaded=True,
                                 request_handler=_S2LWSGIRequestHandler)
            server.__class__ = _PooledWSGIServer   # swap to use pool
            # _S2LWSGIRequestHandler intercepts WS upgrades at the raw-socket
            # layer for ALL server instances (see class docstring). It needs
            # to know THIS server's upstream host for its "not a special
            # /__s2l_*__/ path" fallback — without this it defaults to
            # SITE_URL (the main host) for every CDN port too, silently
            # misrouting any WS connection opened directly against a CDN
            # mini-server to the wrong upstream.
            server.s2l_target_base = f"https://{cdn_host}"
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

def _preregister_cdn_hosts_from_disk() -> int:
    """OFFLINE-mode safety: bring up MULTIPORT servers for every host we
    already have cached assets for, so URL rewrites have a live destination.

    In OFFLINE mode, _register_cdn_host is only called from upstream-fetch
    success paths — which never run. Without this pre-registration, every
    CDN asset 404s at the connection level because no mini-server is
    listening on the rewritten URL's port.
    """
    if not os.path.isdir(SRC_FOLDER):
        return 0
    main_host_dir = MAIN_HOST.removeprefix("www.")
    n = 0
    try:
        for host_dir in os.listdir(SRC_FOLDER):
            full = os.path.join(SRC_FOLDER, host_dir)
            if not os.path.isdir(full):
                continue
            if host_dir == main_host_dir:
                continue
            with _cdn_port_lock:
                already = host_dir in _cdn_host_port
            if not already:
                _register_cdn_host(host_dir)
                n += 1
    except Exception as e:
        log(f"CDN pre-registration error: {_short_exc(e)}", "WARN")
    return n

def _proxy_target(host: str, tail: str) -> str | None:
    """Resolve an external host+path into the correct local-proxy URL.

    Shared by the HTML/CSS rewriter and the JSON/webmanifest rewriter so both
    reach a real destination (MAIN_HOST, a registered CDN's dedicated
    MULTIPORT port, or the catch-all /__s2l_ext__/ route) through identical
    routing logic — one place to get this right instead of two copies that
    can quietly drift apart.

    Absolute https://MAIN_HOST/... baked into server-rendered content (e.g. an
    <iframe src="https://some-cdn/..."> deep inside a nested iframe) must
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
    # Strip the port from the captured host before matching against MAIN_HOST.
    # The regexes feeding this function capture host:port (e.g. "example.com:443"),
    # which would fail the `host in _mh` check and incorrectly route MAIN_HOST
    # assets through /__s2l_ext__/ — losing session cookies and auth.
    host_no_port = host.split(":", 1)[0]
    if host_no_port in _mh:
        return f"http://localhost:{PORT}{tail}"
    if any(bad in host for bad in CDN_BLOCK):
        return None
    with _cdn_port_lock:
        port = _cdn_host_port.get(host, 0)
        if port == 0:
            port = _cdn_host_port.get(host_no_port, 0)
    if port > 0 and MULTIPORT:
        return f"http://localhost:{port}{tail}"
    # MUST be absolute (see docstring above) — this exact line was previously
    # returning a bare "/__s2l_ext__/host/path" relative reference. On the
    # main-port page that happens to resolve correctly by coincidence, but the
    # SAME rewritten HTML/JSON is also served verbatim on every CDN sub-port
    # (MULTIPORT) and via /__s2l_ext__/ itself — there the browser resolves
    # the relative path against THAT origin instead, e.g. an index.html
    # served from :8084 turned "/__s2l_ext__/other-cdn.com/x" into
    # "http://localhost:8084/__s2l_ext__/other-cdn.com/x", which 400'd because
    # port 8084's Flask instance only knows how to proxy its own single CDN
    # host, not arbitrary third parties. That 400 was exactly what broke
    # loading the next item from the real CDN (error-style body) — the
    # failed request was the app's own manifest/config fetch.
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
        r'"https?://([a-zA-Z0-9\-._]+(?::\d+)?)((?:/[^"\\<>])?)"',
        _rep, text, flags=re.IGNORECASE)
    return text.encode("utf-8")

def _rewrite_ext_urls(html_bytes: bytes) -> bytes:
    """Rewrite ALL external https:// URLs in HTML attributes, srcset, and CSS url()."""
    if not PROXY_CDN or not html_bytes:
        return html_bytes
    try:
        html = html_bytes.decode("utf-8", "ignore")
    except Exception:
        return html_bytes

    def _attr_rep(m):
        host, tail = m.group(3), m.group(4) or "/"
        r = _proxy_target(host, tail)
        return f"{m.group(1)}{m.group(2)}{r}{m.group(5)}" if r else m.group(0)
    # Path is optional (/? instead of /) so bare-host URLs like
    # href="https://example.com" (no trailing slash) also get rewritten.
    html = re.sub(
        r'((?:src|href|poster|data-src|data-href|action)\s*=\s*)(["\'])https?://([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'<>]*)?(["\'])',
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
            # Handle https://, http://, and protocol-relative //host/path
            if url.startswith(("https://", "http://")):
                p = urlparse(url)
                r = _proxy_target(p.netloc, p.path + (f"?{p.query}" if p.query else ""))
                if r: url = r
            elif url.startswith("//"):
                p = urlparse("https:" + url)
                r = _proxy_target(p.netloc, p.path + (f"?{p.query}" if p.query else ""))
                if r: url = r
            parts.append(url + desc)
        return m.group(1) + ", ".join(parts) + m.group(3)
    html = re.sub(r'(srcset\s*=\s*["\'])([^"\']+)(["\'])', _ss_rep, html, flags=re.IGNORECASE)

    def _css_rep(m):
        host, tail = m.group(1), m.group(2)
        r = _proxy_target(host, tail)
        return f"url({r})" if r else m.group(0)
    html = re.sub(r'url\(["\']?https?://([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'\)\s<>]*)["\']?\)',
                  _css_rep, html, flags=re.IGNORECASE)

    # Strip CSP delivered via <meta http-equiv="Content-Security-Policy" ...> —
    # the HTTP header is already stripped in filter_resp, but pages that set CSP
    # via meta tag (frame-ancestors, script-src, etc.) bypass that and still get
    # enforced by the browser, blocking framing/scripts loaded through the proxy.
    html = re.sub(
        r'<meta[^>]+http-equiv\s*=\s*["\']content-security-policy["\'][^>]*>',
        '', html, flags=re.IGNORECASE)

    # Strip <base href="..."> tags — a base tag pointing at the real CDN host
    # changes how ALL relative URLs in the document resolve, causing every
    # relative src/href to bypass the proxy and go straight to the real internet.
    # The proxy already rewrites all absolute URLs, so the base tag is redundant.
    html = re.sub(r'<base\s[^>]*>', '', html, flags=re.IGNORECASE)

    # Strip <link rel="preconnect"> and <link rel="dns-prefetch"> tags.
    # These hint the browser to open a TCP/TLS connection to the real CDN host
    # BEFORE the rewritten URL is fetched — but the rewritten URL points to
    # localhost, not the real CDN. The preconnect to the real host is wasted
    # work AND can trigger CORS/TLS errors when the browser sees the real host
    # trying to set cookies it can't validate against localhost. Removing them
    # is safe — the proxy is the only connection the browser ever needs.
    html = re.sub(
        r'<link\b[^>]*\brel\s*=\s*["\'](?:preconnect|dns-prefetch)["\'][^>]*>',
        '', html, flags=re.IGNORECASE)

    # ── Strip SRI (integrity="...") attributes from <script> and <link> tags ──
    # CRITICAL FIX: s2l intentionally rewrites URLs inside CSS/JS resources so
    # they load through the proxy. This changes the bytes, which changes the
    # hash, which makes the browser REFUSE to apply the resource — the page
    # then renders with NO styles and NO scripts (the classic "blank-ish page"
    # symptom on sites like discord.com that ship SRI on their CSS bundle).
    # Stripping integrity= here is the equivalent of stripping CSP: we are
    # intentionally modifying the content, so the integrity check is wrong.
    # The crossorigin attribute is left alone — s2l already serves everything
    # with Access-Control-Allow-Origin: *, so anonymous CORS works fine.
    html = re.sub(
        r'\s+integrity\s*=\s*["\'][^"\']*["\']',
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
            r'(content\s*=\s*["\'][^"\']*?url\s*=\s*)https?://([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'<>]*)?(["\'])',
            _content_rep, tag, flags=re.IGNORECASE)
    html = re.sub(r'<meta\b[^>]*>', _meta_refresh_rep, html, flags=re.IGNORECASE)

    # ── Rewrite CDN URLs inside <script> blocks ───────────────────────────────
    # Hydration payloads (framework state embedded as JSON inside a <script>
    # tag — Next.js __NEXT_DATA__, Redux/Nuxt/Remix state, etc.) commonly embed
    # absolute CDN URLs that client-side JS reads at hydration time to build
    # iframes, image sources, or WebSocket endpoints. Those live inside
    # <script> tags and are invisible to the HTML-attribute regexes above.
    # Without this pass such a URL stays raw and unproxied; the browser then
    # points straight at the real origin → refused/blocked connection.
    def _script_url_rep(m2):
        host = m2.group(1)
        # Don't default empty tail to "/" — that produces double-slash URLs
        # (http://localhost:PORT/__s2l_ext__/host//path) when the SPA later
        # concatenates its own path. Keep it empty so concatenation works.
        tail = m2.group(2) or ""
        if any(bad in host for bad in CDN_BLOCK):
            return m2.group(0)
        r = _proxy_target(host, tail)
        return r if r else m2.group(0)

    def _script_block_rep(mb):
        open_tag = mb.group(1)
        body = mb.group(2)
        # Skip JSON-LD and static JSON data blocks — their URLs are identifiers
        # (e.g. "@type": "https://schema.org/Person"), not fetchable resources.
        # Rewriting them breaks SEO/structured data and framework hydration state.
        _type_m = re.search(r'\btype\s*=\s*["\']([^"\']+)["\']', open_tag, re.IGNORECASE)
        if _type_m:
            _script_type = _type_m.group(1).strip().lower()
            if _script_type in ("application/ld+json", "application/json",
                                "importmap", "application/graphql"):
                return mb.group(0)
        # Skip script blocks that look like JSON (first non-whitespace char is {).
        # Many SPAs emit untyped <script>window.__INITIAL_STATE__ = {...}</script>
        # whose URLs are identifiers, not fetchable resources. Rewriting them
        # can cause hydration mismatches.
        _stripped = body.lstrip()
        if _stripped and _stripped[0] == "{":
            return mb.group(0)
        # Skip webpack/module-federation chunks and minified bundle fragments.
        # These contain URLs as object KEYS (cache lookups, integrity maps),
        # and rewriting them produces a key mismatch → silent asset load failure.
        # Heuristics: minified code (long lines), IIFE wrappers, webpack runtime.
        if len(body) > 2000 and (
                "(function()" in body[:200] or "!function(" in body[:200]
                or "webpackChunk" in body[:500] or "__webpack_require__" in body[:2000]):
            return mb.group(0)
        # Skip template-script fragments that embed URLs as React/Vue JSX
        # string identifiers — these would be picked up by the URL regex but
        # are NOT fetchable resources, they're props/state.
        if "data-react-" in body[:500] or "__NEXT_DATA__" in body[:500]:
            return mb.group(0)
        return mb.group(1) + re.sub(
            r'https?://([a-zA-Z0-9\-._]{2,}(?::\d+)?)((?:/[^\s"\'\\<>`]*)?)',
            _script_url_rep,
            body,
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
_VISITED_MAX = 100000  # cap to prevent unbounded memory growth
saved_paths:    set = set()
_SAVED_PATHS_MAX = 50000  # cap to prevent unbounded memory growth
content_hashes: set = set()
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

def _mark_host_dead(netloc: str) -> None:
    """Mark a host as dead for DEAD_HOST_TTL seconds."""
    with _dead_hosts_lock:
        _dead_hosts[netloc] = time.time() + DEAD_HOST_TTL
        # Prune expired entries periodically to prevent unbounded growth.
        # Only prune when the dict gets large (>500 entries) to avoid
        # scanning on every call.
        if len(_dead_hosts) > 500:
            now = time.time()
            expired = [k for k, v in _dead_hosts.items() if now > v]
            for k in expired:
                _dead_hosts.pop(k, None)

def _is_host_dead(netloc: str) -> bool:
    """Check if a host is currently marked dead (with TTL expiry)."""
    with _dead_hosts_lock:
        expiry = _dead_hosts.get(netloc)
        if expiry is None:
            return False
        if time.time() > expiry:
            # TTL expired — host is no longer considered dead, allow retry.
            _dead_hosts.pop(netloc, None)
            return False
        return True

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

# Schemes we recognize. `wss?` covers ws:// and wss:// (modern realtime APIs).
# `ftp`/`file` are rare in practice but cost nothing to include and are
# occasionally useful for completeness when scraping legacy pages.
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

def _clean_url_match(m: bytes) -> bytes:
    """Strip trailing punctuation that the greedy path class captures.

    These characters are valid inside URLs (e.g. a comma in a query string)
    but almost never appear at the very end of a real URL — they're sentence
    punctuation, JS operators, or closing delimiters that the regex hoovers
    up because they're in the path character class.

    Also strips a SINGLE trailing closing-paren when the match contains an
    UNMATCHED opening paren — this is the common case of `url(//host/path)`
    in CSS, where the `)` is the CSS function close, not part of the URL.
    The regex can't express "balanced parens" so we approximate here: count
    opens vs closes; if there's one extra close, drop it.
    """
    while m and m[-1] in _URL_TRAILING:
        m = m[:-1]
    # Unbalanced-paren fixup: if the trimmed match has more `)` than `(`,
    # drop exactly one trailing `)`. This is the standard CSS `url(/path)`
    # case and costs almost nothing for non-CSS matches (which are balanced
    # or have no parens at all).
    if m and m[-1] == ord(b")"):
        opens  = m.count(b"(")
        closes = m.count(b")")
        if closes > opens:
            m = m[:-1]
    return m

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
    if not _cdn_thread_sem.acquire(blocking=False):  # pylint: disable=consider-using-with
        return   # too many concurrent CDN fetches, skip silently
    try:
        with visited_lock:
            if u in visited:
                return
            visited.add(u)
        if _is_host_dead(netloc):
            return

        # Disk-cache fast path: if this CDN URL is already on disk from a
        # previous run AND SKIP_CRAWL_CACHE is enabled, skip the network
        # fetch entirely. Assets don't need link extraction (they're not
        # HTML), so we just register the host for MULTIPORT and bail.
        # When SKIP_CRAWL_CACHE = False, we always re-fetch to force a
        # fresh pull of every CDN asset.
        if SKIP_CRAWL_CACHE and _resolve_cached_path(u) is not None:
            _register_cdn_host(netloc)
            stats.inc("crawl_cache_hits")
            log(f"CDN-bg {Fore.CYAN}CACHED{Style.RESET_ALL} {_fmt_host(netloc)}{urlparse(u).path}", "CDN")
            return

        try:
            r    = _get_proxy_session().get(u, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
            body = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            # Never cache bot/CAPTCHA pages — they would poison the asset cache
            if _is_bot_page(body, r.status_code, urlparse(u).path):
                log(f"Bot page from CDN {netloc} — skipping cache", "WARN")
                _mark_host_dead(netloc)
                return
            if r.status_code < 400:
                if CACHE_CDN:
                    save_queue.put((u, body, r.headers.get("Content-Type", "")))
                _register_cdn_host(netloc)
                stats.inc("cdn_fetched")
                log(f"CDN-bg GET {r.status_code} {_fmt_host(netloc)}{urlparse(u).path} {_fmt_size(len(body))}", "CDN")
                # Push to GUI traffic log so background CDN fetches appear in
                # the Hook Inspector — they're real requests the user needs to
                # see, not just terminal noise.
                _parsed_u = urlparse(u)
                _gui_push_raw("GET", _parsed_u.path or "/", r.status_code,
                              r.headers.get("Content-Type", "application/octet-stream"),
                              body, display_tag="[CDN-BG]",
                              origin=_fmt_host(netloc),
                              _skip_log=True)   # caller already logged
                if CAPTURE and CAPTURE_CDN:
                    parsed_u = urlparse(u)
                    ctx_cdn  = HookContext(
                        method="GET", url=u, path=parsed_u.path,
                        query=parsed_u.query, req_headers={}, req_body=b"",
                        resp_status=r.status_code, resp_headers=filter_resp(_resp_headers_dict(r)),
                        resp_body=body,
                        resp_ct=r.headers.get("Content-Type", "application/octet-stream"),
                    )
                    _maybe_capture(ctx_cdn)
        except _CONN_ERRORS as e:
            _mark_host_dead(netloc)
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
        except queue.Empty:
            pass
        else:
            # CRITICAL: wrap the per-item body in try/except+finally so any
            # exception (local_path, hashlib, lock contention, etc.) doesn't
            # kill the single save-worker thread — without task_done() the
            # save_queue.join() in crawl_parallel() hangs forever, blocking
            # startup entirely.
            try:
                # Safety gate: never cache bot-detection / CAPTCHA pages.
                # These can arrive from background CDN fetches or upstream retries.
                if _is_bot_page(data):
                    log(f"Bot page detected — NOT caching {urlparse(u).netloc}{urlparse(u).path}", "WARN")
                    continue
                p = local_path(u)
                h = hashlib.sha1(data).hexdigest()
                with content_lock:
                    if h in content_hashes:
                        continue
                    content_hashes.add(h)
                    # Cap memory — clear if too large (prevents OOM on long runs)
                    if len(content_hashes) > _CONTENT_HASHES_MAX:
                        content_hashes.clear()
                        content_hashes.add(h)
                # NOTE: we track saved_paths by the ORIGINAL path here. If the
                # write later remaps p to p/index.html (directory collision),
                # _save_worker updates saved_paths with the final write path
                # after a successful write. This ensures a failed write does NOT
                # permanently mark the URL as saved (which would make it
                # uncacheable for the rest of the run).
                original_p = p
                with save_lock:
                    if original_p in saved_paths:
                        continue
                    # Tentatively mark as in-progress; removed on write failure.
                    saved_paths.add(original_p)
                    # Cap memory — clear if too large (prevents OOM on long runs)
                    if len(saved_paths) > _SAVED_PATHS_MAX:
                        saved_paths.clear()
                        saved_paths.add(original_p)
                # Record the real upstream Content-Type when it disagrees with what
                # the on-disk path/extension implies (e.g. an extensionless CSS/JS
                # URL that local_path() collapsed to .../index.html) — see resolve_mime().
                if ctype and _ct_base(ctype) != _ct_base(guess_mime(p)):
                    _save_ctype_sidecar(p, ctype)
                batch.append((original_p, p, data))
            except Exception as e:
                log(f"save-worker item error {u}: {e}", "ERROR")
            finally:
                save_queue.task_done()
        now = time.time()
        if batch and (len(batch) >= SAVE_BATCH or now - last_flush >= SAVE_INTERVAL):
            for original_p, p, data in batch:
                try:
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    if os.path.isdir(p):
                        p = os.path.join(p, "index.html")
                    with open(p, "wb") as f:
                        f.write(data)
                    # Write succeeded — record the FINAL write path in saved_paths
                    # (may differ from original_p due to directory-collision remap).
                    if p != original_p:
                        with save_lock:
                            saved_paths.discard(original_p)
                            saved_paths.add(p)
                    stats.inc("saved")
                except Exception as e:
                    log(f"save error {p}: {e}", "ERROR")
                    # Write FAILED — remove from saved_paths so a future request
                    # for the same URL can retry instead of being permanently
                    # skipped (which would make it uncacheable for the run).
                    with save_lock:
                        saved_paths.discard(original_p)
                        saved_paths.discard(p)
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
            log(f"{rec.method} {_fmt_host(urlparse(rec.url).netloc)}{urlparse(rec.url).path or '/'} {rec.resp_status}"
                f" {_fmt_size(len(rec.resp_body))} → {rel}", "CAPTURE")
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
    # After crawl finishes, the proxy serves fresh from upstream — no need
    # to re-crawl. In procedural mode (CRAWL=False), _crawl_done is never
    # set and no workers are running, so this is always a no-op.
    if not _crawl_done.is_set():
        url_queue.put(normalize_url(u))

def _resolve_cached_path(u: str) -> str | None:
    """Return the on-disk path of a URL's cached body, or None if not cached.

    Handles the directory-collision remap that _save_worker does: a URL whose
    local_path() is "X" might actually be saved as "X/index.html" when "X"
    collided with another URL that already created a directory at that path.

    This is the foundation of resumable CRAWL: every URL the crawler is about
    to fetch is first checked here. A hit means we already downloaded it in a
    previous run — no need to hit the network again.
    """
    p = local_path(u)
    if os.path.isfile(p):
        return p
    # Directory-collision remap: real file lives one level deeper.
    if os.path.isdir(p):
        idx = os.path.join(p, "index.html")
        if os.path.isfile(idx):
            return idx
    return None

def _extract_and_enqueue_links(data: bytes, base_url: str) -> None:
    """Parse an HTML body and enqueue every same-domain link found.

    Pulled out of _crawl() so it can run both on freshly-fetched pages AND on
    pages read from the disk cache — without it, a CRAWL restart would skip
    re-fetching the root page (good) but never discover its linked sub-pages
    (bad), so the second run would only re-cache exactly the URLs that were
    enqueued directly by the user.
    """
    try:
        soup = BeautifulSoup(data.decode("utf-8", "ignore"), "lxml")
    except Exception:
        return
    for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
        v = tag.get("href") or tag.get("src")
        if not v:
            continue
        abs_url = normalize_url(urljoin(base_url, v))
        parsed  = urlparse(abs_url)
        if parsed.hostname in _LOCAL_HOSTS:
            continue
        if is_allowed_domain(parsed.netloc):
            enqueue(abs_url)
        elif PROXY_CDN and is_external_domain(parsed.netloc) and tag.name in _ASSET_TAGS:
            try:
                _ext_asset_queue.put_nowait(abs_url)
            except queue.Full:
                pass

    if DUMP_ALL:
        for m in URL_REGEX.findall(data[:SCAN_LIMIT]):
            try:
                cleaned = _clean_url_match(m)
                if not cleaned:
                    continue
                found = cleaned.decode("utf-8", "ignore")
                if found.startswith(("data:", "javascript:", "blob:", "mailto:", "tel:")):
                    continue
                p = urlparse(found)
                if p.hostname in _LOCAL_HOSTS:
                    continue
                enqueue(urljoin(base_url, found))
            except Exception:
                pass

def _crawl(u: str) -> None:
    with visited_lock:
        if u in visited:
            return
        visited.add(u)
        # Cap memory — if visited set grows too large, clear oldest half.
        # (Sets are unordered so "oldest" is approximate; this is purely a
        # memory bound, not an LRU eviction.)
        if len(visited) > _VISITED_MAX:
            _to_remove = len(visited) - _VISITED_MAX // 2
            for _i, _u in enumerate(list(visited)):
                if _i >= _to_remove:
                    break
                visited.discard(_u)

    if url_depth(u) > CRAWL_DEPTH:
        return
    if any(x in u for x in BLOCK_PATHS):
        return

    netloc = urlparse(u).netloc
    if not is_allowed_domain(netloc):
        return
    if _is_host_dead(netloc):
        return

    path_short = urlparse(u).path or "/"
    host_disp  = _fmt_host(netloc)

    # ── Disk-cache fast path ────────────────────────────────────────────
    # If the URL was already cached in a previous run AND SKIP_CRAWL_CACHE
    # is enabled, read it from disk instead of re-downloading. This makes
    # CRAWL resumable: turn it off, turn it back on, and it picks up where
    # it left off without re-fetching a single byte of what's already on
    # disk.
    #
    # Set SKIP_CRAWL_CACHE = False to force a full re-download on every
    # start (e.g. when you suspect cached files are stale or corrupted).
    if SKIP_CRAWL_CACHE:
        cached_p = _resolve_cached_path(u)
    else:
        cached_p = None
    if cached_p is not None:
        try:
            with open(cached_p, "rb") as f:
                data = f.read()
        except OSError as e:
            log(f"cache read error {u}: {_short_exc(e)} — re-fetching", "WARN")
            cached_p = None   # fall through to network fetch
        else:
            # Prefer the recorded real Content-Type (sidecar) over the
            # path-extension guess — the save-worker writes a sidecar
            # whenever upstream's CT disagrees with the extension.
            ct = _load_ctype_sidecar(cached_p) or guess_mime(cached_p)
            stats.inc("crawl_cache_hits")
            log(f"{Fore.CYAN}CACHED{Style.RESET_ALL}  {host_disp}{path_short}  {_fmt_size(len(data))}", "CRAWL")
            _gui_push_raw("GET", path_short, 200, ct, data,
                          display_tag="[CRAWL]", origin=host_disp,
                          _skip_log=True)
            # Already on disk — don't re-queue for save.
            # Still extract links so sub-resources get discovered/crawled.
            if is_html(data, ct):
                _extract_and_enqueue_links(data, u)
            return

    # ── Network fetch path ──────────────────────────────────────────────
    try:
        r = _get_proxy_session().get(u, timeout=(TIMEOUT_CONN, TIMEOUT_READ), verify=False)
    except _CONN_ERRORS as e:
        log(f"Skip {netloc} — {_short_exc(e)}", "WARN")
        _mark_host_dead(netloc)
        stats.inc("conn_errors")
        return
    except Exception as e:
        log(f"Crawl error {u} — {_short_exc(e)}", "WARN")
        stats.inc("conn_errors")
        return

    stats.inc("crawled")

    # Always decompress before processing
    data = decompress_body(r.content, r.headers.get("Content-Encoding", ""))
    ct   = r.headers.get("Content-Type", "")

    sc_c = (Fore.GREEN if r.status_code < 300
            else Fore.YELLOW if r.status_code < 400
            else Fore.RED)
    log(f"GET {sc_c}{r.status_code}{Style.RESET_ALL}  {host_disp}{path_short}  {_fmt_size(len(data))}", "CRAWL")
    # Push crawl fetches to the GUI traffic log so they appear in the Hook
    # Inspector alongside proxied requests — the user needs to see what the
    # crawler is fetching, not just the terminal.
    _gui_push_raw("GET", path_short, r.status_code, ct, data,
                  display_tag="[CRAWL]", origin=host_disp,
                  _skip_log=True)   # caller already logged

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
        _extract_and_enqueue_links(data, u)

def _crawl_worker() -> None:
    # Use a sentinel value to signal shutdown. A 3s timeout caused workers to
    # exit prematurely when the queue was temporarily empty between batches of
    # enqueued URLs — url_queue.join() would then hang forever because
    # unprocessed items never got task_done().
    #
    # CRITICAL: the try/except+finally mirrors _discovery_worker. Without it,
    # any exception from _crawl() (e.g. lxml FeatureNotFound, parsing error)
    # kills the worker WITHOUT calling task_done(), so url_queue.join() in
    # crawl_parallel() hangs forever, blocking startup entirely.
    while True:
        u = url_queue.get()
        if u is None:
            url_queue.task_done()
            return
        try:
            _crawl(u)
        except Exception as e:
            log(f"crawl error {u}: {_short_exc(e)}", "WARN")
        finally:
            url_queue.task_done()

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
_ext_asset_queue: queue.Queue = queue.Queue(maxsize=0)
_ext_asset_started = threading.Event()

def _start_ext_asset_workers() -> None:
    """Start a small pool of workers for external asset prefetch."""
    if _ext_asset_started.is_set():
        return
    _ext_asset_started.set()
    _n = min(8, _DISCOVERY_WORKERS)
    for i in range(_n):
        threading.Thread(target=_ext_asset_worker, daemon=True,
                         name=f"ext-asset-{i}").start()

def _ext_asset_worker() -> None:
    """Worker that fetches external assets from the queue."""
    while True:
        try:
            u = _ext_asset_queue.get()
            if u is None:
                return
            _fetch_external_asset(u)
        except Exception:
            pass
        finally:
            _ext_asset_queue.task_done()

def _discovery_worker() -> None:
    """Long-lived crawl worker — processes the url_queue forever.

    Started after the initial crawl finishes. The initial crawl's WORKERS
    threads all exit (via None sentinels); these workers take over and keep
    draining the queue so enqueue() calls from the proxy handler are honored.
    """
    while True:
        u = url_queue.get()
        if u is None:
            url_queue.task_done()
            return
        try:
            _crawl(u)
        except Exception as e:
            log(f"discovery crawl error {u}: {_short_exc(e)}", "WARN")
        finally:
            url_queue.task_done()

def _start_discovery_workers() -> None:
    if _discovery_started.is_set():
        return
    _discovery_started.set()
    for i in range(_DISCOVERY_WORKERS):
        threading.Thread(target=_discovery_worker, daemon=True,
                         name=f"discovery-{i}").start()

def _extract_links_async(html_body: bytes, base_url: str) -> None:
    """Extract links from an HTML body (served by the proxy) and enqueue any
    same-domain URLs for background crawling. Runs in its own thread so the
    proxy response is never delayed by link parsing.

    This is the key fix for the "reload more than once" bug: without it, the
    first navigation to a page serves it live but never discovers its
    sub-resources, so the browser has to fetch each one on demand (and some
    JS may fail if a dependency isn't ready). With it, the discovery workers
    pre-cache linked pages and assets in the background.
    """
    try:
        soup = BeautifulSoup(html_body.decode("utf-8", "ignore"), "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
            v = tag.get("href") or tag.get("src")
            if not v:
                continue
            abs_url = normalize_url(urljoin(base_url, v))
            parsed  = urlparse(abs_url)
            if parsed.hostname in _LOCAL_HOSTS:
                continue
            if is_allowed_domain(parsed.netloc):
                enqueue(abs_url)
            elif PROXY_CDN and is_external_domain(parsed.netloc) and tag.name in _ASSET_TAGS:
                # Use bounded pool instead of spawning a thread per asset —
                # prevents "can't start new thread" on pages with many links.
                try:
                    _ext_asset_queue.put_nowait(abs_url)
                except queue.Full:
                    pass  # queue full — skip this asset, not critical
    except Exception:
        pass

def crawl_parallel() -> None:
    workers = [threading.Thread(target=_crawl_worker, daemon=True, name=f"crawl-{i}")
               for i in range(WORKERS)]
    for w in workers:
        w.start()
    url_queue.join()
    # Enqueue sentinels so all INITIAL workers exit cleanly (one per worker).
    # The persistent discovery workers are started AFTER, so they never see
    # these sentinels.
    for _ in range(WORKERS):
        url_queue.put(None)
    save_queue.join()
    _crawl_done.set()   # signals initial crawl is complete
    # Start persistent discovery workers so newly-discovered URLs keep flowing.
    _start_discovery_workers()

# ──────────────────────────────────────────────────────────────────────────────
# Streaming helper  (large binary responses — video, WASM, big downloads)
# ──────────────────────────────────────────────────────────────────────────────

# Content types that are streamed (stream=True in the request, never buffered).
# HTML / JSON / CSS / JS are always buffered so hooks and URL rewriting can work.
# Images are NOT here — they're usually small (<1MB) and buffering them is safe.
# Only true large binary formats that would OOM the process belong here.
_STREAM_CTS = frozenset({
    "video/mp4", "video/webm", "video/ogg", "video/mpeg", "video/mp2t",
    "audio/mpeg", "audio/ogg", "audio/webm", "audio/mp4", "audio/aac",
    "audio/x-m4a", "audio/x-wav", "audio/wav",
    "application/octet-stream",
    "application/zip", "application/x-tar", "application/gzip",
    "application/wasm",
})

# Minimum Content-Length (bytes) that triggers streaming even for non-binary CTs
_STREAM_MIN_BYTES = 5 * 1024 * 1024  # 5 MB

def _should_stream(ct: str, cl: int = 0) -> bool:
    """Return True if this response should be streamed without full buffering."""
    base = _ct_base(ct)
    # application/octet-stream is ambiguous — many APIs use it for small JSON.
    # Only stream it when the response is actually large, so URL/hook rewriting
    # still applies to small octet-stream responses.
    if base == "application/octet-stream":
        return bool(cl) and cl > _STREAM_MIN_BYTES
    if base in _STREAM_CTS:
        return True
    if cl and cl > _STREAM_MIN_BYTES:
        return True
    return False

# Maximum raw (compressed) body size we'll accumulate in RAM for disk caching
# during streaming. Bodies larger than this are streamed to the browser but
# NOT cached to disk (avoids OOM on huge video/download streams).
_STREAM_CACHE_MAX_BYTES = 64 * 1024 * 1024  # 64 MB

def _stream_resp(upstream_r, method: str, target: str) -> Response:
    """Stream a large upstream response (fetched with stream=True) to the browser.

    Body is not buffered in RAM (beyond a bounded cache buffer for disk writes).
    After the generator finishes the plain (decompressed) content is queued for
    disk caching — but only if the response completed normally AND fits within
    _STREAM_CACHE_MAX_BYTES, to avoid OOM on huge media files.

    Decompression is done per-chunk via a streaming decompressor so the browser
    receives plain bytes (Content-Encoding is stripped). This is critical:
    curl_cffi's impersonation can override Accept-Encoding at the libcurl level,
    so upstream may return gzip/br bytes even when we asked for identity.
    """
    ct    = upstream_r.headers.get("Content-Type", "application/octet-stream")
    sc    = upstream_r.status_code
    enc   = (upstream_r.headers.get("Content-Encoding", "") or "").lower().strip()
    out_h = filter_resp(_resp_headers_dict(upstream_r))
    out_h["Access-Control-Allow-Origin"]   = "*"
    out_h["Access-Control-Expose-Headers"] = "*"
    out_h["Cache-Control"] = "public, max-age=86400"
    # We decompress before yielding (see _gen below), so strip the header.
    # Also strip Content-Length when we're decompressing — the decompressed
    # size differs from the compressed Content-Length, and a mismatch causes
    # the browser to truncate or stall the stream.
    out_h.pop("content-encoding", None)
    out_h.pop("Content-Encoding", None)
    if enc and enc != "identity":
        out_h.pop("content-length", None)
        out_h.pop("Content-Length", None)

    # Build a streaming decompressor matching the upstream encoding.
    # Falls back to identity (pass-through) if the encoding is unknown/unsupported.
    # Handles stacked encodings (e.g. "gzip, br") by applying them in reverse order.
    _enc_chain: list = []   # list of decompressor specs applied in order
    for _enc_part in [e.strip() for e in enc.split(",") if e.strip()]:
        if _enc_part in ("gzip", "x-gzip"):
            _enc_chain.append(("gzip", zlib.decompressobj(zlib.MAX_WBITS | 16)))
        elif _enc_part == "deflate":
            # Try zlib-wrapped first; persistent raw-deflate fallback handled in _decompress_chunk.
            _enc_chain.append(("deflate", zlib.decompressobj(zlib.MAX_WBITS)))
        elif _enc_part in ("br", "brotli") and _BROTLI_OK:
            _enc_chain.append(("br", _brotli.Decompressor()))
        elif _enc_part == "zstd" and _ZSTD_OK:
            # ZstdDecompressionObj is the incremental API (not stream_reader,
            # which is read-only and raises on .write()).
            _enc_chain.append(("zstd", _zstd.ZstdDecompressor().decompressobj()))
        else:
            # Unknown/unsupported encoding — pass through (can't decompress).
            log(f"Unsupported stream encoding {_enc_part!r} on {urlparse(target).path}", "WARN")

    _deflate_fallback: zlib.decompressobj | None = None  # persistent raw-deflate obj

    def _decompress_chunk(chunk: bytes) -> bytes:
        """Decompress one raw chunk through the encoding chain."""
        nonlocal _deflate_fallback
        if not _enc_chain:
            return chunk  # identity / no supported encoding → pass through
        data = chunk
        for _enc_name, _decomp in _enc_chain:
            try:
                if _enc_name == "br":
                    data = _decomp.process(data)
                elif _enc_name == "zstd":
                    data = _decomp.decompress(data)
                elif _enc_name == "deflate":
                    out = _decomp.decompress(data)
                    if not out and _deflate_fallback is None:
                        # First chunk produced nothing — maybe raw deflate (no zlib header).
                        # Create a PERSISTENT raw-deflate decompressor (NOT per-chunk:
                        # raw deflate is a stream, each chunk depends on prior state).
                        try:
                            _deflate_fallback = zlib.decompressobj(-zlib.MAX_WBITS)
                            out = _deflate_fallback.decompress(data)
                        except Exception:
                            _deflate_fallback = None
                    elif _deflate_fallback is not None:
                        out = _deflate_fallback.decompress(data)
                    data = out
                else:  # gzip
                    data = _decomp.decompress(data)
            except Exception:
                # On failure, pass current data through rather than killing the stream.
                return data
        return data

    def _gen():
        raw_chunks: list[bytes] = []
        total = 0
        completed = False
        # Capture the first few KB of the DECOMPRESSED body for the GUI traffic
        # log. Without this, streamed responses (audio/video 206, large files)
        # show "(empty response)" in the Hook Inspector body editor because the
        # body is never buffered — it goes straight to the browser. We grab a
        # bounded prefix so the user can at least see what kind of content it is.
        _gui_preview = bytearray()
        _GUI_PREVIEW_MAX = 4096
        try:
            for chunk in upstream_r.iter_content(chunk_size=65536):
                if chunk:
                    # Bound the cache buffer — if the response is huge, stop
                    # accumulating and skip disk caching entirely (avoids OOM).
                    if total < _STREAM_CACHE_MAX_BYTES:
                        raw_chunks.append(chunk)
                    total += len(chunk)
                    _dec = _decompress_chunk(chunk)
                    if len(_gui_preview) < _GUI_PREVIEW_MAX:
                        _gui_preview.extend(_dec[:_GUI_PREVIEW_MAX - len(_gui_preview)])
                    yield _dec
            completed = True
        except Exception as e:
            log(f"Stream error {urlparse(target).path}: {_short_exc(e)}", "WARN")
        finally:
            # Only cache if the stream completed normally AND we stayed within
            # the memory bound AND it's a full response (200, or 206 that
            # covers the ENTIRE file — detected via Content-Range starting at 0).
            # A 206 for "Range: bytes=1000-2000" is a partial slice and must
            # NOT be cached (would overwrite the full file with a slice).
            # But a 206 for "Range: bytes=0-" returns the whole file with
            # status 206 — that IS cacheable.
            _cr_header = ""
            try:
                _cr_header = upstream_r.headers.get("Content-Range", "") or ""
            except Exception:
                pass
            _is_full_content = (sc == 200) or (
                sc == 206 and _cr_header and _cr_header.startswith("bytes 0-"))
            if (method == "GET" and _is_full_content and completed
                    and raw_chunks and total <= _STREAM_CACHE_MAX_BYTES):
                full  = b"".join(raw_chunks)
                plain = decompress_body(full, enc)
                if not _is_wire_payload(plain) and not _is_bot_page(plain, sc):
                    save_queue.put((target, plain, ct))
                    stats.inc("saved")
            # Single consolidated stream log line. Include "not cached" note
            # when the stream was too large or didn't complete — previously
            # this was a second separate log line, creating duplicate entries.
            _not_cached_note = ""
            if total > _STREAM_CACHE_MAX_BYTES:
                _not_cached_note = " — too large, not cached"
            elif not completed:
                _not_cached_note = " — incomplete, not cached"
            log(f"STREAM {method} {sc} {_fmt_host(urlparse(target).netloc)}{urlparse(target).path}"
                f" [{_fmt_size(total)}]{_not_cached_note}", "→")
            # Push to the GUI traffic log. For 206/204 and other non-2xx the
            # preview may be empty — that's fine, _gui_push_raw handles it.
            # For large streams we show the preview prefix with a note.
            _preview_bytes = bytes(_gui_preview)
            if HOOK_GUI:
                _path = urlparse(target).path or "/"
                if total > _GUI_PREVIEW_MAX and _preview_bytes:
                    _gui_body = (_preview_bytes
                                 + f"\n... (streamed {_fmt_size(total)} total, showing first {_fmt_size(len(_preview_bytes))})"
                                   .encode("utf-8", "replace"))
                else:
                    _gui_body = _preview_bytes
                _gui_push_raw(method, _path, sc, ct, _gui_body,
                              display_tag="[STREAM]",
                              origin=_fmt_host(urlparse(target).netloc))
            # CRITICAL: always close the upstream response, even on client
            # disconnect (GeneratorExit is a BaseException, not caught by
            # the `except Exception` above). Without this, the underlying
            # socket is never released back to the connection pool, leaking
            # file descriptors under sustained client disconnects —
            # especially bad on curl_cffi HTTP/2 where one leaked handle
            # can pin the entire connection.
            try:
                upstream_r.close()
            except Exception:
                pass

    return Response(stream_with_context(_gen()), status=sc,
                    headers=out_h, content_type=ct)

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

def _ws_unmask(payload: bytes, mask_key: bytes) -> bytes:
    """XOR payload with a 4-byte mask key. Used by both client→server (mask)
    and server→client (unmask) directions — XOR is its own inverse.

    Fast path: use int.from_bytes + struct for 8-byte chunks when payload is
    large enough that the per-byte Python loop would dominate. Falls back to
    the simple loop for tiny payloads.
    """
    if not payload:
        return b""
    if len(payload) < 64:
        return bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))
    # Build a 4-byte → 8-byte mask by repeating, then XOR via int ops.
    # This is ~10-50× faster than the per-byte loop on large frames.
    mk4 = mask_key
    mk8 = mk4 + mk4
    out = bytearray(len(payload))
    n8 = len(payload) & ~7    # largest multiple of 8 ≤ len
    mk_int = int.from_bytes(mk8, "little")
    # Process 8 bytes at a time
    for i in range(0, n8, 8):
        chunk = int.from_bytes(payload[i:i+8], "little")
        out[i:i+8] = (chunk ^ mk_int).to_bytes(8, "little")
    # Tail (0-7 bytes)
    for i in range(n8, len(payload)):
        out[i] = payload[i] ^ mk4[i & 3]
    return bytes(out)

# Per-tunnel deflate contexts. We keep separate inflate/deflate objects
# per tunnel because RFC 7692 mandates that context persists across messages
# within the same connection. Keyed by id(tunnel) so cleanup is automatic
# when the tunnel object is GC'd.
_ws_inflate_ctx:  dict = {}
_ws_deflate_ctx:  dict = {}
_ws_ctx_lock = threading.Lock()

def _ws_deflate_init(tunnel_id: int) -> None:
    """Initialize permessage-deflate contexts for one tunnel direction."""
    if not WS_DEFLATE:
        return
    with _ws_ctx_lock:
        if tunnel_id not in _ws_deflate_ctx:
            _ws_deflate_ctx[tunnel_id] = zlib.compressobj(
                zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED,
                -_WS_PMDE_WINDOW_BITS, _WS_PMDE_MEM_LEVEL,
                zlib.Z_DEFAULT_STRATEGY,
            )
        if tunnel_id not in _ws_inflate_ctx:
            # -zlib.MAX_WBITS = raw deflate (no zlib header)
            _ws_inflate_ctx[tunnel_id] = zlib.decompressobj(-_WS_PMDE_WINDOW_BITS)

def _ws_deflate_msg(tunnel_id: int, data: bytes) -> bytes:
    """Compress one WS message per RFC 7692: deflate, append 0x00 0x00 0xFF 0xFF."""
    c = _ws_deflate_ctx.get(tunnel_id)
    if c is None:
        return data
    out = c.compress(data) + c.flush(zlib.Z_SYNC_FLUSH)
    # RFC 7692 §7.2.1: strip the trailing 0x00 0x00 0xFF 0xFF
    if out.endswith(b"\x00\x00\xff\xff"):
        out = out[:-4]
    return out

def _ws_inflate_msg(tunnel_id: int, data: bytes) -> bytes:
    """Decompress one permessage-deflate message."""
    d = _ws_inflate_ctx.get(tunnel_id)
    if d is None:
        return data
    # RFC 7692 §7.2.2: append the trailing 4 bytes before decompressing
    try:
        return d.decompress(data + b"\x00\x00\xff\xff") + d.flush()
    except Exception:
        # If decompression fails (context desync, missing tail), return raw
        # data so the message isn't silently dropped — the app layer will
        # likely fail to parse it, which is more debuggable than a hang.
        return data

def _ws_deflate_cleanup(tunnel_id: int) -> None:
    """Release deflate contexts for a closed tunnel."""
    with _ws_ctx_lock:
        _ws_deflate_ctx.pop(tunnel_id, None)
        _ws_inflate_ctx.pop(tunnel_id, None)

# Message hook registry — called for every WS message in either direction.
# Signature: fn(direction: str, opcode: int, payload: bytes, tunnel_id: int)
# direction is "in" (browser→server) or "out" (server→browser)
_WS_MSG_HOOKS: list[Callable] = []

def on_ws_message(fn: Callable) -> Callable:
    """Register a hook called for every WS message passing through any tunnel."""
    _WS_MSG_HOOKS.append(fn)
    log(f"ws msg hook → {fn.__name__}()", "HOOK")
    return fn

def _run_ws_msg_hooks(direction: str, opcode: int, payload: bytes, tunnel_id: int) -> None:
    if not _WS_MSG_HOOKS:
        return
    for fn in _WS_MSG_HOOKS:
        try:
            fn(direction, opcode, payload, tunnel_id)
        except Exception as exc:
            log(f"ws msg hook {fn.__name__} raised: {exc}", "ERROR")

class _WSTunnel:
    """Stateful wrapper around a browser↔upstream WS tunnel.

    Tracks per-tunnel deflate contexts, ping/pong keepalive, reconnect
    attempts, and message accounting. Used by _pump_ws_frames_v2 below.
    """
    def __init__(self, client_sock, srv, ws_url: str, use_deflate: bool):
        self.client_sock = client_sock
        self.srv         = srv
        self.ws_url      = ws_url
        self.use_deflate = use_deflate
        self.tunnel_id   = id(self)
        self.stop        = threading.Event()
        self.last_pong   = time.time()
        self.msgs_in     = 0
        self.msgs_out    = 0
        self.bytes_in    = 0
        self.bytes_out   = 0
        # Bookkeeping for the deflate direction flag — RFC 7692 says the
        # RSV1 bit on the FIRST frame of a message indicates compression.
        # Continuation frames never carry it.
        self._client_deflate_seen = False
        self._srv_deflate_seen    = False

    def close(self):
        """Close both sides and release deflate contexts."""
        self.stop.set()
        for s in (self.srv, self.client_sock):
            try: s.close()
            except Exception: pass
        _ws_deflate_cleanup(self.tunnel_id)

def _ws_keepalive_loop(tunnel: "_WSTunnel") -> None:
    """Send periodic pings to the upstream server. If pong doesn't arrive
    within WS_PONG_TIMEOUT, declare the tunnel dead and tear it down.

    FIXED: the previous version initialized `tunnel.last_pong = time.time()`
    in __init__ and then checked `time.time() - last_pong > WS_PONG_TIMEOUT`
    on EVERY ping cycle. With WS_PING_INTERVAL=20s and WS_PONG_TIMEOUT=30s,
    that meant: T=0 tunnel opens (last_pong=T0), T=20 first ping sent +
    immediately checked → 20 < 30, OK; T=40 second ping sent + checked →
    40 > 30 → tunnel declared dead at T=40, regardless of whether the
    upstream ever ponged. The 40-second death we saw on a real-time
    gateway matches this exactly.

    The fix: track the timestamp of the LAST PING SENT separately from
    last_pong. After sending a ping, wait WS_PONG_TIMEOUT for a pong. If
    last_pong hasn't advanced past the ping-send time by then, declare the
    tunnel dead. This correctly handles both cases:
      - Upstream is alive → pong arrives within milliseconds → last_pong
        advances → check passes → next ping cycle.
      - Upstream is dead → no pong ever → last_pong stays at the previous
        value → check fires after WS_PONG_TIMEOUT → tunnel torn down.

    Guard: this function must NEVER be called with WS_PING_INTERVAL <= 0.
    The caller (_pump_ws_frames_v2) already guards against that, but we
    double-check here to prevent a tight CPU spin: Event.wait(0) returns
    immediately (False), which would turn the while-loop into a 100% CPU
    busy loop. Bail out safely instead.
    """
    if WS_PING_INTERVAL <= 0:
        return   # keepalive disabled — should never be called, but guard anyway
    _last_ping_sent = 0.0   # timestamp of the most recent ping we sent
    while not tunnel.stop.wait(WS_PING_INTERVAL):
        if tunnel.stop.is_set():
            return
        try:
            # Send ping to upstream. For _CffiUpstreamWS this previously was
            # a no-op (see fix in _CffiUpstreamWS.send_frame) — now it sends
            # a real WS PING frame, which the upstream responds to with PONG.
            ok = tunnel.srv.send_frame(_WS_OPCODE_PING, os.urandom(4))
            if not ok:
                log(f"WS keepalive: upstream send failed → {tunnel.ws_url}", "DEBUG")
                tunnel.stop.set()
                return
            _last_ping_sent = time.time()
            # Wait for the pong window to elapse. If the tunnel is closed
            # externally during this wait, exit cleanly.
            if tunnel.stop.wait(WS_PONG_TIMEOUT):
                return   # tunnel was closed while we were waiting
            if tunnel.stop.is_set():
                return
            # If no pong arrived since we sent the ping (i.e. last_pong is
            # still older than _last_ping_sent), declare the tunnel dead.
            # The 0.5s grace absorbs thread-scheduling jitter on busy systems.
            if tunnel.last_pong < _last_ping_sent - 0.5:
                log(f"WS keepalive: pong timeout → {tunnel.ws_url}", "WARN")
                tunnel.stop.set()
                return
        except Exception:
            tunnel.stop.set()
            return

def _pump_ws_frames_v2(client_sock, srv, ws_url: str, use_deflate: bool = False) -> None:
    """v2 frame pump: bidirectional relay with ping/pong keepalive, deflate,
    message hooks, and auto-reconnect.

    Drop-in replacement for _pump_ws_frames when WS_PING_INTERVAL > 0 or
    WS_DEFLATE is enabled. Otherwise the original _pump_ws_frames is used.
    """
    tunnel = _WSTunnel(client_sock, srv, ws_url, use_deflate)
    if use_deflate:
        _ws_deflate_init(tunnel.tunnel_id)

    log(f"WS tunnel open → {ws_url}  (deflate={'on' if use_deflate else 'off'}, "
        f"ping={WS_PING_INTERVAL}s)", "INFO")

    # Start keepalive thread
    keepalive_t = None
    if WS_PING_INTERVAL > 0:
        keepalive_t = threading.Thread(
            target=_ws_keepalive_loop, args=(tunnel,),
            daemon=True, name=f"ws-keepalive-{tunnel.tunnel_id}",
        )
        keepalive_t.start()

    def _fwd_client_to_srv():
        """Browser → upstream server.

        FIXED (zlib/permessage-deflate): uses the actual RSV1 bit from the
        browser's frame to decide whether to inflate, instead of blindly
        assuming every frame is compressed when use_deflate=True. Per RFC 7692
        §6.1, RSV1 is only set on the FIRST frame of a message; continuation
        frames never carry it. We forward the decompressed message to the
        upstream WITHOUT RSV1 (we never negotiated permessage-deflate with
        the upstream, so it must receive plain frames).
        """
        frag_opcode = None
        frag_buf = bytearray()
        frag_compressed = False
        while not tunnel.stop.is_set():
            frame = _ws_read_frame(client_sock)
            if frame is None:
                break
            fin, rsv1, opcode, payload = frame
            if WS_LOG_FRAMES:
                _op_name = _WS_OP_NAMES.get(opcode, f"?{opcode}")
                log(f"WS→ {ws_url} fin={fin} rsv1={int(rsv1)} op={_op_name} len={len(payload)}", "DEBUG")
            if opcode == _WS_OPCODE_CLOSE:
                tunnel.srv.send_frame(_WS_OPCODE_CLOSE, payload)
                break
            if opcode in (_WS_OPCODE_CONTINUATION, _WS_OPCODE_TEXT, _WS_OPCODE_BINARY):
                # RFC 7692 §6.1: RSV1 on the FIRST frame of a message marks
                # the entire message as compressed. Continuation frames never
                # carry RSV1 — the compressed/not-compressed decision is made
                # once, at the first frame, and sticks for the whole message.
                if opcode != _WS_OPCODE_CONTINUATION:
                    frag_opcode = opcode
                    frag_buf = bytearray(payload)
                    frag_compressed = use_deflate and rsv1
                else:
                    frag_buf += payload
                if not fin:
                    continue
                msg = bytes(frag_buf)
                if frag_compressed:
                    msg = _ws_inflate_msg(tunnel.tunnel_id, msg)
                _run_ws_msg_hooks("in", frag_opcode or opcode, msg, tunnel.tunnel_id)
                # Apply GUI WS hooks (editable hex overrides) then push the
                # post-hook payload to the GUI traffic log for inspection.
                # Wrapped in try/except so a hook error NEVER kills the tunnel —
                # the site stays functional even if the hook has a bad pattern
                # or the GUI queue is full.
                try:
                    msg = _apply_gui_ws_hooks("in", frag_opcode or opcode, msg,
                                              ws_url, tunnel.tunnel_id)
                except Exception as _hk:
                    log(f"WS hook error (in): {_short_exc(_hk)}", "WARN")
                try:
                    _gui_ws_push("in", frag_opcode or opcode, msg, ws_url, tunnel.tunnel_id)
                except Exception:
                    pass
                tunnel.msgs_in  += 1
                tunnel.bytes_in += len(msg)
                # Forward to upstream uncompressed (no RSV1) — we did not
                # negotiate permessage-deflate with the upstream, so it would
                # choke on a frame with RSV1 set.
                ok = tunnel.srv.send_frame(frag_opcode or opcode, msg)
                frag_opcode, frag_buf, frag_compressed = None, bytearray(), False
                if not ok:
                    break
            elif opcode == _WS_OPCODE_PING:
                # Browser pings us → respond with pong directly
                try:
                    client_sock.sendall(_ws_make_frame(_WS_OPCODE_PONG, payload))
                except Exception:
                    break
            elif opcode == _WS_OPCODE_PONG:
                # Browser PONG — unsolicited (we never PING the browser).
                # Do NOT update tunnel.last_pong here: that field tracks
                # UPSTREAM PONGs (responses to our keepalive pings sent via
                # _ws_keepalive_loop). A browser PONG has nothing to do with
                # upstream health — updating last_pong here would mask a dead
                # upstream and prevent the keepalive thread from detecting it.
                pass
        tunnel.stop.set()

    def _fwd_srv_to_client():
        """Upstream server → browser.

        FIXED (zlib/permessage-deflate): the previous code unconditionally
        set `frag_compressed = use_deflate` and then called _ws_inflate_msg on
        every upstream frame — but we NEVER negotiate permessage-deflate with
        the upstream (Sec-WebSocket-Extensions is stripped from the outbound
        handshake), so upstream frames are NEVER permessage-deflate compressed.
        Trying to inflate them either errored out (raw deflate vs. zlib-wrapped
        application payloads → "incorrect header check") or silently returned the
        raw bytes via the exception handler, then re-compressed the result and
        shipped it to the browser WITHOUT setting RSV1 — so the browser
        couldn't tell it was compressed and applied its own (application-level
        zlib-stream) decompressor to mangled bytes.

        The correct flow: upstream frames are always plain (rsv1=False in
        practice). We never inflate. If the browser negotiated permessage-
        deflate, we re-compress the message and SET RSV1 on the outgoing
        frame so the browser knows to inflate.
        """
        frag_opcode = None
        frag_buf = bytearray()
        while not tunnel.stop.is_set():
            frame = srv.recv_frame()
            if frame is None:
                break
            fin, rsv1, opcode, payload = frame
            if WS_LOG_FRAMES:
                _op_name = _WS_OP_NAMES.get(opcode, f"?{opcode}")
                log(f"WS← {ws_url} fin={fin} rsv1={int(rsv1)} op={_op_name} len={len(payload)}", "DEBUG")
            if opcode == _WS_OPCODE_CLOSE:
                try:
                    client_sock.sendall(_ws_make_frame(_WS_OPCODE_CLOSE, payload))
                except Exception:
                    pass
                break
            if opcode == _WS_OPCODE_PONG:
                tunnel.last_pong = time.time()
                continue
            if opcode == _WS_OPCODE_PING:
                # Upstream pings us → respond with pong to upstream
                if not srv.send_frame(_WS_OPCODE_PONG, payload):
                    break
                continue
            if opcode in (_WS_OPCODE_CONTINUATION, _WS_OPCODE_TEXT, _WS_OPCODE_BINARY):
                if opcode != _WS_OPCODE_CONTINUATION:
                    frag_opcode = opcode
                    frag_buf = bytearray(payload)
                else:
                    frag_buf += payload
                if not fin:
                    continue
                msg = bytes(frag_buf)
                # Upstream never compresses (we didn't negotiate), so there is
                # nothing to inflate here. (If curl_cffi DID auto-negotiate
                # permessage-deflate upstream-side, it already decompressed
                # the payload for us — rsv1 is reported False either way, and
                # we must not double-inflate.)
                _run_ws_msg_hooks("out", frag_opcode or opcode, msg, tunnel.tunnel_id)
                # Apply GUI WS hooks (editable hex overrides) then push the
                # post-hook payload to the GUI traffic log for inspection.
                # Wrapped in try/except so a hook error NEVER kills the tunnel.
                try:
                    msg = _apply_gui_ws_hooks("out", frag_opcode or opcode, msg,
                                              ws_url, tunnel.tunnel_id)
                except Exception as _hk:
                    log(f"WS hook error (out): {_short_exc(_hk)}", "WARN")
                try:
                    _gui_ws_push("out", frag_opcode or opcode, msg, ws_url, tunnel.tunnel_id)
                except Exception:
                    pass
                tunnel.msgs_out  += 1
                tunnel.bytes_out += len(msg)
                # Re-compress for the browser leg if it negotiated deflate,
                # and SET RSV1 so the browser actually knows to inflate.
                out_payload = msg
                out_rsv1    = False
                if use_deflate:
                    out_payload = _ws_deflate_msg(tunnel.tunnel_id, msg)
                    out_rsv1    = True
                try:
                    client_sock.sendall(_ws_make_frame(
                        frag_opcode or opcode, out_payload, fin=fin, rsv1=out_rsv1))
                except Exception:
                    break
                frag_opcode, frag_buf = None, bytearray()
        tunnel.stop.set()

    t1 = threading.Thread(target=_fwd_client_to_srv, daemon=True, name="ws-c2s")
    t2 = threading.Thread(target=_fwd_srv_to_client, daemon=True, name="ws-s2c")
    t1.start(); t2.start()
    tunnel.stop.wait()

    tunnel.close()
    # Join relay threads with a timeout — they may be blocked in recv() and
    # closing the sockets doesn't always unblock curl_cffi's recv on all
    # platforms. Without this join, daemon threads accumulate (one pair per
    # WS connection) and leak file descriptors / memory.
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    if keepalive_t is not None:
        keepalive_t.join(timeout=2.0)
    _ws_deflate_cleanup(tunnel.tunnel_id)
    log(f"WS tunnel closed ← {ws_url}  (in={tunnel.msgs_in}/{_fmt_size(tunnel.bytes_in)}, "
        f"out={tunnel.msgs_out}/{_fmt_size(tunnel.bytes_out)})", "INFO")

def _ws_handshake_accept(key: str) -> str:
    """Compute the Sec-WebSocket-Accept value for the handshake response."""
    magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    sha1  = hashlib.sha1((key + magic).encode()).digest()
    return base64.b64encode(sha1).decode()

def _recv_exact(sock, n: int) -> bytes | None:
    """Read exactly n bytes, or return None if the peer closes first.

    Also guards against the worst WS stability bug: a peer that goes quiet
    WITHOUT closing the socket (NAT timeout, mobile sleep, half-open TCP).
    The original loop would block forever on recv() in that case, pinning
    one worker thread per stuck WS tunnel. We now set a socket-level
    timeout so the recv() raises socket.timeout instead, which the caller
    already converts to None → clean tunnel shutdown.
    """
    buf = b""
    try:
        sock.settimeout(WS_PONG_TIMEOUT)
    except Exception:
        pass
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except (socket.timeout, TimeoutError):
            return None
        except OSError:
            return None
        if not chunk:
            return None
        buf += chunk
    return buf

def _ws_read_frame(sock) -> tuple[bool, bool, int, bytes] | None:
    """Read a single WebSocket frame.

    FIXED: now returns (fin, rsv1, opcode, payload) — the RSV1 bit is required
    to correctly implement permessage-deflate (RFC 7692 §6.1): only frames
    with RSV1 set on the FIRST frame of a message are compressed. The previous
    3-tuple discarded RSV1, forcing the pump to guess whether a frame was
    compressed based solely on the negotiated use_deflate flag — which broke
    for any frame the peer chose not to compress, and made it impossible to
    distinguish permessage-deflate from an application's unrelated zlib-stream scheme.

    Returns None on EOF, timeout, or protocol error.
    """
    try:
        h = _recv_exact(sock, 2)
        if h is None:
            return None
        b0, b1  = h
        fin     = bool(b0 & 0x80)
        rsv1    = bool(b0 & 0x40)   # permessage-deflate marker (RFC 7692 §6.1)
        opcode  = b0 & 0x0F
        masked  = bool(b1 & 0x80)
        pay_len = b1 & 0x7F
        if pay_len == 126:
            l2 = _recv_exact(sock, 2)
            if l2 is None:
                return None
            pay_len = struct.unpack("!H", l2)[0]
        elif pay_len == 127:
            l8 = _recv_exact(sock, 8)
            if l8 is None:
                return None
            pay_len = struct.unpack("!Q", l8)[0]
        mask_key = b""
        if masked:
            mask_key = _recv_exact(sock, 4)
            if mask_key is None:
                return None
        # Cap payload size to prevent a malicious or buggy peer from
        # exhausting memory with a multi-GB frame. 8 MiB is well above any
        # legitimate WS message (typical gateway payloads are < 100 KB;
        # even the largest real-time APIs cap at ~1 MB). Anything bigger is
        # almost certainly an attack or a desync.
        if pay_len > WS_MAX_MSG_BYTES:
            log(f"WS frame too large ({pay_len} bytes > {WS_MAX_MSG_BYTES}) — dropping",
                "WARN")
            return None
        # Use bytearray + extend for better memory performance on large frames
        # (bytearray.extend avoids the quadratic copy of bytes concatenation).
        _payload = bytearray()
        _remaining = pay_len
        while _remaining > 0:
            chunk = sock.recv(min(65536, _remaining))
            if not chunk:
                return None
            _payload.extend(chunk)
            _remaining -= len(chunk)
        payload = bytes(_payload)
        if masked:
            # Fast C-level unmask instead of per-byte Python loop — the
            # original `bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))`
            # path was the #1 CPU hot spot on busy WS tunnels (active
            # gateways can push hundreds of small frames per second).
            mk = mask_key
            payload = _ws_unmask(payload, mk)
        return fin, rsv1, opcode, payload
    except Exception:
        return None

def _ws_make_frame(opcode: int, payload: bytes, mask: bool = False,
                   fin: bool = True, rsv1: bool = False) -> bytes:
    """Build a WebSocket frame (server → client: no mask; client → server: masked).

    `fin=False` is required to correctly relay one piece of a fragmented
    message — leaving this hardcoded to True (as if every frame were always
    a complete, standalone message) silently mis-tags every continuation
    frame in a fragmented message as if it were the final one.

    FIXED: `rsv1=True` sets the RSV1 bit (0x40) on the first byte. This is
    MANDATORY for permessage-deflate (RFC 7692 §6.1): the decompressor on the
    receiving side will not attempt to inflate a message whose first frame
    lacks RSV1. The previous version had no way to set RSV1, so every
    "compressed" frame it emitted was indistinguishable from a plain frame —
    the receiver either parsed raw deflate bytes as JSON (garbage) or, on
    an application using its own zlib-stream, applied its decompressor to the
    mangled payload and crashed with "zlib error, -3, incorrect header check".
    """
    l = len(payload)
    h = bytearray()
    b0 = (0x80 if fin else 0x00) | (0x40 if rsv1 else 0x00) | opcode
    h.append(b0)   # FIN + RSV1 + opcode
    if l < 126:
        h.append((0x80 if mask else 0) | l)
    elif l < 65536:
        h.append((0x80 if mask else 0) | 126)
        h.extend(struct.pack("!H", l))
    else:
        h.append((0x80 if mask else 0) | 127)
        h.extend(struct.pack("!Q", l))
    if mask:
        mk = os.urandom(4)
        h.extend(mk)
        payload = _ws_unmask(payload, mk)   # mask == unmask XOR semantics
    return bytes(h) + payload

class _RawSocketUpstreamWS:
    """Upstream WS connection over a bare Python `ssl` socket.

    This was the only implementation before, and it's why WS endpoints
    behind an anti-bot WAF (CF-protected gateways, similar Cloudflare-fronted
    realtime APIs) failed the handshake every single time: those edges
    fingerprint the *opening TLS ClientHello* (JA3/JA4), and Python's `ssl`
    module produces one that's trivially distinguishable from a real
    browser's no matter what cipher list or ALPN gets bolted onto it —
    that's a property of OpenSSL's fixed extension set/ordering, not
    something fixable from Python's ssl API. Kept as the fallback for
    targets that don't need spoofing, or for when curl_cffi is unavailable.
    """

    def __init__(self, ws_url: str, extra_headers: dict):
        parsed  = urlparse(ws_url)
        use_ssl = ws_url.startswith("wss://")
        host    = parsed.hostname    # strips [] from IPv6 literals
        port    = parsed.port or (443 if use_ssl else 80)
        path_qs = parsed.path or "/"
        if parsed.query:
            path_qs += "?" + parsed.query
        # For the Host header, reconstruct the netloc with brackets for IPv6.
        # parsed.hostname strips brackets, but HTTP Host: must include them.
        if ":" in (host or "") and not host.startswith("["):
            host_hdr = f"[{host}]"
        else:
            host_hdr = host or ""
        if port and not ((use_ssl and port == 443) or (not use_ssl and port == 80)):
            host_hdr = f"{host_hdr}:{port}"
        # SNI: don't send an IP literal as server_hostname — many servers/WAFs
        # reject SNI that looks like an IP (RFC 6066 says SNI is for DNS names).
        # Pass None for IP literals so OpenSSL sends an empty SNI extension.
        sni_hostname = host if host and not _is_ip_literal(host) else None

        raw = socket.create_connection((host, port), timeout=15)
        srv = raw
        try:
            if use_ssl:
                try:
                    # Best-effort nudge toward a Chrome-like ClientHello (ALPN +
                    # cipher order) — meaningfully less fingerprintable than the
                    # bare interpreter default, but not a real JA3 match. See
                    # _CffiUpstreamWS below for the actual fix.
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                    ctx.check_hostname = False
                    ctx.verify_mode    = ssl.CERT_NONE
                    ctx.set_alpn_protocols(["http/1.1"])
                    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
                    try:
                        ctx.set_ciphers(
                            "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
                            "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
                            "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305"
                        )
                    except ssl.SSLError:
                        pass
                except Exception:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode    = ssl.CERT_NONE
                # Set a longer timeout for the TLS handshake — IPv6 connections
                # to remote servers can take longer than the default, and a timeout
                # here produces "The handshake operation timed out" which the
                # user saw in the log.
                raw.settimeout(30)
                srv = ctx.wrap_socket(raw, server_hostname=sni_hostname)
                raw.settimeout(15)   # back to normal for WS handshake

            # RFC 6455 §4.1: Sec-WebSocket-Key is 16 random bytes, base64-encoded
            # DIRECTLY — no hashing. SHA1 only enters the picture on the SERVER
            # side, to compute Sec-WebSocket-Accept from this key (see
            # _ws_handshake_accept). Hashing it here first produced a 20-byte
            # value (28 base64 chars instead of the required 24), which lenient
            # servers ignore but strict ones reject outright with a 400 before
            # ever reaching the application — silently sabotaging every upstream
            # WS connection this function ever made, independent of TLS/anti-bot
            # concerns.
            key = base64.b64encode(os.urandom(16)).decode()
            hdrs = [
                f"GET {path_qs} HTTP/1.1",
                f"Host: {host_hdr}",
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
                raise ConnectionError(f"WS handshake rejected: {resp[:200]!r}")
            # Preserve any bytes the server sent right after the 101 handshake
            # (pipelined first WS frame). Without this, realtime upstreams that
            # fire a frame immediately after upgrading lose those leading bytes
            # and the tunnel desyncs permanently.
            _hdr_end = resp.index(b"\r\n\r\n") + 4
            _leftover = resp[_hdr_end:]
        except Exception:
            # Any failure past this point — TLS handshake, send, a dropped
            # connection mid-handshake, or an outright rejection — must not
            # leak the socket. This runs on every reconnect attempt a
            # backing-off client makes against a target that's actively
            # blocking it, so a leak here isn't a one-off: it's one held
            # file descriptor per retry, forever, for as long as the target
            # keeps saying no.
            try: srv.close()
            except Exception: pass
            raise
        self._sock = srv
        self._pushback = bytearray(_leftover)   # pre-frame bytes from handshake read
        # Dedicated send-lock: socket.sendall is NOT thread-safe, and the
        # keepalive thread (when WS_PING_INTERVAL > 0) can call send_frame()
        # concurrently with the relay thread. recv stays lock-free to avoid
        # deadlocking on a blocking recv.
        self._send_lock = threading.Lock()

    def _recv_exact_pushback(self, n: int) -> bytes | None:
        """Read exactly n bytes, draining the pushback buffer first, then the
        socket. Returns None on EOF/timeout (same semantics as _recv_exact)."""
        buf = bytearray()
        # Drain pushback first
        if self._pushback:
            take = min(n, len(self._pushback))
            buf.extend(self._pushback[:take])
            del self._pushback[:take]
        # Read the rest from the socket
        while len(buf) < n:
            try:
                chunk = self._sock.recv(n - len(buf))
            except (socket.timeout, TimeoutError, OSError):
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def recv_frame(self):
        # _ws_read_frame now returns (fin, rsv1, opcode, payload). We never
        # negotiate permessage-deflate with the upstream on this path (the
        # Sec-WebSocket-Extensions header is stripped from the outbound
        # handshake), so rsv1 will always be False here in practice — but we
        # pass it through verbatim so the v2 pump can make its own decision.
        #
        # Inline frame reading that drains self._pushback first: we can't
        # delegate to _ws_read_frame(self._sock) because that would bypass
        # the pushback buffer and lose pipelined bytes from the handshake.
        try:
            h = self._recv_exact_pushback(2)
            if h is None:
                return None
            b0, b1  = h
            fin     = bool(b0 & 0x80)
            rsv1    = bool(b0 & 0x40)
            opcode  = b0 & 0x0F
            masked  = bool(b1 & 0x80)
            pay_len = b1 & 0x7F
            if pay_len == 126:
                l2 = self._recv_exact_pushback(2)
                if l2 is None: return None
                pay_len = struct.unpack("!H", l2)[0]
            elif pay_len == 127:
                l8 = self._recv_exact_pushback(8)
                if l8 is None: return None
                pay_len = struct.unpack("!Q", l8)[0]
            mask_key = b""
            if masked:
                mask_key = self._recv_exact_pushback(4)
                if mask_key is None: return None
            if pay_len > WS_MAX_MSG_BYTES:
                log(f"WS upstream frame too large ({pay_len} bytes > {WS_MAX_MSG_BYTES}) — dropping", "WARN")
                return None
            payload = self._recv_exact_pushback(pay_len)
            if payload is None:
                return None
            if masked:
                payload = _ws_unmask(payload, mask_key)
            return fin, rsv1, opcode, payload
        except Exception:
            return None

    def send_frame(self, opcode: int, payload: bytes) -> bool:
        # Serialize sends to prevent frame interleaving when the keepalive
        # thread and the relay thread send concurrently. recv stays lock-free.
        with self._send_lock:
            try:
                self._sock.sendall(_ws_make_frame(opcode, payload, mask=True))
                return True
            except Exception:
                return False

    def close(self):
        try: self._sock.close()
        except Exception: pass

def _get_session_for_ws():
    """Get a session for the WS connection.

    FIX v3: Try to reuse the client session (which has cf_clearance cookies and
    the same TLS fingerprint as the main page). This is critical for
    CF-protected WS endpoints — a fresh session gets 403'd.

    Returns (session, should_close):
      - should_close=False: shared session, do NOT close after use
      - should_close=True:  freshly created, safe to close
    """
    # First try the client session (per-browser, has cookies)
    try:
        sess = _get_client_session()
        if sess is not None and hasattr(sess, "ws_connect"):
            return sess, False
    except Exception:
        pass
    # Fall back to the proxy session
    try:
        sess = _get_proxy_session()
        if sess is not None and hasattr(sess, "ws_connect"):
            return sess, False
    except Exception:
        pass
    # Last resort: create a fresh session
    return _make_session(), True

class _CffiUpstreamWS:
    """Upstream WS connection via curl_cffi's own WebSocket client.

    This is what actually fixes gateway-style endpoints behind anti-bot WAFs:
    the opening TLS handshake goes through the SAME browser-impersonation
    engine _make_session() uses for every plain HTTP request in this file
    (real JA3/JA4, not a hand-tuned approximation), because ws_connect()
    reuses the session's already-impersonated curl handle.

    Trade-off: curl_cffi reassembles fragmented messages internally and
    only hands back / accepts complete ones — it can't stream individual
    wire frames the way the raw-socket path does. That's invisible to any
    real WS consumer (onmessage always sees the whole reassembled message
    either way); it only costs a bit of extra latency/memory on unusually
    large messages.
    """

    def __init__(self, ws_url: str, extra_headers: dict):
        # curl_cffi has a known bug with IPv6 address literals in ws_connect:
        # "TLS connect error: error:00000000:invalid library" — the internal
        # OpenSSL layer chokes on IPv6 SNI. Skip curl_cffi ONLY for IPv6
        # literals and let _ws_connect_upstream fall through to the raw-socket
        # path. IPv4 literals work fine with curl_cffi, so we still benefit
        # from browser TLS fingerprinting for IPv4 hosts.
        _parsed_ws = urlparse(ws_url)
        _ws_host = _parsed_ws.hostname or ""
        if _is_ipv6_literal(_ws_host):
            raise RuntimeError(f"curl_cffi skipped for IPv6 literal {_ws_host} (known IPv6 TLS bug)")
        hdrs = {k: v for k, v in extra_headers.items()
                if k.lower() not in ("host", "upgrade", "connection",
                                      "sec-websocket-key", "sec-websocket-version",
                                      "sec-websocket-extensions", "accept-encoding")}
        # FIX v3: Reuse the CLIENT session (which has cf_clearance cookies +
        # the same TLS fingerprint as the main page) instead of creating a
        # fresh one. This is what fixes CF-protected WS endpoints
        # — a fresh session has no cookies and gets 403'd by Cloudflare.
        sess, should_close = _get_session_for_ws()
        if not hasattr(sess, "ws_connect"):
            if should_close:
                try: sess.close()
                except Exception: pass
            raise RuntimeError("active session backend has no WS support")
        try:
            self._ws = sess.ws_connect(ws_url, headers=hdrs, timeout=15)
        except Exception as exc:
            # A rejected/failed handshake (403 from a WAF, refused upgrade,
            # TLS failure, ...) must not leak the session + its underlying
            # curl handle. Only close if we created it (shared client sessions
            # must NOT be closed here — they're used by other requests).
            if should_close:
                try: sess.close()
                except Exception: pass
                gc.collect()
            # FIX v3: Log at WARN so the user can see WHY the WS failed
            # (was DEBUG — invisible in normal operation).
            log(f"curl_cffi ws_connect failed: {exc}", "WARN")
            raise
        self._sess = sess
        self._should_close = should_close
        # FIXED: curl_cffi's WebSocket object is NOT thread-safe. Concurrent
        # send() (from keepalive thread) and recv() (from relay thread) corrupt
        # internal buffers, producing garbled data that desyncs the browser's
        # application-level decompressor (e.g. zlib-stream: "invalid stored
        # block lengths"). This lock serializes ALL access to self._ws.
        # Note: recv() is a blocking call — while it holds the lock, send()
        # will block until recv() returns. This means keepalive pings only fire
        # BETWEEN messages, not at precise intervals. That's acceptable because
        # WS_PING_INTERVAL defaults to 0 (disabled) — most apps have their own
        # application-level heartbeats that keep the connection alive.
        self._ws_lock = threading.Lock()
        # Dedicated send-lock: serialize sends from the keepalive thread and
        # the relay thread. recv stays lock-free (see recv_frame docstring).
        self._send_lock = threading.Lock()

    def recv_frame(self):
        # Returns (fin, rsv1, opcode, payload) to match _ws_read_frame signature.
        #
        # IMPORTANT: do NOT wrap this in a lock. recv() is a blocking call that
        # can wait indefinitely for the next upstream frame. If a lock were held
        # here, send_frame() (called from the relay thread forwarding browser
        # messages like heartbeats) would block waiting for the lock, causing a
        # deadlock: upstream is quiet → recv blocks → send can't fire → browser
        # heartbeat never reaches upstream → upstream stays quiet → deadlock.
        #
        # The curl_cffi WebSocket tolerates concurrent send()/recv() from
        # different threads in practice (this proxy has always done so). The
        # zlib-stream corruption we saw earlier was caused by a THIRD thread —
        # the keepalive thread — calling send(PING) while recv() was blocked.
        # That thread is now disabled by default (WS_PING_INTERVAL=0), so
        # there's no concurrent send/recv race to protect against.
        #
        # PONG handling: curl_cffi has no CurlWsFlag.PONG. When upstream pongs
        # our PING, libcurl auto-consumes it. recv() returns (b"", 0) for a
        # pure-PONG wakeup — we map that to opcode 10 so the v2 pump (if
        # active) can update last_pong.
        try:
            data, flags = self._ws.recv()
        except Exception:
            return None
        if flags & _CurlWsFlag.CLOSE:
            return (True, False, 8, data)
        if flags & _CurlWsFlag.PING:
            return (True, False, 9, data)
        # No flags + no data → treat as auto-consumed PONG (libcurl eat it)
        if flags == 0 and not data:
            return (True, False, 10, b"")
        # NOTE: do NOT treat CurlWsFlag.OFFSET (value 32) as a PONG marker.
        # OFFSET is libcurl's flag for fragmented-message offset metadata,
        # not a PONG indicator. Treating it as PONG caused real data frames
        # carrying OFFSET to be silently dropped by the v2 pump's PONG handler.
        opcode = 1 if (flags & _CurlWsFlag.TEXT) else 2
        return (True, False, opcode, data)

    def send_frame(self, opcode: int, payload: bytes) -> bool:
        # Serialize sends via _send_lock. recv() stays lock-free to avoid
        # deadlock (it can block indefinitely waiting for upstream frames).
        # The keepalive thread (WS_PING_INTERVAL > 0) and the relay thread
        # both call this — without the lock, concurrent curl_cffi .send()
        # calls can corrupt internal buffers.
        if opcode == 10:
            return True   # PONG — libcurl auto-responds to upstream PINGs
        with self._send_lock:
            if opcode == 9:
                # PING — send a real control frame
                try:
                    self._ws.send(payload or b"", _CurlWsFlag.PING)
                    return True
                except Exception:
                    return False
            flag = {1: _CurlWsFlag.TEXT, 8: _CurlWsFlag.CLOSE}.get(opcode, _CurlWsFlag.BINARY)
            try:
                self._ws.send(payload or b"", flag)
                return True
            except Exception:
                return False

    def close(self):
        try: self._ws.close()
        except Exception: pass
        # Only close the session if we created it — shared client sessions
        # are used by other in-flight HTTP requests and must not be closed.
        if getattr(self, "_should_close", False):
            try: self._sess.close()
            except Exception: pass

def _ws_connect_upstream(ws_url: str, extra_headers: dict):
    """
    Establish the WS connection to the real upstream server. Returns an
    object exposing .recv_frame() -> (fin, rsv1, opcode, payload) | None,
    .send_frame(opcode, payload) -> bool, and .close() — either a
    _CffiUpstreamWS (real browser TLS fingerprint, tried first whenever
    curl_cffi is available) or a _RawSocketUpstreamWS (fallback).

    Raises on total failure — caller must NOT have told the browser
    anything yet.
    """
    if _CURL_CFFI_OK:
        try:
            return _CffiUpstreamWS(ws_url, extra_headers)
        except Exception as e:
            # _CffiUpstreamWS already logs the curl_cffi failure at WARN.
            # Only log here if it was the IPv6-skip (which raises before the
            # WARN log) so the user sees why we fell back to raw socket.
            if "IPv6" in str(e):
                log(f"WS: {e} — using raw socket fallback", "INFO")
    return _RawSocketUpstreamWS(ws_url, extra_headers)

def _pump_ws_frames(client_sock, srv, ws_url: str) -> None:
    """Bidirectionally relay WS frames once both sides are connected and upgraded.

    `srv` is one of the wrapper objects from _ws_connect_upstream
    (_CffiUpstreamWS or _RawSocketUpstreamWS), not a raw socket — it always
    exposes .recv_frame() / .send_frame(opcode, payload) / .close().
    """
    log(f"WS tunnel open → {ws_url}", "INFO")
    stop = threading.Event()

    def _fwd_client_to_srv():
        """Browser → upstream server: frames are already masked by browser."""
        frag_opcode = None
        frag_buf = bytearray()
        while not stop.is_set():
            frame = _ws_read_frame(client_sock)
            if frame is None:
                break
            fin, _rsv1, opcode, payload = frame   # _rsv1 ignored in v1 (no deflate)
            if opcode == 8:   # close
                srv.send_frame(8, payload)
                break
            if opcode in (0, 1, 2):
                # Reassemble fragmented messages here rather than forwarding
                # each wire frame as it arrives: the curl_cffi upstream
                # wrapper has no notion of a partial/fragmented send, only
                # complete messages, so a multi-frame browser message must
                # become exactly one send_frame() call. Cheap for the
                # overwhelming majority of messages, which already arrive
                # as a single fin=True frame — this just skips straight to
                # sending in that case.
                if opcode != 0:
                    frag_opcode = opcode
                    frag_buf = bytearray(payload)
                else:
                    frag_buf += payload
                if not fin:
                    continue
                ok = srv.send_frame(frag_opcode if frag_opcode is not None else opcode, bytes(frag_buf))
                frag_opcode, frag_buf = None, bytearray()
                if not ok:
                    break
            elif opcode == 9:   # ping → pong (respond directly, per RFC 6455)
                try:
                    client_sock.sendall(_ws_make_frame(10, payload))
                except Exception:
                    break
            elif opcode == 10:   # unsolicited pong — pass through
                if not srv.send_frame(10, payload):
                    break
        stop.set()

    def _fwd_srv_to_client():
        """Upstream server → browser: frames are unmasked from server."""
        while not stop.is_set():
            frame = srv.recv_frame()
            if frame is None:
                break
            fin, _rsv1, opcode, payload = frame   # _rsv1 ignored in v1 (no deflate)
            if opcode == 8:
                try:
                    client_sock.sendall(_ws_make_frame(8, payload))
                except Exception:
                    pass
                break
            if opcode in (0, 1, 2):
                try:
                    client_sock.sendall(_ws_make_frame(opcode, payload, fin=fin))
                except Exception:
                    break
            elif opcode == 9:
                if not srv.send_frame(10, payload):
                    break
            elif opcode == 10:
                try:
                    client_sock.sendall(_ws_make_frame(10, payload))
                except Exception:
                    break
        stop.set()

    t1 = threading.Thread(target=_fwd_client_to_srv, daemon=True)
    t2 = threading.Thread(target=_fwd_srv_to_client, daemon=True)
    t1.start(); t2.start()
    stop.wait()
    # Close BOTH sides. Only closing `srv` here leaked the browser-side socket
    # on every WS teardown — harmless for a single connection, but `.io` titles open
    # many short-lived WS connections per session and this exhausted file
    # descriptors over time, surfacing as unrelated-looking "connection refused"
    # errors later in the same run.
    try: srv.close()
    except Exception: pass
    try: client_sock.close()
    except Exception: pass
    # Join relay threads with a timeout to avoid thread accumulation when the
    # blocking recv() doesn't unblock immediately on socket close.
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    log(f"WS tunnel closed ← {ws_url}", "INFO")

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

_SSE_CONTENT_TYPES = frozenset({
    "text/event-stream",
    "application/x-event-stream",
    "application/stream+json",   # some APIs use this for SSE-like streams
})

def _is_sse_response(ct: str) -> bool:
    """Check whether a Content-Type header indicates an SSE stream."""
    if not ct:
        return False
    base = ct.split(";", 1)[0].strip().lower()
    return base in _SSE_CONTENT_TYPES

def _sse_stream_generator(upstream_r, target: str):
    """Yield complete SSE events from upstream_r, reassembling chunks that
    cross event boundaries.

    SSE framing: events are separated by a blank line ("\n\n"). Each event
    is one or more lines of "field: value". We buffer until we see "\n\n"
    then yield the whole event as one chunk.
    """
    buf = b""
    try:
        for chunk in upstream_r.iter_content(chunk_size=4096, decode_unicode=False):
            if not chunk:
                continue
            buf += chunk
            # Split on event boundaries. Per the SSE spec, an event is delimited
            # by a blank line. Servers may use either "\n\n" (LF) or "\r\n\r\n"
            # (CRLF) as the line ending — handle both so CRLF-delimited events
            # don't buffer until the 65KB flush.
            while b"\n\n" in buf or b"\r\n\r\n" in buf:
                if b"\r\n\r\n" in buf and (b"\n\n" not in buf or buf.find(b"\r\n\r\n") < buf.find(b"\n\n")):
                    event, buf = buf.split(b"\r\n\r\n", 1)
                    yield event + b"\r\n\r\n"
                else:
                    event, buf = buf.split(b"\n\n", 1)
                    yield event + b"\n\n"
            # Yield any partial final event if buffer grows large (10KB+)
            # to avoid unbounded memory on a stream that never sends \n\n.
            if len(buf) > 65536:
                yield buf
                buf = b""
    except Exception as exc:
        log(f"SSE upstream stream error {urlparse(target).netloc}: {_short_exc(exc)}", "WARN")
        return
    # Flush any trailing partial event
    if buf:
        yield buf

def _stream_sse_resp(upstream_r, method: str, target: str) -> Response:
    """Build a Flask Response that streams SSE with proper buffering."""
    def _keepalive_injected():
        """Forward SSE events from upstream, injecting keepalive comments
        when the stream goes quiet for more than SSE_HEARTBEAT seconds.

        Uses a background thread to monitor idle time and inject keepalives
        (": ping\\n\\n" is a valid SSE comment per the spec — browsers ignore
        it but it keeps the connection alive through intermediate proxies/CDNs
        that would otherwise close idle connections).
        """
        if SSE_HEARTBEAT <= 0:
            # Heartbeat disabled — just forward chunks unchanged.
            yield from _sse_stream_generator(upstream_r, target)
            return

        buf_q: queue.Queue = queue.Queue(maxsize=256)
        upstream_done = threading.Event()
        last_event_time = [time.time()]

        def _reader():
            try:
                for chunk in _sse_stream_generator(upstream_r, target):
                    last_event_time[0] = time.time()
                    buf_q.put(chunk)
            except Exception as e:
                buf_q.put(e)
            finally:
                upstream_done.set()

        threading.Thread(target=_reader, daemon=True, name="sse-reader").start()

        while True:
            try:
                item = buf_q.get(timeout=1.0)
            except queue.Empty:
                # No event for 1s — check if we need to inject a keepalive.
                if upstream_done.is_set() and buf_q.empty():
                    break
                if time.time() - last_event_time[0] >= SSE_HEARTBEAT:
                    yield b": ping\n\n"
                    last_event_time[0] = time.time()
                continue
            if isinstance(item, Exception):
                log(f"SSE keepalive stream error: {_short_exc(item)}", "WARN")
                break
            yield item
            if upstream_done.is_set() and buf_q.empty():
                break

    headers = filter_resp(_resp_headers_dict(upstream_r))
    # Force chunked: Werkzeug doesn't auto-chunk generators with no Content-Length
    headers.pop("content-length", None)
    headers["Cache-Control"] = "no-cache, no-transform"
    headers["X-Accel-Buffering"] = "no"   # disable nginx buffering if behind one
    ct = upstream_r.headers.get("Content-Type", "text/event-stream")
    log_req(method, 200, urlparse(target).netloc, urlparse(target).path, -1, tag="SSE")
    return Response(
        _keepalive_injected(),
        status=upstream_r.status_code,
        headers=headers,
        content_type=ct,
    )

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

def _pump_tcp_over_ws(client_sock, upstream_sock, label: str,
                      use_udp: bool = False) -> None:
    """Bridge a WebSocket (browser) to a raw TCP/UDP socket (upstream).

    Browser→upstream: parse WS frames, write payload to the socket.
    Upstream→browser: read from the socket, wrap in WS binary frames.

    use_udp: when True, the upstream socket is a SOCK_DGRAM. UDP is
    connectionless — recv() returns b"" for a zero-length datagram, NOT
    on disconnect. Only break on recv() errors or timeout for UDP; break
    on empty data (peer closed) for TCP.
    """
    stop = threading.Event()
    tunnel_id = id(client_sock)
    with _active_tunnels_lock:
        _active_tunnels[tunnel_id] = {"label": label, "started": time.time(),
                                       "bytes_in": 0, "bytes_out": 0}

    def _ws_to_tcp():
        """Read WS frames from browser, write payload to upstream socket."""
        try:
            while not stop.is_set():
                frame = _ws_read_frame(client_sock)
                if frame is None:
                    break
                _fin, _rsv1, opcode, payload = frame   # _fin/_rsv1 ignored (raw tunnel, no deflate)
                if opcode == _WS_OPCODE_CLOSE:
                    break
                if opcode in (_WS_OPCODE_TEXT, _WS_OPCODE_BINARY, _WS_OPCODE_CONTINUATION):
                    if payload:
                        upstream_sock.sendall(payload)
                        with _active_tunnels_lock:
                            if tunnel_id in _active_tunnels:
                                _active_tunnels[tunnel_id]["bytes_in"] += len(payload)
                elif opcode == _WS_OPCODE_PING:
                    # RFC 6455 §5.5.2: server MUST respond to PING with a PONG.
                    # Browsers send keepalive PINGs on long-lived tunnels —
                    # without a PONG response they tear down the connection.
                    try:
                        client_sock.sendall(_ws_make_frame(_WS_OPCODE_PONG, payload))
                    except Exception:
                        break
                # PONG (opcode 10): unsolicited from browser, ignore.
        except Exception:
            pass
        finally:
            stop.set()

    def _tcp_to_ws():
        """Read from upstream socket, wrap in WS binary frames, send to browser."""
        try:
            # Use a finite timeout so the loop can periodically check `stop`.
            # settimeout(None) blocks forever and the thread never exits when
            # the browser disconnects without an explicit close (NAT timeout,
            # mobile sleep) — especially unreliable for UDP sockets.
            upstream_sock.settimeout(5.0)
            while not stop.is_set():
                try:
                    data = upstream_sock.recv(65536)
                except (socket.timeout, TimeoutError):
                    continue   # check stop, then loop
                except OSError:
                    break
                # TCP: empty recv means peer closed the connection → stop.
                # UDP: connectionless, empty datagram (b"") is valid — skip it
                # but keep the tunnel alive. Only break on error/timeout above.
                if not data and not use_udp:
                    break
                if data:
                    # Send as binary WS frame (server→client, unmasked)
                    try:
                        client_sock.sendall(_ws_make_frame(_WS_OPCODE_BINARY, data))
                    except Exception:
                        break
                    with _active_tunnels_lock:
                        if tunnel_id in _active_tunnels:
                            _active_tunnels[tunnel_id]["bytes_out"] += len(data)
        except Exception:
            pass
        finally:
            stop.set()

    t1 = threading.Thread(target=_ws_to_tcp, daemon=True, name=f"tcp-w2t-{tunnel_id}")
    t2 = threading.Thread(target=_tcp_to_ws, daemon=True, name=f"tcp-t2w-{tunnel_id}")
    t1.start(); t2.start()
    stop.wait()

    log(f"Tunnel closed: {label}", "INFO")
    try: upstream_sock.close()
    except Exception: pass
    try: client_sock.close()
    except Exception: pass
    # Join threads with timeout so they don't accumulate when the blocking
    # recv/send doesn't unblock immediately on socket close.
    t1.join(timeout=3.0)
    t2.join(timeout=3.0)
    with _active_tunnels_lock:
        _active_tunnels.pop(tunnel_id, None)

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

class _PooledWSGIServer(ThreadedWSGIServer):
    """ThreadedWSGIServer that uses a bounded thread pool instead of spawning
    a new thread per request. Prevents 'can't start new thread' under load."""
    def process_request(self, request, client_address):
        _http_pool.submit(self.process_request_thread, request, client_address)
    def server_close(self):
        # Don't shut down the shared pool on one server's close — other CDN
        # mini-servers may still be using it. Pool is cleaned up at exit.
        super().server_close()

class _S2LWSGIRequestHandler(WSGIRequestHandler):
    """WSGIRequestHandler with WebSocket upgrade interception.

    Werkzeug 3.x returns 400 Bad Request when the browser sends
    `Connection: Upgrade` — this happens BEFORE the Flask view function is
    called. We override handle_one_request() to detect WS upgrades and
    handle them DIRECTLY, completely bypassing Werkzeug's WSGI pipeline.
    No Flask context needed — we have everything in self.

    THIS IS THE ONLY WS HANDLING PATH. Every server instance in this file
    (main app + every CDN mini-server) is started with
    request_handler=_S2LWSGIRequestHandler, so a request with an Upgrade:
    websocket header NEVER reaches Flask routing — it's fully handled here
    or in _handle_tunnel_direct(). Don't add a second WS code path in a
    Flask view; it will silently never run. If you need a different
    upstream host for the "no /__s2l_*__/ prefix" case (e.g. a new kind of
    mini-server), set `<server>.s2l_target_base` right after make_server(),
    the way _start_cdn_server() does — don't hardcode SITE_URL further down.
    """
    def make_environ(self):
        environ = super().make_environ()
        environ["werkzeug.socket"] = self.connection
        return environ

    def handle_one_request(self):
        """Override to intercept WebSocket upgrades BEFORE WSGI processing."""
        try:
            self.raw_requestline = self.rfile.readline(65537)
            if len(self.raw_requestline) > 65536:
                self.requestline = ''
                self.request_version = ''
                self.command = ''
                self.send_error(414)
                return
            if not self.raw_requestline:
                self.close_connection = True
                return
            if not self.parse_request():
                return
        except Exception:
            return

        # ── WS upgrade detection — BEFORE WSGI ──────────────────────────
        # Werkzeug 3.x 400s on Connection: Upgrade before reaching Flask
        upgrade_hdr = self.headers.get("Upgrade", "").lower().strip()
        if upgrade_hdr == "websocket":
            self._handle_ws_direct()
            return

        # Normal request — use Werkzeug's WSGI path (do_* → run_wsgi)
        mname = 'do_' + self.command
        if not hasattr(self, mname):
            self.send_error(501, f"Unsupported method ({self.command!r})")
            return
        method = getattr(self, mname)
        method()
        self.wfile.flush()

    def _handle_ws_direct(self):
        """Handle WS upgrade directly — NO Flask context needed.

        We have everything we need:
          - self.connection  = raw client socket
          - self.headers     = request headers
          - self.path        = URL path + query string
          - self.rfile       = read stream (for any remaining body data)
        """
        try:
            parsed = urlparse(self.path)
            path = parsed.path or "/"
            query = parsed.query

            # ── Route matching ────────────────────────────────────────────
            ws_prefix_ext = "/__s2l_ws_ext__/"
            ws_prefix_tcp = "/__s2l_tcp__/"
            ws_prefix_udp = "/__s2l_udp__/"

            if path.startswith(ws_prefix_tcp):
                if not TCP_TUNNEL:
                    self.connection.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\nTCP tunneling disabled (set TCP_TUNNEL=True)")
                    return
                self._handle_tunnel_direct(path[len(ws_prefix_tcp):], use_udp=False)
                return
            if path.startswith(ws_prefix_udp):
                if not UDP_TUNNEL:
                    self.connection.sendall(b"HTTP/1.1 403 Forbidden\r\n\r\nUDP tunneling disabled (set UDP_TUNNEL=True)")
                    return
                self._handle_tunnel_direct(path[len(ws_prefix_udp):], use_udp=True)
                return

            # ── Build WS target URL ──────────────────────────────────────
            if path.startswith(ws_prefix_ext):
                wspath = path[len(ws_prefix_ext):]
                ws_host = wspath.split("/", 1)[0]
                ws_path_q = wspath[len(ws_host):] or "/"
                if query:
                    ws_path_q += f"?{query}"
                target = f"https://{ws_host}{ws_path_q}"
                req_display = f"/__s2l_ws_ext__/{wspath}"
            else:
                # Main host WS, or a CDN mini-server's own host if this
                # request came in on one of those ports (see s2l_target_base,
                # set right after make_server() in _start_cdn_server).
                base = getattr(self.server, "s2l_target_base", None) or SITE_URL
                target = f"{base.rstrip('/')}{path}"
                if query:
                    target += f"?{query}"
                req_display = path

            # ── Build forward headers ────────────────────────────────
            fwd_hdrs = {}
            for k, v in self.headers.items():
                fwd_hdrs[k] = v

            client_sock = self.connection

            # OFFLINE guard: no upstream to dial. WebSockets can't be served
            # from disk, so fail fast with a distinct 503 instead of hanging
            # for a connection timeout then returning a misleading 502.
            if OFFLINE:
                try:
                    _offline_body = b"Offline \xe2\x80\x94 no WS upstream"
                    client_sock.sendall(
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_offline_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _offline_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # Parse target for WS URL
            target_parsed = urlparse(target)
            ws_scheme = "wss" if target_parsed.scheme in ("https", "wss") else "ws"
            ws_path = target_parsed.path or "/"
            ws_qs = target_parsed.query
            ws_url = f"{ws_scheme}://{target_parsed.netloc}{ws_path}"
            if ws_qs:
                ws_url += f"?{ws_qs}"

            # WS handshake values
            ws_key = self.headers.get("Sec-WebSocket-Key", "")
            accept = _ws_handshake_accept(ws_key)
            # RFC 6455 §4.2.2: the server must select ONE subprotocol. Echoing
            # the entire comma-separated list back is invalid and strict browsers
            # reject the handshake. Pick the first requested subprotocol.
            proto_req = self.headers.get("Sec-WebSocket-Protocol", "")
            proto_pick = proto_req.split(",")[0].strip() if proto_req else ""
            proto_hdrs = f"Sec-WebSocket-Protocol: {proto_pick}\r\n" if proto_pick else ""

            # Build extra headers for upstream
            extra_fwd = {}
            for k, v in fwd_hdrs.items():
                kl = k.lower()
                if kl in ("host", "upgrade", "connection",
                           "sec-websocket-key", "sec-websocket-version",
                           "sec-websocket-extensions", "accept-encoding"):
                    continue
                extra_fwd[k] = v
            # Origin = MAIN_HOST (what a real browser sends)
            _incoming_origin = next((v for k, v in fwd_hdrs.items() if k.lower() == "origin"), None)
            extra_fwd["Origin"] = _real_origin_for(_incoming_origin)
            if proto_req:
                extra_fwd["Sec-WebSocket-Protocol"] = proto_req

            # ── Connect upstream FIRST ────────────────────────────────────
            log(f"WS-direct: {req_display} → {ws_url}", "WS")
            try:
                srv = _ws_connect_upstream(ws_url, extra_fwd)
            except Exception as exc:
                log(f"WS upstream FAILED: {ws_url} — {_short_exc(exc)}", "WARN")
                try:
                    err_msg = str(exc)[:200].encode("utf-8", "replace")
                    _502_body = b"WS upstream: " + err_msg
                    client_sock.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_502_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _502_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # ── Send 101 to browser ──────────────────────────────────────
            # Decide permessage-deflate BEFORE building the handshake: if we're
            # going to run the pump in deflate mode, the browser MUST see
            # Sec-WebSocket-Extensions: permessage-deflate in THIS (the only)
            # 101 response, or it'll keep sending/expecting plain frames while
            # we deflate/inflate on our end — corrupting every frame. There is
            # exactly one handshake response; a second one isn't valid HTTP.
            #
            # FIXED: zlib-stream guard. Some endpoints (notably real-time
            # gateways via ?compress=zlib-stream) layer their OWN zlib
            # compression at the application level — it is NOT permessage-
            # deflate (RFC 7692), the frames do NOT set RSV1, and the bytes
            # are zlib-wrapped (2-byte 0x78 0x9C header) rather than raw
            # deflate. If we accept permessage-deflate from the browser on
            # such a tunnel, the v2 pump will try to inflate already-
            # decompressed/plain bytes, fail, re-compress them as raw deflate,
            # and ship them to the browser without RSV1 — the browser's own
            # zlib-stream decompressor then chokes on the double-mangled
            # payload with "zlib error, -3, incorrect header check" and the
            # gateway dies on a reconnect loop. Force permessage-deflate OFF
            # whenever the upstream URL advertises a non-RFC-7692 compression
            # scheme of its own.
            _ext_hdr = self.headers.get("Sec-WebSocket-Extensions", "")
            _upstream_has_own_compression = (
                "compress=zlib-stream" in ws_qs
                or "compress=" in ws_qs        # catch any future variant
            )
            if _upstream_has_own_compression and WS_DEFLATE:
                log(f"WS: upstream advertises its own compression "
                    f"({ws_qs!r}) — disabling permessage-deflate to avoid "
                    f"conflict (zlib-stream != RFC 7692)", "INFO")
            _browser_wants_deflate = (
                "permessage-deflate" in _ext_hdr
                and WS_DEFLATE
                and not _upstream_has_own_compression
            )
            deflate_hdr = "Sec-WebSocket-Extensions: permessage-deflate\r\n" if _browser_wants_deflate else ""
            handshake = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                f"{proto_hdrs}"
                f"{deflate_hdr}"
                "\r\n"
            )
            try:
                client_sock.sendall(handshake.encode())
            except Exception as exc:
                log(f"WS handshake send failed: {exc}", "WARN")
                try: srv.close()
                except Exception: pass
                self.close_connection = True
                return

            # ── Pump frames ──────────────────────────────────────────────
            # WS_AUTO_RECONNECT: if the upstream WS drops, try to re-establish
            # the connection transparently so the browser doesn't see a broken
            # tunnel. We loop until the browser itself closes or a reconnect
            # attempt fails after 3 retries.
            _use_v2 = (WS_PING_INTERVAL > 0 or WS_DEFLATE or WS_LOG_FRAMES
                       or bool(_WS_MSG_HOOKS) or HOOK_GUI)
            _ws_attempts = 0
            _ws_max_reconnects = 3 if WS_AUTO_RECONNECT else 0
            while True:
                if _use_v2:
                    _pump_ws_frames_v2(client_sock, srv, ws_url, use_deflate=_browser_wants_deflate)
                else:
                    _pump_ws_frames(client_sock, srv, ws_url)
                # Check if the browser side is still open — if not, we're done.
                # A closed client_sock means the browser navigated away or
                # closed the tab; no point reconnecting.
                # FIX: sendall(b"") is a no-op on most platforms (returns 0
                # immediately without calling send(2)), so it can't detect a
                # closed browser socket. Use a non-blocking MSG_PEEK recv
                # instead — returns b"" if the browser closed, raises on error.
                try:
                    client_sock.setblocking(False)
                    _probe = client_sock.recv(1, socket.MSG_PEEK)
                    client_sock.setblocking(True)
                    if not _probe:
                        break   # browser closed the connection
                except (BlockingIOError, InterruptedError):
                    # No data ready → socket is still open → restore blocking
                    client_sock.setblocking(True)
                except Exception:
                    break   # probe failed → socket is dead
                if _ws_attempts >= _ws_max_reconnects:
                    break
                _ws_attempts += 1
                log(f"WS upstream dropped — reconnecting (attempt {_ws_attempts}/{_ws_max_reconnects})", "WS")
                try:
                    srv = _ws_connect_upstream(ws_url, extra_fwd)
                except Exception as exc:
                    log(f"WS reconnect failed: {exc}", "WARN")
                    break

            self.close_connection = True

        except Exception as exc:
            log(f"WS-direct error: {exc}", "ERROR")
            self.close_connection = True

    def _handle_tunnel_direct(self, wspath, use_udp=False):
        """Handle TCP/UDP tunnel directly."""
        try:
            # OFFLINE guard: no upstream to dial. Like the WS handler, fail
            # fast with 503 instead of hanging on a connection timeout.
            if OFFLINE:
                try:
                    _offline_body = b"Offline \xe2\x80\x94 no tunnel upstream"
                    self.connection.sendall(
                        b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_offline_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _offline_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            host_port = wspath.split("/", 1)[0].split("?", 1)[0]
            if ":" not in host_port:
                self.connection.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return
            host, _, port_s = host_port.rpartition(":")
            port = int(port_s)
            if not (1 <= port <= 65535):
                self.connection.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return

            client_sock = self.connection
            proto = "UDP" if use_udp else "TCP"
            label = f"{proto} {host}:{port}"
            log(f"Tunnel open: {label}", "TUNNEL")

            # Connect upstream FIRST, then send 101. Sending 101 before
            # connecting leaves the browser thinking the tunnel is established
            # when it isn't — if create_connection fails, the browser hangs.
            try:
                if use_udp:
                    # Detect IPv6 literal to pick the right socket family.
                    # Hardcoding AF_INET breaks /__s2l_udp__/[::1]:53 etc.
                    _fam = socket.AF_INET6 if ":" in host else socket.AF_INET
                    upstream_sock = socket.socket(_fam, socket.SOCK_DGRAM)
                    upstream_sock.settimeout(TUNNEL_TIMEOUT)
                    upstream_sock.connect((host, port))
                else:
                    upstream_sock = socket.create_connection((host, port), timeout=TIMEOUT_CONN)
                    upstream_sock.settimeout(TUNNEL_TIMEOUT)
            except Exception as exc:
                err_msg = str(exc)[:200].encode("utf-8", "replace")
                try:
                    _502_body = b"Tunnel upstream: " + err_msg
                    self.connection.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Connection: close\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: " + str(len(_502_body)).encode() + b"\r\n"
                        b"\r\n"
                        + _502_body
                    )
                except Exception:
                    pass
                self.close_connection = True
                return

            # Upstream connected — NOW tell the browser the tunnel is open.
            ws_key = self.headers.get("Sec-WebSocket-Key", "")
            accept = _ws_handshake_accept(ws_key)
            handshake = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n"
                "\r\n"
            )
            client_sock.sendall(handshake.encode())

            _pump_tcp_over_ws(client_sock, upstream_sock, label, use_udp=use_udp)
            self.close_connection = True
        except Exception as exc:
            log(f"Tunnel error: {exc}", "ERROR")
            self.close_connection = True
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

    # Browsers treat 127.0.0.1 and localhost as different origins. Redirecting
    # between them cross-origin drops Authorization headers and host-only cookies,
    # which breaks SPA auth on the first API call. Only redirect top-level
    # document navigations (not API/XHR/fetch requests), and use 302 (not 307)
    # so browsers don't preserve method/body on a cross-origin hop.
    host = flask_request.host
    if host == "127.0.0.1" or host.startswith("127.0.0.1:"):
        _dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
        if _dest in ("document", "") and flask_request.method == "GET":
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
})

def _is_sw_path(path: str) -> bool:
    """Detect service worker paths. Must be precise — a too-broad match
    (e.g. any path containing "service-worker") clobbers unrelated helper
    modules, web workers, and API endpoints with the no-op SW that nukes
    all browser caches. Only match actual SW entry-point filenames."""
    if path in _SW_PATHS:
        return True
    # Match /service-worker.js, /sw.js, /serviceworker.js at any path depth,
    # plus /something-sw.js. Do NOT match -worker.js (that catches web workers
    # and audio worklets, not service workers) or bare substring matches.
    _sw_re = getattr(_is_sw_path, "_re", None)
    if _sw_re is None:
        _sw_re = re.compile(r"(?:^|/)(?:service[-_]?worker|sw|serviceworker)\.js(?:[?#]|$)", re.IGNORECASE)
        _is_sw_path._re = _sw_re
    return bool(_sw_re.search(path))
# Minimal no-op service worker. Does NOT delete caches — SPAs and real-time
# apps rely on their SW caches for app-shell/bundle loading. Wiping them on
# every activate forces a full re-fetch of every chunk, which on flaky networks
# hangs the SPA init indefinitely (blank page). The no-op SW just claims clients
# and passes fetch events through without interception, so the SPA's own fetch
# patches (injected by _inject_sw_clear) handle URL rewriting.
_SW_NOOP = (
    b"/* S2L no-op SW: claims clients, does not intercept fetch or delete caches */\n"
    b"self.addEventListener('install', e => e.waitUntil(self.skipWaiting()));\n"
    b"self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));\n"
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
            fwd["User-Agent"] = _sanitize_ua(browser_ua)
        else:
            device = _effective_device()
            ua = _sanitize_ua(UA_PROFILES.get(device, UA_PROFILES["macintosh"]))
            fwd["User-Agent"] = ua
    else:
        ua = _sanitize_ua(UA_PROFILES.get(DEVICE, UA_PROFILES["macintosh"]))
        fwd["User-Agent"] = ua
    # NOTE: do NOT mutate sess.headers here — the session is shared across
    # concurrent requests from the same client IP. Mutating it races with
    # other threads using the same session (RuntimeError: dict changed size
    # during iteration inside requests' header merging). The per-request
    # `fwd` headers already override session-level headers via kwargs["headers"].

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
    # asset bundle servers use it to pick format.
    # CRITICAL: do NOT rewrite Accept for script/style/image/font/worker requests.
    # Browsers send Accept: */* for <script src>, <link rel=stylesheet>, dynamic
    # import(), etc. Rewriting it to text/html makes content-negotiating servers
    # return HTML instead of the JS chunk → the chunk fails to parse → SPA halts
    # partway through boot (shell visible, main content blank).
    _sec_dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
    _is_asset = _sec_dest in ("script", "style", "image", "font", "video",
                              "audio", "track", "worker", "manifest", "fetch")
    _bare_accept = not orig_accept or orig_accept.strip() in ("*/*", "")
    if not is_api_req and _bare_accept and not _is_asset:
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
    # NOTE: do NOT mutate sess.headers["Accept-Encoding"] — the session is
    # shared across concurrent requests, and mutating it races with other
    # threads. The per-request `fwd` header overrides session-level headers.

    # Use a longer read timeout for streaming requests (video/audio/large
    # downloads) — the default TIMEOUT_READ=12s is too short for slow CDNs
    # and causes playback to stall after ~30-40s.
    _read_timeout = TIMEOUT_STREAM_READ if stream else TIMEOUT_READ
    kwargs: dict = {
        "headers":         fwd,
        "timeout":         (TIMEOUT_CONN, _read_timeout),
        "allow_redirects": True,
        "verify":          False,
        "stream":          stream,
    }

    fwd.pop("Cookie",  None)
    fwd.pop("cookie",  None)
    merged_cookies = _flatten_cookiejar(sess.cookies, prefer_host=urlparse(target).hostname or "")
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
    # multiplexed responses (chunked API endpoints using HTTP/2).
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
        # FIX: rotate the CLIENT session (the one that made this request),
        # NOT _proxy_local.s (thread-local, used only by background crawl
        # threads — rotating it here is a no-op that leaks the new session).
        # Mirrors the WAF-retry pattern in the proxy() route.
        try:
            _cid = _client_id()
        except Exception:
            _cid = None
        if _cid is not None:
            with _CLIENT_LOCK:
                _old_sess = _CLIENT_SESSIONS.get(_cid)
            if _old_sess is not None:
                try: _old_sess.close()
                except Exception: pass
            _fresh = _make_session()
            _fresh.headers["User-Agent"] = fwd.get("User-Agent",
                _sanitize_ua(UA_PROFILES["macintosh"]))
            with _CLIENT_LOCK:
                _CLIENT_SESSIONS[_cid] = _fresh
            sess = _fresh
        else:
            # Background thread (no flask context) — fall back to _proxy_local.s
            _old_sess = getattr(_proxy_local, "s", None)
            if _old_sess is not None:
                try: _old_sess.close()
                except Exception: pass
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
    # COOP/COEP are passive now (see filter_resp) — whatever ctx.resp_headers
    # already carries (origin's own choice, or the top-level-HTML heuristic
    # fallback) is the final answer; no flag to re-check here.

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

def _parse_range_header(range_hdr: str, file_size: int) -> tuple[int, int] | None:
    """Parse a Range: bytes=X-Y header. Returns (start, end_inclusive) or None."""
    if not range_hdr or file_size <= 0:
        return None
    try:
        m = re.match(r"bytes=(\d*)-(\d*)", range_hdr.strip())
        if not m:
            return None
        start_s, end_s = m.group(1), m.group(2)
        if not start_s and not end_s:
            return None
        if not start_s:
            # Suffix range: bytes=-N → last N bytes
            n = int(end_s)
            if n <= 0:
                return None
            start = max(0, file_size - n)
            end = file_size - 1
        elif not end_s:
            start = int(start_s)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s)
        if start < 0 or start >= file_size or end < start:
            return None
        end = min(end, file_size - 1)
        return (start, end)
    except (ValueError, AttributeError):
        return None

def _serve_cached(lp: str) -> tuple[bytes, str] | None:
    """Load a cached file from disk and apply HTML/CSS/JSON rewrites as needed."""
    sidecar_ct = _load_ctype_sidecar(lp)
    # A sidecar means the real upstream type disagreed with the on-disk path —
    # e.g. an extensionless CSS/JS URL that collapsed to .../index.html.
    # Trust the sidecar over the naive ".html" extension check.
    looks_html = lp.endswith(".html") or lp.endswith("/index.html")
    is_html_file = looks_html and (sidecar_ct is None or sidecar_ct.startswith("text/html"))

    if is_html_file:
        try:
            with open(lp, "rb") as f:
                data = f.read()
        except OSError:
            return None
        if _is_wire_payload(data) or not _is_valid_html(data):
            # OFFLINE: never destroy irreplaceable cache — serve what we have
            # rather than permanently 404'ing the resource for the rest of the
            # session. In online mode, purging lets us re-fetch a clean copy.
            if OFFLINE:
                log(f"Stale cache kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                log(f"Stale RSC cache — purging {os.path.relpath(lp)}", "WARN")
                try:
                    os.remove(lp)
                    os.remove(_ctype_sidecar_path(lp))
                except OSError:
                    pass
                return None
        # Purge bot/CAPTCHA pages that slipped into cache before this guard existed
        elif _is_bot_page(data):
            if OFFLINE:
                log(f"Bot page kept (OFFLINE) — {os.path.relpath(lp)}", "WARN")
            else:
                log(f"Bot page in cache — purging {os.path.relpath(lp)}", "WARN")
                try:
                    os.remove(lp)
                    os.remove(_ctype_sidecar_path(lp))
                except OSError:
                    pass
                return None
        if PROXY_CDN:
            data = _rewrite_ext_urls(data)
        data = rewrite_abs_urls(data)   # strip absolute MAIN_HOST URLs → proxy-relative
        # Always inject the S2L JS runtime — it patches fetch/XHR/WebSocket/
        # Worker/innerHTML so JS-initiated requests go through the proxy instead
        # of hitting the real (unreachable in OFFLINE) host. Without it, HTML
        # pages load but SPAs can't fetch data/config → "nothing works."
        data = _inject_sw_clear(data)
        return data, "text/html; charset=utf-8"

    try:
        with open(lp, "rb") as f:
            data = f.read()
    except OSError:
        return None
    ct = sidecar_ct or guess_mime(lp)
    # Rewrite external URLs inside cached CSS files so images/fonts load through the proxy
    if PROXY_CDN and (lp.endswith(".css") or "css" in ct):
        data = _rewrite_ext_urls(data)
    elif PROXY_CDN and (lp.endswith((".json", ".webmanifest")) or "json" in ct):
        data = _rewrite_json_urls(data)
    return data, ct

def _cached_response(lp: str, target: str, method: str, req_path: str) -> Response | None:
    """Build a Flask Response from a cached file on disk."""
    result = _serve_cached(lp)
    if result is None:
        return None

    data, ct = result
    log(f"{method} HIT {_fmt_host(urlparse(target).netloc)}{req_path} {_fmt_size(len(data))}", "←")
    ctx = HookContext(
        method       = method,
        url          = target,
        path         = req_path,
        query        = flask_request.query_string.decode("utf-8", "ignore"),
        req_headers  = filter_fwd(dict(flask_request.headers)),
        req_body     = b"",
        resp_status  = 200,
        resp_headers = {
            # HTML + JSON: never cache at browser level (multi-site on same origin,
            # and JSON API responses must always be fresh — caching them for 24h
            # freezes SPA data and causes stale-state render failures).
            # Static assets (JS/CSS/images/fonts): allow long caching.
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
                             if ct.startswith("text/html") or "json" in ct
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
    # _raw_path_after() for why this matters (object-key %2F paths, etc.)
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
    if method in _SAFE_METHODS and (CACHE_CDN or OFFLINE) and os.path.isfile(lp):
        result = _serve_cached(lp)
        if result is not None:
            data, ct = result
            log(f"EXT {method} HIT {_fmt_host(ext_host)}{ext_path} {_fmt_size(len(data))}", "CDN")
            _gui_push_raw("GET", ext_path, 200, ct, data,
                          display_tag="[CDN]", origin=ext_host)
            resp = Response(data, content_type=ct)
            resp.headers["Cache-Control"] = "public, max-age=86400"
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Expose-Headers"] = "*"
            return resp
    if OFFLINE:
        log(f"EXT {method} 404 {_fmt_host(ext_host)}{ext_path} (not cached)", "CDN")
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
        # the exact path a brand-new per-host CDN (e.g. a freshly-seen
        # <uuid>.cdn.example.com) goes through on its first request, before it
        # has its own MULTIPORT port, so it's the one most likely to need
        # whatever session/consent state the main page already established.
        # Decide whether to stream based on Range header and path extension.
        # Without this, video/audio from external CDNs (e.g. video
        # streaming CDNs) is fully buffered in memory with a 12s read
        # timeout, causing playback to stall after ~30-40s.
        _has_range = bool(flask_request.headers.get("Range", ""))
        _do_stream_ext = method in _SAFE_METHODS and (
            _has_range or _should_stream(guess_mime(ext_path)))
        kw["stream"] = _do_stream_ext
        r    = _get_client_session().request(method, real_url, **kw)
        # CRITICAL: when stream=True was used, ALWAYS hand off to _stream_resp.
        # Falling through to r.content loses chunks on curl_cffi HTTP/2 for
        # large binary files (.tar bundles, .wasm, video, etc), producing
        # "0.0B" bodies even though the server returned 200 with real content.
        # The stream-mode r.content read is unreliable — _stream_resp reads
        # chunk-by-chunk and is always correct.
        if _do_stream_ext:
            return _stream_resp(r, method, real_url)
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
        out_headers = filter_resp(_resp_headers_dict(r))
        out_headers["Access-Control-Allow-Origin"]  = "*"
        out_headers["Access-Control-Expose-Headers"] = "*"
        out_headers["Cross-Origin-Resource-Policy"] = "cross-origin"
        if method in _SAFE_METHODS:
            out_headers["Cache-Control"] = "public, max-age=86400"
        # Rewrite URLs inside CDN HTML/CSS pages so their sub-resources also load
        # through the proxy (critical for cross-origin iframes served via /__s2l_ext__/).
        _ct_base_e = ct.split(";")[0].strip().lower()
        if r.status_code < 400 and PROXY_CDN:
            if "text/html" in _ct_base_e:
                body = _rewrite_ext_urls(body)
                body = rewrite_abs_urls(body)
                body = _inject_sw_clear(body)
                out_headers.pop("content-length", None)
                out_headers.pop("Content-Length", None)
            elif "text/css" in _ct_base_e:
                body = _rewrite_ext_urls(body)
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

        log(f"EXT {method} {ctx.resp_status} {_fmt_host(ext_host)}{ext_path} {_fmt_size(len(ctx.resp_body))}", "CDN")
        _gui_push_raw(method, ext_path, ctx.resp_status, ctx.resp_ct, ctx.resp_body,
                      display_tag="[EXT]", origin=_fmt_host(ext_host),
                      _skip_log=True)   # caller already logged
        if CAPTURE and CAPTURE_CDN:
            _maybe_capture(ctx)
        return Response(ctx.resp_body, status=ctx.resp_status, headers=ctx.resp_headers, content_type=ctx.resp_ct)
    except _CONN_ERRORS as exc:
        return Response(f"CDN unreachable: {_short_exc(exc)}", status=502)
    except Exception as exc:
        return Response(f"CDN error: {_short_exc(exc)}", status=502)

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

@app.route("/__s2l_ws_ext__/<path:wspath>", methods=["GET"])
def ws_ext(_wspath: str) -> Response:
    return Response(
        "This path is a WebSocket tunnel — send Upgrade: websocket",
        status=426,
        headers={"Upgrade": "websocket"},
    )

@app.route("/__s2l_tcp__/<path:tcppath>", methods=["GET"])
def tcp_ext(_tcppath: str) -> Response:
    return Response("This path is a raw TCP tunnel — send Upgrade: websocket",
                    status=426, headers={"Upgrade": "websocket"})

@app.route("/__s2l_udp__/<path:udppath>", methods=["GET"])
def udp_ext(_udppath: str) -> Response:
    return Response("This path is a raw UDP tunnel — send Upgrade: websocket",
                    status=426, headers={"Upgrade": "websocket"})

# ──────────────────────────────────────────────────────────────────────────────
# FIREFOX_PROXY — MITM forward proxy for the actual browser
#
# Firefox (like every browser) exempts "localhost"/"127.0.0.1" from whatever
# proxy is configured in its network settings — there's no way to see traffic
# the browser sends to our OWN reverse-proxy port by pointing Firefox's proxy
# at us. The workaround: Firefox does NOT exempt the real target domain, so
# if the browser tab stays on the real domain, that traffic DOES get routed
# through this proxy like any other site — and for the one domain actively
# being cloned (MAIN_HOST, or a registered MULTIPORT CDN host) we
# transparently redirect it to the local site2local server instead of the
# real internet. That's the "passthru": the browser never notices, it's just
# talking to the real domain as far as it's concerned. Every OTHER domain
# visited while this is on gets relayed to the real upstream untouched,
# purely for inspection/hooking (the Firefox Proxy tab below).
#
# This is a real man-in-the-middle: it terminates TLS using a locally
# generated root CA (one-time Firefox import — instructions are logged at
# startup) and a fresh leaf certificate per host, signed by that CA. Nothing
# here calls out anywhere; the CA only ever needs to be trusted by the one
# browser pointed at this port, on this machine.
# ──────────────────────────────────────────────────────────────────────────────

try:
    from cryptography import x509
    from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    _CRYPTOGRAPHY_OK = True
except ImportError:
    _CRYPTOGRAPHY_OK = False

_CA_DIR       = os.path.join("site_data", "_ca")
_CA_KEY_PATH  = os.path.join(_CA_DIR, "s2l_root_ca.key")
_CA_CERT_PATH = os.path.join(_CA_DIR, "s2l_root_ca.pem")
_LEAF_DIR     = os.path.join(_CA_DIR, "leafs")

_ca_lock = threading.Lock()
_ca_pair = None   # (key, cert) — loaded/generated lazily, only if FIREFOX_PROXY is used

_leaf_cache: dict[str, str] = {}   # host -> path to a combined cert+key PEM
_leaf_lock  = threading.Lock()

def _ensure_root_ca():
    """Load the persisted root CA, generating one on first use.

    Cached to disk (not regenerated per run) specifically so the one-time
    Firefox import stays valid across restarts, regardless of which SITE is
    currently configured — the CA is shared across every site2local session.
    """
    global _ca_pair
    with _ca_lock:
        if _ca_pair is not None:
            return _ca_pair
        os.makedirs(_CA_DIR, exist_ok=True)
        if os.path.isfile(_CA_KEY_PATH) and os.path.isfile(_CA_CERT_PATH):
            try:
                with open(_CA_KEY_PATH, "rb") as f:
                    key = serialization.load_pem_private_key(f.read(), password=None)
                with open(_CA_CERT_PATH, "rb") as f:
                    cert = x509.load_pem_x509_certificate(f.read())
                _ca_pair = (key, cert)
                return _ca_pair
            except Exception as e:
                log(f"Existing root CA unreadable ({e}) — generating a new one", "WARN")

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, "S2L Local MITM Root CA"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "site2local (local only)"),
        ])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(issuer)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=3650))
                .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
                .add_extension(x509.KeyUsage(
                    digital_signature=True, key_cert_sign=True, crl_sign=True,
                    content_commitment=False, key_encipherment=False, data_encipherment=False,
                    key_agreement=False, encipher_only=False, decipher_only=False), critical=True)
                .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
                .sign(key, hashes.SHA256()))
        with open(_CA_KEY_PATH, "wb") as f:
            f.write(key.private_bytes(serialization.Encoding.PEM,
                                       serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
        with open(_CA_CERT_PATH, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        log(f"Generated new root CA → {_CA_CERT_PATH}", "INFO")
        _ca_pair = (key, cert)
        return _ca_pair

def _get_leaf_pem(host: str) -> str:
    """Return a cert+key PEM path for `host`, generating (and disk-caching)
    one signed by the root CA the first time this host is seen."""
    with _leaf_lock:
        cached = _leaf_cache.get(host)
        if cached and os.path.isfile(cached):
            return cached
        os.makedirs(_LEAF_DIR, exist_ok=True)
        safe = re.sub(r"[^\w\-.]", "_", host)
        pem_path = os.path.join(_LEAF_DIR, f"{safe}.pem")
        if os.path.isfile(pem_path):
            _leaf_cache[host] = pem_path
            return pem_path

        ca_key, ca_cert = _ensure_root_ca()
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, host)])
        now = datetime.datetime.now(datetime.timezone.utc)
        cert = (x509.CertificateBuilder()
                .subject_name(subject).issuer_name(ca_cert.subject)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=825))
                .add_extension(x509.SubjectAlternativeName([x509.DNSName(host)]), critical=False)
                .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
                .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
                .sign(ca_key, hashes.SHA256()))
        with open(pem_path, "wb") as f:
            f.write(key.private_bytes(serialization.Encoding.PEM,
                                       serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        _leaf_cache[host] = pem_path
        return pem_path

def _mitm_read_headers(rfile) -> tuple[bytes, list] | None:
    """Read one HTTP start-line + headers from a buffered socket file object."""
    start_line = rfile.readline(8192)
    if not start_line or start_line in (b"\r\n", b"\n"):
        start_line = rfile.readline(8192)   # tolerate a stray leading blank line
    if not start_line:
        return None
    headers = []
    while True:
        line = rfile.readline(8192)
        if line in (b"\r\n", b"\n", b""):
            break
        if b":" not in line:
            continue
        k, _, v = line.partition(b":")
        headers.append((k.decode("latin-1").strip(), v.decode("latin-1", "replace").strip()))
    return start_line.rstrip(b"\r\n"), headers

def _mitm_read_body(rfile, headers: list, cap: int = 64 * 1024 * 1024) -> bytes:
    """Read a body per Content-Length or chunked Transfer-Encoding. `cap`
    bounds how much is buffered for logging/hooking on very large bodies."""
    hmap = {k.lower(): v for k, v in headers}
    if "chunked" in hmap.get("transfer-encoding", "").lower():
        body = b""
        while True:
            size_line = rfile.readline(64)
            if not size_line:
                break
            try:
                size = int(size_line.split(b";")[0].strip(), 16)
            except ValueError:
                break
            if size == 0:
                while True:
                    t = rfile.readline(8192)
                    if t in (b"\r\n", b"\n", b""):
                        break
                break
            chunk = rfile.read(size)
            if len(body) < cap:
                body += chunk
            rfile.read(2)
        return body
    cl = hmap.get("content-length")
    if cl is not None:
        try:
            n = int(cl)
        except ValueError:
            return b""
        return rfile.read(n) if n > 0 else b""
    # No Content-Length and no chunked encoding → HTTP/1.0 style: body is
    # delimited by connection close. Read until EOF (capped) so we don't
    # silently drop the body and send Content-Length: 0 to the client.
    body = b""
    while len(body) < cap:
        chunk = rfile.read(min(65536, cap - len(body)))
        if not chunk:
            break
        body += chunk
    return body

def _mitm_upstream_ssl_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    try:
        ctx.set_alpn_protocols(["http/1.1"])
    except NotImplementedError:
        pass
    return ctx

def _mitm_process_request(parsed_req, rfile, wfile, dest_host: str, dest_port: int,
                           dest_tls: bool, host_hdr: str, tag: str) -> bool:
    """Handle exactly one HTTP request already read off `rfile`: forward it to
    (dest_host, dest_port), relay the response back over `wfile`, log/hook it.
    Returns False when the connection should close."""
    # OFFLINE guard: no upstream to dial — fail fast with 503 instead of hanging
    # for the connect timeout then returning a misleading 502.
    if OFFLINE:
        try:
            wfile.write(b"HTTP/1.1 503 Service Unavailable\r\n"
                        b"Connection: close\r\n"
                        b"Content-Length: 0\r\n\r\n")
            wfile.flush()
        except Exception:
            pass
        return False
    start_line, headers = parsed_req
    try:
        method, target, _proto = start_line.decode("latin-1").split(" ", 2)
    except ValueError:
        return False
    p = urlparse(target)
    path = target if target.startswith("/") else (p.path or "/") + (f"?{p.query}" if p.query else "")
    hmap  = dict(headers)
    body  = _mitm_read_body(rfile, headers)
    body  = _apply_fwd_req_hooks(method, path, hmap, body)
    up = None
    try:
        raw = socket.create_connection((dest_host, dest_port), timeout=15)
        up  = _mitm_upstream_ssl_context().wrap_socket(raw, server_hostname=dest_host) if dest_tls else raw

        req_lines = [f"{method} {path} HTTP/1.1", f"Host: {host_hdr}"]
        for k, v in headers:
            # Strip hop-by-hop headers. transfer-encoding MUST be stripped —
            # we already set Content-Length below, and sending both
            # Transfer-Encoding: chunked AND Content-Length is invalid HTTP
            # that strict upstreams reject.
            if k.lower() in ("host", "content-length", "transfer-encoding",
                             "proxy-connection", "connection", "keep-alive",
                             "te", "trailers", "upgrade"):
                continue
            req_lines.append(f"{k}: {v}")
        req_lines.append(f"Content-Length: {len(body)}")
        req_lines.append("Connection: keep-alive")
        up.sendall(("\r\n".join(req_lines) + "\r\n\r\n").encode("latin-1", "replace") + body)

        up_rfile = up.makefile("rb")
        resp_parsed = _mitm_read_headers(up_rfile)
        if resp_parsed is None:
            return False
        resp_start, resp_headers = resp_parsed
        resp_body = _mitm_read_body(up_rfile, resp_headers)

        status_txt = resp_start.decode("latin-1", "replace")
        try:
            status_code = int(status_txt.split(" ")[1])
        except Exception:
            status_code = 0
        # Decide ONCE whether this client connection stays open, and use that
        # same decision both for the header we actually send and for the
        # return value — previously the header always claimed "keep-alive"
        # while the return value could independently decide to close right
        # after, so a client that believed the header would reuse a
        # connection we'd already torn down, surfacing as a stray connection
        # reset on its next request.
        resp_hmap  = {k.lower(): v for k, v in resp_headers}
        keep_alive = (hmap.get("connection", "").lower() != "close"
                      and resp_hmap.get("connection", "").lower() != "close")
        out_lines = [status_txt]
        for k, v in resp_headers:
            if k.lower() in ("content-length", "transfer-encoding", "connection"):
                continue
            out_lines.append(f"{k}: {v}")
        out_lines.append(f"Content-Length: {len(resp_body)}")
        out_lines.append(f"Connection: {'keep-alive' if keep_alive else 'close'}")
        wfile.write(("\r\n".join(out_lines) + "\r\n\r\n").encode("latin-1", "replace") + resp_body)
        wfile.flush()

        _gui_fwd_push(tag, method, dest_host if tag == "clone" else urlparse(f"//{host_hdr}").hostname or host_hdr,
                      path, status_code, hmap, body)
        log(f"[{tag}] {method} {status_code} {_fmt_host(host_hdr)}{path} {_fmt_size(len(resp_body))}", "CDN")
        return keep_alive
    except Exception as e:
        log(f"MITM {tag} relay error {host_hdr}{path}: {_short_exc(e)}", "WARN")
        _gui_fwd_push(tag, method, host_hdr, path, 502, hmap, body)
        return False
    finally:
        if up is not None:
            try: up.close()
            except Exception: pass

def _mitm_handle_connection(client_sock: socket.socket, _client_addr) -> None:
    client_sock.settimeout(30)
    work_sock = client_sock
    try:
        rfile0 = client_sock.makefile("rb")
        parsed = _mitm_read_headers(rfile0)
        if parsed is None:
            return
        start_line, _headers = parsed
        parts = start_line.decode("latin-1", "replace").split(" ")
        if len(parts) < 2:
            return
        first_method, first_target = parts[0].upper(), parts[1]
        pending_first = None

        if first_method == "CONNECT":
            host, _, port_s = first_target.partition(":")
            port = int(port_s) if port_s.isdigit() else 443
            # FIX: check _CRYPTOGRAPHY_OK BEFORE sending the 200. Previously
            # the 200 was sent first, then we returned without a TLS handshake —
            # the browser believed the tunnel was established and surfaced a
            # confusing TLS error instead of a clear proxy error.
            if not _CRYPTOGRAPHY_OK:
                try:
                    client_sock.sendall(
                        b"HTTP/1.1 502 Bad Gateway\r\n"
                        b"Content-Type: text/plain\r\n"
                        b"Content-Length: 52\r\n"
                        b"\r\n"
                        b"MITM TLS unavailable: 'cryptography' package not installed"
                    )
                except Exception:
                    pass
                return
            client_sock.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(_get_leaf_pem(host))
            try:
                ctx.set_alpn_protocols(["http/1.1"])
            except NotImplementedError:
                pass
            try:
                work_sock = ctx.wrap_socket(client_sock, server_side=True)
            except Exception as e:
                log(f"MITM TLS handshake failed for {host}: {_short_exc(e)}", "WARN")
                return
            fixed_route = True   # a CONNECT tunnel belongs to one dest for its whole life
        else:
            # Plain-HTTP proxying: absolute-form request line, e.g.
            # "GET http://example.com/path HTTP/1.1" — no CONNECT involved,
            # and the request we already read is the first one to relay.
            p = urlparse(first_target)
            host, port = p.hostname, (p.port or 80)
            pending_first = parsed
            if not host:
                return
            fixed_route = False  # a keep-alive plain-HTTP connection can hop hosts per request

        def _route_for(h: str, prt: int, use_tls: bool):
            is_clone   = (h == MAIN_HOST)
            clone_port = PORT
            if not is_clone:
                with _cdn_port_lock:
                    cp = _cdn_host_port.get(h, 0)
                if cp and cp > 0:
                    is_clone, clone_port = True, cp
            if is_clone:
                return "127.0.0.1", clone_port, False, f"localhost:{clone_port}", "clone"
            return h, prt, use_tls, h, "passthru"

        dest_host, dest_port, dest_tls, host_hdr, tag = _route_for(host, port, first_method == "CONNECT")

        rfile = work_sock.makefile("rb")
        wfile = work_sock.makefile("wb")
        while True:
            if pending_first is not None:
                parsed_req, pending_first = pending_first, None
            else:
                parsed_req = _mitm_read_headers(rfile)
                if parsed_req is None:
                    break
                if not fixed_route:
                    # Browsers reuse one keep-alive connection to a plain-HTTP
                    # forward proxy across requests for DIFFERENT destination
                    # hosts. Re-resolving routing only for the very first
                    # request (as before) silently sent every later request
                    # on this connection to that first host too — re-derive
                    # it per request instead.
                    try:
                        _sl, _ = parsed_req
                        _m, _t, _ = _sl.decode("latin-1").split(" ", 2)
                        _pp = urlparse(_t)
                        if _pp.hostname:
                            host, port = _pp.hostname, (_pp.port or 80)
                            dest_host, dest_port, dest_tls, host_hdr, tag = _route_for(host, port, False)
                    except Exception:
                        pass   # malformed/relative request line — keep previous routing
            if not _mitm_process_request(parsed_req, rfile, wfile, dest_host, dest_port,
                                          dest_tls, host_hdr, tag):
                break
    except (ConnectionError, OSError, socket.timeout):
        pass
    except ssl.SSLError as e:
        log(f"MITM TLS error: {_short_exc(e)}", "DEBUG")
    except Exception as e:
        log(f"MITM connection error: {_short_exc(e)}", "WARN")
    finally:
        try: work_sock.close()
        except Exception: pass
        if work_sock is not client_sock:
            try: client_sock.close()
            except Exception: pass

def _start_firefox_proxy() -> None:
    if not _CRYPTOGRAPHY_OK:
        log("FIREFOX_PROXY needs the 'cryptography' package "
            "(pip install cryptography --break-system-packages) — staying disabled", "ERROR")
        return
    _ensure_root_ca()
    log(f"Root CA ready → {_CA_CERT_PATH}", "INFO")
    log("One-time step: import that .pem in Firefox → Settings → Privacy & "
        "Security → Certificates → View Certificates → Authorities → Import "
        "→ check 'Trust this CA to identify websites'. Then set Firefox's "
        f"manual proxy (HTTP + HTTPS/SSL) to 127.0.0.1 : {FIREFOX_PROXY_PORT}.", "INFO")

    def _serve() -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            srv.bind((HOST, FIREFOX_PROXY_PORT))
        except OSError as e:
            log(f"FIREFOX_PROXY bind failed on {FIREFOX_PROXY_PORT}: {e}", "ERROR")
            return
        srv.listen(128)
        log(f"FIREFOX_PROXY listening on {HOST}:{FIREFOX_PROXY_PORT}", "INFO")
        while True:
            try:
                client_sock, addr = srv.accept()
            except OSError:
                break
            threading.Thread(target=_mitm_handle_connection, args=(client_sock, addr),
                              daemon=True, name=f"mitm-{addr[0]}:{addr[1]}").start()

    threading.Thread(target=_serve, daemon=True, name="firefox-proxy-listener").start()

# ── Main proxy ────────────────────────────────────────────────────────────────

@app.route("/", defaults={"path": ""}, methods=_ALL_METHODS)
@app.route("/<path:path>",             methods=_ALL_METHODS)
def proxy(path: str) -> Response:
    method   = flask_request.method.upper()
    target   = _upstream_url(path)
    req_path = "/" + path.lstrip("/")
    stats.inc("proxied")
    _has_range = bool(flask_request.headers.get("Range", ""))
    purpose = flask_request.headers.get("Purpose", "").lower()
    _sec_dest = flask_request.headers.get("Sec-Fetch-Dest", "").lower()
    # Only short-circuit SPECULATIVE prefetches on top-level document requests.
    # Returning 204 for ALL prefetch headers breaks <link rel="preload"> —
    # Chrome sends Purpose: prefetch for preloaded CSS/JS/fonts too, and the
    # page then tries to use resources that were never fetched.
    # SKIP this short-circuit in OFFLINE mode: a prefetched page that IS
    # cached on disk should be served, not 204'd (the browser may skip
    # navigating to it, showing a blank page even though content exists).
    if (not OFFLINE
            and purpose in ("prefetch", "prerender")
            and _sec_dest in ("document", "", "frame", "iframe")):
        _gui_push_raw(method, req_path, 204, "text/plain", b"",
                      origin=MAIN_HOST)
        return Response(status=204)  # No Content
    # NOTE: no Upgrade:websocket branch here — _S2LWSGIRequestHandler
    # intercepts every WS upgrade at the socket layer before this view ever
    # runs (see its docstring). A request reaching this function is
    # guaranteed to not be a WS upgrade.

    # Service worker — no-op SW
    if _is_sw_path(req_path):
        _gui_push_raw(method, req_path, 200, "application/javascript", _SW_NOOP,
                      origin=MAIN_HOST)
        resp = Response(_SW_NOOP, content_type="application/javascript; charset=utf-8")
        resp.headers["Service-Worker-Allowed"] = "/"
        resp.headers["Cache-Control"] = "no-store"
        return resp

    if any(x in target for x in BLOCK_PATHS):
        _gui_push_raw(method, req_path, 403, "text/plain", b"Blocked",
                      origin=MAIN_HOST)
        return Response("Blocked", status=403)

    if method == "OPTIONS":
        _gui_push_raw("OPTIONS", req_path, 204, "text/plain", b"",
                      origin=MAIN_HOST)
        r = Response(status=204)
        _req_origin = flask_request.headers.get("Origin", "")
        r.headers.update({
            "Allow":                        ", ".join(_ALL_METHODS),
            "Access-Control-Allow-Origin":  _req_origin if _req_origin else "*",
            "Access-Control-Allow-Credentials": "true" if _req_origin else None,
            "Access-Control-Allow-Methods": ", ".join(_ALL_METHODS),
            "Access-Control-Allow-Headers": flask_request.headers.get(
                                            "Access-Control-Request-Headers", "*"),
            "Access-Control-Max-Age": "86400",
        })
        # Remove None values (Credential header when no Origin)
        r.headers = {k: v for k, v in r.headers.items() if v is not None}
        return r

    if OFFLINE:
        if method in _SAFE_METHODS:
            lp = local_path(target)
            if os.path.isfile(lp):
                # Range request support in OFFLINE mode — critical for
                # video/audio playback. Without 206 Partial Content, HTML5
                # <video> players interpret a full 200 as a truncated stream
                # and loop/stall indefinitely.
                rng = flask_request.headers.get("Range", "")
                if rng:
                    try:
                        file_size = os.path.getsize(lp)
                    except OSError:
                        file_size = 0
                    rng_parsed = _parse_range_header(rng, file_size)
                    if rng_parsed is not None:
                        start, end = rng_parsed
                        _range_ok = True
                        try:
                            with open(lp, "rb") as _rf:
                                _rf.seek(start)
                                _slice = _rf.read(end - start + 1)
                        except OSError:
                            _range_ok = False
                        if _range_ok:
                            ct_off = resolve_mime(lp)
                            _rng_resp = Response(_slice, status=206, content_type=ct_off)
                            _rng_resp.headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
                            _rng_resp.headers["Accept-Ranges"] = "bytes"
                            _rng_resp.headers["Content-Length"] = str(len(_slice))
                            _rng_resp.headers["Access-Control-Allow-Origin"] = "*"
                            _rng_resp.headers["Cross-Origin-Resource-Policy"] = "cross-origin"
                            log(f"{method} HIT {_fmt_host(urlparse(target).netloc)}{req_path} 206 [{_fmt_size(len(_slice))}]", "←")
                            _gui_push_raw(method, req_path, 206, ct_off, _slice,
                                          origin=MAIN_HOST,
                                          _skip_log=True)   # caller already logged
                            return _rng_resp
                        # Range read failed — fall through to full-file serve below
                r = _cached_response(lp, target, method, req_path)
                if r is not None:
                    return r
            # Fallback: try alternate CDN hosts that may have this path cached.
            # In OFFLINE mode the main-host 404 fallback below never runs, so
            # without this an asset cached on a CDN host but requested
            # against the main host returns 404.
            if PROXY_CDN:
                req_path_cdn = urlparse(target).path
                req_qs = flask_request.query_string.decode("utf-8", "ignore")
                with _cdn_port_lock:
                    _cdn_snap = dict(_cdn_host_port)
                for _cdn_host in _cdn_snap:
                    _cdn_url = f"https://{_cdn_host}{req_path_cdn}"
                    if req_qs:
                        _cdn_url += f"?{req_qs}"
                    _cdn_lp = local_path(_cdn_url)
                    if os.path.isfile(_cdn_lp):
                        _cdn_r = _cached_response(_cdn_lp, _cdn_url, method, req_path)
                        if _cdn_r is not None:
                            log(f"OFFLINE CDN fallback hit {_fmt_host(_cdn_host)}{req_path_cdn}", "CDN")
                            return _cdn_r
        elif method in _BODY_METHODS and os.path.isfile(local_path(target)):
            # POST/PUT to a path that has a cached GET body — return 405 so
            # the client gets a deterministic status instead of misleading 404.
            return Response("Offline — method not supported, GET cached",
                            status=405, headers={"Allow": "GET, HEAD"})
        _gui_push_raw(method, req_path, 404, "text/plain", b"Offline",
                      origin=MAIN_HOST)
        return Response("Offline — not cached", status=404)

    if method in _SAFE_METHODS:
        lp = local_path(target)
        # IMPORTANT: Range requests MUST always hit upstream.
        # Serving a full file from disk cache in response to "Range: bytes=X-Y"
        # causes the browser to receive a 200 instead of 206 Partial Content —
        # video players (HTML5 <video>) interpret this as a truncated
        # stream and loop / stall indefinitely.
        if os.path.isfile(lp) and not _has_range:
            cached = _cached_response(lp, target, method, req_path)
            if cached is not None:
                return cached

    raw_body = flask_request.get_data(cache=True) if method in _BODY_METHODS else b""
    ctx = _build_ctx(method, target, raw_body)

    if _REQ_HOOKS:
        stats.inc("hooks_run", _run_hooks(_REQ_HOOKS, ctx))

    # Decide whether to use stream=True for the upstream request.
    # stream=True is needed for: Range requests (audio/video 206), and URLs
    # whose extension suggests streamable content (video/audio/wasm/large bin).
    # For everything else (HTML/CSS/JS/JSON/fonts/images), stream=False is
    # more reliable — curl_cffi's HTTP/2 stream mode can return empty bytes
    # for small static files, causing the "Stream-mode empty body" warnings
    # and forcing an expensive re-fetch.
    _req_ext = os.path.splitext(urlparse(target).path)[1].lower()
    _looks_streamable = _req_ext in (
        ".mp4", ".webm", ".ogg", ".ogv", ".mpeg", ".mp2t", ".m3u8",
        ".mp3", ".m4a", ".aac", ".wav", ".opus",
        ".wasm", ".zip", ".tar", ".gz", ".tgz", ".bz2",
    )
    _do_stream = method in _SAFE_METHODS and (_has_range or _looks_streamable)

    try:
        upstream_r = _do_upstream(method, target, ctx, stream=_do_stream)
    except _CONN_ERRORS as exc:
        short = _short_exc(exc)
        log(f"{method} {urlparse(target).path} — {short}", "WARN")
        stats.inc("conn_errors")
        # Retry once with a fresh session on connection-level errors.
        # Try curl_cffi first (best CF bypass), then fall back to
        # cloudscraper (older but sometimes works when cffi's DNS/TLS fails
        # on specific networks), then plain requests as last resort.
        #
        # FIX: rotate the CLIENT session (per-IP, the one _do_upstream uses),
        # NOT _proxy_local.s (thread-local — _do_upstream ignores it, so the
        # old retry was a no-op that leaked sessions without rotating anything).
        try:
            _cid = _client_id()
        except Exception:
            _cid = None
        if _cid is not None:
            with _CLIENT_LOCK:
                _old_sess = _CLIENT_SESSIONS.get(_cid)
            if _old_sess is not None:
                try: _old_sess.close()
                except Exception: pass
        upstream_r = None
        last_exc = exc
        # Primary: fresh curl_cffi session
        for _attempt, _sess_factory in (
            (1, _make_session),
            (2, _make_cloudscraper_session),
            (3, _make_requests_session),
        ):
            try:
                _rsess = _sess_factory()
            except Exception:
                continue
            # Store the fresh session so _do_upstream picks it up
            if _cid is not None:
                with _CLIENT_LOCK:
                    _CLIENT_SESSIONS[_cid] = _rsess
            else:
                _proxy_local.s = _rsess   # fallback for background threads
            try:
                upstream_r = _do_upstream(method, target, ctx, stream=_do_stream)
                log(f"Retry #{_attempt} succeeded {urlparse(target).path}", "INFO")
                break
            except Exception as exc2:
                last_exc = exc2
                short2 = _short_exc(exc2)
                log(f"Retry #{_attempt} failed {urlparse(target).path} — {short2}", "WARN")
                try: _rsess.close()
                except Exception: pass
                continue
        if upstream_r is None:
            short2 = _short_exc(last_exc)
            log(f"All retries failed {urlparse(target).path} — {short2}", "ERROR")
            err_body = (f"Connection error: {short}\n"
                        f"Retry: {short2}\n\n"
                        f"This may be caused by:\n"
                        f"  • The server closed the connection prematurely\n"
                        f"  • A network interruption or timeout\n"
                        f"  • Cloudflare or WAF blocking the request\n"
                        f"  • DNS resolution failure (check internet connection)")
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
            _hist_origin = _fmt_host(urlparse(hist_url).netloc)
            _gui_push_raw(
                method,
                hist_path,
                hist_resp.status_code,
                hist_ct,
                hist_body if hist_body else f"(redirect {hist_resp.status_code})".encode(),
                display_tag="[REDIR]",
                origin=_hist_origin,
            )

    # ── Streaming: hand off immediately for large/binary content ─────────────
    # Check the actual Content-Type now that we have the response headers.
    # If it should be streamed, do it NOW before consuming any body bytes.
    _real_ct = upstream_r.headers.get("Content-Type", "")
    _real_cl = int(upstream_r.headers.get("Content-Length", "0") or 0)
    # SSE: detect text/event-stream and use the dedicated SSE streamer
    # which re-buffers to event boundaries and injects keepalives.
    if SSE_PROXY and _is_sse_response(_real_ct) and method in _SAFE_METHODS:
        return _stream_sse_resp(upstream_r, method, target)
    # Streaming hand-off: if we fetched with stream=True AND the response is
    # streamable content (audio/video/large binary) OR a 206 Partial Content
    # (Range request), hand off to _stream_resp. CRITICAL: when stream=True
    # was used, we must NOT fall through to upstream_r.content for 206/range
    # responses — curl_cffi's HTTP/2 stream mode can return empty bytes for
    # partial content chunks, producing "0.0B" bodies that break audio/video
    # playback. _stream_resp reads the stream chunk-by-chunk and is reliable.
    _has_content_range = bool(upstream_r.headers.get("Content-Range", ""))
    if (_do_stream and method in _SAFE_METHODS
            and (_should_stream(_real_ct, _real_cl)
                 or upstream_r.status_code == 206
                 or _has_content_range)):
        return _stream_resp(upstream_r, method, target)

    # Decompress upstream body before we do anything with it.
    # With stream=True, upstream_r.content consumes the stream now.
    # If the body comes back empty despite Content-Length > 0, curl_cffi's
    # stream mode may have lost chunks — re-fetch with stream=False.
    enc  = upstream_r.headers.get("Content-Encoding", "")
    try:
        body = decompress_body(upstream_r.content, enc)
    except Exception as e:
        log(f"Body consume error on {req_path}: {_short_exc(e)}", "WARN")
        body = b""
    # Stream-mode empty-body recovery: if stream=True returned an empty body
    # but the response should have content, re-fetch with stream=False.
    # Guard against infinite recursion via a thread-local flag.
    if (not body and method in _SAFE_METHODS
            and upstream_r.status_code == 200
            and not getattr(_proxy_local, "in_refetch", False)):
        _cl_check = upstream_r.headers.get("Content-Length", "")
        _ct_check = upstream_r.headers.get("Content-Type", "")
        if (_cl_check and int(_cl_check or 0) > 0) or "text/html" in _ct_check:
            log(f"Stream-mode empty body on {req_path} — re-fetching with stream=False", "WARN")
            try:
                _proxy_local.in_refetch = True
                _rf_r = _do_upstream(method, target, ctx, stream=False)
                _rf_body = decompress_body(_rf_r.content, _rf_r.headers.get("Content-Encoding", ""))
                if _rf_body:
                    body = _rf_body
                    upstream_r = _rf_r
                    log(f"Stream re-fetch succeeded: {_fmt_size(len(body))}", "INFO")
            except Exception as e:
                log(f"Stream re-fetch failed: {_short_exc(e)}", "WARN")
            finally:
                _proxy_local.in_refetch = False

    # ── WAF block detection + retry ─────────────────────────────────────────
    # Detects classic 403/503 block pages and plain-text "blocked" bodies.
    # On detection, rotates the CLIENT session (not the proxy session —
    # _do_upstream uses _get_client_session, so rotating _proxy_local.s is
    # a no-op) and retries once, preserving cf_clearance cookies.
    # IMPORTANT: skip the block check entirely for API paths and 429 responses.
    # API endpoints return small JSON bodies that can false-positive, and 429
    # is a legitimate rate-limit signal — treating it as a block causes a retry
    # cascade that doubles the request rate and pushes the API into a sustained
    # 429 loop, starving the SPA of data.
    _api_exempt = any(req_path.startswith(p) for p in _BOT_EXEMPT_PREFIXES)
    _looks_blocked = (
        not _api_exempt
        and upstream_r.status_code != 429
        and method in _SAFE_METHODS
        and (_is_cf_block(body, upstream_r.status_code, dict(upstream_r.headers))
             or _is_raw_block_text(body, upstream_r.status_code))
    )
    if _looks_blocked:
        log(f"WAF block detected on {req_path} — rotating client session and retrying", "WARN")
        try:
            # Rotate the CLIENT session (the one _do_upstream actually uses),
            # preserving cf_clearance cookies from the old session.
            _old_client_sess = _get_client_session()
            _fresh = _make_session()
            try:
                for _c in _old_client_sess.cookies:
                    if _c.name in ("cf_clearance", "cf_chl_2", "cf_chl_rc_z"):
                        _fresh.cookies.set(_c.name, _c.value, domain=_c.domain, path=_c.path)
            except Exception:
                pass
            _browser_ua_r = flask_request.headers.get("User-Agent", "")
            if _browser_ua_r:
                _fresh.headers["User-Agent"] = _sanitize_ua(_browser_ua_r)
            with _CLIENT_LOCK:
                _cid = _client_id()
                _CLIENT_SESSIONS[_cid] = _fresh
            # FIX: close the OLD session before it's orphaned. Without this,
            # its connection pool (up to POOL_MAXSIZE=64 conns) and file
            # descriptors leak — repeated WAF blocks accumulate leaked sessions.
            try:
                _old_client_sess.close()
            except Exception:
                pass
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
    #   B) API endpoints using chunked Transfer-Encoding.
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
    # FIXED: previously excluded only 204/304. But 4xx (404 Not Found, 429 Too
    # Many Requests) and 5xx responses also legitimately have small bodies that
    # curl_cffi sometimes returns empty (it doesn't always read the body for
    # error responses). Re-fetching those caused cascading 429s on
    # rate-limited APIs — every 404 for an endpoint triggered a
    # retry that itself got rate-limited, compounding the problem. Now we only
    # re-fetch decompression failures for 2xx (excluding 204), where an empty
    # body really does indicate a bug.
    _decomp_failed = (
        len(body) == 0
        and not _cl_explicit_zero
        and _cl_header > 0
        and 200 <= upstream_r.status_code < 300
        and upstream_r.status_code != 204
    )

    # Case B: JSON API endpoint, empty body, chunked (no Content-Length), status 200
    # Only for GET/HEAD — POST/DELETE intentionally return empty 200.
    # FIXED: was triggering on 4xx/5xx too because the original check was
    # `status_code == 200`. But rate-limited APIs return 429 with an empty JSON body
    # when rate-limited, and 404 with an empty body for non-existent resources
    # — re-fetching those just burns the rate-limit budget faster. Now we
    # explicitly require 200 (or 206 for partial content) AND Content-Type
    # starts with application/json.
    _empty_api = (
        method in _SAFE_METHODS
        and upstream_r.status_code in (200, 206)
        and len(body) == 0
        and not _cl_explicit_zero
        and _is_json_ct
    )

    # Case C: HTML page with empty body — re-fetch when the server PROMISED
    # bytes (Content-Length > 0) OR when there's no Content-Length at all
    # (chunked transfer) and the body came out empty. Some SPA routes
    # intentionally return 200 with empty body, but for a top-level HTML
    # document that's almost always a bug (CF challenge, decompression failure,
    # curl_cffi stream issue) — re-fetch with identity encoding to recover.
    _empty_html = (
        method in _SAFE_METHODS
        and upstream_r.status_code == 200
        and len(body) == 0
        and _is_html_ct
        and not _cl_explicit_zero
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
            retry_fwd      = dict(ctx.req_headers)
            # Force no compression on retry — defeats decompression failures
            retry_fwd["Accept-Encoding"] = "identity"
            # Preserve Authorization header explicitly (Bearer tokens)
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
                    with open(cdn_lp, "rb") as _cdnf:
                        cdn_data = _cdnf.read()
                except OSError as e:
                    log(f"CDN disk hit unreadable {cdn_lp}: {_short_exc(e)}", "WARN")
                else:
                    log(f"CDN hit {_fmt_host(cdn_host)}{req_path_cdn}", "CDN")
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
                    log(f"CDN fallback {_fmt_host(cdn_host)}{req_path_cdn} {cdn_r.status_code}", "CDN")
                    out = filter_resp(_resp_headers_dict(cdn_r))
                    out["Access-Control-Allow-Origin"]  = "*"
                    out["Cross-Origin-Resource-Policy"]  = "cross-origin"
                    return Response(cdn_body, status=cdn_r.status_code, headers=out, content_type=ct)
            except Exception as exc:
                log(f"CDN fetch failed {cdn_url}: {_short_exc(exc)}", "WARN")
    _resp_ct_raw = upstream_r.headers.get("Content-Type", "")
    ctx.resp_status  = upstream_r.status_code
    ctx.resp_headers = filter_resp(_resp_headers_dict(upstream_r), body,
                                    is_top_level_html="text/html" in _resp_ct_raw)
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
                ctx.resp_headers = filter_resp(_resp_headers_dict(refetch_r), body, is_top_level_html=True)
                log(f"RSC re-fetch succeeded {urlparse(target).path}", "INFO")
            else:
                log("RSC re-fetch also returned RSC — serving as-is", "WARN")
        except Exception as exc:
            log(f"RSC re-fetch error: {_short_exc(exc)}", "WARN")
    sc_color = (Fore.GREEN if upstream_r.status_code < 300
                else Fore.YELLOW if upstream_r.status_code < 400
                else Fore.RED)
    _is_api  = any(x in ctx.resp_ct for x in ("json", "xml", "event-stream"))
    _lbl     = "API" if _is_api else "HTML" if "text/html" in ctx.resp_ct else "   "
    _suppress = is_subresource(ctx.resp_ct) and not _is_api
    _host_disp = _fmt_host(urlparse(target).netloc)
    if not _suppress:
        log(f"{method} {sc_color}{upstream_r.status_code}{Style.RESET_ALL}"
            f" {_lbl} {_host_disp}{req_path} {_fmt_size(len(body))}"
            + (f" [{platform}]" if platform != "Unknown" else ""), "→")
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
        # Mark this URL as visited so the crawler doesn't fetch it again.
        # The proxy already fetched it — re-fetching is wasteful and creates
        # duplicate log entries. enqueue() still runs for link extraction
        # but _crawl() will skip it immediately via the visited set.
        _norm_target = normalize_url(target)
        with visited_lock:
            visited.add(_norm_target)
        enqueue(target)
        # Extract links from served HTML so the discovery workers pre-cache
        # sub-pages and assets — this is what makes the FIRST page load work
        # fully without needing a reload to trigger crawling.
        # Only in bulk mode (CRAWL=True) — in procedural mode (CRAWL=False)
        # there are no discovery workers to process the enqueued links.
        if CRAWL and is_html(ctx.resp_body, ctx.resp_ct) and not OFFLINE:
            # Use a bounded pool — one thread per page would exhaust the OS
            # thread limit on large sites with hundreds of pages.
            try:
                _link_pool.submit(_extract_links_async, ctx.resp_body, target)
            except Exception:
                pass

    # HTML + CSS + JSON post-processing
    _is_html = is_html(ctx.resp_body, ctx.resp_ct)
    _is_css  = "text/css" in ctx.resp_ct
    _is_json = "json" in ctx.resp_ct.lower()
    if _is_html or _is_css:
        _body = ctx.resp_body
        if PROXY_CDN:
            _body = _rewrite_ext_urls(_body)
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

    # Final blank-page guard. If after ALL recovery the body is still
    # empty AND it's a top-level HTML document (status 200, text/html, GET),
    # try ONE last re-fetch with stream=False + identity encoding (the stream
    # mode of curl_cffi can sometimes lose chunks). If that also fails, show
    # a diagnostic page instead of a blank white screen.
    if (method in _SAFE_METHODS
            and ctx.resp_status == 200
            and len(ctx.resp_body) == 0
            and "text/html" in (ctx.resp_ct or "")
            and not flask_request.headers.get("Range")):
        # Last-resort re-fetch with stream=False + identity encoding
        try:
            _last_fwd = dict(ctx.req_headers)
            _last_fwd["Accept-Encoding"] = "identity"
            _last_ctx = _dc_replace(ctx, req_headers=_last_fwd)
            _last_r = _do_upstream(method, target, _last_ctx, stream=False)
            _last_body = _last_r.content
            if _last_body and len(_last_body) > 0:
                ctx.resp_body = _last_body
                ctx.resp_ct = _last_r.headers.get("Content-Type", "text/html")
                ctx.resp_headers = filter_resp(_resp_headers_dict(_last_r), _last_body,
                                                is_top_level_html=True)
                ctx.resp_headers.pop("content-length", None)
                ctx.resp_headers.pop("Content-Length", None)
                log(f"Last-resort re-fetch succeeded for {req_path}: {_fmt_size(len(_last_body))}", "INFO")
            else:
                log(f"Last-resort re-fetch also empty for {req_path}", "WARN")
        except Exception as e:
            log(f"Last-resort re-fetch error: {_short_exc(e)}", "WARN")
    if (method in _SAFE_METHODS
            and ctx.resp_status == 200
            and len(ctx.resp_body) == 0
            and "text/html" in (ctx.resp_ct or "")
            and not flask_request.headers.get("Range")):
        ctx.resp_ct = "text/html; charset=utf-8"
        ctx.resp_body = (
            b"<!DOCTYPE html><html><head><meta charset=\"utf-8\">"
            b"<title>S2L \xe2\x80\x94 Empty Response</title>"
            b"<style>body{font-family:system-ui,sans-serif;max-width:600px;"
            b"margin:50px auto;padding:20px;color:#333}"
            b"h1{color:#e74c3c}code{background:#f4f4f4;padding:2px 6px;"
            b"border-radius:3px}</style></head><body>"
            b"<h1>Empty response from upstream</h1>"
            b"<p>The server returned HTTP 200 but with an empty body. This "
            b"usually means:</p>"
            b"<ul>"
            b"<li>The page is behind a CAPTCHA / bot challenge that S2L couldn't bypass</li>"
            b"<li>The upstream server detected the proxy and returned an empty page</li>"
            b"<li>A compression/decompression issue occurred</li>"
            b"</ul>"
            b"<p>Try:</p>"
            b"<ul>"
            b"<li>Refreshing the page (S2L auto-rotates sessions on failures)</li>"
            b"<li>Checking the S2L console for CF block warnings</li>"
            b"<li>Installing <code>curl_cffi</code> for better Cloudflare bypass</li>"
            b"</ul>"
            b"<p><small>Path: <code>" + req_path.encode("utf-8", "replace") + b"</code></small></p>"
            b"</body></html>"
        )
        ctx.resp_headers.pop("content-length", None)
        ctx.resp_headers.pop("Content-Length", None)

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
    if CRAWL and SKIP_CRAWL_CACHE:
                      flags.append(f"{G}skip-crawl-cache{R}")
    elif CRAWL and not SKIP_CRAWL_CACHE:
                      flags.append(f"{Y}no-skip-crawl-cache{R}")
    if OFFLINE:       flags.append(f"{Y}offline{R}")
    if DUMP_ALL:      flags.append(f"{M}dump-all{R}")
    if PROXY_CDN and CACHE_CDN:   flags.append(f"{M}cdn:cache{R}")
    elif PROXY_CDN:               flags.append(f"{Y}cdn:live{R}")
    if MULTIPORT:     flags.append(f"{M}multiport{R}")
    if SHOW_HIDDEN:   flags.append(f"{G}show-hidden{R}")
    if SCAN_PATHS:    flags.append(f"{Style.BRIGHT+Fore.RED}scan:{SCAN_PATHS}{R}")
    if CAPTURE:       flags.append(f"{Style.BRIGHT+Fore.RED}capture{R}")
    if HOOK_GUI:      flags.append(f"{Y}hook-gui{R}")
    if FIREFOX_PROXY: flags.append(f"{Style.BRIGHT+Fore.CYAN}firefox-proxy:{FIREFOX_PROXY_PORT}{R}")
    if WS_PING_INTERVAL > 0:  flags.append(f"{G}ws-keepalive:{WS_PING_INTERVAL}s{R}")
    if WS_DEFLATE:            flags.append(f"{G}ws-deflate{R}")
    if WS_AUTO_RECONNECT:     flags.append(f"{G}ws-reconnect{R}")
    if SSE_PROXY:             flags.append(f"{C}sse{R}")
    if SSE_HEARTBEAT > 0:     flags.append(f"{C}sse-hb:{SSE_HEARTBEAT}s{R}")
    if _CURL_CFFI_OK:         flags.append(f"{C}h2{R}")
    if TCP_TUNNEL:            flags.append(f"{M}tcp-tunnel{R}")
    if UDP_TUNNEL:            flags.append(f"{M}udp-tunnel{R}")
    flag_str   = f" {DIM}·{R} ".join(flags) if flags else f"{DIM}none{R}"

    hook_str   = (f"{Y}{n_hooks} hook{'s' if n_hooks != 1 else ''}{R}"
                  if n_hooks else f"{DIM}none{R}")
    device_str = (f"{W}{DEVICE}{R} {DIM}(auto-mirrors browser UA){R}"
                  if DEVICE == "auto" else f"{W}{DEVICE}{R}")

    bypass_eng = (f"{G}curl_cffi{R} {DIM}(TLS profile auto-matched to device){R}" if _CURL_CFFI_OK
                  else f"{Y}cloudscraper{R} {DIM}(pip install curl-cffi for better CF bypass){R}")

    # Box banner with dynamic right border. Body lines contain ANSI color codes
    # (zero visual width) so we measure VISIBLE width by stripping ANSI before
    # padding. The box auto-expands if any line is wider than _BOX_W.
    _title = "S I T E  2  L O C A L"
    _ver   = "V 8 . 0"
    _title_core = f"  {_title}    {_ver}  "

    # Build body lines first so we can measure their visible widths.
    _l_target  = f"  {DIM}Target:  {R}  {G}{MAIN_HOST}{R} {DIM}({ip}){R}"
    _l_proxy   = f"  {DIM}Proxy:   {R}  {W}http://{HOST}:{PORT}{R}"
    _l_device  = f"  {DIM}Device:  {R}  {device_str}"
    _l_bypass  = f"  {DIM}Bypass:  {R}  {bypass_eng}"
    _l_workers = f"  {DIM}Workers: {R}  {W}{WORKERS}{R}"
    _l_timeout = f"  {DIM}Timeout: {R}  {W}connect={TIMEOUT_CONN}s  read={TIMEOUT_READ}s{R}"
    _l_retries = f"  {DIM}Retries: {R}  {W}{RETRIES}× backoff={BACKOFF}s{R}"
    _l_hooks   = f"  {DIM}Hooks:   {R}  {hook_str}"
    _l_flags   = f"  {DIM}Flags:   {R}  {flag_str}"
    _body_lines = [_l_target, _l_proxy, _l_device, _l_bypass, _l_workers,
                   _l_timeout, _l_retries, _l_hooks, _l_flags]

    _BOX_W = max(54, len(_title_core),
                 max(len(_ANSI_ESC.sub("", ln)) for ln in _body_lines) + 1)

    def _pad_r(content: str) -> str:
        vw = len(_ANSI_ESC.sub("", content))
        if vw >= _BOX_W:
            return content
        return content + " " * (_BOX_W - vw)

    _pad_left = (_BOX_W - len(_title_core)) // 2
    _pad_right = _BOX_W - len(_title_core) - _pad_left
    _title_line = " " * _pad_left + _title_core + " " * _pad_right

    def _row(content: str) -> str:
        return f"{C}  ║{R}{_pad_r(content)}{C}║{R}"

    _rainbow_print(
        f"\n"
        f"{C}  ╔{'═' * _BOX_W}╗{R}\n"
        f"{C}  ║{W}{_title_line}{R}{C}║{R}\n"
        f"{C}  ╠{'═' * _BOX_W}╣{R}\n"
        + "\n".join(_row(ln) for ln in _body_lines) + "\n"
        f"{C}  ╚{'═' * _BOX_W}╝{R}"
    )

    if PROXY_CDN and MULTIPORT:
        _rainbow_print(f"  {M}▶ MULTIPORT{R} {DIM}CDN hosts get a dedicated port starting at {PORT+1}{R}")
        if sys.stdin.isatty():
            _rainbow_print(f"  {M}▶ MULTIPORT viewer{R} {DIM}press {Y}'1'{R}{DIM} in the terminal to open the live CDN table ({Y}ESC{R}{DIM} to close){R}")
    elif PROXY_CDN:
        _rainbow_print(f"  {M}▶ CDN{R} {DIM}assets via {_EXT_PREFIX}/ (single port){R}")
    if CAPTURE:
        skip   = "static skipped" if CAPTURE_SKIP_STATIC else "all captured"
        body   = "req body on" if CAPTURE_BODIES else "req body off"
        others = " · CDN included" if CAPTURE_CDN else ""
        _rainbow_print(f"  {Style.BRIGHT+Fore.RED}▶ CAPTURE{R} {DIM}({skip} · {body}{others}) → {DATA_FOLDER}/captures/{R}")
    if SHOW_HIDDEN:
        _rainbow_print(f"  {G}▶ SHOW_HIDDEN{R} {DIM}hidden elements revealed in every HTML page{R}")
    if SCAN_PATHS:
        n_wl, n_paths = _scan_paths_summary()
        _rate_disp = (f"{SCANS_PER_SECOND}/s" if SCANS_PER_SECOND and SCANS_PER_SECOND > 0
                      else "unlimited")
        _bar_disp = f"{G}tqdm{R}{DIM}" if _TQDM_OK else f"{DIM}log"
        _rainbow_print(f"  {Style.BRIGHT+Fore.RED}▶ SCAN_PATHS{R} {DIM}mode={SCAN_PATHS} · {n_wl} wordlist(s) · "
              f"{n_paths} paths · rate={_rate_disp} · progress={_bar_disp} → "
              f"{DATA_FOLDER}/hidden_paths.json{R}")
    if HOOK_GUI:
        _rainbow_print(f"  {Y}▶ HOOK_GUI{R} {DIM}Tkinter traffic inspector + live hook editor{R}")
    if n_hooks:
        _rainbow_print(f"  {Y}▶ HOOKS{R}")
        for pat, mset, fn in _REQ_HOOKS:
            _rainbow_print(f"  {DIM}  req  [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")
        for pat, mset, fn in _RESP_HOOKS:
            _rainbow_print(f"  {DIM}  resp [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")

# ──────────────────────────────────────────────────────────────────────────────
# MULTIPORT CDN Viewer  (CLI, hotkey '1' to open, ESC to close)
#
# Pure-terminal live inspector for every CDN host promoted to its own port.
# Runs in a background daemon thread, watches stdin in cbreak mode for '1',
# then switches to the alternate screen buffer (xterm `\x1b[?1049h`) which
# preserves the main screen's scrollback — ESC returns to the main screen
# with logs intact. Only starts when MULTIPORT is on AND stdin is a TTY.
# ──────────────────────────────────────────────────────────────────────────────

def _render_multiport_viewer() -> str:
    """Build the alternate-screen CDN table as a single string.

    Pulled out of the viewer loop so it can be unit-tested without a TTY.
    """
    with _cdn_port_lock:
        # Snapshot under the lock so the dict doesn't mutate mid-render.
        # Filter out sentinels: port < 0 means "server is starting" (not
        # ready yet), port == 0 means "registered but no dedicated port"
        # (single-port /__s2l_ext__/ mode). Only positive ports are real
        # MULTIPORT listeners — those are what the user wants to see.
        snap = sorted(
            ((h, p) for h, p in _cdn_host_port.items() if p > 0),
            key=lambda hp: hp[1],
        )
        pending = sum(1 for _, p in _cdn_host_port.items() if p < 0)
        unported = sum(1 for _, p in _cdn_host_port.items() if p == 0)

    W = Style.BRIGHT + Fore.WHITE
    C = Style.BRIGHT + Fore.CYAN
    G = Style.BRIGHT + Fore.GREEN
    Y = Style.BRIGHT + Fore.YELLOW
    M = Style.BRIGHT + Fore.MAGENTA
    D = Style.DIM
    R = Style.RESET_ALL

    lines: list[str] = []
    # Title bar — centered-ish, eye-catching
    lines.append(f"{M}╔══════════════════════════════════════════════════════════════════════╗{R}")
    lines.append(f"{M}║{W}   S2L · MULTIPORT CDN Viewer                                          {M}║{R}")
    lines.append(f"{M}╠══════════════════════════════════════════════════════════════════════╣{R}")
    lines.append(f"{M}║{D}   Press {R}{Y}ESC{R}{D} to return — logging continues unaffected in the       {M}║{R}")
    lines.append(f"{M}║{D}   background. Refreshes every 0.5 s.                                {M}║{R}")
    lines.append(f"{M}╠══════════════════════════════════════════════════════════════════════╣{R}")
    # Column header
    lines.append(f"{M}║{R} {C}#{R}  {C}CDN HOST{' ' * 38}{R} {C}PORT{' ' * 4}{R} {C}LOCAL URL{' ' * 22}{M}║{R}")
    lines.append(f"{M}╠══════════════════════════════════════════════════════════════════════╣{R}")
    if not snap:
        lines.append(f"{M}║{R}{D}   (no MULTIPORT CDNs registered yet — they appear here as the    {M}║{R}")
        lines.append(f"{M}║{R}{D}    proxy registers them on first hit)                            {M}║{R}")
        lines.append(f"{M}║{R}                                                                  {M}║{R}")
        lines.append(f"{M}║{R}{D}    Tip: browse the target site in your browser — every external   {M}║{R}")
        lines.append(f"{M}║{R}{D}    asset host will get its own row here in real time.             {M}║{R}")
    else:
        for i, (host, port) in enumerate(snap, start=1):
            url = f"http://localhost:{port}"
            # Truncate host if too long
            host_disp = host if len(host) <= 42 else host[:39] + "..."
            # Pad to align columns
            host_padded = host_disp.ljust(42)
            port_str = f":{port}".ljust(8)
            url_padded = url.ljust(30)
            row_color = G if i % 2 == 1 else W
            lines.append(
                f"{M}║{R} {row_color}{i:>2}{R} {host_padded} {Y}{port_str}{R} {D}{url_padded}{M}║{R}"
            )
    # Footer / status summary
    lines.append(f"{M}╠══════════════════════════════════════════════════════════════════════╣{R}")
    summary_parts = [f"{G}{len(snap)}{R}{D} active MULTIPORT host(s){R}"]
    if pending:
        summary_parts.append(f"{Y}{pending}{R}{D} starting…{R}")
    if unported:
        summary_parts.append(f"{D}{unported} shared-port{R}")
    summary = "  ·  ".join(summary_parts)
    # Truncate footer if too wide
    lines.append(f"{M}║{R} {summary}{' ' * max(0, 67 - len(_ANSI_ESC.sub('', summary)))}{M}║{R}")
    lines.append(f"{M}╚══════════════════════════════════════════════════════════════════════╝{R}")
    return "\n".join(lines)

# Cross-platform keypress detection. On Unix we use termios + tty + select;
# on Windows we'd use msvcrt. We only need this for the MULTIPORT viewer —
# everywhere else the script just reads no stdin.
def _multiport_viewer_loop() -> None:
    """Background thread: watches stdin for '1' → opens viewer; ESC → closes.

    Never raises into the main thread. Returns silently on any setup failure
    (no TTY, termios unsupported, etc.) so the rest of the app keeps running.
    """
    if not MULTIPORT:
        return
    # Don't run in OFFLINE-only / no-TTY contexts (e.g. piped input, IDE
    # consoles without a real terminal). The viewer is interactive — without
    # a real TTY it can neither capture individual keypresses nor switch to
    # the alternate screen buffer, so it would just sit there spinning.
    try:
        if not sys.stdin.isatty():
            return
    except Exception:
        return

    try:
        import termios
        import tty
        import select as _select
    except ImportError:
        # Windows: termios/tty don't exist. Supporting msvcrt adds complexity
        # for a feature whose main use case is the Linux dev terminal — skip
        # silently rather than half-implementing.
        return

    try:
        fd = sys.stdin.fileno()
    except Exception:
        return

    # Save the original terminal state ONCE. We restore it on every exit
    # path, but we only capture it here at thread start so a buggy restore
    # can't clobber a state we never captured.
    try:
        original_terms = termios.tcgetattr(fd)
    except (termios.error, OSError):
        return

    # Switch stdin to cbreak mode: characters are available immediately
    # without Enter, no echo, signals (Ctrl+C) still work.
    try:
        tty.setcbreak(fd)
    except (termios.error, OSError):
        return

    # Tracks whether we're currently inside the alternate-screen viewer.
    # We MUST know this so the restore-on-exit path knows whether to emit
    # the leave-alt-screen sequence.
    in_viewer = [False]
    # We refresh the viewer on this cadence so newly-registered CDNs appear.
    REFRESH_S = 0.5

    def _leave_viewer():
        if not in_viewer[0]:
            return
        sys.stdout.write("\x1b[?1049l")  # leave alternate screen buffer
        sys.stdout.flush()
        in_viewer[0] = False

    def _enter_viewer():
        if in_viewer[0]:
            return
        sys.stdout.write("\x1b[?1049h")  # enter alternate screen buffer
        sys.stdout.flush()
        in_viewer[0] = True

    def _redraw_viewer():
        """Render one frame inside the alternate screen."""
        sys.stdout.write("\x1b[H\x1b[2J")  # cursor to top-left + clear
        sys.stdout.write(_rainbow_text(_render_multiport_viewer()))
        sys.stdout.write(_rainbow_text(
            f"\n\n  {Style.DIM}(refreshing every {REFRESH_S}s — "
            f"press {Fore.YELLOW}ESC{Style.RESET_ALL}{Style.DIM} to return){Style.RESET_ALL}\n"))
        sys.stdout.flush()

    try:
        while True:
            try:
                # Poll stdin with a short timeout so we can refresh the
                # viewer if active, and so SIGINT still gets handled.
                r, _, _ = _select.select([sys.stdin], [], [], REFRESH_S if in_viewer[0] else 0.5)
            except (OSError, ValueError):
                break
            if r:
                try:
                    ch = sys.stdin.read(1)
                except Exception:
                    ch = ""
                if ch == "1" and not in_viewer[0]:
                    _enter_viewer()
                    _redraw_viewer()
                    # Announce the viewer entry on the MAIN screen too —
                    # since alt-screen hides everything, a one-line hint
                    # left on the main screen after ESC reminds the user
                    # what just happened (without polluting logs).
                    # We do this by writing to stderr AFTER leaving alt
                    # screen — see _leave_viewer exit path.
                    continue
                if in_viewer[0]:
                    if ch == "\x1b" or ch == "q":
                        _leave_viewer()
                        _rainbow_print(f"{Fore.CYAN}[MULTIPORT viewer closed — "
                              f"logging was preserved]{Style.RESET_ALL}")
                        continue
                    # Any other key inside the viewer → ignore (the user is
                    # just looking, not typing). Refresh will happen on the
                    # next select timeout.
            # Time-driven refresh while inside the viewer.
            if in_viewer[0]:
                try:
                    _redraw_viewer()
                except Exception as _e:
                    # Rendering must NEVER kill the viewer thread — log to
                    # stderr and bail out of the viewer cleanly.
                    try:
                        sys.stderr.write(f"[MULTIPORT viewer render error: {_e}]\n")
                    except Exception:
                        pass
                    _leave_viewer()
    except Exception:
        # Catch-all: never propagate viewer errors into the main app.
        pass
    finally:
        # Always leave alt-screen and restore termios on thread exit.
        try:
            _leave_viewer()
        except Exception:
            pass
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, original_terms)
        except Exception:
            pass

# Module-level handle so the SIGINT handler can restore termios on forced
# exit (Ctrl+C while the viewer is open leaves the terminal in cbreak mode
# if we don't explicitly restore it on shutdown).
_MULTIPORT_VIEWER_STARTED = False

def _start_multiport_viewer() -> None:
    """Start the MULTIPORT CDN viewer thread (idempotent, daemon)."""
    global _MULTIPORT_VIEWER_STARTED
    if _MULTIPORT_VIEWER_STARTED:
        return
    _MULTIPORT_VIEWER_STARTED = True
    threading.Thread(target=_multiport_viewer_loop, daemon=True,
                     name="multiport-viewer").start()

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
    if FIREFOX_PROXY and not _CRYPTOGRAPHY_OK:
        log("FIREFOX_PROXY=True needs the 'cryptography' package "
            "(pip install cryptography --break-system-packages) — disabling", "ERROR")
        FIREFOX_PROXY = False
    if FIREFOX_PROXY and not HOOK_GUI:
        log("FIREFOX_PROXY works without HOOK_GUI too (passthru/passthrough "
            "still runs), but the Firefox Proxy request log/hook tab needs "
            "HOOK_GUI=True to be visible.", "WARN")
    def _sigint_handler(_sig, _frame):
        # Clear the ^C that the terminal echoes on Ctrl+C.
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
        _rainbow_print(f"{Fore.YELLOW}{Style.BRIGHT}Shutting down...{Style.RESET_ALL}")
        if GRACEFUL_SHUTDOWN:
            # Drain in-flight requests so no cached data is lost. Daemon
            # threads (Flask, CDN servers) will be killed by os._exit after.
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
            log("Drain complete.", "INFO")
        _rainbow_print(f"{Fore.YELLOW}{Style.BRIGHT}Script finished by user command{Style.RESET_ALL}")
        # os._exit avoids the "[1]+ Killed" shell message that sys.exit
        # produces from a signal handler. Kills daemon threads immediately
        # without running atexit handlers (which is what we want).
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
                # Scan ALL files, not just .html — bot pages can be cached
                # under any extension when the URL had no extension (collapsed
                # to /index.html) but the real CT was JSON/text/etc.
                if fn.endswith(_CTYPE_SIDECAR_EXT):
                    continue   # sidecars are removed alongside their body
                fp = os.path.join(root, fn)
                try:
                    with open(fp, "rb") as f:
                        data = f.read(8192)
                    if _is_bot_page(data):
                        os.remove(fp)
                        # Also remove the orphaned ctype sidecar so a stale
                        # Content-Type doesn't persist for a deleted file.
                        sidecar = _ctype_sidecar_path(fp)
                        if os.path.isfile(sidecar):
                            try: os.remove(sidecar)
                            except OSError: pass
                        purged += 1
                        log(f"Purged bot-page cache: {os.path.relpath(fp)}", "WARN")
                except Exception:
                    pass
        if purged:
            log(f"Startup bot-cache purge: removed {purged} poisoned file(s)", "INFO")

    threading.Thread(target=_purge_bot_cache, daemon=True, name="bot-cache-purge").start()

    if SCAN_PATHS and not OFFLINE:
        # Run the interactive status-filter prompt in the MAIN thread BEFORE
        # starting the scanner daemon — input() can't safely run inside the
        # daemon because the MULTIPORT viewer thread (started below) puts
        # stdin in cbreak mode, which would make input() read single chars
        # instead of lines. Doing it here, synchronously, also guarantees
        # the filter is set before any scan happens.
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

    if FIREFOX_PROXY:
        _start_firefox_proxy()

    # OFFLINE: pre-register CDN hosts from disk so MULTIPORT servers start
    # and URL rewrites point at live destinations instead of dead ports.
    if OFFLINE and PROXY_CDN and MULTIPORT:
        _n_pre = _preregister_cdn_hosts_from_disk()
        if _n_pre:
            log(f"OFFLINE: pre-registered {_n_pre} CDN host(s) from disk cache", "INFO")
        try:
            _cached_count = sum(len(files) for _, _, files in os.walk(SRC_FOLDER))
        except Exception:
            _cached_count = 0
        if _cached_count == 0:
            log(f"OFFLINE is ON but {SRC_FOLDER} has 0 cached files — every request will 404.", "ERROR")
        else:
            log(f"OFFLINE serving {_cached_count} cached file(s) from {SRC_FOLDER}", "INFO")

    if CRAWL and not OFFLINE:
        def _bg_crawl() -> None:
            enqueue(SITE_URL)
            crawl_parallel()
            s = stats.snapshot()
            with _cdn_port_lock:
                cdn_map = dict(_cdn_host_port)
            log(f"Crawl done — {s['crawled']} fetched · {s['crawl_cache_hits']} cache-hits · "
                f"{s['saved']} cached · "
                f"{s['cdn_fetched']} cdn · {s['captured']} captured · "
                f"{s['revealed']} revealed · {s['conn_errors']} unreachable · "
                f"{s['http_errors']} HTTP errors")
            for host, port in cdn_map.items():
                log(f"  CDN {host} → http://localhost:{port}", "CDN")
        threading.Thread(target=_bg_crawl, daemon=True, name="crawl-main").start()
        log("Crawler started in background.")
    elif CRAWL and OFFLINE:
        log("CRAWL ignored in OFFLINE mode — no upstream available", "WARN")

    # Discovery workers: only start in bulk mode (CRAWL=True). In procedural
    # mode (CRAWL=False), there is NO background crawling — the proxy serves
    # pages on-demand from upstream, caching them as they're requested. This
    # is the original "proxy-on-demand only" behavior.
    if CRAWL and not OFFLINE and not _discovery_started.is_set():
        _start_discovery_workers()
        log(f"Discovery workers started ({_DISCOVERY_WORKERS} threads).", "INFO")

    # External asset prefetch pool — always start (used by proxy in any mode
    # except OFFLINE). Bounded pool prevents thread exhaustion.
    if not OFFLINE:
        _start_ext_asset_workers()

    # MULTIPORT CDN viewer — press '1' in the terminal to open a live table
    # of every CDN host that has been promoted to its own port. ESC returns
    # to the main screen with logs untouched (alt-screen buffer preserves
    # the scrollback). No-op when MULTIPORT is off or stdin isn't a TTY.
    if MULTIPORT:
        _start_multiport_viewer()
        if sys.stdin.isatty():
            log("MULTIPORT viewer ready — press '1' in the terminal to open the CDN table",
                "INFO")

    log(f"Listening on http://{HOST}:{PORT}")

    if HOOK_GUI:
        # On Linux/X11 Tkinter MUST run on the main thread.
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
