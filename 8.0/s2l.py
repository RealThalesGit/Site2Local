#!/usr/bin/env python3
import os, sys, re, csv, json, time, socket, queue, subprocess, shutil
import hashlib, mimetypes, logging, threading, warnings
from dataclasses import dataclass, field
from typing import Callable
from urllib.parse import urljoin, urlparse, urldefrag
import requests
import requests.adapters
import urllib3
import urllib3.util.retry
import cloudscraper
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from flask import Flask, Response, send_file, request as flask_request
import colorama
from colorama import Fore, Style

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

sys.setrecursionlimit(20_000)
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# early default so log() works before CONFIG is parsed
ENABLE_RAINBOW_LOGS = False

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

_LOLCAT_PALETTE = (Fore.RED, Fore.YELLOW, Fore.GREEN, Fore.CYAN, Fore.BLUE, Fore.MAGENTA)

_LEVEL_COLOR: dict[str, str] = {
    "INFO":   Fore.GREEN,
    "WARN":   Fore.YELLOW,
    "ERROR":  Fore.RED    + Style.BRIGHT,
    "DEBUG":  Fore.CYAN   + Style.DIM,
    "MIRROR": Fore.BLUE,
    "HOOK":   Fore.YELLOW + Style.BRIGHT,
    "SCAN":   Fore.RED    + Style.BRIGHT,
    "REVEAL":  Fore.GREEN   + Style.BRIGHT,
    "CDN":     Fore.MAGENTA + Style.BRIGHT,
    "CAPTURE": Fore.BLUE    + Style.BRIGHT,
    "CRAWL":   Fore.CYAN,
}

_ANSI_ESC   = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_log_lock   = threading.Lock()
_LOLCAT_BIN = shutil.which("lolcat")

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

def _lolcat_pipe(text: str) -> str:
    try:
        proc = subprocess.run(
            [_LOLCAT_BIN, "-f", "-t"],   # -f force color, -t truecolor 24-bit
            input=text.encode(),
            capture_output=True,
            timeout=1,
        )
        return proc.stdout.decode("utf-8", "ignore").rstrip("\n")
    except Exception:
        return _lolcat_internal(text)

def log(msg: str, level: str = "INFO") -> None:
    ts    = time.strftime("%H:%M:%S")
    color = _LEVEL_COLOR.get(level, Fore.WHITE)
    tag   = f"{color}[{level}]{Style.RESET_ALL}"
    line  = f"{Style.DIM}{ts}{Style.RESET_ALL} {tag} {msg}"
    with _log_lock:
        if ENABLE_RAINBOW_LOGS:
            print(_lolcat_pipe(line) if _LOLCAT_BIN else _lolcat_internal(line))
        else:
            print(line)

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

RAW_SITE_URL    = "example.com"
HOST            = "0.0.0.0"
PORT            = 8080
HEADER_DEVICE   = "macintosh"   # auto · mobile · tablet · desktop · macintosh · ie11 · iphone · ipad · symbian · bot
MIMETYPE_FILE   = "mimetypes.csv"
REQUEST_TIMEOUT = 12
CONNECT_TIMEOUT = 6
MAX_WORKERS     = 60
MAX_FILENAME    = 180
SAVE_BATCH      = 32
SAVE_FLUSH_TIME = 0.15
MAX_URL_DEPTH   = 7
MAX_REGEX_SCAN  = 512 * 1024
CRAWL_RETRIES   = 2
CRAWL_BACKOFF   = 0.4
ENABLE_CRAWLING  = True
OFFLINE_MODE     = False
SAVE_ERROR_PAGES = False
DUMP_FRENESIS    = False   # regex-mine response bodies for URLs (crawls EVERYTHING)
DOWNLOAD_OTHERS  = True   # download + proxy external assets (CDN/third-party)
                           # rewrites HTML so browser fetches them via /__s2l_ext__/
                           # also used as fallback upstream when MAIN_HOST returns 404
DISABLE_MIMETYPES_READING = False
ENABLE_RAINBOW_LOGS = False  # uses system lolcat if installed, else internal rainbow
SHOW_HIDDEN       = False  # strip display:none/hidden/disabled from HTML
SCAN_HIDDEN_PATHS = False  # probe sensitive paths (admin, .git, .env…) at startup
JUST_GIVEME_EVERYTHING = False  # dump every proxied request+response as JSON to DATA_FOLDER/captures/
CAPTURE_REQUEST_BODY   = True   # include request body in capture (POST, PUT, PATCH…)
CAPTURE_SKIP_STATIC    = True   # skip images/fonts/JS/CSS from capture (reduces noise)
HIDDEN_PATHS_EXTRA: list = []

# ──────────────────────────────────────────────────────────────────────────────
# Filters
# ──────────────────────────────────────────────────────────────────────────────

CF_BLOCK_PATHS   = ("/cdn-cgi/", "__cf_chl_", "challenge-platform", "orchestrate/chl")
CDN_BLACKLIST    = ()
FRENESIS_TARGETS = {}

_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade",
    "host", "content-length", "accept-encoding",
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

_CONN_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.SSLError,
    urllib3.exceptions.NewConnectionError,
    urllib3.exceptions.MaxRetryError,
    urllib3.exceptions.ConnectTimeoutError,
    urllib3.exceptions.ReadTimeoutError,
    ConnectionRefusedError,
    OSError,
)

# CDN proxy route prefix — browser fetches CDN assets through here after HTML rewrite
_EXT_PREFIX = "/__s2l_ext__"

# ──────────────────────────────────────────────────────────────────────────────
# Platform detection
# ──────────────────────────────────────────────────────────────────────────────

def detect_platform(headers: dict) -> str:
    h      = {k.lower(): v.lower() for k, v in headers.items()}
    server = h.get("server", "")
    via    = h.get("via", "")
    cf_ray = headers.get("CF-RAY", "")

    if "cloudflare" in server or cf_ray:             return "Cloudflare"
    if "cloudfront" in server or "cloudfront" in via \
       or any("x-amz-cf" in k for k in h):           return "AWS CloudFront"
    if "akamai" in server or "akamaized" in server \
       or "akamai" in via:                            return "Akamai"
    if "fastly" in server or "fastly" in via:         return "Fastly"
    if "incapsula" in server or "imperva" in server:  return "Imperva/Incapsula"
    if "sucuri" in server:                            return "Sucuri WAF"
    if "cf-cache-status" in h:                        return "Cloudflare (Cache)"
    if "oracle" in server:                            return "Oracle Cloud"
    if "varnish" in server or "varnish" in via:       return "Varnish"
    if "edgecast" in server:                          return "EdgeCast CDN"
    if "nginx" in server:                             return "Nginx"
    if "apache" in server:                            return "Apache"
    return "Unknown"

# ──────────────────────────────────────────────────────────────────────────────
# Hook system
# ──────────────────────────────────────────────────────────────────────────────
#
#   @on_request(methods="POST", pattern=r"/api/login")
#   def add_flag(ctx: HookContext) -> None:
#       body = ctx.req_json()
#       body["remember_me"] = True
#       ctx.set_req_json(body)
#
#   @on_response(pattern=r"/api/user/me")
#   def patch_user(ctx: HookContext) -> None:
#       data = ctx.json()
#       data["premium"] = True
#       ctx.set_json(data)
#
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
# ═══════════════════════════════════════════════════════════════════════════════

# @on_request(methods="POST", pattern=r".*")
# def _dbg_post(ctx: HookContext) -> None:
#     log(f"POST body ({len(ctx.req_body)} B): {ctx.req_body[:200]}", "DEBUG")

# @on_response(pattern=r"/api/user/me")
# def _patch_user(ctx: HookContext) -> None:
#     try:
#         d = ctx.json(); d["premium"] = True; ctx.set_json(d)
#     except Exception: pass

# ═══════════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────────
# UA profiles + device detection
# ──────────────────────────────────────────────────────────────────────────────

UA_PROFILES: dict[str, str] = {
    "mobile":    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
    "tablet":    "Mozilla/5.0 (Linux; Android 13; SM-T837A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "desktop":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "macintosh": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "ie11":      "Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko",
    "iphone":    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "ipad":      "Mozilla/5.0 (iPad; CPU OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "symbian":   "Mozilla/5.0 (Symbian/3; Series60/5.2 NokiaN8-00/012.002; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/533.4 (KHTML, like Gecko) NokiaBrowser/7.3.0 Mobile Safari/533.4 3gpp-gba",
    "bot":       "Googlebot/2.1 (+http://www.google.com/bot.html)",
}

_MOBILE_DEVICES = frozenset({"mobile", "iphone", "ipad", "symbian", "tablet"})

def _sanitize_ua(ua: str) -> str:
    return re.sub(r"[\t\r\n]+", " ", ua).strip()

def _detect_device_from_ua(ua: str) -> str:
    ua = ua.lower()
    if "ipad"     in ua:                       return "ipad"
    if "iphone"   in ua or "ipod" in ua:       return "iphone"
    if "android"  in ua and "mobile" in ua:    return "mobile"
    if "android"  in ua:                       return "tablet"
    if "macintosh" in ua or "mac os x" in ua:  return "macintosh"
    if "windows"  in ua:                       return "desktop"
    if "bot"      in ua or "crawl" in ua:      return "bot"
    return "desktop"

def _effective_device() -> str:
    if HEADER_DEVICE != "auto":
        return HEADER_DEVICE
    try:
        return _detect_device_from_ua(flask_request.headers.get("User-Agent", ""))
    except RuntimeError:
        return "macintosh"

# ──────────────────────────────────────────────────────────────────────────────
# Session factory  (per-thread, Cloudflare-aware)
# ──────────────────────────────────────────────────────────────────────────────

_RETRY_POLICY = urllib3.util.retry.Retry(
    total            = CRAWL_RETRIES,
    backoff_factor   = CRAWL_BACKOFF,
    status_forcelist = {429, 500, 502, 503, 504},
    allowed_methods  = {"GET", "HEAD"},
    raise_on_status  = False,
)

_thread_local = threading.local()

def _make_session(device: str | None = None) -> cloudscraper.CloudScraper:
    d      = device or HEADER_DEVICE
    mobile = d in _MOBILE_DEVICES
    # Let cloudscraper build its own CF-compliant browser fingerprint.
    # Overriding the UA it crafts breaks Cloudflare JS challenge solving.
    s = cloudscraper.create_scraper(
        browser={
            "browser":  "chrome",
            "platform": "android" if mobile else "darwin",
            "desktop":  not mobile,
        },
        delay=3,
    )
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    s.keep_alive = False
    s.verify     = True
    adapter = requests.adapters.HTTPAdapter(
        max_retries      = _RETRY_POLICY,
        pool_connections = 4,
        pool_maxsize     = 8,
        pool_block       = False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return s

def _session() -> cloudscraper.CloudScraper:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = _make_session()
    return _thread_local.session

_proxy_session: cloudscraper.CloudScraper | None = None
_proxy_session_lock = threading.Lock()

def _get_proxy_session() -> cloudscraper.CloudScraper:
    global _proxy_session
    with _proxy_session_lock:
        if _proxy_session is None:
            _proxy_session = _make_session()
    return _proxy_session

def _short_exc(exc: Exception) -> str:
    msg = str(exc)
    for pat in (
        r"HTTPSConnectionPool\(host='([^']+)'",
        r"nodename nor servname provided",
        r"SSL: (.+)",
        r"Caused by (.+)",
    ):
        m = re.search(pat, msg, re.IGNORECASE)
        if m:
            return (m.group(1) if m.lastindex else m.group(0)).strip()
    first = msg.splitlines()[0]
    return first[:120] + ("…" if len(first) > 120 else "")

# ──────────────────────────────────────────────────────────────────────────────
# MIME helpers
# ──────────────────────────────────────────────────────────────────────────────

_mime_cache: set | None = None
_mime_lock  = threading.Lock()

def _load_mimes() -> set:
    global _mime_cache
    if DISABLE_MIMETYPES_READING:
        return set()
    with _mime_lock:
        if _mime_cache is not None:
            return _mime_cache
        mimes: set = set()
        try:
            with open(MIMETYPE_FILE, newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if row and not row[0].startswith("#"):
                        mimes.add(row[0].strip().lower())
            log(f"Loaded {len(mimes)} MIME types from {MIMETYPE_FILE}")
        except FileNotFoundError:
            log(f"{MIMETYPE_FILE} not found — extension detection only", "WARN")
        except Exception as e:
            log(f"MIME load failed: {e}", "WARN")
        _mime_cache = mimes
        return mimes

def guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def _ct_base(ct: str) -> str:
    return ct.split(";")[0].strip().lower()

def is_static_asset(ct: str) -> bool:
    return _ct_base(ct) in _STATIC_CTS

# ──────────────────────────────────────────────────────────────────────────────
# General utils
# ──────────────────────────────────────────────────────────────────────────────

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

def filter_fwd(headers: dict) -> dict:
    return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}

def filter_resp(headers: dict) -> dict:
    skip = _HOP_BY_HOP | {"content-encoding"}
    return {k: v for k, v in headers.items() if k.lower() not in skip}

def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}GB"

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

# ──────────────────────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────────────────────

def _safe_seg(name: str) -> str:
    if len(name) <= MAX_FILENAME:
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
    return os.path.join(SRC_FOLDER, p.netloc.replace("www.", ""), *parts)

# ──────────────────────────────────────────────────────────────────────────────
# Site init
# ──────────────────────────────────────────────────────────────────────────────

def build_base_url(raw: str) -> str | None:
    s = _make_session()
    for scheme in ("https://", "http://"):
        try:
            r = s.get(scheme + raw, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
            if r.status_code < 500:
                platform = detect_platform(dict(r.headers))
                log(f"Resolved {raw} → {r.url}  [{platform}]  IP: {resolve_ip(urlparse(r.url).netloc)}")
                return r.url
        except Exception as e:
            log(f"Probe {scheme+raw}: {_short_exc(e)}", "WARN")
    return None

SITE_URL  = build_base_url(RAW_SITE_URL) or f"http://{RAW_SITE_URL}"
MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.replace("www.", "").replace(".", "_")

SRC_FOLDER  = os.path.join("site_src",  SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)

os.makedirs(SRC_FOLDER,  exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Hidden path scanner  (SCAN_HIDDEN_PATHS)
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
    paths  = _DEFAULT_HIDDEN_PATHS + list(HIDDEN_PATHS_EXTRA)
    sess   = _make_session()
    found  = []
    origin = f"{urlparse(SITE_URL).scheme}://{MAIN_HOST}"

    log(f"Path scanner — probing {len(paths)} paths on {MAIN_HOST}", "SCAN")

    for p in paths:
        url = f"{origin}/{p.lstrip('/')}"
        try:
            r = sess.head(url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT), allow_redirects=False)
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
    if any(bad in netloc for bad in CDN_BLACKLIST):
        return False
    return DUMP_FRENESIS and (not FRENESIS_TARGETS or netloc in FRENESIS_TARGETS)

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})

def is_external_domain(netloc: str) -> bool:
    host = netloc.split(":")[0]  # strip port if present
    return (bool(netloc)
            and netloc != MAIN_HOST
            and host not in _LOCAL_HOSTS
            and not any(bad in netloc for bad in CDN_BLACKLIST))

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
# CDN host registry + per-CDN mini-servers  (DOWNLOAD_OTHERS)
#
# Every external CDN host we successfully download gets:
#   1. An entry in _cdn_host_port: { "cdn.foo.com": 8081, … }
#   2. A dedicated Flask mini-server on PORT+N that serves only its cached assets
#   3. HTML rewriting that points e.g. https://cdn.foo.com/foo.js
#      → http://localhost:8081/foo.js
#
# This way each CDN has its own port and the browser can fetch assets normally
# without any path tricks.  Falls back to /__s2l_ext__/ for hosts not yet
# assigned a port (discovered during live proxy before the port is up).
# ──────────────────────────────────────────────────────────────────────────────

_cdn_host_port: dict[str, int] = {}   # cdn_host → port number
_cdn_port_lock  = threading.Lock()
_cdn_next_port  = PORT + 1            # incremented each time a new CDN registers


def _cdn_base(cdn_host: str) -> str | None:
    """Return http://localhost:PORT for a registered CDN host, or None."""
    with _cdn_port_lock:
        p = _cdn_host_port.get(cdn_host)
    return f"http://localhost:{p}" if p else None


def _get_cdn_hosts() -> frozenset:
    with _cdn_port_lock:
        return frozenset(_cdn_host_port.keys())


def _start_cdn_server(cdn_host: str, port: int) -> None:
    """Spin up a tiny WSGI server on `port` that serves cached assets for cdn_host."""
    cdn_app = Flask(f"cdn_{cdn_host}", static_folder=None)

    @cdn_app.route("/", defaults={"p": ""})
    @cdn_app.route("/<path:p>")
    def _cdn_serve(p: str):
        cdn_url = f"https://{cdn_host}/{p.lstrip('/')}"
        qs      = flask_request.query_string.decode("utf-8", "ignore")
        if qs:
            cdn_url += f"?{qs}"
        lp = local_path(cdn_url)
        if os.path.isfile(lp):
            log(f"Cache  {cdn_host}/{p}  [port {port}]", "DEBUG")
            return send_file(lp, mimetype=guess_mime(lp))
        if OFFLINE_MODE:
            return Response("Offline", status=404)
        # Fetch on demand and cache
        try:
            r = _session().get(cdn_url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
            if r.status_code < 400:
                save_queue.put((cdn_url, r.content))
                stats.inc("cdn_fetched")
            ct = r.headers.get("Content-Type", "application/octet-stream")
            log(f"GET {r.status_code}  {cdn_host}/{p}  [{_fmt_size(len(r.content))}]  [port {port}]", "CDN")
            return Response(r.content, status=r.status_code,
                            headers=filter_resp(dict(r.headers)), content_type=ct)
        except Exception as exc:
            return Response(str(exc), status=502)

    def _run():
        import logging as _lg
        _lg.getLogger("werkzeug").setLevel(_lg.ERROR)
        cdn_app.run(host=HOST, port=port, threaded=True, debug=False, use_reloader=False)

    threading.Thread(target=_run, daemon=True, name=f"cdn-server-{cdn_host}").start()
    log(f"CDN server  {cdn_host}  → http://localhost:{port}", "CDN")


def _register_cdn_host(netloc: str) -> None:
    global _cdn_next_port
    with _cdn_port_lock:
        if netloc in _cdn_host_port:
            return
        port = _cdn_next_port
        _cdn_next_port += 1
        _cdn_host_port[netloc] = port
    _start_cdn_server(netloc, port)


def _rewrite_ext_urls(html_bytes: bytes, base_url: str) -> bytes:
    """Rewrite CDN asset URLs to point at their dedicated local port."""
    if not DOWNLOAD_OTHERS:
        return html_bytes
    known = _get_cdn_hosts()
    if not known:
        return html_bytes
    try:
        soup    = BeautifulSoup(html_bytes.decode("utf-8", "ignore"), "lxml")
        changed = False
        for tag in soup.find_all(["script", "img", "link", "source", "video", "audio", "iframe"]):
            for attr in ("src", "href"):
                val = tag.get(attr, "")
                if not val or val.startswith("data:") or val.startswith("javascript:"):
                    continue
                abs_url = normalize_url(urljoin(base_url, val))
                p       = urlparse(abs_url)
                if p.netloc not in known:
                    continue
                base = _cdn_base(p.netloc)
                if base:
                    # Dedicated port: http://localhost:8081/foo.js
                    rewritten = f"{base}{p.path}"
                else:
                    # Fallback: /__s2l_ext__/cdn.host/foo.js
                    rewritten = f"{_EXT_PREFIX}/{p.netloc}{p.path}"
                if p.query:
                    rewritten += f"?{p.query}"
                tag[attr] = rewritten
                changed = True
        return soup.encode("utf-8") if changed else html_bytes
    except Exception as e:
        log(f"cdn rewrite failed: {e}", "WARN")
        return html_bytes

# ──────────────────────────────────────────────────────────────────────────────
# Queues & shared state
# ──────────────────────────────────────────────────────────────────────────────

visited:        set = set()
saved_paths:    set = set()
content_hashes: set = set()

visited_lock = threading.Lock()
save_lock    = threading.Lock()
content_lock = threading.Lock()

url_queue  = queue.Queue()
save_queue = queue.Queue()

_dead_hosts: set = set()
_dead_hosts_lock = threading.Lock()

URL_REGEX = re.compile(
    rb"((?:https?:)?\/\/[a-zA-Z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+|\/[a-zA-Z0-9_\-\/\.]{2,})"
)

_ASSET_TAGS = frozenset({"script", "img", "link", "source", "video", "audio"})

# ──────────────────────────────────────────────────────────────────────────────
# Save worker
# ──────────────────────────────────────────────────────────────────────────────

def _save_worker() -> None:
    batch:      list  = []
    last_flush: float = time.time()

    while True:
        try:
            u, data = save_queue.get(timeout=SAVE_FLUSH_TIME)
            p = local_path(u)
            h = hashlib.sha1(data).hexdigest()

            with content_lock:
                if h in content_hashes:
                    save_queue.task_done()
                    continue
                content_hashes.add(h)

            with save_lock:
                if p in saved_paths:
                    save_queue.task_done()
                    continue
                saved_paths.add(p)

            batch.append((p, data))
            save_queue.task_done()
            stats.inc("saved")
        except queue.Empty:
            pass

        now = time.time()
        if batch and (len(batch) >= SAVE_BATCH or now - last_flush >= SAVE_FLUSH_TIME):
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
# Capture system  (JUST_GIVEME_EVERYTHING)
#
# Every proxied request+response is serialised as JSON under:
#   DATA_FOLDER/captures/<path segments>/<METHOD>_<ts>.json
# Body bytes are stored as UTF-8 when possible, base64 otherwise.
# CAPTURE_SKIP_STATIC = True  skips images, fonts, JS, CSS (reduces noise).
# CAPTURE_REQUEST_BODY = False omits the request body from the record.
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
    fname  = f"{method}{qs_tag}_{int(time.time() * 1000)}.json"
    return os.path.join(DATA_FOLDER, "captures", *segs, fname)

def _encode_body(data: bytes) -> tuple[str, str]:
    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        import base64
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
            "s2l":        "capture",
            "ts":         rec.ts,
            "request": {
                "method":    rec.method,
                "url":       rec.url,
                "query":     rec.query,
                "headers":   rec.req_headers,
                "body":      rb_enc if CAPTURE_REQUEST_BODY else None,
                "encoding":  rb_how if CAPTURE_REQUEST_BODY else "omitted",
            },
            "response": {
                "status":    rec.resp_status,
                "ct":        rec.resp_ct,
                "headers":   rec.resp_headers,
                "body":      pb_enc,
                "encoding":  pb_how,
                "size":      len(rec.resp_body),
            },
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
    if not JUST_GIVEME_EVERYTHING:
        return
    if CAPTURE_SKIP_STATIC and is_static_asset(ctx.resp_ct):
        return
    _capture_queue.put(_CaptureRecord(
        method       = ctx.method,
        url          = ctx.url,
        query        = ctx.query,
        req_headers  = ctx.req_headers,
        req_body     = ctx.req_body,
        resp_status  = ctx.resp_status,
        resp_headers = ctx.resp_headers,
        resp_body    = ctx.resp_body,
        resp_ct      = ctx.resp_ct,
    ))

# ──────────────────────────────────────────────────────────────────────────────
# Crawler
# ──────────────────────────────────────────────────────────────────────────────

def enqueue(u: str) -> None:
    url_queue.put(normalize_url(u))


def _fetch_external_asset(u: str) -> None:
    """DOWNLOAD_OTHERS: fetch a single external/CDN asset and cache it. No recursion."""
    with visited_lock:
        if u in visited:
            return
        visited.add(u)

    netloc = urlparse(u).netloc
    with _dead_hosts_lock:
        if netloc in _dead_hosts:
            return

    try:
        r = _session().get(u, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
        if r.status_code < 400:
            save_queue.put((u, r.content))
            _register_cdn_host(netloc)      # mark host as known → future HTML rewrites cover it
            stats.inc("cdn_fetched")
            log(f"{netloc}{urlparse(u).path}  [{_fmt_size(len(r.content))}]", "CDN")
    except _CONN_ERRORS as e:
        with _dead_hosts_lock:
            _dead_hosts.add(netloc)
        log(f"cdn unreachable {netloc} — {_short_exc(e)}", "WARN")
    except Exception as e:
        log(f"cdn error {u} — {_short_exc(e)}", "WARN")


def _crawl(u: str) -> None:
    with visited_lock:
        if u in visited:
            return
        visited.add(u)

    if url_depth(u) > MAX_URL_DEPTH:
        return
    if any(x in u for x in CF_BLOCK_PATHS):
        return

    netloc = urlparse(u).netloc
    if not is_allowed_domain(netloc):
        return

    with _dead_hosts_lock:
        if netloc in _dead_hosts:
            return

    sess = _session()

    try:
        r = sess.get(u, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
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
    log(f"GET {r.status_code}  {path_short}  [{_fmt_size(len(r.content))}]", "CRAWL")

    if r.status_code >= 400:
        stats.inc("http_errors")
        if SAVE_ERROR_PAGES:
            save_queue.put((u, r.content))
        return

    data = r.content
    ct   = r.headers.get("Content-Type", "")

    if SHOW_HIDDEN and is_html(data, ct):
        data = _reveal_hidden(data)
        stats.inc("revealed")

    # Always save the ORIGINAL (unrewritten) HTML so cached pages don't have
    # localhost URLs baked in — rewriting happens on the fly when serving.
    save_queue.put((u, data))
    log(f"Cached {path_short}", "DEBUG")

    # Extract links from the original HTML (before any rewriting)
    if is_html(data, ct):
        soup = BeautifulSoup(data.decode("utf-8", "ignore"), "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
            v = tag.get("href") or tag.get("src")
            if not v:
                continue
            abs_url = normalize_url(urljoin(u, v))
            parsed  = urlparse(abs_url)
            # Skip any localhost URLs (could be stale rewrites from a previous run)
            if parsed.hostname in _LOCAL_HOSTS:
                continue
            if is_allowed_domain(parsed.netloc):
                enqueue(abs_url)
            elif DOWNLOAD_OTHERS and is_external_domain(parsed.netloc) and tag.name in _ASSET_TAGS:
                threading.Thread(target=_fetch_external_asset, args=(abs_url,), daemon=True).start()

    if DUMP_FRENESIS:
        for m in URL_REGEX.findall(data[:MAX_REGEX_SCAN]):
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
            u = url_queue.get(timeout=5)
        except queue.Empty:
            return
        _crawl(u)
        url_queue.task_done()


def crawl_parallel() -> None:
    workers = [
        threading.Thread(target=_crawl_worker, daemon=True, name=f"crawl-{i}")
        for i in range(MAX_WORKERS)
    ]
    for w in workers:
        w.start()
    url_queue.join()
    save_queue.join()

# ──────────────────────────────────────────────────────────────────────────────
# Proxy  —  all HTTP methods · hooks · reveal · CDN ext route
# ──────────────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder=None)

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


def _do_upstream(method: str, target: str, ctx: HookContext, host_override: str | None = None):
    sess = _get_proxy_session()

    if HEADER_DEVICE == "auto":
        device = _effective_device()
        sess.headers.update({"User-Agent": _sanitize_ua(
            UA_PROFILES.get(device, UA_PROFILES["macintosh"]))})

    fwd         = dict(ctx.req_headers)
    fwd["Host"] = host_override or MAIN_HOST

    kwargs: dict = {
        "headers":         fwd,
        "cookies":         flask_request.cookies,
        "timeout":         (CONNECT_TIMEOUT, REQUEST_TIMEOUT),
        "allow_redirects": True,
        "verify":          True,
    }
    if method in _BODY_METHODS and ctx.req_body:
        kwargs["data"] = ctx.req_body

    return sess.request(method, target, **kwargs)


def _make_flask_resp(ctx: HookContext, method: str) -> Response:
    body = b"" if method == "HEAD" else ctx.resp_body
    return Response(
        body,
        status       = ctx.resp_status,
        headers      = ctx.resp_headers,
        content_type = ctx.resp_ct,
    )


# ── /__s2l_ext__/<host>/<path>  —  CDN asset reverse-proxy ───────────────────
#
# The browser sends requests here after HTML rewriting (DOWNLOAD_OTHERS).
# Strategy: serve from local cache if available, else fetch from real host
# and cache for future requests.

@app.route(f"{_EXT_PREFIX}/<path:extpath>", methods=_ALL_METHODS)
def ext_asset(extpath: str) -> Response:
    # extpath = "cdn.foo.com/some/asset.js"  (query string in flask_request)
    slash = extpath.find("/")
    if slash == -1:
        return Response("Bad ext path — expected /<host>/<path>", status=400)

    ext_host = extpath[:slash]
    ext_path = extpath[slash:]
    qs       = flask_request.query_string.decode("utf-8", "ignore")
    real_url = f"https://{ext_host}{ext_path}"
    if qs:
        real_url += f"?{qs}"

    lp = local_path(real_url)
    if os.path.isfile(lp):
        log(f"Cache {ext_host}{ext_path}", "DEBUG")
        return send_file(lp, mimetype=guess_mime(lp))

    if OFFLINE_MODE:
        return Response("Offline — CDN asset not cached", status=404)

    try:
        r = _session().get(real_url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
        if r.status_code < 400:
            save_queue.put((real_url, r.content))
            _register_cdn_host(ext_host)
            stats.inc("cdn_fetched")
        ct = r.headers.get("Content-Type", "application/octet-stream")
        log(f"ext  {ext_host}{ext_path}  {r.status_code}  [{_fmt_size(len(r.content))}]", "CDN")
        return Response(r.content, status=r.status_code,
                        headers=filter_resp(dict(r.headers)), content_type=ct)
    except _CONN_ERRORS as exc:
        return Response(f"CDN unreachable: {_short_exc(exc)}", status=502)
    except Exception as exc:
        return Response(f"CDN error: {_short_exc(exc)}", status=502)


# ── Main proxy route ───────────────────────────────────────────────────────────

@app.route("/", defaults={"path": ""}, methods=_ALL_METHODS)
@app.route("/<path:path>",             methods=_ALL_METHODS)
def proxy(path: str) -> Response:
    method = flask_request.method.upper()

    # If the browser is requesting a path that looks like it belongs to a CDN
    # (Referer is external, or path starts with a known CDN host segment),
    # try to serve it from CDN cache / fetch it before hitting MAIN_HOST.
    # This handles JS/CSS that reference CDN assets by relative path.
    if DOWNLOAD_OTHERS and method in _SAFE_METHODS:
        req_path = "/" + path.lstrip("/")
        for cdn_host in _get_cdn_hosts():
            cdn_url = f"https://{cdn_host}{req_path}"
            cdn_lp  = local_path(cdn_url)
            if os.path.isfile(cdn_lp):
                log(f"Cache {cdn_host}{req_path}", "DEBUG")
                return send_file(cdn_lp, mimetype=guess_mime(cdn_lp))

    target = _upstream_url(path)
    stats.inc("proxied")

    if any(x in target for x in CF_BLOCK_PATHS):
        return Response("Blocked", status=403)

    if method == "OPTIONS":
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

    if OFFLINE_MODE:
        if method in _SAFE_METHODS:
            lp = local_path(target)
            if os.path.isfile(lp):
                if DOWNLOAD_OTHERS and lp.endswith(".html"):
                    data = open(lp, "rb").read()
                    data = _rewrite_ext_urls(data, target)
                    return Response(data, content_type="text/html; charset=utf-8")
                return send_file(lp, mimetype=guess_mime(lp))
        return Response("Offline — not cached", status=404)

    # Static assets: serve from cache immediately (skip fresh fetch)
    if method in _SAFE_METHODS:
        lp = local_path(target)
        if os.path.isfile(lp):
            if DOWNLOAD_OTHERS and lp.endswith(".html"):
                # Apply CDN rewrite on-the-fly, never saved into the cache
                data = open(lp, "rb").read()
                data = _rewrite_ext_urls(data, target)
                log(f"Cache {urlparse(target).path}", "DEBUG")
                return Response(data, content_type="text/html; charset=utf-8")
            if not lp.endswith(".html"):
                log(f"Cache {urlparse(target).path}", "DEBUG")
                return send_file(lp, mimetype=guess_mime(lp))

    raw_body = flask_request.get_data() if method in _BODY_METHODS else b""
    ctx = _build_ctx(method, target, raw_body)

    if _REQ_HOOKS:
        stats.inc("hooks_run", _run_hooks(_REQ_HOOKS, ctx))

    try:
        upstream_r = _do_upstream(method, target, ctx)
    except _CONN_ERRORS as exc:
        short = _short_exc(exc)
        log(f"{method} {urlparse(target).path} — {short}", "WARN")
        stats.inc("conn_errors")
        return Response(f"Connection error: {short}", status=502)
    except Exception as exc:
        log(f"{method} {urlparse(target).path} — {_short_exc(exc)}", "ERROR")
        stats.inc("conn_errors")
        return Response(f"Upstream error: {_short_exc(exc)}", status=502)

    # If main host returned 404 and DOWNLOAD_OTHERS is on, try CDN hosts with same path
    if upstream_r.status_code == 404 and DOWNLOAD_OTHERS and method in _SAFE_METHODS:
        req_path = urlparse(target).path
        req_qs   = flask_request.query_string.decode("utf-8", "ignore")
        with _cdn_port_lock:
            host_port_snapshot = dict(_cdn_host_port)
        for cdn_host, _port in host_port_snapshot.items():
            cdn_url = f"https://{cdn_host}{req_path}"
            if req_qs:
                cdn_url += f"?{req_qs}"
            cdn_lp = local_path(cdn_url)
            if os.path.isfile(cdn_lp):
                log(f"CDN hit {cdn_host}{req_path}", "CDN")
                return send_file(cdn_lp, mimetype=guess_mime(cdn_lp))
            try:
                cdn_r = _session().get(cdn_url, timeout=(CONNECT_TIMEOUT, REQUEST_TIMEOUT))
                if cdn_r.status_code < 400:
                    save_queue.put((cdn_url, cdn_r.content))
                    stats.inc("cdn_fetched")
                    log(f"CDN fallback {cdn_host}{req_path}  {cdn_r.status_code}", "CDN")
                    ct = cdn_r.headers.get("Content-Type", "application/octet-stream")
                    return Response(cdn_r.content, status=cdn_r.status_code,
                                    headers=filter_resp(dict(cdn_r.headers)), content_type=ct)
            except Exception:
                pass

    ctx.resp_status  = upstream_r.status_code
    ctx.resp_headers = filter_resp(dict(upstream_r.headers))
    ctx.resp_body    = upstream_r.content
    ctx.resp_ct      = upstream_r.headers.get("Content-Type", "application/octet-stream")
    platform         = detect_platform(dict(upstream_r.headers))

    log(f"{method} {urlparse(target).path}  {upstream_r.status_code}  [{platform}]  {_fmt_size(len(ctx.resp_body))}", "MIRROR")

    if upstream_r.status_code >= 400:
        stats.inc("http_errors")

    if SHOW_HIDDEN and is_html(ctx.resp_body, ctx.resp_ct):
        ctx.resp_body = _reveal_hidden(ctx.resp_body)
        ctx.resp_headers.pop("content-length", None)
        stats.inc("revealed")

    # Save the original (unrewritten) body so cached HTML never has localhost URLs
    if method == "GET" and ctx.resp_status < 400:
        save_queue.put((target, ctx.resp_body))
        enqueue(target)

    # Rewrite CDN URLs on-the-fly for the browser response only (never saved)
    if DOWNLOAD_OTHERS and is_html(ctx.resp_body, ctx.resp_ct):
        ctx.resp_body = _rewrite_ext_urls(ctx.resp_body, target)
        ctx.resp_headers.pop("content-length", None)

    if _RESP_HOOKS:
        stats.inc("hooks_run", _run_hooks(_RESP_HOOKS, ctx))

    _maybe_capture(ctx)
    return _make_flask_resp(ctx, method)

# ──────────────────────────────────────────────────────────────────────────────
# Startup banner
# ──────────────────────────────────────────────────────────────────────────────

def _banner() -> None:
    W   = Style.BRIGHT + Fore.WHITE
    C   = Style.BRIGHT + Fore.CYAN
    G   = Style.BRIGHT + Fore.GREEN
    Y   = Style.BRIGHT + Fore.YELLOW
    M   = Style.BRIGHT + Fore.MAGENTA
    DIM = Style.DIM
    R   = Style.RESET_ALL

    ip      = resolve_ip(MAIN_HOST)
    n_hooks = len(_REQ_HOOKS) + len(_RESP_HOOKS)

    flags = []
    if ENABLE_CRAWLING:        flags.append(f"{G}crawl{R}")
    if OFFLINE_MODE:           flags.append(f"{Y}offline{R}")
    if DUMP_FRENESIS:          flags.append(f"{M}frenesis{R}")
    if DOWNLOAD_OTHERS:        flags.append(f"{M}download-others{R}")
    if SHOW_HIDDEN:            flags.append(f"{G}show-hidden{R}")
    if SCAN_HIDDEN_PATHS:      flags.append(f"{Style.BRIGHT+Fore.RED}scan-paths{R}")
    if JUST_GIVEME_EVERYTHING: flags.append(f"{Style.BRIGHT+Fore.RED}capture-all{R}")
    flag_str = "  ".join(flags) if flags else f"{DIM}none{R}"

    hook_str   = (f"{Y}{n_hooks} hook{'s' if n_hooks != 1 else ''}{R}"
                  if n_hooks else f"{DIM}none{R}")
    device_str = (f"{W}{HEADER_DEVICE}{R}  {DIM}(auto-mirrors browser UA){R}"
                  if HEADER_DEVICE == "auto" else f"{W}{HEADER_DEVICE}{R}")

    print(f"""
{C}  ╔══════════════════════════════════════════════════════╗{R}
{C}  ║{W}           S I T E  2  L O C A L                    {C}║{R}
{C}  ╠══════════════════════════════════════════════════════╣{R}
{C}  ║{R}  {DIM}Target:  {R}  {G}{MAIN_HOST}{R}  {DIM}({ip}){R}
{C}  ║{R}  {DIM}Proxy:   {R}  {W}http://{HOST}:{PORT}{R}
{C}  ║{R}  {DIM}Device:  {R}  {device_str}
{C}  ║{R}  {DIM}Workers: {R}  {W}{MAX_WORKERS}{R}
{C}  ║{R}  {DIM}Timeout: {R}  {W}connect={CONNECT_TIMEOUT}s  read={REQUEST_TIMEOUT}s{R}
{C}  ║{R}  {DIM}Retries: {R}  {W}{CRAWL_RETRIES}× backoff={CRAWL_BACKOFF}s{R}
{C}  ║{R}  {DIM}Hooks:   {R}  {hook_str}
{C}  ║{R}  {DIM}Flags:   {R}  {flag_str}
{C}  ╚══════════════════════════════════════════════════════╝{R}
""")

    if DOWNLOAD_OTHERS:
        print(f"  {M}▶ DOWNLOAD_OTHERS{R}  {DIM}each CDN gets a dedicated port starting at {PORT+1}{R}\n")
    if JUST_GIVEME_EVERYTHING:
        skip = "static skipped" if CAPTURE_SKIP_STATIC else "static included"
        body = "req body on" if CAPTURE_REQUEST_BODY else "req body off"
        print(f"  {Style.BRIGHT+Fore.RED}▶ JUST_GIVEME_EVERYTHING{R}  {DIM}({skip} · {body}){R}")
        print(f"  {DIM}→  {DATA_FOLDER}/captures/{R}\n")
    if SHOW_HIDDEN:
        print(f"  {G}▶ SHOW_HIDDEN{R}  {DIM}hidden elements revealed in every HTML page{R}\n")
    if SCAN_HIDDEN_PATHS:
        total = len(_DEFAULT_HIDDEN_PATHS) + len(HIDDEN_PATHS_EXTRA)
        print(f"  {Style.BRIGHT+Fore.RED}▶ SCAN_HIDDEN_PATHS{R}"
              f"  {DIM}probing {total} paths → {DATA_FOLDER}/hidden_paths.json{R}\n")
    if n_hooks:
        print(f"  {Y}▶ HOOKS{R}")
        for pat, mset, fn in _REQ_HOOKS:
            print(f"  {DIM}  req  [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")
        for pat, mset, fn in _RESP_HOOKS:
            print(f"  {DIM}  resp [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")
        print()

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)

    # Silence Flask startup banner ("* Serving Flask app…  * Debug mode: off")
    import click
    click.echo = lambda *a, **kw: None

    if JUST_GIVEME_EVERYTHING:
        os.makedirs(os.path.join(DATA_FOLDER, "captures"), exist_ok=True)
        threading.Thread(target=_capture_worker, daemon=True, name="capture-worker").start()

    _banner()
    _load_mimes()

    if SCAN_HIDDEN_PATHS:
        threading.Thread(target=_run_path_scanner, daemon=True, name="path-scanner").start()

    if ENABLE_CRAWLING:
        def _bg_crawl() -> None:
            enqueue(SITE_URL)
            crawl_parallel()
            s = stats.snapshot()
            with _cdn_port_lock:
                cdn_map = dict(_cdn_host_port)
            log(
                f"Crawl done — "
                f"{s['crawled']} fetched · "
                f"{s['saved']} cached · "
                f"{s['cdn_fetched']} cdn · "
                f"{s['captured']} captured · "
                f"{s['revealed']} revealed · "
                f"{s['conn_errors']} unreachable · "
                f"{s['http_errors']} HTTP errors"
            )
            if cdn_map:
                for host, port in cdn_map.items():
                    log(f"  CDN  {host}  → http://localhost:{port}", "CDN")
        threading.Thread(target=_bg_crawl, daemon=True, name="crawl-main").start()
        log("Crawler started in background.")

    log(f"Listening on http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
