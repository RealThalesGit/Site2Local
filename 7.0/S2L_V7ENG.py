#!/usr/bin/env python3
# thank u chatgpt!
import os
import sys
import threading
import hashlib
import mimetypes
import socket
import queue
import warnings
import re
import logging
import click
import time
import csv
from urllib.parse import urljoin, urlparse, urldefrag
import cloudscraper
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from flask import Flask, Response, send_file
import colorama
from colorama import Fore, Style
# =========================================================
# GLOBAL SETUP
# =========================================================

sys.setrecursionlimit(20000)
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# =========================================================
# LOGGING
# =========================================================

class Colors:
    RESET = Style.RESET_ALL
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    CYAN = Fore.CYAN
    MAGENTA = Fore.MAGENTA

# lolcat rainbow
_LOLCAT_COLORS = (
    Fore.RED,
    Fore.YELLOW,
    Fore.GREEN,
    Fore.CYAN,
    Fore.BLUE,
    Fore.MAGENTA,
)

def lolcat_text(text: str) -> str:
    if not ENABLE_RAINBOW_LOGS:
        return text
    out = []
    i = 0
    for ch in text:
        if ch.isspace():
            out.append(ch)
        else:
            out.append(_LOLCAT_COLORS[i % len(_LOLCAT_COLORS)] + ch)
            i += 1
    return "".join(out) + Style.RESET_ALL

def log(msg, level="INFO"):
    color = {
        "INFO": Colors.GREEN,
        "WARN": Colors.YELLOW,
        "ERROR": Colors.RED,
        "DEBUG": Colors.CYAN,
        "MIRROR": Colors.MAGENTA,
    }.get(level, Colors.GREEN)

    full = f"[Site2Local] [{level}] {msg}"

    if ENABLE_RAINBOW_LOGS:
        print(lolcat_text(full))
    else:
        print(f"{color}{full}{Colors.RESET}")

# =========================================================
# CONFIG / FLAGS
# =========================================================

RAW_SITE_URL = "example.com" # self explanatory
PORT = 8080
HOST = "0.0.0.0"
HEADER_DEVICE = "macintosh"  # mobile, tablet, desktop, macintosh, ie11, iphone, ipad, symbian, bot
MIMETYPE_FILE = "mimetypes.csv" # loads all the static mimetypes! csv from iana
ENABLE_CRAWLING = True # if you dont want to download it all again, just disable it
OFFLINE_MODE = False # yes
SAVE_ERROR_PAGES = False # deeeeeeebug
DUMP_FRENESIS = False # get them
DISABLE_INTERESTING_EXT_READ = False # you want to use only dynamic ext reading if you want to disable this
ENABLE_RAINBOW_LOGS = False # fun mode
REQUEST_TIMEOUT = 6 # strategic pause =)
MAX_WORKERS = 60 # take care
MAX_FILENAME = 180
SAVE_BATCH = 32
SAVE_FLUSH_TIME = 0.15
MAX_URL_DEPTH = 7
MAX_REGEX_SCAN = 512 * 1024
INTERESTING_EXT_FILE = MIMETYPE_FILE

# =========================================================
# FILTERS
# =========================================================

CF_BLOCK_PATHS = (
    "/cdn-cgi/",
    "__cf_chl_",
    "challenge-platform",
    "orchestrate/chl",
) # block em all!

CDN_BLACKLIST = () # blacklist an cdn or site

FRENESIS_TARGETS = {} # hey all scott here and this is good, real good

COMMON_PARAMS = ["id", "page", "q", "search", "ref"]

# =========================================================
# UA PROFILES
# =========================================================

UA_PROFILES = {
    "mobile": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36",
    "tablet": "Mozilla/5.0 (Linux; Android 13; SM-T837A) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "macintosh": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "ie11": "Mozilla/5.0 (Windows NT 10.0; Trident/7.0; rv:11.0) like Gecko",
    "iphone": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "ipad": "Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "symbian": "Mozilla/5.0 (Symbian/3; Series60/5.2 NokiaN8-00/012.002; Profile/MIDP-2.1 Configuration/CLDC-1.1 ) AppleWebKit/533.4 (KHTML, like Gecko) NokiaBrowser/7.3.0 Mobile Safari/533.4 3gpp-gba",
    "bot": "Googlebot/2.1 (+http://www.google.com/bot.html)",
} # thank you via web browser for these

def sanitize_ua(ua: str) -> str:
    ua = re.sub(r"[\t\r\n]+", " ", ua).strip()
    ua = re.sub(r" Version/[^ ]+", "", ua)
    ua = re.sub(r"Chrome/\d+\.\d+\.\d+\.\d+", "Chrome/116.0.0.0", ua)
    return ua

def get_headers(device: str) -> dict:
    if device not in UA_PROFILES:
        log(f"Invalid UA '{device}', fallback to macintosh", "WARN")
        device = "macintosh" # macintosh because i feel like it

    return {
        "User-Agent": sanitize_ua(UA_PROFILES[device]),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }

# =========================================================
# SCRAPER INIT
# =========================================================

scraper = cloudscraper.create_scraper()
scraper.headers.update(get_headers(HEADER_DEVICE))
scraper.keep_alive = True

# =========================================================
# MIME TYPE HANDLING
# =========================================================

_mime_cache = None
_mime_lock = threading.Lock()

def load_interesting_mimetypes():
    global _mime_cache

    if DISABLE_INTERESTING_EXT_READ:
        return set()

    with _mime_lock:
        if _mime_cache is not None:
            return _mime_cache

        mimes = set()
        try:
            with open(MIMETYPE_FILE, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    val = row[0].strip().lower()
                    if not val or val.startswith("#"):
                        continue
                    mimes.add(val)
            log(f"Loaded {len(mimes)} mimetypes from {MIMETYPE_FILE}", "INFO")
        except FileNotFoundError:
            log(f"MIME file not found: {MIMETYPE_FILE}", "WARN")
        except Exception as e:
            log(f"Failed loading MIME file: {e}", "ERROR")

        _mime_cache = mimes
        return mimes

def is_interesting_mimetype(content_type: str) -> bool:
    if DISABLE_INTERESTING_EXT_READ or not content_type:
        return False
    ct = content_type.split(";")[0].strip().lower()
    return ct in load_interesting_mimetypes()

# =========================================================
# UTILS
# =========================================================

def strip_fragment(u):
    return urldefrag(u)[0]

def normalize_url(u: str) -> str:
    u = strip_fragment(u).strip()
    if u.startswith("//"):
        u = "https:" + u
    return u

def resolve_ip(host):
    try:
        return socket.gethostbyname(host)
    except Exception:
        return "unresolved"

def url_depth(u: str) -> int:
    return urlparse(u).path.count("/")

def is_probably_html(data: bytes, ct: str) -> bool:
    if ct and "text/html" in ct.lower():
        return True
    head = data[:1024].lower()
    return b"<html" in head or b"<!doctype" in head

# =========================================================
# PATH HANDLING
# =========================================================

def safe_filename(name):
    if len(name) <= MAX_FILENAME:
        return name
    base, ext = os.path.splitext(name)
    h = hashlib.sha1(name.encode()).hexdigest()[:12]
    return f"{base[:32]}_{h}{ext}"

def local_path(u):
    p = urlparse(normalize_url(u))
    path = p.path or "/"
    if path.endswith("/") or not os.path.splitext(path)[1]:
        path = path.rstrip("/") + "/index.html"
    parts = [safe_filename(x) for x in path.split("/") if x]
    return os.path.join(SRC_FOLDER, p.netloc.replace("www.", ""), *parts)

# =========================================================
# SITE INIT
# =========================================================

def build_base_url(raw):
    for scheme in ("https://", "http://"):
        try:
            r = scraper.get(scheme + raw, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if r.status_code < 500:
                log(f"Crawling {raw} | IP {resolve_ip(urlparse(r.url).netloc)}", "INFO")
                return r.url
        except Exception:
            pass
    return None

SITE_URL = build_base_url(RAW_SITE_URL) or "http://" + RAW_SITE_URL
MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.replace("www.", "").replace(".", "_")

SRC_FOLDER = os.path.join("site_src", SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)

os.makedirs(SRC_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

# =========================================================
# DOMAIN POLICY
# =========================================================

def is_allowed_domain(netloc):
    if not netloc:
        return False
    if netloc == MAIN_HOST:
        return True
    if any(bad in netloc for bad in CDN_BLACKLIST):
        return False
    if not DUMP_FRENESIS:
        return False
    return not FRENESIS_TARGETS or netloc in FRENESIS_TARGETS

# =========================================================
# STATE
# =========================================================

visited = set()
saved_paths = set()
content_hashes = set()
site_fingerprints = {}
visited_lock = threading.Lock()
save_lock = threading.Lock()
content_lock = threading.Lock()
fingerprint_lock = threading.Lock()
url_queue = queue.Queue()
save_queue = queue.Queue()
URL_REGEX = re.compile(
    rb'((?:https?:)?\/\/[a-zA-Z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+|'
    rb'\/[a-zA-Z0-9_\-\/\.]{2,})'
)

# =========================================================
# SAVE WORKER
# =========================================================

def save_worker():
    batch = []
    last_flush = time.time()

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

        except queue.Empty:
            pass

        if batch and (len(batch) >= SAVE_BATCH or time.time() - last_flush >= SAVE_FLUSH_TIME):
            for p, data in batch:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                with open(p, "wb") as f:
                    f.write(data)
                log(f"Saved: {p}", "DEBUG")
            batch.clear()
            last_flush = time.time()

threading.Thread(target=save_worker, daemon=True).start()

# =========================================================
# CRAWLER
# =========================================================

def enqueue(u):
    url_queue.put(normalize_url(u))

def fingerprint_html(data):
    try:
        soup = BeautifulSoup(data, "lxml")
        tags = [t.name for t in soup.find_all(True)]
        return hashlib.sha1("".join(tags).encode()).hexdigest()
    except Exception:
        return None

def crawl(u):
    with visited_lock:
        if u in visited:
            return
        visited.add(u)

    if url_depth(u) > MAX_URL_DEPTH:
        return

    if any(x in u for x in CF_BLOCK_PATHS):
        return

    if not is_allowed_domain(urlparse(u).netloc):
        return

    try:
        r = scraper.get(u, timeout=REQUEST_TIMEOUT)
    except Exception:
        return

    if r.status_code >= 400:
        if SAVE_ERROR_PAGES:
            save_queue.put((u, r.content))
        return

    ct = r.headers.get("Content-Type", "")
    if is_interesting_mimetype(ct):
        log(f"Interesting file ({ct}): {u}", "MIRROR")

    data = r.content
    save_queue.put((u, data))

    if is_probably_html(data, ct):
        fp = fingerprint_html(data)
        host = urlparse(u).netloc

        if fp:
            with fingerprint_lock:
                if fp in site_fingerprints and site_fingerprints[fp] != host:
                    log(f"Mirror detected: {host} == {site_fingerprints[fp]}", "MIRROR")
                else:
                    site_fingerprints[fp] = host

        soup = BeautifulSoup(data.decode("utf-8", "ignore"), "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
            v = tag.get("href") or tag.get("src")
            if v:
                enqueue(urljoin(u, v))

    if DUMP_FRENESIS:
        for m in URL_REGEX.findall(data[:MAX_REGEX_SCAN]):
            try:
                found = m.decode("utf-8", "ignore")
                if not found.startswith(("data:", "javascript:")):
                    enqueue(urljoin(u, found))
            except Exception:
                pass

        if "?" not in u and url_depth(u) <= 4:
            base = u.rstrip("/")
            for p in COMMON_PARAMS:
                enqueue(f"{base}?{p}=1")

def worker():
    while True:
        try:
            u = url_queue.get(timeout=5)
        except queue.Empty:
            return
        crawl(u)
        url_queue.task_done()

def crawl_parallel():
    for _ in range(MAX_WORKERS):
        threading.Thread(target=worker, daemon=True).start()
    url_queue.join()
    save_queue.join()

# =========================================================
# PROXY
# =========================================================

app = Flask(__name__, static_folder=None)

def rewrite_html(data):
    try:
        soup = BeautifulSoup(data, "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "source"]):
            for attr in ("href", "src"):
                if tag.has_attr(attr):
                    tag[attr] = urljoin("/", tag[attr])
        return str(soup).encode()
    except Exception:
        return data

@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def proxy(path):
    path = path.lstrip("/")
    target = urljoin(SITE_URL + "/", path)
    local = local_path(target)

    if os.path.isfile(local):
        return send_file(local, mimetype=mimetypes.guess_type(local)[0])

    if OFFLINE_MODE:
        return Response("Offline mode", status=404)

    try:
        r = scraper.get(target, timeout=REQUEST_TIMEOUT)
        content = r.content
        if "text/html" in r.headers.get("Content-Type", ""):
            content = rewrite_html(content)

        save_queue.put((target, r.content))
        enqueue(target)

        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception:
        return Response("Fetch failed", status=502)

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    click.echo = lambda *args, **kwargs: None

    if ENABLE_CRAWLING:
        threading.Thread(
            target=lambda: (
                enqueue(SITE_URL),
                crawl_parallel(),
                log(f"The crawling has been finished, the server is running at {HOST}:{PORT}", "INFO")
            ),
            daemon=True
        ).start()
        log("The crawler has started in the background.", "INFO")

    log(f"Server running on {HOST}:{PORT}", "INFO")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
