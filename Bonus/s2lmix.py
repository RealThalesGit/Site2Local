#!/usr/bin/env python3

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
from flask import Flask, Response, send_file, request
import colorama
from colorama import Fore, Style
import concurrent.futures

sys.setrecursionlimit(20000)
colorama.init(autoreset=True)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

class Colors:
    RESET = Style.RESET_ALL
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    CYAN = Fore.CYAN
    MAGENTA = Fore.MAGENTA

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
    out, i = [], 0
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
    print(lolcat_text(full) if ENABLE_RAINBOW_LOGS else f"{color}{full}{Colors.RESET}")

RAW_SITE_URL = "example.com"
HOST = "0.0.0.0"
PORT = 8080
HEADER_DEVICE = "desktop"
MIMETYPE_FILE = "mimetypes.csv"
ENABLE_CRAWLING = True
OFFLINE_MODE = False
SAVE_TRAFFIC = False
SAVE_ERROR_PAGES = False
DUMP_FRENESIS = False
DISABLE_MIMETYPES_READING = False
ENABLE_RAINBOW_LOGS = False
SHOW_HIDDEN = False
SCAN_HIDDEN_PATHS = False
ACCEPT_ALL_MIRRORS = True
REQUEST_TIMEOUT = 6
MAX_WORKERS = 60
MAX_FILENAME = 180
SAVE_BATCH = 32
SAVE_FLUSH_TIME = 0.15
MAX_URL_DEPTH = 7
MAX_REGEX_SCAN = 512 * 1024

CF_BLOCK_PATHS = (
    "/cdn-cgi/",
    "_cf_chl",
    "challenge-platform",
    "orchestrate/chl",
)

CDN_BLACKLIST = ()
FRENESIS_TARGETS = {}
COMMON_PARAMS = ["id", "page", "q", "search", "ref"]

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
}

def sanitize_ua(ua: str) -> str:
    ua = re.sub(r"[\t\r\n]+", " ", ua)
    ua = re.sub(r" Version/[^ ]+", "", ua)
    ua = re.sub(r"Chrome/\d+.\d+.\d+.\d+", "Chrome/116.0.0.0", ua)
    return ua.strip()

def get_headers(device: str) -> dict:
    if device not in UA_PROFILES:
        log(f"Invalid UA '{device}', fallback to desktop", "WARN")
        device = "desktop"
    
    return {
        "User-Agent": sanitize_ua(UA_PROFILES[device]),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
        "Sec-CH-UA-Platform": '"Windows"' if device == "desktop" else '"Android"',
        "Sec-CH-UA-Mobile": "?0" if device == "desktop" else "?1"
    }

scraper = cloudscraper.create_scraper()
scraper.headers.update(get_headers(HEADER_DEVICE))
scraper.keep_alive = True

_mime_cache = None
_mime_lock = threading.Lock()

def load_interesting_mimetypes():
    global _mime_cache
    
    if DISABLE_MIMETYPES_READING:
        return set()
    
    with _mime_lock:
        if _mime_cache is not None:
            return _mime_cache
        
        mimes = set()
        try:
            with open(MIMETYPE_FILE, newline="", encoding="utf-8") as f:
                for row in csv.reader(f):
                    if row and not row[0].startswith("#"):
                        mimes.add(row[0].strip().lower())
            log(f"Loaded {len(mimes)} mimetypes!", "INFO")
        except Exception as e:
            log(f"MIME load failed: {e}", "ERROR")
        
        _mime_cache = mimes
        return mimes

def is_interesting_mimetype(content_type: str) -> bool:
    if DISABLE_MIMETYPES_READING or not content_type:
        return False
    ct = content_type.split(";")[0].strip().lower()
    return ct in load_interesting_mimetypes()

def strip_fragment(u):
    return urldefrag(u)[0]

def normalize_url(u: str) -> str:
    u = strip_fragment(u).strip()
    return "https:" + u if u.startswith("//") else u

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

def is_valid_url(url):
    p = urlparse(url)
    return bool(p.scheme) and bool(p.netloc)

def decode_response(resp):
    return resp.content, resp.headers.get("Content-Type", "")

def safe_filename(name):
    name = name.replace("\x00", "")
    if len(name) <= MAX_FILENAME:
        return name
    base, ext = os.path.splitext(name)
    h = hashlib.sha1(name.encode()).hexdigest()[:12]
    return f"{base[:32]}_{h}{ext}"

def local_path(u):
    u = strip_fragment(u)
    p = urlparse(normalize_url(u))
    path = p.path or "/"
    if path.endswith("/") or not os.path.splitext(path)[1]:
        path = path.rstrip("/") + "/index.html"
    parts = [safe_filename(x) for x in path.split("/") if x]
    return os.path.join(SRC_FOLDER, p.netloc.replace("www.", ""), *parts)

def is_already_downloaded(url):
    return os.path.isfile(local_path(url))

def build_base_url(raw):
    for scheme in ("https://", "http://"):
        try:
            test_url = scheme + raw
            r = scraper.get(test_url, timeout=REQUEST_TIMEOUT)
            if r.status_code < 500:
                log(f"Using {scheme.upper().strip('://')} for {raw} | IP {resolve_ip(urlparse(r.url).netloc)}", "INFO")
                return r.url
        except Exception:
            continue
    return None

SITE_URL = build_base_url(RAW_SITE_URL)
if SITE_URL is None:
    OFFLINE_MODE = True
    SITE_URL = "http://" + RAW_SITE_URL
    log(f"Site {RAW_SITE_URL} unreachable, enabling OFFLINE MODE", "WARN")

MAIN_HOST = urlparse(SITE_URL).netloc
SITE_NAME = MAIN_HOST.replace("www.", "").replace(".", "_")

SRC_FOLDER = os.path.join("site_src", SITE_NAME)
DATA_FOLDER = os.path.join("site_data", SITE_NAME)

os.makedirs(SRC_FOLDER, exist_ok=True)
os.makedirs(DATA_FOLDER, exist_ok=True)

def is_allowed_domain(netloc):
    if not netloc:
        return False
    if netloc == MAIN_HOST:
        return True
    if any(bad in netloc for bad in CDN_BLACKLIST):
        return False
    return DUMP_FRENESIS and (not FRENESIS_TARGETS or netloc in FRENESIS_TARGETS)

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
    rb'((?:https?:)?//[a-zA-Z0-9\-.~:/?#@!$&\'()*+,;=%]+|/[a-zA-Z0-9_/.:-]{2,}(?:\?[a-zA-Z0-9_\-=&%]+)?)'
)
API_REGEX = re.compile(
    rb'/[a-zA-Z0-9_-]{3,}/[a-zA-Z0-9_-]{2,}(?:/[a-zA-Z0-9_.\-]+)?'
)

def save_worker():
    batch, last_flush = [], time.time()
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
                if os.path.isdir(p):
                    p = os.path.join(p, "index.html")
                with open(p, "wb") as f:
                    f.write(data)
                log(f"Saved: {p}", "DEBUG")
            batch.clear()
            last_flush = time.time()

threading.Thread(target=save_worker, daemon=True).start()

def ask_user_about_mirror(filename, mirror_url):
    global ACCEPT_ALL_MIRRORS
    if ACCEPT_ALL_MIRRORS:
        return True
    print(f"\nA mirror was found. Do you want to download {filename} from {mirror_url}?")
    print("[Y] Yes   [N] No   [A] Accept all from now on")
    while True:
        choice = input("Your choice (Y/N/A): ").strip().lower()
        if choice == "y":
            return True
        elif choice == "n":
            return False
        elif choice == "a":
            ACCEPT_ALL_MIRRORS = True
            return True
        else:
            print("Invalid option, please try again.")

def download_with_mirrors(url, mirrors):
    filename = url.split("/")[-1]
    for mirror in mirrors:
        try:
            if ask_user_about_mirror(filename, mirror):
                log(f"Downloading {filename} from mirror {mirror}", "MIRROR")
                r = scraper.get(mirror, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                return r.content
        except Exception as e:
            log(f"Failed to download from mirror {mirror}: {e}", "WARN")
    log(f"Failed to download {filename} from all mirrors", "ERROR")
    return None

def enqueue(u):
    u = normalize_url(u)
    with visited_lock:
        if u in visited:
            return
    url_queue.put(u)

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
        pass
    
    data = r.content
    save_queue.put((u, data))
    
    if is_probably_html(data, ct):
        soup = BeautifulSoup(data.decode("utf-8", "ignore"), "lxml")
        
        if SHOW_HIDDEN:
            selectors = [
                "[style*='display:none']",
                "[style*='visibility:hidden']",
                "[style*='opacity:0']",
                "[hidden]",
                "[disabled]",
                "[readonly]"
            ]
            for sel in selectors:
                for el in soup.select(sel):
                    if 'style' in el.attrs:
                        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
                    for att in ['hidden', 'disabled', 'readonly']:
                        el.attrs.pop(att, None)
        
        resource_tags = {
            "script": "src",
            "img": "src",
            "link": "href",
            "source": "src",
            "video": "src",
            "audio": "src",
            "iframe": "src",
        }
        
        for tag, attr in resource_tags.items():
            for el in soup.find_all(tag):
                link = el.get(attr)
                if not link:
                    continue
                full_url = urljoin(u, link)
                if is_valid_url(full_url):
                    enqueue(full_url)
        
        for a in soup.find_all("a", href=True):
            link = urljoin(u, a["href"])
            if link.startswith(SITE_URL):
                enqueue(link)
    
    if DUMP_FRENESIS:
        if len(data) > MAX_REGEX_SCAN * 2:
            scan = data[:MAX_REGEX_SCAN] + data[-MAX_REGEX_SCAN:]
        else:
            scan = data
        
        for m in URL_REGEX.findall(scan):
            try:
                found = m.decode("utf-8", "ignore")
                if not found.startswith(("data:", "javascript:")):
                    enqueue(urljoin(u, found))
            except Exception:
                pass
        
        for m in API_REGEX.findall(scan):
            try:
                found = m.decode("utf-8", "ignore")
                enqueue(urljoin(u, found))
            except Exception:
                pass
    
    if SCAN_HIDDEN_PATHS:
        hidden_paths = ["admin", "login", "panel", ".git", ".env", "config", "backup", "db", "private", "secret"]
        for hp in hidden_paths:
            hidden_url = urljoin(SITE_URL + "/", hp)
            try:
                r_hidden = scraper.head(hidden_url, timeout=REQUEST_TIMEOUT)
                if r_hidden.status_code == 200 and hidden_url not in visited:
                    log(f"[HIDDEN PATH FOUND] {hidden_url}", "MAGENTA")
                    enqueue(hidden_url)
            except Exception:
                pass

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

def auto_mode_crawl():
    global SITE_URL
    global OFFLINE_MODE
    
    log(f"Starting AUTO_MODE crawl for {SITE_URL}", "INFO")
    try:
        https_url = "https://" + urlparse(SITE_URL).netloc
        r = scraper.head(https_url, timeout=REQUEST_TIMEOUT)
        if r.status_code < 400:
            log("HTTPS available, using HTTPS", "INFO")
            SITE_URL = https_url
        else:
            http_url = "http://" + urlparse(SITE_URL).netloc
            r2 = scraper.head(http_url, timeout=REQUEST_TIMEOUT)
            if r2.status_code < 400:
                log("HTTPS not available, using HTTP", "INFO")
                SITE_URL = http_url
            else:
                log("Site offline, enabling local OFFLINE mode", "WARN")
                OFFLINE_MODE = True
        
        if not OFFLINE_MODE:
            enqueue(SITE_URL)
            crawl_parallel()
        else:
            log("OFFLINE MODE enabled, serving from cache", "WARN")
    
    except Exception as e:
        log(f"[AUTO_MODE ERROR] {e}", "ERROR")
        OFFLINE_MODE = True

app = Flask(__name__, static_folder=None)

ALLOWED_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"]

@app.route("/", defaults={"path": ""}, methods=ALLOWED_METHODS)
@app.route("/<path:path>", methods=ALLOWED_METHODS)
def proxy(path):
    path = path.lstrip("/")
    target = urljoin(SITE_URL + "/", path)
    local = local_path(target)
    
    if os.path.isfile(local):
        mime = mimetypes.guess_type(local)[0] or "application/octet-stream"
        try:
            return send_file(local, mimetype=mime, conditional=True)
        except Exception as e:
            log(f"[ERROR] Sending file {local}: {e}", "ERROR")
            return Response(f"Error reading file {local}", status=500)
    
    if OFFLINE_MODE:
        return Response("Offline mode", status=404)
    
    try:
        headers = dict(request.headers)
        headers.pop("Host", None)
        
        r = scraper.request(
            method=request.method,
            url=target,
            headers=headers,
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=REQUEST_TIMEOUT
        )
        
        save_queue.put((target, r.content))
        enqueue(target)
        
        resp = Response(r.content, status=r.status_code)
        for h, v in r.headers.items():
            if h.lower() not in ["content-length", "transfer-encoding", "content-encoding"]:
                resp.headers[h] = v
        resp.headers["Access-Control-Allow-Origin"] = "*"
        
        return resp
    
    except Exception as e:
        log(f"[ERROR] Failed to proxy {target}: {e}", "ERROR")
        return Response(f"Failed to fetch {target}", status=502)

if __name__ == "__main__":
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    
    if ENABLE_CRAWLING and not OFFLINE_MODE:
        threading.Thread(
            target=lambda: (
                enqueue(SITE_URL),
                crawl_parallel(),
                log(f"Crawling finished, server at {HOST}:{PORT}", "INFO")
            ),
            daemon=True
        ).start()
        log("Crawler started in background.", "INFO")
    else:
        log("Offline mode or crawling disabled, serving cached files", "WARN")
    
    log(f"Server running on {HOST}:{PORT}", "INFO")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
