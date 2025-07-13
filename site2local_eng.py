# Love you chatgpt!
import os
import json
import hashlib
import mimetypes
import requests
import brotli
import gzip
from bs4 import BeautifulSoup
from flask import Flask, request, Response, send_file
from urllib.parse import urljoin, urlparse
import sys

sys.setrecursionlimit(10000)

# -------------------- SETTINGS --------------------
MODE = "AUTO_MODE"  # AUTO_MODE, HTML_MODE, PHP_MODE, NOHTML_MODE
RAW_SITE_URL = "web.whatsapp.com"
PORT = 80
ENABLE_CRAWLING = True
FORCE_ACCESS_DENIED_BYPASS = False
SCAN_FOR_HIDDEN_PATHS = True
ENABLE_HIDDEN_ELEMENTS = False
SHOW_HIDDEN_ELEMENTS = False
HEADER_DEVICE = "desktop"  # mobile, desktop, tablet, ios, ipad, mac, linux, bot, auto
ACCEPT_ALL_MIRRORS = True
ENABLE_MIRROR_DETECTION = True
SAVE_TRAFFIC_FOR_OFFLINE = True  # Save traffic for offline mode
OFFLINE_MODE = False  # Emulate saved offline responses

SCHEME_CACHE_FILE = "scheme_cache.json"
TRAFFIC_SAVE_FILE = "saved_traffics.trf"

# -------------------- SCHEME CACHE --------------------
def load_cached_scheme(domain):
    if not os.path.exists(SCHEME_CACHE_FILE):
        return None
    try:
        with open(SCHEME_CACHE_FILE, "r") as f:
            cache = json.load(f)
            return cache.get(domain)
    except Exception:
        return None

def save_cached_scheme(domain, scheme):
    cache = {}
    if os.path.exists(SCHEME_CACHE_FILE):
        try:
            with open(SCHEME_CACHE_FILE, "r") as f:
                cache = json.load(f)
        except Exception:
            cache = {}
    cache[domain] = scheme
    with open(SCHEME_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def resolve_url(base_url):
    cached_scheme = load_cached_scheme(base_url)
    schemes_to_try = [cached_scheme] if cached_scheme else ["https", "http"]
    schemes_to_try = [s for s in schemes_to_try if s]

    for scheme in schemes_to_try:
        test_url = f"{scheme}://{base_url}"
        try:
            r = requests.head(test_url, timeout=5)
            if r.status_code < 400:
                print(f"[OK] Using {scheme.upper()} for {base_url}")
                save_cached_scheme(base_url, scheme)
                return test_url
            else:
                print(f"[FAIL] {test_url}: {r.status_code}")
        except Exception:
            print(f"[ERROR] {scheme.upper()} failed for {base_url}, trying fallback...")

    if cached_scheme:
        alt_scheme = "https" if cached_scheme == "http" else "http"
        test_url = f"{alt_scheme}://{base_url}"
        try:
            r = requests.head(test_url, timeout=5)
            if r.status_code < 400:
                print(f"[OK] Using {alt_scheme.upper()} (fallback) for {base_url}")
                save_cached_scheme(base_url, alt_scheme)
                return test_url
            else:
                print(f"[FAIL] {test_url}: {r.status_code}")
        except Exception:
            print(f"[ERROR] fallback {alt_scheme.upper()} failed for {base_url}")

    print(f"[ERROR] Both schemes failed for {base_url}. Cannot continue.")
    return None

SITE_URL = resolve_url(RAW_SITE_URL)
if not SITE_URL:
    print("Could not resolve a valid scheme for the domain.")
    exit(1)

# -------------------- DEVICE DETECTION --------------------
def detect_device():
    if HEADER_DEVICE != "auto":
        return HEADER_DEVICE.lower()
    ua = request.headers.get("User-Agent", "").lower()
    if "android" in ua and ("mobile" in ua or "phone" in ua):
        return "mobile"
    if "iphone" in ua or "ipod" in ua:
        return "ios"
    if "ipad" in ua:
        return "ipad"
    if "macintosh" in ua:
        return "mac"
    if "windows" in ua:
        return "desktop"
    if "linux" in ua:
        return "linux"
    if "bot" in ua or "spider" in ua or "crawler" in ua:
        return "bot"
    return "desktop"

# -------------------- MODERN HEADERS BY DEVICE --------------------
def get_headers_for_device(device):
    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    if device == "mobile":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.7204.101 Mobile Safari/537.36"
            ),
            "Sec-CH-UA": '"Chromium";v="138", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?1",
            "Sec-CH-UA-Platform": '"Android"',
        })

    elif device == "tablet":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Linux; Android 10; Tablet) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.7204.101 Safari/537.36"
            ),
            "Sec-CH-UA": '"Chromium";v="138", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Android"',
        })

    elif device == "ios":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1"
            ),
            "Sec-CH-UA-Mobile": "?1",
            "Sec-CH-UA-Platform": '"iOS"',
        })

    elif device == "ipad":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1"
            ),
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"iPadOS"',
        })

    elif device == "mac":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.7204.101 Safari/537.36"
            ),
            "Sec-CH-UA": '"Chromium";v="138", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"macOS"',
        })

    elif device == "linux":
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.7204.101 Safari/537.36"
            ),
            "Sec-CH-UA": '"Chromium";v="138", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Linux"',
        })

    elif device == "bot":
        base_headers = {
            "User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            "Accept-Encoding": "gzip, deflate",
            "Cache-Control": "no-cache",
        }

    else:  # fallback desktop Windows
        base_headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.7204.101 Safari/537.36"
            ),
            "Sec-CH-UA": '"Chromium";v="138", "Not=A?Brand";v="99"',
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
        })

    return base_headers

# -------------------- PATHS --------------------
device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"
SITE_NAME = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
SITE_SRC = os.path.join("site_src", f"{SITE_NAME}_{device_type}")
SITE_DATA = os.path.join("site_data", f"{SITE_NAME}_{device_type}")

EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {
    ".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".woff2",
    ".ttf", ".eot", ".ico", ".json", ".webp", ".mp4", ".webm", ".ogg", ".mp3",
    ".wav", ".m4a", ".ttf", ".otf", ".pdf", ".txt", ".csv", ".xml", ".zip",
    ".rar", ".7z", ".tar", ".gz", ".bz2"
}

visited = set()
saved_traffics = {}

app = Flask(__name__, static_folder=None)

# -------------------- UTILITIES --------------------
def is_valid_url(url):
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

def local_path(url):
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    return os.path.join(SITE_SRC, p.netloc, path.lstrip("/"))

def save_content(url, content):
    path = local_path(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    return path

def try_decompress(r):
    content = r.content
    encoding = r.headers.get("Content-Encoding", "")
    if "br" in encoding:
        try:
            return brotli.decompress(content)
        except Exception:
            pass
    if "gzip" in encoding:
        try:
            return gzip.decompress(content)
        except Exception:
            pass
    return content

def already_downloaded(url):
    return os.path.exists(local_path(url))

def modify_html_for_visibility(soup):
    for el in soup.select("[style*='display:none'], [style*='visibility:hidden'], [style*='opacity:0']"):
        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
    for el in soup.select("[hidden]"):
        del el['hidden']
    for el in soup.select("[disabled]"):
        del el['disabled']
    for el in soup.select("[readonly]"):
        del el['readonly']
    for el in soup.find_all(attrs={"data-href": True}):
        href = el["data-href"]
        el.name = "a"
        el["href"] = href
        el["style"] = "display:inline-block !important; background:yellow; border:2px dashed red;"
        el.string = el.get_text() or href
    for el in soup.find_all(attrs={"data-link": True}):
        href = el["data-link"]
        el.name = "a"
        el["href"] = href
        el["style"] = "display:inline-block !important; background:yellow; border:2px dashed red;"
        el.string = el.get_text() or href

def save_traffic(url, content, headers, status_code):
    global saved_traffics
    key = url
    saved_traffics[key] = {
        "content": content.hex(),
        "headers": dict(headers),
        "status_code": status_code
    }
    with open(TRAFFIC_SAVE_FILE, "w") as f:
        json.dump(saved_traffics, f)

def load_saved_traffic():
    global saved_traffics
    if os.path.exists(TRAFFIC_SAVE_FILE):
        try:
            with open(TRAFFIC_SAVE_FILE, "r") as f:
                saved_traffics = json.load(f)
        except Exception:
            saved_traffics = {}

# -------------------- DOWNLOAD AND CRAWL --------------------
def download(url):
    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return local_path(url)

    if OFFLINE_MODE:
        # Emulate offline response if saved
        load_saved_traffic()
        key = url
        if key in saved_traffics:
            print(f"[OFFLINE] Emulated response for {url}")
            content = bytes.fromhex(saved_traffics[key]["content"])
            path = save_content(url, content)
            return path
        else:
            print(f"[OFFLINE] No saved response for {url}")
            return None

    device = detect_device()
    headers = get_headers_for_device(device)
    try:
        print(f"[GET] {url} [{device}]")
        r = requests.get(url, timeout=10, headers=headers)
        r.raise_for_status()
        content = try_decompress(r)

        if SAVE_TRAFFIC_FOR_OFFLINE:
            save_traffic(url, content, r.headers, r.status_code)

        return save_content(url, content)
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return None

def is_html(content):
    return content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()

def crawl(url):
    if url in visited:
        return
    visited.add(url)
    saved = download(url)
    if not saved:
        return

    with open(saved, "rb") as f:
        content = f.read()
    if not is_html(content):
        return
    soup = BeautifulSoup(content, "html.parser")

    if SHOW_HIDDEN_ELEMENTS:
        modify_html_for_visibility(soup)
        with open(saved, "w", encoding="utf-8") as f:
            f.write(str(soup))

    tags = {"script": "src", "link": "href", "img": "src", "source": "src", "video": "src", "audio": "src"}
    for tag, attr in tags.items():
        for r in soup.find_all(tag):
            src = r.get(attr)
            if src:
                full = urljoin(url, src)
                if is_valid_url(full) and urlparse(full).netloc == urlparse(SITE_URL).netloc:
                    crawl(full)
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a['href'])
        if link.startswith(SITE_URL):
            crawl(link)
    if SCAN_FOR_HIDDEN_PATHS:
        hidden_paths = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for test in hidden_paths:
            try:
                test_url = urljoin(SITE_URL + "/", test)
                r = requests.get(test_url, headers={"Accept-Encoding": "br, gzip"})
                if r.status_code == 200 and is_html(r.content):
                    print(f"[HIDDEN] {test_url}")
                    crawl(test_url)
            except Exception:
                pass

# -------------------- FLASK PROXY --------------------
@app.route('/', defaults={'path': ''}, methods=["GET", "POST"])
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    target = urljoin(SITE_URL + "/", path)
    if request.query_string:
        target += "?" + request.query_string.decode()
    local = local_path(target)

    if request.method == "POST":
        data = request.get_data()
        os.makedirs(SITE_DATA, exist_ok=True)
        h = hashlib.sha256(target.encode() + data).hexdigest()
        with open(os.path.join(SITE_DATA, h + ".json"), "wb") as f:
            f.write(data)
        try:
            r = requests.post(target, data=data, headers=request.headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Error: {e}", status=502)

    if os.path.exists(local):
        mime = mimetypes.guess_type(local)[0] or "application/octet-stream"
        return send_file(local, mimetype=mime, conditional=True)

    # Try to emulate offline mode before downloading online
    if OFFLINE_MODE:
        load_saved_traffic()
        if target in saved_traffics:
            saved = saved_traffics[target]
            content = bytes.fromhex(saved["content"])
            headers = saved.get("headers", {})
            status = saved.get("status_code", 200)
            print(f"[OFFLINE] Serving saved content for {target}")
            return Response(content, status=status, content_type=headers.get("Content-Type", "text/html"))

    try:
        headers = get_headers_for_device(detect_device())
        r = requests.get(target, headers=headers)
        r.raise_for_status()
        content = try_decompress(r)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "wb") as f:
            f.write(content)

        if SAVE_TRAFFIC_FOR_OFFLINE:
            save_traffic(target, content, r.headers, r.status_code)

        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type", "text/html"))
    except Exception as e:
        return Response(f"Error accessing {target}: {e}", status=502)

if __name__ == "__main__":
    print(f"Downloading: {SITE_URL} (mode: {MODE})")
    os.makedirs(SITE_SRC, exist_ok=True)
    os.makedirs(SITE_DATA, exist_ok=True)
    crawl(SITE_URL)
    print(f"Files folder: {SITE_SRC}")
    print(f"POSTs folder: {SITE_DATA}")
    print(f"Access via: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
