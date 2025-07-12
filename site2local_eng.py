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
import platform

# Enable Windows fixes (long paths, invalid names)
try:
    import lib_windows_eng
    lib_windows.ENABLE_WIN_LIB()
except Exception as e:
    print(f"[WARN] lib_windows could not be enabled: {e}")

sys.setrecursionlimit(10000)

# -------------------- CONFIGURATION --------------------
MODE = "AUTO_MODE"
SITE_URL = "https://discord.com"
PORT = 80
FORCE_ACCESS_DENIED_BYPASS = True
SCAN_FOR_HIDDEN_PATHS = True
ENABLE_HIDDEN_ELEMENTS = True
SHOW_HIDDEN_ELEMENTS = True
ENABLE_CRAWLING = True
HEADER_DEVICE = "desktop"
ACCEPT_ALL_MIRRORS_REQUEST = True

# -------------------- DEVICE DETECTION --------------------
def detect_device():
    if HEADER_DEVICE != "auto":
        return HEADER_DEVICE.lower()
    ua = request.headers.get("User-Agent", "").lower()
    if "android" in ua and "mobile" in ua: return "mobile"
    if "iphone" in ua or "ipad" in ua: return "mobile"
    if "android" in ua: return "tablet"
    if any(x in ua for x in ["windows", "macintosh", "linux"]): return "desktop"
    if "bot" in ua: return "bot"
    return "desktop"

# -------------------- HEADERS --------------------
def get_headers_for_device(device):
    if device == "mobile":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif device == "tablet":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif device == "bot":
        return {
            "User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Accept-Encoding": "gzip, deflate"
        }
    else:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }

# -------------------- PATHS --------------------
device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"
SITE_NAME = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
SITE_SRC = os.path.join("site_src", f"{SITE_NAME}_{device_type}")
SITE_DATA = os.path.join("site_data", f"{SITE_NAME}_{device_type}")
EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".json", ".webp"}

visited = set()
MAX_VISITED = 1000

app = Flask(__name__, static_folder=None)

# -------------------- HELPERS --------------------
def is_valid_url(url):
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

def local_path(url):
    from lib_windows import safe_path
    p = urlparse(url)
    path = p.path
    if path.endswith("/"): path += "index.html"
    if not os.path.splitext(path)[1]: path = os.path.join(path, "index.html")
    return safe_path(os.path.join(SITE_SRC, p.netloc, path.lstrip("/")))

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
        try: return brotli.decompress(content)
        except: pass
    if "gzip" in encoding:
        try: return gzip.decompress(content)
        except: pass
    return content

def already_downloaded(url):
    return os.path.exists(local_path(url))

def modify_html_for_visibility(soup):
    for el in soup.select("[style*='display:none'], [style*='visibility:hidden'], [style*='opacity:0']"):
        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
    for attr in ["hidden", "disabled", "readonly"]:
        for el in soup.select(f"[{attr}]"):
            del el[attr]
    for el in soup.find_all(attrs={"data-href": True}):
        el.name = "a"
        el["href"] = el["data-href"]
        el.string = el.get_text() or el["data-href"]

# -------------------- MIRRORS --------------------
def check_and_download_mirror(url):
    global ACCEPT_ALL_MIRRORS_REQUEST
    if ACCEPT_ALL_MIRRORS_REQUEST:
        return crawl(url)
    print(f"\n[Mirror detected] {url}")
    choice = input("Download mirror? (Y)es / (N)o / (A)ccept all: ").strip().upper()
    if choice == "Y":
        return crawl(url)
    elif choice == "A":
        ACCEPT_ALL_MIRRORS_REQUEST = True
        return crawl(url)

# -------------------- DOWNLOAD AND CRAWLING --------------------
def download(url):
    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return local_path(url)
    headers = get_headers_for_device(detect_device())
    try:
        print(f"[GET] {url}")
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return save_content(url, try_decompress(r))
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return None

def is_html(content):
    return content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()

def crawl(url):
    global visited
    if len(visited) >= MAX_VISITED:
        print("[WARNING] Maximum visited URLs limit reached")
        return
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
    tags_attrs = {"script": "src", "link": "href", "img": "src", "source": "src", "video": "src", "audio": "src"}
    for tag, attr in tags_attrs.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if src:
                full_url = urljoin(url, src)
                if is_valid_url(full_url):
                    if urlparse(full_url).netloc == urlparse(SITE_URL).netloc:
                        crawl(full_url)
                    else:
                        check_and_download_mirror(full_url)
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a['href'])
        if link.startswith(SITE_URL):
            crawl(link)
    if SCAN_FOR_HIDDEN_PATHS:
        hidden_paths = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for hp in hidden_paths:
            try:
                hp_url = urljoin(SITE_URL + "/", hp)
                r = requests.get(hp_url, headers={"Accept-Encoding": "br, gzip"})
                if r.status_code == 200 and is_html(r.content):
                    print(f"[HIDDEN] {hp_url}")
                    crawl(hp_url)
            except:
                pass

# -------------------- FLASK --------------------
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

    try:
        headers = get_headers_for_device(detect_device())
        r = requests.get(target, headers=headers)
        r.raise_for_status()
        content = try_decompress(r)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "wb") as f:
            f.write(content)
        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception as e:
        return Response(f"Remote error: {e}", status=500)

# -------------------- MAIN --------------------
if __name__ == '__main__':
    os.makedirs(SITE_SRC, exist_ok=True)
    os.makedirs(SITE_DATA, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Starting crawl of site: {SITE_URL} (mode: {MODE})")
        crawl(SITE_URL)
    else:
        print("Crawling disabled.")
    print(f"Files saved in: {os.path.abspath(SITE_SRC)}")
    print(f"POSTs saved in: {os.path.abspath(SITE_DATA)}")
    print(f"Server running at: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
