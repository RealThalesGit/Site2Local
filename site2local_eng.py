import os
import sys
import json
import hashlib
import mimetypes
import requests
import brotli
import gzip
from bs4 import BeautifulSoup
from flask import Flask, request, Response, send_file
from urllib.parse import urljoin, urlparse
import platform

# Import Windows lib to handle long and invalid paths
try:
    from lib_windows_eng import safe_path, remove_long_path_prefix, enable_windows_lib
    enable_windows_lib()
except Exception as e:
    print(f"[WARN] lib_windows_eng could not be enabled: {e}")

sys.setrecursionlimit(10000)

# ---------------- GLOBAL CONFIGURATION ----------------

MODE = "AUTO_MODE"
SITE_URL = "https://google.cat"
PORT = 80

FORCE_ACCESS_DENIED_BYPASS = False  # Enable tactics to avoid Access Denied blocks
SCAN_FOR_HIDDEN_PATHS = False       # Enable search for hidden URLs (admin, login, etc)
ENABLE_HIDDEN_ELEMENTS = False      # During crawling, enable hidden elements in HTML
SHOW_HIDDEN_ELEMENTS = False        # During HTTP response, show hidden elements
ENABLE_CRAWLING = True              # Enable automatic crawler
HEADER_DEVICE = "desktop"           # desktop, mobile, tablet, bot, auto
ACCEPT_ALL_MIRRORS = True           # Automatically accept downloading mirrors

# ----------- Internal variables -------------
visited = set()
MAX_VISITED = 1000
app = Flask(__name__, static_folder=None)

# Define paths to save files locally
SITE_NAME = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
SITE_SRC = os.path.join("site_src", f"{SITE_NAME}_{HEADER_DEVICE}")
SITE_DATA = os.path.join("site_data", f"{SITE_NAME}_{HEADER_DEVICE}")

# ---------- Utility functions -------------

def detect_device():
    if HEADER_DEVICE != "auto":
        return HEADER_DEVICE.lower()
    ua = request.headers.get("User-Agent", "").lower()
    if "android" in ua and "mobile" in ua:
        return "mobile"
    if "iphone" in ua or "ipad" in ua:
        return "mobile"
    if "android" in ua:
        return "tablet"
    if any(x in ua for x in ["windows", "macintosh", "linux"]):
        return "desktop"
    if "bot" in ua:
        return "bot"
    return "desktop"

def get_headers_for_device(device):
    base_headers = {
        "desktop": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "mobile": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "tablet": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "bot": {
            "User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Accept-Encoding": "gzip, deflate",
        },
    }
    return base_headers.get(device, base_headers["desktop"])

def local_path(url):
    """Safe path with prefix for reading/writing local files"""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    full_path = os.path.join(SITE_SRC, p.netloc, path.lstrip("/"))
    return safe_path(full_path)

def local_path_for_flask(url):
    """Path without prefix for sending files via Flask"""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    return os.path.join(SITE_SRC, p.netloc, path.lstrip("/"))

def try_decompress(r):
    content = r.content
    encoding = r.headers.get("Content-Encoding", "")
    if "br" in encoding:
        try:
            return brotli.decompress(content)
        except Exception as e:
            print(f"[WARN] Brotli decompress failed: {e}")
    if "gzip" in encoding:
        try:
            return gzip.decompress(content)
        except Exception as e:
            print(f"[WARN] Gzip decompress failed: {e}")
    return content

def already_downloaded(url):
    return os.path.exists(local_path(url))

def save_content(url, content):
    path = local_path(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    return path

def modify_html_for_visibility(soup):
    """Removes CSS hiding and attributes to show hidden content"""
    for el in soup.select("[style*='display:none'], [style*='visibility:hidden'], [style*='opacity:0']"):
        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
    for attr in ["hidden", "disabled", "readonly"]:
        for el in soup.select(f"[{attr}]"):
            del el[attr]
    # Convert data-href attributes to href to make clickable
    for el in soup.find_all(attrs={"data-href": True}):
        el.name = "a"
        el["href"] = el["data-href"]
        el.string = el.get_text() or el["data-href"]

def is_valid_url(url):
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

# ---------------- Mirror Handling -----------------

def ask_user_about_mirror(filename, mirrorurl):
    global ACCEPT_ALL_MIRRORS
    if ACCEPT_ALL_MIRRORS:
        return True
    print(f"\nMirror detected: {mirrorurl}\nFile: {filename}")
    print("[Y] Yes   [N] No   [A] Accept all")
    while True:
        choice = input("Your choice (Y/N/A): ").strip().lower()
        if choice == "y":
            return True
        elif choice == "n":
            return False
        elif choice == "a":
            ACCEPT_ALL_MIRRORS = True
            return True

def check_and_download_mirror(url):
    if ask_user_about_mirror(os.path.basename(urlparse(url).path), url):
        crawl(url)

# --------------- Crawling & Download ----------------

def download(url):
    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return local_path(url)
    device = detect_device()
    headers = get_headers_for_device(device)

    # Bypass Access Denied (simple example)
    if FORCE_ACCESS_DENIED_BYPASS:
        headers["Referer"] = SITE_URL
        headers["Cookie"] = "security_bypass=true"

    try:
        print(f"[GET] {url}")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        content = try_decompress(r)
        return save_content(url, content)
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return None

def is_html(content):
    c = content.strip().lower()
    return c.startswith(b"<!doctype") or b"<html" in c

def crawl(url):
    global visited
    if len(visited) >= MAX_VISITED:
        print("[WARNING] Maximum number of visited URLs reached")
        return
    if url in visited:
        return
    visited.add(url)

    saved_path = download(url)
    if not saved_path:
        return
    with open(saved_path, "rb") as f:
        content = f.read()

    if not is_html(content):
        return

    soup = BeautifulSoup(content, "html.parser")

    if ENABLE_HIDDEN_ELEMENTS:
        modify_html_for_visibility(soup)
        with open(saved_path, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Search links for crawling
    tags_attrs = {
        "script": "src",
        "link": "href",
        "img": "src",
        "source": "src",
        "video": "src",
        "audio": "src",
    }
    for tag, attr in tags_attrs.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if not src:
                continue
            full_url = urljoin(url, src)
            if is_valid_url(full_url):
                if urlparse(full_url).netloc == urlparse(SITE_URL).netloc:
                    crawl(full_url)
                else:
                    check_and_download_mirror(full_url)

    # Search internal <a href> links
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        if link.startswith(SITE_URL):
            crawl(link)

    # Scan hidden paths (admin, .git, etc)
    if SCAN_FOR_HIDDEN_PATHS:
        common_hidden = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for hp in common_hidden:
            hp_url = urljoin(SITE_URL + "/", hp)
            try:
                r = requests.get(hp_url, headers=get_headers_for_device(detect_device()), timeout=10)
                if r.status_code == 200 and is_html(r.content):
                    print(f"[HIDDEN] {hp_url}")
                    crawl(hp_url)
            except:
                pass

# ----------------- Flask Proxy ----------------------

@app.route("/", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def proxy(path):
    target = urljoin(SITE_URL + "/", path)
    if request.query_string:
        target += "?" + request.query_string.decode()

    local_internal = local_path(target)
    local_flask = remove_long_path_prefix(local_internal)

    if request.method == "POST":
        data = request.get_data()
        os.makedirs(SITE_DATA, exist_ok=True)
        h = hashlib.sha256(target.encode() + data).hexdigest()
        with open(safe_path(os.path.join(SITE_DATA, h + ".json")), "wb") as f:
            f.write(data)
        try:
            r = requests.post(target, data=data, headers=request.headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Error: {e}", status=502)

    if os.path.exists(local_internal):
        mime = mimetypes.guess_type(local_flask)[0] or "application/octet-stream"
        if SHOW_HIDDEN_ELEMENTS:
            # Reads and modifies content to show hidden elements in the response
            with open(local_internal, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            soup = BeautifulSoup(html, "html.parser")
            modify_html_for_visibility(soup)
            return Response(str(soup), mimetype="text/html")
        else:
            return send_file(local_flask, mimetype=mime, conditional=True)

    # If file doesn't exist locally, fetch online and save
    try:
        headers = get_headers_for_device(detect_device())
        if FORCE_ACCESS_DENIED_BYPASS:
            headers["Referer"] = SITE_URL
            headers["Cookie"] = "security_bypass=true"
        r = requests.get(target, headers=headers)
        r.raise_for_status()
        content = try_decompress(r)
        os.makedirs(os.path.dirname(local_internal), exist_ok=True)
        with open(local_internal, "wb") as f:
            f.write(content)
        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception as e:
        return Response(f"Remote error: {e}", status=500)

# -------------------- MAIN ----------------------------

if __name__ == "__main__":
    os.makedirs(SITE_SRC, exist_ok=True)
    os.makedirs(SITE_DATA, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Starting crawling of site: {SITE_URL} (mode: {MODE})")
        crawl(SITE_URL)
    else:
        print("Crawling disabled.")
    print(f"Files saved at: {os.path.abspath(SITE_SRC)}")
    print(f"POSTs saved at: {os.path.abspath(SITE_DATA)}")
    print(f"Server running at: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
