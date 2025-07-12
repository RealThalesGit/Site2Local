import os
import sys
import json
import hashlib
import mimetypes
import threading
import requests
import brotli
import gzip
from bs4 import BeautifulSoup
from flask import Flask, request, Response, send_file
from urllib.parse import urljoin, urlparse

# Increase recursion limit for large sites
sys.setrecursionlimit(20000)

# ---------------------- CONFIGURATION ----------------------
BASE_URL = "https://example.com"  # Base site for crawling and proxy
PORT = 8080  # Port for running Flask locally

MODE = "AUTO_MODE"  # Could be used for future behavior decisions
HEADER_DEVICE = "desktop"  # mobile, tablet, desktop, bot, auto

ENABLE_CRAWLING = True  # Enable automatic crawler
SCAN_HIDDEN_PATHS = False  # Scan for common hidden paths (ex: /admin, /.git)
ENABLE_HIDDEN_ELEMENTS = False  # Make hidden HTML elements visible
SHOW_HIDDEN_ELEMENTS = False  # Highlight hidden elements visually

FORCE_ACCESS_DENIED_BYPASS = False  # Try to bypass 403/401 errors with extra headers

# After confirming 'A', accept all mirrors automatically
accept_all_mirrors =True

# --------------------- GLOBAL VARIABLES ---------------------
visited_urls = set()
lock = threading.Lock()

device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"
site_name = urlparse(BASE_URL).netloc.replace("www.", "").replace(".", "_")
site_src_dir = os.path.join("site_src", f"{site_name}_{device_type}")
site_data_dir = os.path.join("site_data", f"{site_name}_{device_type}")

EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".json", ".webp"}

app = Flask(__name__, static_folder=None)

# --------------------- HELPER FUNCTIONS ---------------------

def detect_device() -> str:
    """Detect device type based on HEADER_DEVICE or User-Agent."""
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

def get_headers_for_device(device: str) -> dict:
    """Return realistic HTTP headers for the device type."""
    base_headers = {
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": BASE_URL,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    headers_map = {
        "mobile": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip, deflate",
            "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
            "Sec-CH-UA-Platform": '"Android"',
            "Sec-CH-UA-Mobile": "?1",
        },
        "desktop": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36",
            "Accept-Encoding": "br, gzip, deflate",
            "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-CH-UA-Mobile": "?0",
        },
        "tablet": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36",
            "Accept-Encoding": "br, gzip, deflate",
            "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
            "Sec-CH-UA-Platform": '"Android"',
            "Sec-CH-UA-Mobile": "?0",
        },
        "bot": {
            "User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Googlebot"',
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?0",
        },
    }
    return {**base_headers, **headers_map.get(device, headers_map["desktop"])}

def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.scheme) and bool(p.netloc)
    except:
        return False

def local_path_from_url(url: str) -> str:
    """Generate local path to save content for the URL."""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    safe_netloc = p.netloc.replace(":", "_")
    local_path = os.path.join(site_src_dir, safe_netloc, path.lstrip("/"))
    return local_path

def save_content(url: str, content: bytes) -> str:
    """Save content to local path corresponding to URL."""
    path = local_path_from_url(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    print(f"[SAVED] {url} -> {path}")
    return path

def try_decompress(response: requests.Response) -> bytes:
    """Try decompressing content if needed (brotli, gzip)."""
    encoding = response.headers.get("Content-Encoding", "").lower()
    content = response.content
    try:
        if "br" in encoding:
            return brotli.decompress(content)
        if "gzip" in encoding:
            return gzip.decompress(content)
    except Exception:
        pass
    return content

def already_downloaded(url: str) -> bool:
    """Check if local file for URL already exists."""
    return os.path.exists(local_path_from_url(url))

def modify_html_visibility(soup: BeautifulSoup):
    """Make and highlight hidden HTML elements visible for debugging."""
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
            if sel.startswith("[style"):
                el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
            else:
                for attr in ["hidden", "disabled", "readonly"]:
                    if attr in el.attrs:
                        del el.attrs[attr]

def extract_css_urls(css_text: str) -> set:
    """Extract URLs from CSS background and other properties using regex."""
    import re
    urls = set(re.findall(r'url\((?:\'|")?(.*?)(?:\'|")?\)', css_text))
    return urls

# --------------------- CRAWLING AND DOWNLOAD ---------------------

def crawl(url: str):
    """Recursive function to crawl and download site resources."""
    with lock:
        if url in visited_urls:
            return
        visited_urls.add(url)

    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return

    device = device_type
    headers = get_headers_for_device(device)
    try:
        print(f"[GET] {url} [{device}]")
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        content = try_decompress(r)
    except Exception as e:
        print(f"[ERROR] {url}: {e}")
        return

    save_path = save_content(url, content)

    # If not HTML, stop crawling here
    if not (content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()):
        return

    soup = BeautifulSoup(content, "html.parser")

    if ENABLE_HIDDEN_ELEMENTS and SHOW_HIDDEN_ELEMENTS:
        modify_html_visibility(soup)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Extract CSS URLs from embedded styles and inline styles
    css_texts = [style_tag.string for style_tag in soup.find_all("style") if style_tag.string]
    inline_styles = [el.get("style", "") for el in soup.find_all(style=True)]
    for css_text in css_texts + inline_styles:
        for css_url in extract_css_urls(css_text):
            full_url = urljoin(url, css_url)
            if is_valid_url(full_url) and full_url.startswith(BASE_URL):
                crawl(full_url)

    # Extract URLs from relevant tags
    tags_attrs = {
        "script": "src",
        "link": "href",
        "img": "src",
        "source": "src",
        "video": "src",
        "audio": "src",
        "iframe": "src"
    }
    for tag, attr in tags_attrs.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if src:
                full_url = urljoin(url, src)
                if is_valid_url(full_url):
                    if urlparse(full_url).netloc == urlparse(BASE_URL).netloc:
                        crawl(full_url)
                    else:
                        if accept_all_mirrors:
                            print(f"[MIRROR AUTO] {full_url}")
                            crawl(full_url)
                        else:
                            check_and_download_mirror(full_url)

    # Internal navigation links
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a['href'])
        if href.startswith(BASE_URL):
            crawl(href)

    # Scan common hidden paths
    if SCAN_HIDDEN_PATHS:
        common_hidden = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for path in common_hidden:
            hidden_url = urljoin(BASE_URL + "/", path)
            if hidden_url not in visited_urls:
                try:
                    r_hidden = requests.get(hidden_url, headers=headers, timeout=5)
                    if r_hidden.status_code == 200 and (r_hidden.content.strip().lower().startswith(b"<!doctype") or b"<html" in r_hidden.content.lower()):
                        print(f"[HIDDEN] {hidden_url}")
                        crawl(hidden_url)
                except Exception:
                    pass

# --------------------- MIRRORS / CDN ---------------------

def check_and_download_mirror(url: str):
    """Ask user whether to download external mirrors or CDNs."""
    global accept_all_mirrors
    domain = urlparse(url).netloc
    filename = os.path.basename(urlparse(url).path) or "index.html"

    if accept_all_mirrors:
        print(f"[MIRROR AUTO] Automatically accepting mirror: {url}")
        crawl(url)
        return

    print(f"\nMirror/CDN detected: {domain}/{filename}")
    print("Download it? (Y)es / (N)o / (A)ccept all mirrors automatically from now on")

    while True:
        choice = input("Your choice: ").strip().upper()
        if choice == 'Y':
            crawl(url)
            break
        elif choice == 'N':
            print("Skipping mirror.")
            break
        elif choice == 'A':
            accept_all_mirrors = True
            crawl(url)
            break
        else:
            print("Please answer Y, N, or A.")

# --------------------- FLASK PROXY ---------------------

@app.route('/', defaults={'path': ''}, methods=["GET", "POST"])
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    target_url = urljoin(BASE_URL + "/", path)
    if request.query_string:
        target_url += "?" + request.query_string.decode()

    local_file = local_path_from_url(target_url)

    if request.method == "POST":
        data = request.get_data()
        os.makedirs(site_data_dir, exist_ok=True)
        hash_post = hashlib.sha256(target_url.encode() + data).hexdigest()
        with open(os.path.join(site_data_dir, hash_post + ".json"), "wb") as f:
            f.write(data)
        try:
            r = requests.post(target_url, data=data, headers=request.headers, timeout=10)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"POST error: {e}", status=502)

    if os.path.exists(local_file):
        mime_type = mimetypes.guess_type(local_file)[0] or "application/octet-stream"
        return send_file(local_file, mimetype=mime_type, conditional=True)

    try:
        headers = get_headers_for_device(detect_device())
        r = requests.get(target_url, headers=headers, timeout=10)
        r.raise_for_status()
        content = try_decompress(r)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        with open(local_file, "wb") as f:
            f.write(content)
        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception as e:
        return Response(f"Remote error: {e}", status=500)

# --------------------- MAIN EXECUTION ---------------------

def main():
    os.makedirs(site_src_dir, exist_ok=True)
    os.makedirs(site_data_dir, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Starting crawling site: {BASE_URL} (mode: {MODE}, device: {device_type})")
        crawl(BASE_URL)
        print(f"Crawling complete. Files saved in: {os.path.abspath(site_src_dir)}")
    else:
        print("Crawling disabled.")
    print(f"Server running at http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
