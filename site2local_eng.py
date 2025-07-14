import os
import sys
import threading
import hashlib
import mimetypes
import json
import requests
import brotli
import gzip
import time
import colorama
from colorama import Fore, Style
import concurrent.futures
from bs4 import BeautifulSoup
from flask import Flask, request, Response, send_file
from urllib.parse import urljoin, urlparse, urldefrag

# -------------------- AJUSTES GERAIS --------------------
sys.setrecursionlimit(20000)  # Permite crawl profundo
colorama.init(autoreset=True)  # Habilita cores no terminal

# -------------------- CORES PARA LOGS --------------------
class Colors:
    RESET = Style.RESET_ALL
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    CYAN = Fore.CYAN
    MAGENTA = Fore.MAGENTA

def log(message, level="INFO"):
    colors_map = {
        "INFO": Colors.GREEN,
        "WARN": Colors.YELLOW,
        "ERROR": Colors.RED,
        "DEBUG": Colors.CYAN,
        "MAGENTA": Colors.MAGENTA
    }
    color = colors_map.get(level, Colors.GREEN)
    print(f"{color}[Site2Local] [{level}] {message}{Colors.RESET}")

# -------------------- CONFIGURAÇÕES --------------------
raw_site_url = "web.whatsapp.com"  # Sem https:// ou http://
PORT = 80
HEADER_DEVICE = "desktop"  # desktop, mobile, tablet, bot, auto

OFFLINE_MODE = False
SAVE_TRAFFIC = False
ENABLE_CRAWL = True
SHOW_HIDDEN = True
SEARCH_SECRET_PATHS = True

ACCEPT_ALL_MIRRORS = False  # Controla se aceita todos mirrors automaticamente

# -------------------- CONSTRUÇÃO DA URL BASE --------------------
def build_base_url(raw_url):
    for scheme in ["https://", "http://"]:
        test_url = scheme + raw_url
        try:
            r = requests.head(test_url, timeout=5)
            if r.status_code < 400:
                log(f"Using {scheme.upper().strip('://')} for {raw_url}")
                return test_url
        except Exception:
            continue
    log(f"Site {raw_url} unreachable, activating OFFLINE mode", "WARN")
    return None

SITE_URL = build_base_url(raw_site_url)
if SITE_URL is None:
    OFFLINE_MODE = True
    SITE_URL = "http://" + raw_site_url  # fallback fictício

# -------------------- PASTAS POR DISPOSITIVO --------------------
site_name = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"

SRC_FOLDER = os.path.join("site_src", f"{site_name}_{device_type}")
DATA_FOLDER = os.path.join("site_data", f"{site_name}_{device_type}")
TRAFFIC_CACHE_FILE = os.path.join(DATA_FOLDER, "traffic_cache.json")

EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {
    ".js", ".mjs", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".mp4", ".webm", ".ogg", ".mp3", ".wav",
    ".json", ".pdf", ".txt", ".xml", ".csv",
    ".zip", ".rar", ".7z"
}

# -------------------- VARIÁVEIS GLOBAIS --------------------
visited = set()
downloaded_files = set()
traffic_lock = threading.Lock()
saved_traffic = {}

mirrors_found = {}  # Armazena mirrors por nome de arquivo

# -------------------- FLASK APP --------------------
app = Flask(__name__, static_folder=None)

# -------------------- FUNÇÕES AUXILIARES --------------------
def strip_fragment(url):
    return urldefrag(url)[0]

def is_valid_url(url):
    p = urlparse(url)
    return bool(p.scheme) and bool(p.netloc)

def local_path(url):
    url = strip_fragment(url)
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    if path.startswith("/"):
        path = path[1:]
    return os.path.join(SRC_FOLDER, p.netloc.replace("www.", ""), *path.split('/'))

def is_already_downloaded(url):
    return os.path.isfile(local_path(url))

def save_file(url, content):
    path = local_path(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    log(f"File saved: {path}", "DEBUG")
    downloaded_files.add(url)
    return path

def decompress_content(response):
    try:
        encoding = response.headers.get("Content-Encoding", "").lower()
        if "br" in encoding:
            return brotli.decompress(response.content)
        if "gzip" in encoding:
            return gzip.decompress(response.content)
    except Exception as e:
        log(f"Failed to decompress content: {e}", "WARN")
    return response.content

def get_headers(device):
    base_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": SITE_URL,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
        "Sec-CH-UA-Platform": '"Android"' if device == "mobile" else '"Windows"',
        "Sec-CH-UA-Mobile": "?1" if device == "mobile" else "?0"
    }

    if device == "mobile":
        base_headers["User-Agent"] = "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Mobile Safari/537.36"
    elif device == "tablet":
        base_headers["User-Agent"] = "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"
    elif device == "bot":
        base_headers["User-Agent"] = "Googlebot/2.1 (+http://www.google.com/bot.html)"
        base_headers["Accept"] = "*/*"
    else:
        base_headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"

    return base_headers

def detect_device():
    if HEADER_DEVICE != "auto":
        return HEADER_DEVICE.lower()
    ua = request.headers.get("User-Agent", "").lower()
    if "android" in ua and ("mobile" in ua or "phone" in ua):
        return "mobile"
    if "iphone" in ua or "ipod" in ua:
        return "ios"
    if "ipad" in ua:
        return "tablet"
    if "macintosh" in ua:
        return "mac"
    if "windows" in ua:
        return "desktop"
    if "linux" in ua:
        return "linux"
    if "bot" in ua:
        return "bot"
    return "desktop"

# -------------------- FUNÇÃO PARA DIÁLOGO ENCANTADO DE MIRRORS --------------------
def ask_mirror_dialog(filename, mirror_url):
    global ACCEPT_ALL_MIRRORS
    if ACCEPT_ALL_MIRRORS:
        return True
    print(f"\n[Mirror found] Download '{filename}' from mirror: {mirror_url}?")
    print("[S] Yes    [N] No    [A] Accept all from now on")
    while True:
        choice = input("Your choice (S/N/A): ").strip().lower()
        if choice == "s":
            return True
        elif choice == "n":
            return False
        elif choice == "a":
            ACCEPT_ALL_MIRRORS = True
            return True
        else:
            print("Invalid option. Please choose S, N, or A.")

def extract_filename(url):
    path = urlparse(url).path
    return os.path.basename(path) or "index.html"

# -------------------- CRAWLER ENCANTADO COM MIRRORS --------------------
def crawl_url(url):
    url = strip_fragment(url)
    if url in visited:
        return
    visited.add(url)

    filename = extract_filename(url)
    # Guarda mirrors encontrados para esse arquivo
    mirrors_found.setdefault(filename, [])
    if url not in mirrors_found[filename]:
        mirrors_found[filename].append(url)

    if is_already_downloaded(url):
        log(f"[CACHE] {url}")
        return

    # Se múltiplos mirrors existem e não aceitou todos, perguntar
    if len(mirrors_found[filename]) > 1 and not ACCEPT_ALL_MIRRORS:
        if not ask_mirror_dialog(filename, url):
            log(f"[MIRROR SKIPPED] {url}")
            return

    device = device_type if HEADER_DEVICE != "auto" else "desktop"
    headers = get_headers(device)

    try:
        log(f"[GET] {url} [{device}]")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        content = decompress_content(r)
        if not content:
            return

        # Se não é HTML, salva direto
        if b"<html" not in content[:500].lower():
            save_file(url, content)
            return

        soup = BeautifulSoup(content, "html.parser")

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

            content = soup.encode("utf-8")

        save_file(url, content)

        # Extrai recursos estáticos
        resource_tags = {
            "script": "src",
            "img": "src",
            "link": "href",
            "source": "src",
            "video": "src",
            "audio": "src",
            "iframe": "src",
        }

        discovered_urls = []

        for tag, attr in resource_tags.items():
            for el in soup.find_all(tag):
                link = el.get(attr)
                if not link:
                    continue
                full_url = urljoin(url, link)
                if is_valid_url(full_url):
                    discovered_urls.append(full_url)

        # Links <a> internos
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if link.startswith(SITE_URL):
                discovered_urls.append(link)

        # Busca caminhos secretos
        if SEARCH_SECRET_PATHS:
            secret_paths = ["admin", "login", "panel", ".git", ".env", "config", "backup", "db", "private", "secret"]
            for secret in secret_paths:
                secret_url = urljoin(SITE_URL + "/", secret)
                try:
                    r = requests.head(secret_url, headers=headers, timeout=5)
                    if r.status_code == 200 and secret_url not in visited:
                        log(f"[SECRET PATH FOUND] {secret_url}", "MAGENTA")
                        discovered_urls.append(secret_url)
                except Exception:
                    pass

        # Crawl paralelo para acelerar
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(crawl_url, discovered_urls)

    except Exception as e:
        log(f"[ERROR] {url}: {e}", "ERROR")

# -------------------- AUTO MODE PARA DEFINIR HTTP OU HTTPS --------------------
def auto_mode_crawl():
    global SITE_URL, OFFLINE_MODE

    log(f"Starting AUTO_MODE crawl for {SITE_URL}")
    try:
        https_url = "https://" + urlparse(SITE_URL).netloc
        r = requests.head(https_url, timeout=5)
        if r.status_code < 400:
            log("HTTPS available, using HTTPS")
            SITE_URL = https_url
        else:
            http_url = "http://" + urlparse(SITE_URL).netloc
            r2 = requests.head(http_url, timeout=5)
            if r2.status_code < 400:
                log("HTTPS unavailable, using HTTP")
                SITE_URL = http_url
            else:
                log("Site offline, enabling OFFLINE mode", "WARN")
                OFFLINE_MODE = True

        if not OFFLINE_MODE:
            crawl_url(SITE_URL)
        else:
            log("OFFLINE MODE enabled, serving from cache only")

    except Exception as e:
        log(f"[AUTO_MODE ERROR] {e}", "ERROR")
        OFFLINE_MODE = True

# -------------------- FUNÇÕES DE CACHE DE TRÁFEGO --------------------
def load_traffic_cache():
    global saved_traffic
    if os.path.exists(TRAFFIC_CACHE_FILE):
        try:
            with open(TRAFFIC_CACHE_FILE, "r") as f:
                saved_traffic = json.load(f)
            log(f"Cache loaded with {len(saved_traffic)} URLs", "DEBUG")
        except Exception as e:
            log(f"Failed to load cache: {e}", "WARN")
            saved_traffic = {}
    else:
        saved_traffic = {}

def save_traffic_cache():
    with traffic_lock:
        try:
            os.makedirs(DATA_FOLDER, exist_ok=True)
            with open(TRAFFIC_CACHE_FILE, "w") as f:
                json.dump(saved_traffic, f)
            log(f"Cache saved with {len(saved_traffic)} URLs", "DEBUG")
        except Exception as e:
            log(f"Failed to save cache: {e}", "WARN")

# -------------------- FLASK ROUTE PARA PROXY --------------------
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    full_url = urljoin(SITE_URL + "/", path)
    if request.query_string:
        full_url += "?" + request.query_string.decode()

    local_file = local_path(full_url)

    if request.method == "POST":
        data = request.get_data()
        os.makedirs(DATA_FOLDER, exist_ok=True)
        h = hashlib.sha256(full_url.encode() + data).hexdigest()
        with open(os.path.join(DATA_FOLDER, h + ".json"), "wb") as f:
            f.write(data)
        try:
            headers = get_headers(detect_device())
            r = requests.post(full_url, data=data, headers=headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"[POST ERROR] {e}", status=502)

    if os.path.exists(local_file):
        mime = mimetypes.guess_type(local_file)[0] or "application/octet-stream"
        try:
            return send_file(local_file, mimetype=mime, conditional=True)
        except Exception as e:
            return Response(f"[ERROR SERVING FILE] {e}", status=500)

    if OFFLINE_MODE:
        if full_url in saved_traffic:
            cache = saved_traffic[full_url]
            content = bytes.fromhex(cache["content"])
            return Response(content, status=cache.get("status", 200), content_type=cache.get("headers", {}).get("Content-Type", "text/html"))

    try:
        headers = get_headers(detect_device())
        r = requests.get(full_url, headers=headers, timeout=15)
        r.raise_for_status()
        content = decompress_content(r)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        with open(local_file, "wb") as f:
            f.write(content)
        if SAVE_TRAFFIC:
            with traffic_lock:
                saved_traffic[full_url] = {
                    "content": content.hex(),
                    "headers": dict(r.headers),
                    "status": r.status_code,
                    "timestamp": time.time()
                }
                save_traffic_cache()
        return Response(content, status=r.status_code, content_type=r.headers.get("Content-Type", "text/html"))
    except Exception as e:
        return Response(f"[FETCH ERROR] {full_url}: {e}", status=502)

# -------------------- EXECUÇÃO PRINCIPAL --------------------
if __name__ == "__main__":
    load_traffic_cache()

    device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"

    if ENABLE_CRAWL:
        if HEADER_DEVICE == "auto":
            device_type = "desktop"
        log(f"Starting crawl for {raw_site_url} ({device_type})")
        if not OFFLINE_MODE:
            auto_mode_crawl()
        else:
            log("OFFLINE MODE enabled, only cache will be used")

    log(f"Server running at http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
