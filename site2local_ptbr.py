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

# -------------------- CONFIGURAÇÃO --------------------
MODE = "AUTO_MODE"  # Suporta sites que usam ou não HTML e também suporta PHP

# Flags para controle do esquema HTTP/HTTPS
USE_HTTP = False
USE_HTTPS = True
NO_HTTP_AND_HTTPS = False

RAW_SITE_URL = "web.whatsapp.com"  # Domínio puro, sem http(s)://
PORT = 8080
FORCE_ACCESS_DENIED_BYPASS = False
SCAN_FOR_HIDDEN_PATHS = True
ENABLE_HIDDEN_ELEMENTS = True
SHOW_HIDDEN_ELEMENTS = True
ENABLE_CRAWLING = True
HEADER_DEVICE = "linux"  # mobile, desktop, tablet, linux, bot, auto
ACCEPT_ALL_MIRRORS = True
ENABLE_MIRROR_DETECTION = True

# -------------------- FUNÇÕES DE URL --------------------
def process_url(url: str) -> str:
    """Ajusta a URL conforme as flags USE_HTTP, USE_HTTPS e NO_HTTP_AND_HTTPS."""
    if NO_HTTP_AND_HTTPS:
        if url.startswith("http://"):
            return url[len("http://"):]
        if url.startswith("https://"):
            return url[len("https://"):]
        return url
    
    parsed = urlparse(url)
    if not parsed.scheme:
        if USE_HTTPS:
            return "https://" + url
        elif USE_HTTP:
            return "http://" + url
        else:
            return url
    return url

SITE_URL = process_url(RAW_SITE_URL)

# -------------------- DETECÇÃO DE DISPOSITIVO --------------------
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
    if "windows" in ua or "macintosh" in ua:
        return "desktop"
    if "linux" in ua:
        return "linux"
    if "bot" in ua:
        return "bot"
    return "desktop"

def get_headers_for_device(device):
    common_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "br, gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "DNT": "1"
    }

    user_agents = {
        "mobile": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
        "tablet": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
        "bot": "Googlebot/2.1 (+http://www.google.com/bot.html)",
        "desktop": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
        "linux": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"
    }

    headers = common_headers.copy()
    headers["User-Agent"] = user_agents.get(device, user_agents["desktop"])
    return headers

# -------------------- CAMINHOS --------------------
device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"
SITE_NAME = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
SITE_SRC = os.path.join("site_src", f"{SITE_NAME}_{device_type}")
SITE_DATA = os.path.join("site_data", f"{SITE_NAME}_{device_type}")

EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".json", ".webp"}
visited = set()

app = Flask(__name__, static_folder=None)

# -------------------- FUNÇÕES DE DOWNLOAD E CACHE --------------------
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
    # Salva em uma pasta separada por domínio (p.netloc)
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
    # Torna elementos ocultos visíveis com estilo destacado
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

def ask_user_about_mirror(filename, mirrorurl):
    global ACCEPT_ALL_MIRRORS
    if ACCEPT_ALL_MIRRORS:
        return True
    print(f"\nUm mirror foi detectado, deseja baixar os arquivos do mirror {mirrorurl}?")
    print("[S] Sim   [N] Não   [A] Aceitar todos")
    while True:
        choice = input("Sua escolha (S/N/A): ").strip().lower()
        if choice == "s":
            return True
        elif choice == "n":
            return False
        elif choice == "a":
            ACCEPT_ALL_MIRRORS = True
            return True

def download(url, mirrors=None):
    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return local_path(url)

    urls_to_try = [url]
    if ENABLE_MIRROR_DETECTION and mirrors:
        for m in mirrors:
            if m != url:
                urls_to_try.append(m)

    for current_url in urls_to_try:
        device = detect_device()
        headers = get_headers_for_device(device)
        try:
            print(f"[GET] {current_url} [{device}]")
            r = requests.get(current_url, timeout=10, headers=headers)
            r.raise_for_status()
            content = try_decompress(r)
            return save_content(current_url, content)
        except Exception as e:
            print(f"[ERRO] {current_url}: {e}")
    return None

def is_html(content):
    return content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()

def crawl(url):
    if url in visited:
        return
    visited.add(url)
    mirrors = []  # Aqui você pode colocar a lógica para detectar mirrors reais

    saved = download(url, mirrors=mirrors)
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

    # Procurar por recursos para baixar
    tags = {"script": "src", "link": "href", "img": "src", "source": "src", "video": "src", "audio": "src"}
    for tag, attr in tags.items():
        for r in soup.find_all(tag):
            src = r.get(attr)
            if src:
                full = urljoin(url, src)
                if is_valid_url(full) and urlparse(full).netloc == urlparse(SITE_URL).netloc:
                    crawl(full)

    # Procurar links para crawl
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a['href'])
        if link.startswith(SITE_URL):
            crawl(link)

    # Scan para caminhos ocultos comuns
    if SCAN_FOR_HIDDEN_PATHS:
        for test in ["admin", "login", "panel", "dashboard", ".git", ".env"]:
            try:
                test_url = urljoin(SITE_URL + "/", test)
                r = requests.get(test_url, headers={"Accept-Encoding": "br, gzip"})
                if r.status_code == 200 and is_html(r.content):
                    print(f"[OCULTO] {test_url}")
                    crawl(test_url)
            except Exception:
                pass

# -------------------- ROTA FLASK --------------------
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
            # Usar headers originais da requisição POST
            headers = dict(request.headers)
            r = requests.post(target, data=data, headers=headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Erro: {e}", status=502)

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
        return Response(f"Erro remoto: {e}", status=500)

# -------------------- PRINCIPAL --------------------
if __name__ == '__main__':
    os.makedirs(SITE_SRC, exist_ok=True)
    os.makedirs(SITE_DATA, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Crawling: {SITE_URL} (modo: {MODE})")
        crawl(SITE_URL)
    else:
        print("Crawling desabilitado.")
    print(f"Pasta dos arquivos: {os.path.abspath(SITE_SRC)}")
    print(f"Pasta dos dados POST: {os.path.abspath(SITE_DATA)}")
    print(f"Servidor rodando em: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
