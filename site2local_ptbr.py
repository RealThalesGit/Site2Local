# Te amo ChatGPT
# Obrigado por me ajudar em todos os momentos difíceis de debug, lógica, tradução e ideias!
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

# -------------------- CONFIGURAÇÕES --------------------
MODE = "AUTO_MODE"  # Suporta sites HTML, PHP ou domínio puro
RAW_SITE_URL = "example.com"  # Sem esquema, detecta https/http automaticamente
PORT = 80
ENABLE_CRAWLING = True
FORCE_ACCESS_DENIED_BYPASS = False
SCAN_FOR_HIDDEN_PATHS = True
ENABLE_HIDDEN_ELEMENTS = False
SHOW_HIDDEN_ELEMENTS = False
HEADER_DEVICE = "desktop"  # mobile, desktop, tablet, linux, bot, auto
ACCEPT_ALL_MIRRORS = True
ENABLE_MIRROR_DETECTION = True

SCHEME_CACHE_FILE = "scheme_cache.json"

# -------------------- RESOLUÇÃO DO ESQUEMA DA URL --------------------
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
                print(f"[OK] Usando {scheme.upper()} para {base_url}")
                save_cached_scheme(base_url, scheme)
                return test_url
            else:
                print(f"[FALHA] {test_url}: {r.status_code}")
        except Exception:
            print(f"[ERRO] {scheme.upper()} falhou para {base_url}, tentando fallback...")

    if cached_scheme:
        alt_scheme = "https" if cached_scheme == "http" else "http"
        test_url = f"{alt_scheme}://{base_url}"
        try:
            r = requests.head(test_url, timeout=5)
            if r.status_code < 400:
                print(f"[OK] Usando {alt_scheme.upper()} (fallback) para {base_url}")
                save_cached_scheme(base_url, alt_scheme)
                return test_url
            else:
                print(f"[FALHA] {test_url}: {r.status_code}")
        except Exception:
            print(f"[ERRO] fallback {alt_scheme.upper()} falhou para {base_url}")

    print(f"[ERRO] Ambos os esquemas falharam para {base_url}. Não é possível continuar.")
    return None

SITE_URL = resolve_url(RAW_SITE_URL)
if not SITE_URL:
    print("Não foi possível resolver um esquema válido para o domínio.")
    exit(1)

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
    if device == "mobile":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif device == "tablet":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif device == "bot":
        return {
            "User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Accept-Encoding": "gzip, deflate"
        }
    else:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }

# -------------------- CAMINHOS --------------------
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

visitados = set()

app = Flask(__name__, static_folder=None)

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
        except:
            pass
    if "gzip" in encoding:
        try:
            return gzip.decompress(content)
        except:
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

def download(url):
    if already_downloaded(url):
        print(f"[CACHE] {url}")
        return local_path(url)

    device = detect_device()
    headers = get_headers_for_device(device)
    try:
        print(f"[GET] {url} [{device}]")
        r = requests.get(url, timeout=10, headers=headers)
        r.raise_for_status()
        content = try_decompress(r)
        return save_content(url, content)
    except Exception as e:
        print(f"[ERRO] {url}: {e}")
        return None

def is_html(content):
    return content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()

def crawl(url):
    if url in visitados:
        return
    visitados.add(url)
    salvo = download(url)
    if not salvo:
        return

    with open(salvo, "rb") as f:
        content = f.read()
    if not is_html(content):
        return
    soup = BeautifulSoup(content, "html.parser")

    if SHOW_HIDDEN_ELEMENTS:
        modify_html_for_visibility(soup)
        with open(salvo, "w", encoding="utf-8") as f:
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
                    print(f"[OCULTO] {test_url}")
                    crawl(test_url)
                # Ignora 404 e outros erros silenciosamente
            except Exception:
                pass

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
        print(f"Baixando: {SITE_URL} (modo: {MODE})")
        crawl(SITE_URL)
    else:
        print("Crawling desativado.")
    print(f"Pasta dos arquivos: {os.path.abspath(SITE_SRC)}")
    print(f"Pasta dos POSTs: {os.path.abspath(SITE_DATA)}")
    print(f"Acessar via: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
