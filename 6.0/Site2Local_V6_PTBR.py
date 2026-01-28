# Amo você, chatgpt!
import os
import sys
import threading
import hashlib
import mimetypes
import json
import requests
import time
import colorama
from colorama import Fore, Style
import concurrent.futures
from bs4 import BeautifulSoup
from flask import Flask, request, Response, send_file
from urllib.parse import urljoin, urlparse, urldefrag

# -------------------- CONFIGURAÇÕES GERAIS --------------------
sys.setrecursionlimit(20000)  # Varredura profunda (crawl)
colorama.init(autoreset=True)  # Cores no terminal

# -------------------- CORES PARA LOG --------------------
class Cores:
    RESET = Style.RESET_ALL
    VERMELHO = Fore.RED
    VERDE = Fore.GREEN
    AMARELO = Fore.YELLOW
    CIANO = Fore.CYAN
    MAGENTA = Fore.MAGENTA

def log(msg, nivel="INFO"):
    cor = {
        "INFO": Cores.VERDE,
        "WARN": Cores.AMARELO,
        "ERROR": Cores.VERMELHO,
        "DEBUG": Cores.CIANO
    }.get(nivel, Cores.VERDE)
    print(f"{cor}[Site2Local] [{nivel}] {msg}{Cores.RESET}")

# -------------------- CONFIGURAÇÃO DO USUÁRIO --------------------
raw_site_url = "speedtest.net"  # Sem http:// ou https://
PORT = 8080
HEADER_DEVICE = "mobile"  # desktop, mobile, tablet, bot, auto

OFFLINE_MODE = False
SAVE_TRAFFIC = False
ENABLE_CRAWLING = True
SHOW_HIDDEN = True
SCAN_HIDDEN_PATHS = True

ACCEPT_ALL_MIRRORS = True  # Depreciado, mantido para compatibilidade
# -------------------- CONSTRUÇÃO DA URL BASE --------------------
def build_base_url(raw_url):
    for esquema in ["https://", "http://"]:
        url_teste = esquema + raw_url
        try:
            r = requests.head(url_teste, timeout=5)
            if r.status_code < 400:
                log(f"Usando {esquema.upper().strip('://')} para {raw_url}")
                return url_teste
        except Exception:
            continue
    log(f"Site {raw_url} inacessível, ativando MODO OFFLINE", "WARN")
    return None

SITE_URL = build_base_url(raw_site_url)
if SITE_URL is None:
    OFFLINE_MODE = True
    SITE_URL = "http://" + raw_site_url  # Fallback fictício

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

# -------------------- APLICAÇÃO FLASK --------------------
app = Flask(__name__, static_folder=None)
# -------------------- FUNÇÕES UTILITÁRIAS --------------------
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
    log(f"Arquivo salvo: {path}", "DEBUG")
    downloaded_files.add(url)
    return path
def get_headers(device):
    base = {
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
        "Sec-CH-UA-Platform": '"Windows"' if device == "desktop" else '"Android"',
        "Sec-CH-UA-Mobile": "?0" if device == "desktop" else "?1"
    }
    if device == "mobile":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Mobile Safari/537.36"
    elif device == "tablet":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"
    elif device == "bot":
        base["User-Agent"] = "Googlebot/2.1 (+http://www.google.com/bot.html)"
        base["Accept"] = "*/*"
    else:  # desktop
        base["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"
    return base

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
# -------------------- FUNÇÃO DE CRAWLING --------------------
def crawl_url(url):
    url = strip_fragment(url)
    if url in visited:
        return
    visited.add(url)

    if is_already_downloaded(url):
        log(f"[CACHE] {url}")
        return

    device = device_type if HEADER_DEVICE != "auto" else "desktop"
    headers = get_headers(device)

    try:
        log(f"[GET] {url} [{device}]")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        content = r.content  # Salva conteúdo bruto, sem decompressão

        # Salva direto se não for HTML
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

        # Links <a> internos ao site
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if link.startswith(SITE_URL):
                discovered_urls.append(link)

        if SCAN_HIDDEN_PATHS:
            hidden_paths = ["admin", "login", "panel", ".git", ".env", "config", "backup", "db", "private", "secret"]
            for hp in hidden_paths:
                hidden_url = urljoin(SITE_URL + "/", hp)
                try:
                    r = requests.head(hidden_url, headers=headers, timeout=5)
                    if r.status_code == 200 and hidden_url not in visited:
                        log(f"[CAMINHO OCULTO ENCONTRADO] {hidden_url}", "MAGENTA")
                        discovered_urls.append(hidden_url)
                except Exception:
                    pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(crawl_url, discovered_urls)

    except Exception as e:
        log(f"[ERRO] {url}: {e}", "ERROR")
# -------------------- MODO AUTOMÁTICO DE CRAWLING --------------------
def auto_mode_crawl():
    global SITE_URL
    global OFFLINE_MODE

    log(f"Iniciando varredura AUTOMÁTICA para {SITE_URL}")
    try:
        https_url = "https://" + urlparse(SITE_URL).netloc
        r = requests.head(https_url, timeout=5)
        if r.status_code < 400:
            log("HTTPS disponível, usando HTTPS")
            SITE_URL = https_url
        else:
            http_url = "http://" + urlparse(SITE_URL).netloc
            r2 = requests.head(http_url, timeout=5)
            if r2.status_code < 400:
                log("HTTPS não disponível, usando HTTP")
                SITE_URL = http_url
            else:
                log("Site offline, ativando modo OFFLINE local")
                OFFLINE_MODE = True

        if not OFFLINE_MODE:
            crawl_url(SITE_URL)
        else:
            log("MODO OFFLINE ativado, servindo do cache")

    except Exception as e:
        log(f"[ERRO MODO AUTOMÁTICO] {e}", "ERROR")
        OFFLINE_MODE = True
# -------------------- SUPORTE A MIRRORS --------------------
def ask_user_about_mirror(filename, mirrorurl):
    global ACCEPT_ALL_MIRRORS
    if ACCEPT_ALL_MIRRORS:
        return True
    print(f"\nUm mirror foi encontrado. Deseja baixar o arquivo {filename} do mirror {mirrorurl}?")
    print("[S] Sim   [N] Não   [A] Aceitar todos daqui para frente")
    while True:
        choice = input("Sua escolha (S/N/A): ").strip().lower()
        if choice == "s":
            return True
        elif choice == "n":
            return False
        elif choice == "a":
            ACCEPT_ALL_MIRRORS = True
            return True
        else:
            print("Opção inválida, tente novamente.")

def download_with_mirrors(url, mirrors):
    filename = url.split("/")[-1]
    for mirror in mirrors:
        try:
            if ask_user_about_mirror(filename, mirror):
                log(f"Baixando {filename} do mirror {mirror}")
                r = requests.get(mirror, timeout=15)
                r.raise_for_status()
                return r.content
        except Exception as e:
            log(f"Falha ao baixar do mirror {mirror}: {e}", "WARN")
    log(f"Falha ao baixar {filename} de todos os mirrors", "ERROR")
    return None
# -------------------- PROXY FLASK --------------------
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
            return Response(f"[ERRO POST] {e}", status=502)

    if os.path.exists(local_file):
        mime = mimetypes.guess_type(local_file)[0] or "application/octet-stream"
        try:
            return send_file(local_file, mimetype=mime, conditional=True)
        except Exception as e:
            log(f"[ERRO] Enviando arquivo {local_file}: {e}", "ERROR")
            return Response(f"Erro ao ler arquivo {local_file}", status=500)

    # Se arquivo não existe localmente, tenta buscar do site remoto
    device = detect_device()
    headers = get_headers(device)
    try:
        log(f"[PROXY GET] {full_url} [{device}]")
        r = requests.get(full_url, headers=headers, timeout=15, stream=True)
        r.raise_for_status()
        content = r.content  # Salva conteúdo comprimido bruto, sem decompressão
        save_file(full_url, content)

        # Responde com headers originais para correto tratamento do conteúdo
        response = Response(content, status=r.status_code)
        response.headers['Content-Type'] = r.headers.get('Content-Type', 'application/octet-stream')
        response.headers['Content-Encoding'] = r.headers.get('Content-Encoding', '')
        response.headers['Cache-Control'] = r.headers.get('Cache-Control', 'no-cache')

        return response

    except requests.exceptions.RequestException as e:
        log(f"[ERRO] Falha ao proxy {full_url}: {e}", "ERROR")
        return Response(f"Falha ao buscar {full_url}", status=502)
# -------------------- PRINCIPAL --------------------
if __name__ == "__main__":
    if not OFFLINE_MODE and ENABLE_CRAWLING:
        log(f"Iniciando varredura para {SITE_URL} ({device_type})")
        auto_mode_crawl()
    else:
        log("Modo offline ou crawling desabilitado, servindo arquivos em cache")

    os.makedirs(SRC_FOLDER, exist_ok=True)
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)
