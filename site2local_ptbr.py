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

# Aumenta limite de recursão para sites grandes
sys.setrecursionlimit(20000)

# ---------------------- CONFIGURAÇÃO ----------------------
SITE_URL = "https://example.com"  # Site base para crawling e proxy
PORT = 8080  # Porta para rodar o Flask localmente

MODE = "AUTO_MODE"  # Pode ser usado para decidir comportamento no futuro
HEADER_DEVICE = "desktop"  # mobile, tablet, desktop, bot, auto

ENABLE_CRAWLING = True  # Habilita crawler automático
SCAN_FOR_HIDDEN_PATHS = True  # Busca caminhos ocultos comuns
ENABLE_HIDDEN_ELEMENTS = True  # Torna visíveis elementos ocultos no HTML
SHOW_HIDDEN_ELEMENTS = True  # Realça elementos ocultos visualmente

FORCE_ACCESS_DENIED_BYPASS = False  # Tenta burlar 403/401 com headers extras

# Após confirmar 'A' aceitar todos os mirrors
accept_all_mirrors = True

# --------------------- VARIÁVEIS GLOBAIS ---------------------
visited_urls = set()
lock = threading.Lock()

device_type = HEADER_DEVICE if HEADER_DEVICE != "auto" else "desktop"
site_name = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
site_src_dir = os.path.join("site_src", f"{site_name}_{device_type}")
site_data_dir = os.path.join("site_data", f"{site_name}_{device_type}")

EXT_HTML = {".html", ".htm"}
EXT_STATIC = EXT_HTML | {".js", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".woff2", ".ttf", ".eot", ".ico", ".json", ".webp"}

app = Flask(__name__, static_folder=None)

# --------------------- FUNÇÕES AUXILIARES ---------------------

def detect_device() -> str:
    """Detecta o tipo de dispositivo baseado no HEADER_DEVICE ou User-Agent."""
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
    """Retorna headers HTTP realistas para o tipo de dispositivo."""
    base_headers = {
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": SITE_URL,
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
    """Gera o caminho local para salvar o conteúdo do URL."""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    safe_netloc = p.netloc.replace(":", "_")
    # Para evitar duplicar domínios, cria estrutura: site_src/<site_name>_<device>/<netloc>/<path>
    local_path = os.path.join(site_src_dir, safe_netloc, path.lstrip("/"))
    return local_path

def save_content(url: str, content: bytes) -> str:
    """Salva o conteúdo no caminho local correspondente ao URL."""
    path = local_path_from_url(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    print(f"[SALVO] {url} -> {path}")
    return path

def try_decompress(response: requests.Response) -> bytes:
    """Tenta descomprimir conteúdo se necessário (brotli, gzip)."""
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
    """Verifica se o arquivo local já existe para o URL."""
    return os.path.exists(local_path_from_url(url))

def modify_html_visibility(soup: BeautifulSoup):
    """Mostra e destaca elementos HTML ocultos para depuração."""
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
    """Extrai URLs de background e outras propriedades CSS via regex."""
    import re
    urls = set(re.findall(r'url\((?:\'|")?(.*?)(?:\'|")?\)', css_text))
    return urls

# --------------------- CRAWLING E DOWNLOAD ---------------------

def crawl(url: str):
    """Função recursiva para crawling e download dos recursos do site."""
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
        print(f"[ERRO] {url}: {e}")
        return

    save_path = save_content(url, content)

    # Se não for HTML, para o crawling aqui
    if not (content.strip().lower().startswith(b"<!doctype") or b"<html" in content.lower()):
        return

    # Parseia HTML para procurar links e recursos
    soup = BeautifulSoup(content, "html.parser")

    if ENABLE_HIDDEN_ELEMENTS and SHOW_HIDDEN_ELEMENTS:
        modify_html_visibility(soup)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Extrai URLs de CSS embutido para pegar backgrounds, fonts etc
    css_texts = [style_tag.string for style_tag in soup.find_all("style") if style_tag.string]
    inline_styles = [el.get("style", "") for el in soup.find_all(style=True)]
    for css_text in css_texts + inline_styles:
        for css_url in extract_css_urls(css_text):
            full_url = urljoin(url, css_url)
            if is_valid_url(full_url) and full_url.startswith(SITE_URL):
                crawl(full_url)

    # Extrai URLs de tags relevantes
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
                    # Mesmo domínio: crawl normal
                    if urlparse(full_url).netloc == urlparse(SITE_URL).netloc:
                        crawl(full_url)
                    else:
                        # Possível mirror ou CDN externo
                        if accept_all_mirrors:
                            print(f"[MIRROR AUTO] {full_url}")
                            crawl(full_url)
                        else:
                            check_and_download_mirror(full_url)

    # Links de navegação interna
    for a in soup.find_all("a", href=True):
        href = urljoin(url, a['href'])
        if href.startswith(SITE_URL):
            crawl(href)

    # Busca caminhos ocultos comuns (admin, .git, etc)
    if SCAN_FOR_HIDDEN_PATHS:
        common_hidden = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for path in common_hidden:
            hidden_url = urljoin(SITE_URL + "/", path)
            if hidden_url not in visited_urls:
                try:
                    r_hidden = requests.get(hidden_url, headers=headers, timeout=5)
                    if r_hidden.status_code == 200 and (r_hidden.content.strip().lower().startswith(b"<!doctype") or b"<html" in r_hidden.content.lower()):
                        print(f"[OCULTO] {hidden_url}")
                        crawl(hidden_url)
                except Exception:
                    pass

# --------------------- MIRRORS/CDN ---------------------

def check_and_download_mirror(url: str):
    """Consulta o usuário para baixar mirrors externos ou CDNs."""
    global accept_all_mirrors
    domain = urlparse(url).netloc
    filename = os.path.basename(urlparse(url).path) or "index.html"

    if accept_all_mirrors:
        print(f"[MIRROR AUTO] Aceitando mirror automaticamente: {url}")
        crawl(url)
        return

    print(f"\nMirror/CDN detectado: {domain}/{filename}")
    print("Deseja baixar? (Y)es / (N)o / (A)ccept all mirrors automatically from now on")

    while True:
        choice = input("Sua escolha: ").strip().upper()
        if choice == 'Y':
            crawl(url)
            break
        elif choice == 'N':
            print("Pulando mirror.")
            break
        elif choice == 'A':
            accept_all_mirrors = True
            crawl(url)
            break
        else:
            print("Por favor, responda Y, N ou A.")

# --------------------- PROXY FLASK ---------------------

@app.route('/', defaults={'path': ''}, methods=["GET", "POST"])
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    target_url = urljoin(SITE_URL + "/", path)
    if request.query_string:
        target_url += "?" + request.query_string.decode()

    local_file = local_path_from_url(target_url)

    if request.method == "POST":
        # Salvar dados POST para análise/debug
        data = request.get_data()
        os.makedirs(site_data_dir, exist_ok=True)
        hash_post = hashlib.sha256(target_url.encode() + data).hexdigest()
        with open(os.path.join(site_data_dir, hash_post + ".json"), "wb") as f:
            f.write(data)
        try:
            r = requests.post(target_url, data=data, headers=request.headers, timeout=10)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Erro no POST: {e}", status=502)

    # Se arquivo local existe, serve direto
    if os.path.exists(local_file):
        mime_type = mimetypes.guess_type(local_file)[0] or "application/octet-stream"
        return send_file(local_file, mimetype=mime_type, conditional=True)

    # Senão, tenta baixar e salvar localmente
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
        return Response(f"Erro remoto: {e}", status=500)

# --------------------- EXECUÇÃO PRINCIPAL ---------------------

def main():
    os.makedirs(site_src_dir, exist_ok=True)
    os.makedirs(site_data_dir, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Iniciando crawling do site: {SITE_URL} (modo: {MODE}, dispositivo: {device_type})")
        crawl(SITE_URL)
        print(f"Crawling completo. Arquivos salvos em: {os.path.abspath(site_src_dir)}")
    else:
        print("Crawling desativado.")
    print(f"Servidor rodando em http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
