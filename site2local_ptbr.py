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

# Ativa correções para Windows (caminhos longos, nomes inválidos)
try:
    import lib_windows_ptbr
    lib_windows_ptbr.ENABLE_WIN_LIB()
except Exception as e:
    print(f"[WARN] lib_windows não pôde ser ativado: {e}")

sys.setrecursionlimit(10000)

# -------------------- CONFIGURAÇÃO --------------------
MODE = "AUTO_MODE"
SITE_URL = "https://discord.com"
PORT = 80
FORCE_ACCESS_DENIED_BYPASS = False
SCAN_FOR_HIDDEN_PATHS = False
ENABLE_HIDDEN_ELEMENTS = False
SHOW_HIDDEN_ELEMENTS = False
ENABLE_CRAWLING = True
HEADER_DEVICE = "desktop"
ACCEPT_ALL_MIRRORS_REQUEST = True

# -------------------- DETECÇÃO DE DISPOSITIVO --------------------
def detectar_dispositivo():
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
def obter_headers_para_dispositivo(dispositivo):
    if dispositivo == "mobile":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif dispositivo == "tablet":
        return {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip"
        }
    elif dispositivo == "bot":
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

visitados = set()
MAX_VISITADOS = 1000

app = Flask(__name__, static_folder=None)

# -------------------- FUNÇÕES AUXILIARES --------------------
def url_valida(url):
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

def caminho_local(url):
    from lib_windows_ptbr import safe_path
    p = urlparse(url)
    path = p.path
    if path.endswith("/"): path += "index.html"
    if not os.path.splitext(path)[1]: path = os.path.join(path, "index.html")
    return safe_path(os.path.join(SITE_SRC, p.netloc, path.lstrip("/")))

def salvar_conteudo(url, conteudo):
    path = caminho_local(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(conteudo)
    return path

def tentar_descomprimir(r):
    conteudo = r.content
    encoding = r.headers.get("Content-Encoding", "")
    if "br" in encoding:
        try: return brotli.decompress(conteudo)
        except: pass
    if "gzip" in encoding:
        try: return gzip.decompress(conteudo)
        except: pass
    return conteudo

def ja_baixado(url):
    return os.path.exists(caminho_local(url))

def modificar_html_para_visibilidade(soup):
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
def checar_e_baixar_mirror(url):
    global ACCEPT_ALL_MIRRORS_REQUEST
    if ACCEPT_ALL_MIRRORS_REQUEST:
        return crawl(url)
    print(f"\n[Mirror detectado] {url}")
    escolha = input("Baixar mirror? (S)Sim / (N)Não / (A)ceitar todos: ").strip().upper()
    if escolha == "S":
        return crawl(url)
    elif escolha == "A":
        ACCEPT_ALL_MIRRORS_REQUEST = True
        return crawl(url)

# -------------------- DOWNLOAD E CRAWLING --------------------
def baixar(url):
    if ja_baixado(url):
        print(f"[CACHE] {url}")
        return caminho_local(url)
    headers = obter_headers_para_dispositivo(detectar_dispositivo())
    try:
        print(f"[GET] {url}")
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return salvar_conteudo(url, tentar_descomprimir(r))
    except Exception as e:
        print(f"[ERRO] {url}: {e}")
        return None

def eh_html(conteudo):
    return conteudo.strip().lower().startswith(b"<!doctype") or b"<html" in conteudo.lower()

def crawl(url):
    global visitados
    if len(visitados) >= MAX_VISITADOS:
        print("[AVISO] Limite máximo de URLs visitadas atingido")
        return
    if url in visitados:
        return
    visitados.add(url)
    salvo = baixar(url)
    if not salvo:
        return
    with open(salvo, "rb") as f:
        conteudo = f.read()
    if not eh_html(conteudo):
        return
    soup = BeautifulSoup(conteudo, "html.parser")
    if SHOW_HIDDEN_ELEMENTS:
        modificar_html_para_visibilidade(soup)
        with open(salvo, "w", encoding="utf-8") as f:
            f.write(str(soup))
    tags_atributos = {"script": "src", "link": "href", "img": "src", "source": "src", "video": "src", "audio": "src"}
    for tag, attr in tags_atributos.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if src:
                url_completa = urljoin(url, src)
                if url_valida(url_completa):
                    if urlparse(url_completa).netloc == urlparse(SITE_URL).netloc:
                        crawl(url_completa)
                    else:
                        checar_e_baixar_mirror(url_completa)
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
                if r.status_code == 200 and eh_html(r.content):
                    print(f"[OCULTO] {hp_url}")
                    crawl(hp_url)
            except:
                pass

# -------------------- FLASK --------------------
@app.route('/', defaults={'path': ''}, methods=["GET", "POST"])
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    alvo = urljoin(SITE_URL + "/", path)
    if request.query_string:
        alvo += "?" + request.query_string.decode()
    local = caminho_local(alvo)

    if request.method == "POST":
        dados = request.get_data()
        os.makedirs(SITE_DATA, exist_ok=True)
        h = hashlib.sha256(alvo.encode() + dados).hexdigest()
        with open(os.path.join(SITE_DATA, h + ".json"), "wb") as f:
            f.write(dados)
        try:
            r = requests.post(alvo, data=dados, headers=request.headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Erro: {e}", status=502)

    if os.path.exists(local):
        mime = mimetypes.guess_type(local)[0] or "application/octet-stream"
        return send_file(local, mimetype=mime, conditional=True)

    try:
        headers = obter_headers_para_dispositivo(detectar_dispositivo())
        r = requests.get(alvo, headers=headers)
        r.raise_for_status()
        conteudo = tentar_descomprimir(r)
        os.makedirs(os.path.dirname(local), exist_ok=True)
        with open(local, "wb") as f:
            f.write(conteudo)
        return Response(conteudo, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception as e:
        return Response(f"Erro remoto: {e}", status=500)

# -------------------- MAIN --------------------
if __name__ == '__main__':
    os.makedirs(SITE_SRC, exist_ok=True)
    os.makedirs(SITE_DATA, exist_ok=True)
    if ENABLE_CRAWLING:
        print(f"Iniciando crawling do site: {SITE_URL} (modo: {MODE})")
        crawl(SITE_URL)
    else:
        print("Crawling desativado.")
    print(f"Arquivos salvos em: {os.path.abspath(SITE_SRC)}")
    print(f"POSTs salvos em: {os.path.abspath(SITE_DATA)}")
    print(f"Servidor rodando em: http://127.0.0.1:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
