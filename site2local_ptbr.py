# Te amo chatgpt!
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
class Cores:
    RESET = Style.RESET_ALL
    RED = Fore.RED
    GREEN = Fore.GREEN
    YELLOW = Fore.YELLOW
    CYAN = Fore.CYAN
    MAGENTA = Fore.MAGENTA

def log(mensagem, nivel="INFO"):
    mapa_cores = {
        "INFO": Cores.GREEN,
        "WARN": Cores.YELLOW,
        "ERROR": Cores.RED,
        "DEBUG": Cores.CYAN,
        "MAGENTA": Cores.MAGENTA
    }
    cor = mapa_cores.get(nivel, Cores.GREEN)
    print(f"{cor}[Site2Local] [{nivel}] {mensagem}{Cores.RESET}")

# -------------------- CONFIGURAÇÕES --------------------
raw_site_url = "web.whatsapp.com"  # Sem https:// ou http://
PORTA = 80
CABECALHO_DISPOSITIVO = "desktop"  # desktop, mobile, tablet, bot, auto

MODO_OFFLINE = False
SALVAR_TRAFEGO = False
ATIVAR_CRAWL = True
MOSTRAR_OCULTOS = True
BUSCAR_CAMINHOS_SECRETOS = True

ACEITAR_TODOS_MIRRORS = False  # Controla se aceita todos mirrors automaticamente

# -------------------- CONSTRUÇÃO DA URL BASE --------------------
def construir_url_base(url_crua):
    for esquema in ["https://", "http://"]:
        url_teste = esquema + url_crua
        try:
            r = requests.head(url_teste, timeout=5)
            if r.status_code < 400:
                log(f"Usando {esquema.upper().strip('://')} para {url_crua}")
                return url_teste
        except Exception:
            continue
    log(f"Site {url_crua} inacessível, ativando modo OFFLINE", "WARN")
    return None

SITE_URL = construir_url_base(raw_site_url)
if SITE_URL is None:
    MODO_OFFLINE = True
    SITE_URL = "http://" + raw_site_url  # fallback fictício

# -------------------- PASTAS POR DISPOSITIVO --------------------
nome_site = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
tipo_dispositivo = CABECALHO_DISPOSITIVO if CABECALHO_DISPOSITIVO != "auto" else "desktop"

PASTA_SRC = os.path.join("site_src", f"{nome_site}_{tipo_dispositivo}")
PASTA_DADOS = os.path.join("site_data", f"{nome_site}_{tipo_dispositivo}")
ARQUIVO_CACHE_TRAFEGO = os.path.join(PASTA_DADOS, "traffic_cache.json")

EXT_HTML = {".html", ".htm"}
EXT_ESTATICOS = EXT_HTML | {
    ".js", ".mjs", ".css", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
    ".woff", ".woff2", ".ttf", ".otf", ".eot",
    ".mp4", ".webm", ".ogg", ".mp3", ".wav",
    ".json", ".pdf", ".txt", ".xml", ".csv",
    ".zip", ".rar", ".7z"
}

# -------------------- VARIÁVEIS GLOBAIS --------------------
visitados = set()
arquivos_baixados = set()
lock_trafego = threading.Lock()
trafegos_salvos = {}

mirrors_encontrados = {}  # Armazena mirrors por nome de arquivo

# -------------------- APP FLASK --------------------
app = Flask(__name__, static_folder=None)

# -------------------- FUNÇÕES AUXILIARES --------------------
def remover_fragmento(url):
    return urldefrag(url)[0]

def url_valida(url):
    p = urlparse(url)
    return bool(p.scheme) and bool(p.netloc)

def caminho_local(url):
    url = remover_fragmento(url)
    p = urlparse(url)
    caminho = p.path
    if caminho.endswith("/"):
        caminho += "index.html"
    if not os.path.splitext(caminho)[1]:
        caminho = os.path.join(caminho, "index.html")
    if caminho.startswith("/"):
        caminho = caminho[1:]
    return os.path.join(PASTA_SRC, p.netloc.replace("www.", ""), *caminho.split('/'))

def ja_baixado(url):
    return os.path.isfile(caminho_local(url))

def salvar_arquivo(url, conteudo):
    caminho = caminho_local(url)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "wb") as f:
        f.write(conteudo)
    log(f"Arquivo salvo: {caminho}", "DEBUG")
    arquivos_baixados.add(url)
    return caminho

def descomprimir_conteudo(resposta):
    try:
        codificacao = resposta.headers.get("Content-Encoding", "").lower()
        if "br" in codificacao:
            return brotli.decompress(resposta.content)
        if "gzip" in codificacao:
            return gzip.decompress(resposta.content)
    except Exception as e:
        log(f"Falha ao descomprimir conteúdo: {e}", "WARN")
    return resposta.content

def obter_headers(dispositivo):
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
        "Sec-CH-UA-Platform": '"Android"' if dispositivo == "mobile" else '"Windows"',
        "Sec-CH-UA-Mobile": "?1" if dispositivo == "mobile" else "?0"
    }

    if dispositivo == "mobile":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/115.0.5790.171 Mobile Safari/537.36"
    elif dispositivo == "tablet":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/115.0.5790.171 Safari/537.36"
    elif dispositivo == "bot":
        base["User-Agent"] = "Googlebot/2.1 (+http://www.google.com/bot.html)"
        base["Accept"] = "*/*"
    else:
        base["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/115.0.5790.171 Safari/537.36"

    return base

def detectar_dispositivo():
    if CABECALHO_DISPOSITIVO != "auto":
        return CABECALHO_DISPOSITIVO.lower()
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

# -------------------- DIÁLOGO ENCANTADO DE MIRRORS --------------------
def perguntar_sobre_mirror(nome_arquivo, url_mirror):
    global ACEITAR_TODOS_MIRRORS
    if ACEITAR_TODOS_MIRRORS:
        return True
    print(f"\n[Mirror encontrado] Deseja baixar '{nome_arquivo}' do mirror: {url_mirror}?")
    print("[S] Sim    [N] Não    [A] Aceitar todos daqui para frente")
    while True:
        escolha = input("Sua escolha (S/N/A): ").strip().lower()
        if escolha == "s":
            return True
        elif escolha == "n":
            return False
        elif escolha == "a":
            ACEITAR_TODOS_MIRRORS = True
            return True
        else:
            print("Opção inválida. Escolha S, N ou A.")

def extrair_nome_arquivo(url):
    caminho = urlparse(url).path
    return os.path.basename(caminho) or "index.html"

# -------------------- CRAWLER ENCANTADO COM MIRRORS --------------------
def crawl_url(url):
    url = remover_fragmento(url)
    if url in visitados:
        return
    visitados.add(url)

    nome_arquivo = extrair_nome_arquivo(url)
    # Guarda mirrors encontrados para este arquivo
    mirrors_encontrados.setdefault(nome_arquivo, [])
    if url not in mirrors_encontrados[nome_arquivo]:
        mirrors_encontrados[nome_arquivo].append(url)

    if ja_baixado(url):
        log(f"[CACHE] {url}")
        return

    # Se múltiplos mirrors existem e não aceitou todos, pergunta
    if len(mirrors_encontrados[nome_arquivo]) > 1 and not ACEITAR_TODOS_MIRRORS:
        if not perguntar_sobre_mirror(nome_arquivo, url):
            log(f"[MIRROR IGNORADO] {url}")
            return

    dispositivo = tipo_dispositivo if CABECALHO_DISPOSITIVO != "auto" else "desktop"
    headers = obter_headers(dispositivo)

    try:
        log(f"[GET] {url} [{dispositivo}]")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        conteudo = descomprimir_conteudo(r)
        if not conteudo:
            return

        # Se não for HTML, salva direto
        if b"<html" not in conteudo[:500].lower():
            salvar_arquivo(url, conteudo)
            return

        soup = BeautifulSoup(conteudo, "html.parser")

        if MOSTRAR_OCULTOS:
            seletores = [
                "[style*='display:none']",
                "[style*='visibility:hidden']",
                "[style*='opacity:0']",
                "[hidden]",
                "[disabled]",
                "[readonly]"
            ]
            for sel in seletores:
                for el in soup.select(sel):
                    if 'style' in el.attrs:
                        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
                    for att in ['hidden', 'disabled', 'readonly']:
                        el.attrs.pop(att, None)

            conteudo = soup.encode("utf-8")

        salvar_arquivo(url, conteudo)

        tags_recursos = {
            "script": "src",
            "img": "src",
            "link": "href",
            "source": "src",
            "video": "src",
            "audio": "src",
            "iframe": "src",
        }

        urls_descobertas = []

        for tag, attr in tags_recursos.items():
            for el in soup.find_all(tag):
                link = el.get(attr)
                if not link:
                    continue
                url_final = urljoin(url, link)
                if url_valida(url_final):
                    urls_descobertas.append(url_final)

        # Links internos <a>
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if link.startswith(SITE_URL):
                urls_descobertas.append(link)

        # Busca caminhos secretos
        if BUSCAR_CAMINHOS_SECRETOS:
            caminhos_secretos = ["admin", "login", "panel", ".git", ".env", "config", "backup", "db", "private", "secret"]
            for c in caminhos_secretos:
                oculto = urljoin(SITE_URL + "/", c)
                try:
                    r = requests.head(oculto, headers=headers, timeout=5)
                    if r.status_code == 200 and oculto not in visitados:
                        log(f"[CAMINHO SECRETO] {oculto}", "MAGENTA")
                        urls_descobertas.append(oculto)
                except Exception:
                    pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(crawl_url, urls_descobertas)

    except Exception as e:
        log(f"[ERRO] {url}: {e}", "ERROR")

# -------------------- MODO AUTOMÁTICO PARA DEFINIR HTTP OU HTTPS --------------------
def modo_auto_crawl():
    global SITE_URL, MODO_OFFLINE

    log(f"Iniciando crawl no MODO_AUTO para {SITE_URL}")
    try:
        url_https = "https://" + urlparse(SITE_URL).netloc
        r = requests.head(url_https, timeout=5)
        if r.status_code < 400:
            log("HTTPS disponível, usando HTTPS")
            SITE_URL = url_https
        else:
            url_http = "http://" + urlparse(SITE_URL).netloc
            r2 = requests.head(url_http, timeout=5)
            if r2.status_code < 400:
                log("HTTPS indisponível, usando HTTP")
                SITE_URL = url_http
            else:
                log("Site offline, ativando modo OFFLINE", "WARN")
                MODO_OFFLINE = True

        if not MODO_OFFLINE:
            crawl_url(SITE_URL)
        else:
            log("MODO OFFLINE ativado, servindo apenas do cache")

    except Exception as e:
        log(f"[ERRO NO MODO_AUTO] {e}", "ERROR")
        MODO_OFFLINE = True

# -------------------- FUNÇÕES DE CACHE DE TRÁFEGO --------------------
def carregar_cache_trafego():
    global trafegos_salvos
    if os.path.exists(ARQUIVO_CACHE_TRAFEGO):
        try:
            with open(ARQUIVO_CACHE_TRAFEGO, "r") as f:
                trafegos_salvos = json.load(f)
            log(f"Cache carregado com {len(trafegos_salvos)} URLs", "DEBUG")
        except Exception as e:
            log(f"Falha ao carregar cache: {e}", "WARN")
            trafegos_salvos = {}
    else:
        trafegos_salvos = {}

def salvar_cache_trafego():
    with lock_trafego:
        try:
            os.makedirs(PASTA_DADOS, exist_ok=True)
            with open(ARQUIVO_CACHE_TRAFEGO, "w") as f:
                json.dump(trafegos_salvos, f)
            log(f"Cache salvo com {len(trafegos_salvos)} URLs", "DEBUG")
        except Exception as e:
            log(f"Falha ao salvar cache: {e}", "WARN")

# -------------------- ROTA FLASK PARA PROXY --------------------
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    url_completa = urljoin(SITE_URL + "/", path)
    if request.query_string:
        url_completa += "?" + request.query_string.decode()

    arquivo_local = caminho_local(url_completa)

    if request.method == "POST":
        dados = request.get_data()
        os.makedirs(PASTA_DADOS, exist_ok=True)
        h = hashlib.sha256(url_completa.encode() + dados).hexdigest()
        with open(os.path.join(PASTA_DADOS, h + ".json"), "wb") as f:
            f.write(dados)
        try:
            headers = obter_headers(detectar_dispositivo())
            r = requests.post(url_completa, data=dados, headers=headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"[ERRO NO POST] {e}", status=502)

    if os.path.exists(arquivo_local):
        mime = mimetypes.guess_type(arquivo_local)[0] or "application/octet-stream"
        try:
            return send_file(arquivo_local, mimetype=mime, conditional=True)
        except Exception as e:
            return Response(f"[ERRO AO SERVIR ARQUIVO] {e}", status=500)

    if MODO_OFFLINE:
        if url_completa in trafegos_salvos:
            cache = trafegos_salvos[url_completa]
            conteudo = bytes.fromhex(cache["conteudo"])
            return Response(conteudo, status=cache.get("status", 200), content_type=cache.get("headers", {}).get("Content-Type", "text/html"))

    try:
        headers = obter_headers(detectar_dispositivo())
        r = requests.get(url_completa, headers=headers, timeout=15)
        r.raise_for_status()
        conteudo = descomprimir_conteudo(r)
        os.makedirs(os.path.dirname(arquivo_local), exist_ok=True)
        with open(arquivo_local, "wb") as f:
            f.write(conteudo)
        if SALVAR_TRAFEGO:
            with lock_trafego:
                trafegos_salvos[url_completa] = {
                    "conteudo": conteudo.hex(),
                    "headers": dict(r.headers),
                    "status": r.status_code,
                    "timestamp": time.time()
                }
                salvar_cache_trafego()
        return Response(conteudo, status=r.status_code, content_type=r.headers.get("Content-Type", "text/html"))
    except Exception as e:
        return Response(f"[ERRO AO BUSCAR] {url_completa}: {e}", status=502)

# -------------------- EXECUÇÃO PRINCIPAL --------------------
if __name__ == "__main__":
    carregar_cache_trafego()

    tipo_dispositivo = CABECALHO_DISPOSITIVO if CABECALHO_DISPOSITIVO != "auto" else "desktop"

    if ATIVAR_CRAWL:
        if CABECALHO_DISPOSITIVO == "auto":
            tipo_dispositivo = "desktop"
        log(f"Iniciando crawl para {raw_site_url} ({tipo_dispositivo})")
        if not MODO_OFFLINE:
            modo_auto_crawl()
        else:
            log("MODO OFFLINE ativado, apenas cache será usado.")

    log(f"Servidor rodando em http://0.0.0.0:{PORTA}")
    app.run(host="0.0.0.0", port=PORTA, debug=False, threaded=True)
