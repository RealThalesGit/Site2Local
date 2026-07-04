# Te amo chatgpt
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
sys.setrecursionlimit(20000)  # Crawling profundo
colorama.init(autoreset=True)  # Cores no terminal

# -------------------- CORES PARA LOGS --------------------
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

# -------------------- CONFIGURAÇÕES DO USUÁRIO --------------------
site_base_bruto = "speedtest.net"  # Sem http:// ou https://
PORTA = 8080
CABECALHO_DISPOSITIVO = "mobile"  # desktop, mobile, tablet, bot, auto

MODO_OFFLINE = False
SALVAR_TRAFEGO = False
HABILITAR_CRAWLING = True
MOSTRAR_OCULTOS = True
VERIFICAR_CAMINHOS_OCULTOS = True

ACEITAR_TODOS_MIRRORS = True  # Obsoleto, mantido por compatibilidade

# -------------------- CONSTRUÇÃO DA URL BASE --------------------
def construir_url_base(url):
    for esquema in ["https://", "http://"]:
        teste = esquema + url
        try:
            r = requests.head(teste, timeout=5)
            if r.status_code < 400:
                log(f"Usando {esquema.upper().strip('://')} para {url}")
                return teste
        except Exception:
            continue
    log(f"Site {url} inacessível, ativando MODO OFFLINE", "WARN")
    return None

URL_SITE = construir_url_base(site_base_bruto)
if URL_SITE is None:
    MODO_OFFLINE = True
    URL_SITE = "http://" + site_base_bruto

# -------------------- PASTAS POR DISPOSITIVO --------------------
nome_site = urlparse(URL_SITE).netloc.replace("www.", "").replace(".", "_")
tipo_dispositivo = CABECALHO_DISPOSITIVO if CABECALHO_DISPOSITIVO != "auto" else "desktop"

PASTA_SRC = os.path.join("site_src", f"{nome_site}_{tipo_dispositivo}")
PASTA_DADOS = os.path.join("site_data", f"{nome_site}_{tipo_dispositivo}")
ARQ_CACHE_TRAFEGO = os.path.join(PASTA_DADOS, "traffic_cache.json")

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
trava_trafego = threading.Lock()
trafego_salvo = {}

# -------------------- APLICATIVO FLASK --------------------
app = Flask(__name__, static_folder=None)

# -------------------- FUNÇÕES DE UTILIDADE --------------------
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

def cabecalhos_por_dispositivo(dispositivo):
    base = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": URL_SITE,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Sec-CH-UA": '"Chromium";v="115", "Not(A:Brand";v="8"',
        "Sec-CH-UA-Platform": '"Windows"' if dispositivo == "desktop" else '"Android"',
        "Sec-CH-UA-Mobile": "?0" if dispositivo == "desktop" else "?1"
    }
    if dispositivo == "mobile":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Mobile Safari/537.36"
    elif dispositivo == "tablet":
        base["User-Agent"] = "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"
    elif dispositivo == "bot":
        base["User-Agent"] = "Googlebot/2.1 (+http://www.google.com/bot.html)"
        base["Accept"] = "*/*"
    else:  # desktop
        base["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.5790.171 Safari/537.36"
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
    # -------------------- FUNÇÃO DE CRAWLING --------------------
def rastrear_url(url):
    url = remover_fragmento(url)
    if url in visitados:
        return
    visitados.add(url)

    if ja_baixado(url):
        log(f"[CACHE] {url}")
        return

    dispositivo = tipo_dispositivo if CABECALHO_DISPOSITIVO != "auto" else "desktop"
    headers = cabecalhos_por_dispositivo(dispositivo)

    try:
        log(f"[GET] {url} [{dispositivo}]")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        conteudo = r.content  # Salva conteúdo bruto, sem descompressão

        # Salvar diretamente se não for HTML
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
                url_completa = urljoin(url, link)
                if url_valida(url_completa):
                    urls_descobertas.append(url_completa)

        # Links <a> internos ao site
        for a in soup.find_all("a", href=True):
            link = urljoin(url, a["href"])
            if link.startswith(URL_SITE):
                urls_descobertas.append(link)

        if VERIFICAR_CAMINHOS_OCULTOS:
            caminhos_ocultos = ["admin", "login", "panel", ".git", ".env", "config", "backup", "db", "private", "secret"]
            for co in caminhos_ocultos:
                url_oculta = urljoin(URL_SITE + "/", co)
                try:
                    r = requests.head(url_oculta, headers=headers, timeout=5)
                    if r.status_code == 200 and url_oculta not in visitados:
                        log(f"[CAMINHO OCULTO ENCONTRADO] {url_oculta}", "MAGENTA")
                        urls_descobertas.append(url_oculta)
                except Exception:
                    pass

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(rastrear_url, urls_descobertas)

    except Exception as e:
        log(f"[ERRO] {url}: {e}", "ERROR")

# -------------------- CRAWLING AUTOMÁTICO --------------------
def crawl_auto():
    global URL_SITE
    global MODO_OFFLINE

    log(f"Iniciando crawl em MODO AUTOMÁTICO para {URL_SITE}")
    try:
        https_url = "https://" + urlparse(URL_SITE).netloc
        r = requests.head(https_url, timeout=5)
        if r.status_code < 400:
            log("HTTPS disponível, usando HTTPS")
            URL_SITE = https_url
        else:
            http_url = "http://" + urlparse(URL_SITE).netloc
            r2 = requests.head(http_url, timeout=5)
            if r2.status_code < 400:
                log("HTTPS não disponível, usando HTTP")
                URL_SITE = http_url
            else:
                log("Site offline, ativando modo OFFLINE local")
                MODO_OFFLINE = True

        if not MODO_OFFLINE:
            rastrear_url(URL_SITE)
        else:
            log("MODO OFFLINE ativado, servindo do cache")

    except Exception as e:
        log(f"[ERRO NO MODO AUTOMÁTICO] {e}", "ERROR")
        MODO_OFFLINE = True

# -------------------- SUPORTE A MIRRORS --------------------
def perguntar_sobre_mirror(nome_arquivo, url_mirror):
    global ACEITAR_TODOS_MIRRORS
    if ACEITAR_TODOS_MIRRORS:
        return True
    print(f"\nUm mirror foi encontrado. Deseja baixar o arquivo {nome_arquivo} do mirror {url_mirror}?")
    print("[S] Sim   [N] Não   [A] Aceitar todos a partir de agora")
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
            print("Opção inválida, tente novamente.")

def baixar_com_mirrors(url, mirrors):
    nome_arquivo = url.split("/")[-1]
    for mirror in mirrors:
        try:
            if perguntar_sobre_mirror(nome_arquivo, mirror):
                log(f"Baixando {nome_arquivo} do mirror {mirror}")
                r = requests.get(mirror, timeout=15)
                r.raise_for_status()
                return r.content
        except Exception as e:
            log(f"Falha ao baixar do mirror {mirror}: {e}", "WARN")
    log(f"Falha ao baixar {nome_arquivo} de todos os mirrors", "ERROR")
    return None

# -------------------- PROXY COM FLASK --------------------
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>', methods=["GET", "POST"])
def proxy(path):
    url_completa = urljoin(URL_SITE + "/", path)
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
            headers = cabecalhos_por_dispositivo(detectar_dispositivo())
            r = requests.post(url_completa, data=dados, headers=headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"[ERRO POST] {e}", status=502)

    if os.path.exists(arquivo_local):
        mime = mimetypes.guess_type(arquivo_local)[0] or "application/octet-stream"
        try:
            return send_file(arquivo_local, mimetype=mime, conditional=True)
        except Exception as e:
            log(f"[ERRO] Enviando arquivo {arquivo_local}: {e}", "ERROR")
            return Response(f"Erro ao ler arquivo {arquivo_local}", status=500)

    # Se não existe localmente, tenta buscar no site remoto
    dispositivo = detectar_dispositivo()
    headers = cabecalhos_por_dispositivo(dispositivo)
    try:
        log(f"[PROXY GET] {url_completa} [{dispositivo}]")
        r = requests.get(url_completa, headers=headers, timeout=15, stream=True)
        r.raise_for_status()
        conteudo = r.content  # Salvar conteúdo bruto (sem descompressão)
        salvar_arquivo(url_completa, conteudo)

        # Responder com headers originais para tratamento correto do conteúdo
        resposta = Response(conteudo, status=r.status_code)
        resposta.headers['Content-Type'] = r.headers.get('Content-Type', 'application/octet-stream')
        resposta.headers['Content-Encoding'] = r.headers.get('Content-Encoding', '')
        resposta.headers['Cache-Control'] = r.headers.get('Cache-Control', 'no-cache')

        return resposta

    except requests.exceptions.RequestException as e:
        log(f"[ERRO] Falha ao proxy {url_completa}: {e}", "ERROR")
        return Response(f"Falha ao buscar {url_completa}", status=502)

# -------------------- EXECUÇÃO PRINCIPAL --------------------
if __name__ == "__main__":
    if not MODO_OFFLINE and HABILITAR_CRAWLING:
        log(f"Iniciando crawl para {URL_SITE} ({tipo_dispositivo})")
        crawl_auto()
    else:
        log("Modo offline ou crawling desabilitado, servindo arquivos do cache")

    os.makedirs(PASTA_SRC, exist_ok=True)
    app.run(host="0.0.0.0", port=PORTA, debug=False, threaded=True)
