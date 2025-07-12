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

sys.setrecursionlimit(10000)

# ---------------- CONFIGURAÇÃO GLOBAL ----------------

MODO = "AUTO_MODE"
SITE_URL = "https://google.cat"
PORTA = 80

FORCAR_BYPASS_ACESSO_NEGADO = False  # Ativa táticas para evitar bloqueio Access Denied
ESCANEAR_CAMINHOS_OCULTOS = False    # Ativa busca por URLs ocultas (admin, login, etc)
HABILITAR_ELEMENTOS_OCULTOS = False  # Durante crawling, ativa elementos ocultos no HTML
MOSTRAR_ELEMENTOS_OCULTOS = False    # Na resposta HTTP, mostra elementos ocultos
HABILITAR_CRAWLING = True             # Ativa crawler automático
DISPOSITIVO_HEADER = "desktop"       # desktop, mobile, tablet, bot, auto
ACEITAR_TODOS_MIRRORS = True          # Aceitar automaticamente download de mirrors

# ----------- Variáveis internas -------------
visitados = set()
MAX_VISITADOS = 1000
app = Flask(__name__, static_folder=None)

# Definir caminhos para salvar arquivos localmente
NOME_SITE = urlparse(SITE_URL).netloc.replace("www.", "").replace(".", "_")
PASTA_SITE_SRC = os.path.join("site_src", f"{NOME_SITE}_{DISPOSITIVO_HEADER}")
PASTA_SITE_DATA = os.path.join("site_data", f"{NOME_SITE}_{DISPOSITIVO_HEADER}")

# ---------- Funções utilitárias -------------

def detectar_dispositivo():
    # Detecta o tipo de dispositivo baseado no User-Agent ou configuração manual
    if DISPOSITIVO_HEADER != "auto":
        return DISPOSITIVO_HEADER.lower()
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

def obter_headers_por_dispositivo(dispositivo):
    # Retorna headers HTTP apropriados para o tipo de dispositivo
    headers_base = {
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
    return headers_base.get(dispositivo, headers_base["desktop"])

def caminho_local(url):
    # Gera um caminho local seguro para salvar o arquivo baixado
    p = urlparse(url)
    caminho = p.path
    if caminho.endswith("/"):
        caminho += "index.html"
    if not os.path.splitext(caminho)[1]:
        caminho = os.path.join(caminho, "index.html")
    caminho_completo = os.path.join(PASTA_SITE_SRC, p.netloc, caminho.lstrip("/"))
    return caminho_completo

def caminho_local_para_flask(url):
    # Gera caminho local para enviar arquivo via Flask (sem prefixos especiais)
    p = urlparse(url)
    caminho = p.path
    if caminho.endswith("/"):
        caminho += "index.html"
    if not os.path.splitext(caminho)[1]:
        caminho = os.path.join(caminho, "index.html")
    return os.path.join(PASTA_SITE_SRC, p.netloc, caminho.lstrip("/"))

def tentar_descomprimir_se_texto(resposta):
    """
    Tenta descomprimir o conteúdo da resposta se o Content-Type for texto/html, json, etc.
    Caso contrário, retorna o conteúdo bruto sem descompressão.
    """
    # (Função para descompressão condicional conforme tipo de conteúdo, omitida aqui para brevidade)

def arquivo_ja_baixado(url):
    # Verifica se o arquivo já existe localmente para evitar downloads repetidos
    return os.path.exists(caminho_local(url))

def salvar_conteudo(url, conteudo):
    # Salva o conteúdo recebido na URL no caminho local apropriado
    caminho = caminho_local(url)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    with open(caminho, "wb") as f:
        f.write(conteudo)
    return caminho

def modificar_html_para_visibilidade(soup):
    """
    Remove estilos CSS e atributos que escondem elementos para tornar
    o conteúdo oculto visível no HTML.
    """
    # Remove estilos 'display:none', 'visibility:hidden', 'opacity:0' e atributos 'hidden', 'disabled', 'readonly'
    # Além disso, converte atributos 'data-href' em links clicáveis

def url_valida(url):
    # Valida se a URL tem esquema e domínio corretos
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

# ---------------- Controle de Mirrors -----------------

def perguntar_usuario_sobre_mirror(nome_arquivo, url_mirror):
    global ACEITAR_TODOS_MIRRORS
    if ACEITAR_TODOS_MIRRORS:
        return True
    print(f"\nMirror detectado: {url_mirror}\nArquivo: {nome_arquivo}")
    print("[S] Sim   [N] Não   [A] Aceitar todos")
    while True:
        escolha = input("Sua escolha (S/N/A): ").strip().lower()
        if escolha == "s":
            return True
        elif escolha == "n":
            return False
        elif escolha == "a":
            ACEITAR_TODOS_MIRRORS = True
            return True

def checar_e_baixar_mirror(url):
    if perguntar_usuario_sobre_mirror(os.path.basename(urlparse(url).path), url):
        crawl(url)

# --------------- Rastreamento & Download ----------------

def baixar(url):
    if arquivo_ja_baixado(url):
        print(f"[CACHE] {url}")
        return caminho_local(url)
    dispositivo = detectar_dispositivo()
    headers = obter_headers_por_dispositivo(dispositivo)

    # Forçar bypass para Access Denied (exemplo simples)
    if FORCAR_BYPASS_ACESSO_NEGADO:
        headers["Referer"] = SITE_URL
        headers["Cookie"] = "security_bypass=true"

    try:
        print(f"[GET] {url}")
        resposta = requests.get(url, headers=headers, timeout=15)
        resposta.raise_for_status()
        # Não tenta descomprimir arquivos binários; conteúdo retornado "cru"
        conteudo = resposta.content
        return salvar_conteudo(url, conteudo)
    except Exception as e:
        print(f"[ERRO] {url}: {e}")
        return None

def e_html(conteudo):
    c = conteudo.strip().lower()
    return c.startswith(b"<!doctype") or b"<html" in c

def crawl(url):
    global visitados
    if len(visitados) >= MAX_VISITADOS:
        print("[AVISO] Número máximo de URLs visitadas alcançado")
        return
    if url in visitados:
        return
    visitados.add(url)

    caminho_salvo = baixar(url)
    if not caminho_salvo:
        return
    with open(caminho_salvo, "rb") as f:
        conteudo = f.read()

    if not e_html(conteudo):
        return

    soup = BeautifulSoup(conteudo, "html.parser")

    if HABILITAR_ELEMENTOS_OCULTOS:
        modificar_html_para_visibilidade(soup)
        with open(caminho_salvo, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Busca links para crawling
    tags_e_atributos = {
        "script": "src",
        "link": "href",
        "img": "src",
        "source": "src",
        "video": "src",
        "audio": "src",
    }
    for tag, attr in tags_e_atributos.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if not src:
                continue
            url_completa = urljoin(url, src)
            if url_valida(url_completa):
                if urlparse(url_completa).netloc == urlparse(SITE_URL).netloc:
                    crawl(url_completa)
                else:
                    checar_e_baixar_mirror(url_completa)

    # Busca links internos <a href>
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        if link.startswith(SITE_URL):
            crawl(link)

    # Escanear caminhos ocultos (admin, .git, etc)
    if ESCANEAR_CAMINHOS_OCULTOS:
        caminhos_comuns_ocultos = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for hp in caminhos_comuns_ocultos:
            hp_url = urljoin(SITE_URL + "/", hp)
            try:
                r = requests.get(hp_url, headers=obter_headers_por_dispositivo(detectar_dispositivo()), timeout=10)
                if r.status_code == 200 and e_html(r.content):
                    print(f"[OCULTO] {hp_url}")
                    crawl(hp_url)
            except:
                pass

# ----------------- Proxy Flask ----------------------

@app.route("/", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def proxy(path):
    target = urljoin(SITE_URL + "/", path)
    if request.query_string:
        target += "?" + request.query_string.decode()

    local_interno = caminho_local(target)
    local_flask = local_interno  # Sem prefixos especiais para Windows

    if request.method == "POST":
        dados = request.get_data()
        os.makedirs(PASTA_SITE_DATA, exist_ok=True)
        h = hashlib.sha256(target.encode() + dados).hexdigest()
        with open(os.path.join(PASTA_SITE_DATA, h + ".json"), "wb") as f:
            f.write(dados)
        try:
            r = requests.post(target, data=dados, headers=request.headers)
            return Response(r.content, status=r.status_code, content_type=r.headers.get("Content-Type"))
        except Exception as e:
            return Response(f"Erro: {e}", status=502)

    if os.path.exists(local_interno):
        mime = mimetypes.guess_type(local_flask)[0] or "application/octet-stream"
        if MOSTRAR_ELEMENTOS_OCULTOS:
            # Lê e modifica conteúdo para mostrar elementos ocultos na resposta
            with open(local_interno, "r", encoding="utf-8", errors="ignore") as f:
                html = f.read()
            soup = BeautifulSoup(html, "html.parser")
            modificar_html_para_visibilidade(soup)
            return Response(str(soup), mimetype="text/html")
        else:
            return send_file(local_flask, mimetype=mime, conditional=True)

    # Se arquivo não existe localmente, baixa online e salva
    try:
        headers = obter_headers_por_dispositivo(detectar_dispositivo())
        if FORCAR_BYPASS_ACESSO_NEGADO:
            headers["Referer"] = SITE_URL
            headers["Cookie"] = "security_bypass=true"
        r = requests.get(target, headers=headers)
        r.raise_for_status()
        os.makedirs(os.path.dirname(local_interno), exist_ok=True)
        with open(local_interno, "wb") as f:
            f.write(r.content)
        mime = mimetypes.guess_type(local_flask)[0] or "application/octet-stream"
        return send_file(local_flask, mimetype=mime, conditional=True)
    except Exception as e:
        return Response(f"Erro no proxy: {e}", status=502)

if __name__ == "__main__":
    if not os.path.exists(PASTA_SITE_SRC):
        os.makedirs(PASTA_SITE_SRC)
    if HABILITAR_CRAWLING:
        crawl(SITE_URL)
    print(f"Servidor em execução: http://127.0.0.1:{PORTA}")
    app.run(host="0.0.0.0", port=PORTA)
