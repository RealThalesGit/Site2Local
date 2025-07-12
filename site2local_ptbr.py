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

# Importa lib Windows para tratar caminhos longos e inválidos
try:
    from lib_windows_eng import safe_path, remove_long_path_prefix, enable_windows_lib
    enable_windows_lib()
except Exception as e:
    print(f"[AVISO] lib_windows_eng não pôde ser ativada: {e}")

sys.setrecursionlimit(10000)

# ---------------- CONFIGURAÇÃO GLOBAL ----------------

MODO = "AUTO_MODE"
URL_SITE = "https://google.cat"
PORTA = 80

FORCAR_BYPASS_ACCESS_DENIED = False  # Ativa táticas para evitar bloqueios Access Denied
VARREDURA_CAMINHOS_OCULTOS = False  # Ativa busca por URLs ocultas (admin, login etc)
ATIVAR_ELEMENTOS_OCULTOS = False    # Durante crawling, habilita elementos ocultos no HTML
MOSTRAR_ELEMENTOS_OCULTOS = False   # Durante resposta HTTP, exibe elementos ocultos
ATIVAR_CRAWLING = True              # Ativa crawler automático
HEADER_DISPOSITIVO = "desktop"       # desktop, mobile, tablet, bot, auto
ACEITAR_TODOS_MIRRORS = True         # Aceita baixar mirrors automaticamente

# ----------- Variáveis internas -------------
visitados = set()
MAX_VISITADOS = 1000
app = Flask(__name__, static_folder=None)

# Define caminhos para salvar arquivos localmente
NOME_SITE = urlparse(URL_SITE).netloc.replace("www.", "").replace(".", "_")
FONTE_SITE = os.path.join("site_src", f"{NOME_SITE}_{HEADER_DISPOSITIVO}")
DADOS_SITE = os.path.join("site_data", f"{NOME_SITE}_{HEADER_DISPOSITIVO}")

# ---------- Funções utilitárias -------------

def detectar_dispositivo():
    if HEADER_DISPOSITIVO != "auto":
        return HEADER_DISPOSITIVO.lower()
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

def cabecalhos_para_dispositivo(dispositivo):
    base_headers = {
        "desktop": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "mobile": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Mobile Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "tablet": {
            "User-Agent": "Mozilla/5.0 (Linux; Android 10; Tablet) AppleWebKit/537.36 (KHTML, como Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Accept-Encoding": "br, gzip",
        },
        "bot": {
            "User-Agent": "Googlebot/2.1 (+http://www.google.com/bot.html)",
            "Accept-Encoding": "gzip, deflate",
        },
    }
    return base_headers.get(dispositivo, base_headers["desktop"])

def caminho_local(url):
    """Caminho seguro com prefixo para ler/gravar arquivo localmente"""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    caminho_completo = os.path.join(FONTE_SITE, p.netloc, path.lstrip("/"))
    return safe_path(caminho_completo)

def caminho_local_para_flask(url):
    """Caminho sem prefixo para enviar arquivo via Flask"""
    p = urlparse(url)
    path = p.path
    if path.endswith("/"):
        path += "index.html"
    if not os.path.splitext(path)[1]:
        path = os.path.join(path, "index.html")
    return os.path.join(FONTE_SITE, p.netloc, path.lstrip("/"))

def tentar_descompactar(r):
    conteudo = r.content
    encoding = r.headers.get("Content-Encoding", "")
    if "br" in encoding:
        try:
            return brotli.decompress(conteudo)
        except Exception as e:
            print(f"[AVISO] Falha ao descompactar Brotli: {e}")
    if "gzip" in encoding:
        try:
            return gzip.decompress(conteudo)
        except Exception as e:
            print(f"[AVISO] Falha ao descompactar Gzip: {e}")
    return conteudo

def ja_baixado(url):
    return os.path.exists(caminho_local(url))

def salvar_conteudo(url, conteudo):
    path = caminho_local(url)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(conteudo)
    return path

def modificar_html_para_visibilidade(soup):
    """Remove ocultação CSS e atributos para mostrar conteúdos escondidos"""
    for el in soup.select("[style*='display:none'], [style*='visibility:hidden'], [style*='opacity:0']"):
        el['style'] = "display:block !important; visibility:visible !important; opacity:1 !important; background:yellow; border:2px dashed red;"
    for attr in ["hidden", "disabled", "readonly"]:
        for el in soup.select(f"[{attr}]"):
            del el[attr]
    # Converte atributos data-href para href para tornar clicáveis
    for el in soup.find_all(attrs={"data-href": True}):
        el.name = "a"
        el["href"] = el["data-href"]
        el.string = el.get_text() or el["data-href"]

def url_valida(url):
    p = urlparse(url)
    return bool(p.netloc) and bool(p.scheme)

# ---------------- Tratamento de Mirrors -----------------

def perguntar_ao_usuario_sobre_mirror(nome_arquivo, url_mirror):
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

def verificar_e_baixar_mirror(url):
    if perguntar_ao_usuario_sobre_mirror(os.path.basename(urlparse(url).path), url):
        varrer(url)

# --------------- Crawling e Download ----------------

def baixar(url):
    if ja_baixado(url):
        print(f"[CACHE] {url}")
        return caminho_local(url)
    dispositivo = detectar_dispositivo()
    headers = cabecalhos_para_dispositivo(dispositivo)

    # Bypass Access Denied (exemplo simples)
    if FORCAR_BYPASS_ACCESS_DENIED:
        headers["Referer"] = URL_SITE
        headers["Cookie"] = "security_bypass=true"

    try:
        print(f"[GET] {url}")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        conteudo = tentar_descompactar(r)
        return salvar_conteudo(url, conteudo)
    except Exception as e:
        print(f"[ERRO] {url}: {e}")
        return None

def is_html(conteudo):
    c = conteudo.strip().lower()
    return c.startswith(b"<!doctype") or b"<html" in c

def varrer(url):
    global visitados
    if len(visitados) >= MAX_VISITADOS:
        print("[AVISO] Limite máximo de URLs visitadas atingido")
        return
    if url in visitados:
        return
    visitados.add(url)

    caminho_salvo = baixar(url)
    if not caminho_salvo:
        return
    with open(caminho_salvo, "rb") as f:
        conteudo = f.read()

    if not is_html(conteudo):
        return

    soup = BeautifulSoup(conteudo, "html.parser")

    if ATIVAR_ELEMENTOS_OCULTOS:
        modificar_html_para_visibilidade(soup)
        with open(caminho_salvo, "w", encoding="utf-8") as f:
            f.write(str(soup))

    # Busca links para crawling
    tags_attrs = {
        "script": "src",
        "link": "href",
        "img": "src",
        "source": "src",
        "video": "src",
        "audio": "src",
    }
    for tag, attr in tags_attrs.items():
        for el in soup.find_all(tag):
            src = el.get(attr)
            if not src:
                continue
            url_completa = urljoin(url, src)
            if url_valida(url_completa):
                if urlparse(url_completa).netloc == urlparse(URL_SITE).netloc:
                    varrer(url_completa)
                else:
                    verificar_e_baixar_mirror(url_completa)

    # Busca links <a href> internos
    for a in soup.find_all("a", href=True):
        link = urljoin(url, a["href"])
        if link.startswith(URL_SITE):
            varrer(link)

    # Varredura de caminhos ocultos (admin, .git, etc)
    if VARREDURA_CAMINHOS_OCULTOS:
        comuns_ocultos = ["admin", "login", "panel", "dashboard", ".git", ".env"]
        for caminho_oculto in comuns_ocultos:
            url_oculta = urljoin(URL_SITE + "/", caminho_oculto)
            try:
                r = requests.get(url_oculta, headers=cabecalhos_para_dispositivo(detectar_dispositivo()), timeout=10)
                if r.status_code == 200 and is_html(r.content):
                    print(f"[OCULTO] {url_oculta}")
                    varrer(url_oculta)
            except:
                pass

# ----------------- Proxy Flask ----------------------

@app.route("/", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def proxy(path):
    alvo = urljoin(URL_SITE + "/", path)
    if request.query_string:
        alvo += "?" + request.query_string.decode()

    local_interno = caminho_local(alvo)
    local_flask = remove_long_path_prefix(local_interno)

    if request.method == "POST":
        dados = request.get_data()
        os.makedirs(DADOS_SITE, exist_ok=True)
        h = hashlib.sha256(alvo.encode() + dados).hexdigest()
        with open(safe_path(os.path.join(DADOS_SITE, h + ".json")), "wb") as f:
            f.write(dados)
        try:
            r = requests.post(alvo, data=dados, headers=request.headers)
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

    # Se arquivo não existe localmente, busca online e salva
    try:
        headers = cabecalhos_para_dispositivo(detectar_dispositivo())
        if FORCAR_BYPASS_ACCESS_DENIED:
            headers["Referer"] = URL_SITE
            headers["Cookie"] = "security_bypass=true"
        r = requests.get(alvo, headers=headers)
        r.raise_for_status()
        conteudo = tentar_descompactar(r)
        os.makedirs(os.path.dirname(local_interno), exist_ok=True)
        with open(local_interno, "wb") as f:
            f.write(conteudo)
        return Response(conteudo, status=r.status_code, content_type=r.headers.get("Content-Type"))
    except Exception as e:
        return Response(f"Erro remoto: {e}", status=500)

# -------------------- PRINCIPAL ----------------------------

if __name__ == "__main__":
    os.makedirs(FONTE_SITE, exist_ok=True)
    os.makedirs(DADOS_SITE, exist_ok=True)
    if ATIVAR_CRAWLING:
        print(f"Iniciando crawling do site: {URL_SITE} (modo: {MODO})")
        varrer(URL_SITE)
    else:
        print("Crawling desativado.")
    print(f"Arquivos salvos em: {os.path.abspath(FONTE_SITE)}")
    print(f"POSTs salvos em: {os.path.abspath(DADOS_SITE)}")
    print(f"Servidor rodando em: http://127.0.0.1:{PORTA}")
    app.run(host="0.0.0.0", port=PORTA)
