import os
import sys

def ATIVAR_LIB_WINDOWS():
    """
    Ativa suporte a caminhos longos no Windows 10+.
    Essa função deve ser chamada no início do script principal.
    Note que a ativação real do suporte a caminhos longos pode exigir
    configuração adicional via manifest ou registro do Windows.
    """
    if os.name != "nt":
        print("[INFO] Sistema operacional não é Windows, lib_windows desativada.")
        return

    try:
        import ctypes
        import platform

        versao = platform.version()
        versao_principal = int(versao.split('.')[0])

        if versao_principal < 10:
            print("[AVISO] Versão do Windows menor que 10, suporte a caminho longo limitado.")
            return

        kernel32 = ctypes.windll.kernel32

        print("[INFO] O suporte a caminhos longos no Windows requer configuração externa (manifest/registro).")
        print("[INFO] ATIVAR_LIB_WINDOWS() chamada, mas sem alteração direta via API.")

    except Exception as e:
        print(f"[AVISO] Exceção durante ATIVAR_LIB_WINDOWS: {e}")

def caminho_seguro(path: str) -> str:
    """
    Retorna uma versão segura do caminho para Windows,
    substituindo caracteres inválidos e adicionando o prefixo '\\\\?\\'
    para permitir caminhos maiores que 260 caracteres.

    Args:
        path (str): Caminho original.

    Returns:
        str: Caminho limpo e com prefixo para Windows.
    """
    if os.name != "nt":
        return path

    caracteres_invalidos = '<>:"|?*'
    partes = path.split(os.sep)

    partes_limpa = []
    for parte in partes:
        for ch in caracteres_invalidos:
            parte = parte.replace(ch, "_")
        # Remover espaços e pontos ao final do nome, que são inválidos
        parte = parte.rstrip(' .')
        partes_limpa.append(parte)

    caminho_limpo = os.sep.join(partes_limpa)

    if not os.path.isabs(caminho_limpo):
        caminho_limpo = os.path.abspath(caminho_limpo)

    if not caminho_limpo.startswith(r"\\?\"):
        caminho_limpo = r"\\?\\" + caminho_limpo

    return caminho_limpo

def remover_prefixo_caminho_longo(path: str) -> str:
    """
    Remove o prefixo '\\\\?\\' de um caminho Windows para compatibilidade
    com bibliotecas que não suportam esse prefixo.

    Args:
        path (str): Caminho possivelmente com prefixo longo.

    Returns:
        str: Caminho sem o prefixo longo.
    """
    if os.name != "nt":
        return path
    if path.startswith(r"\\?\"):
        return path[4:]
    return path
