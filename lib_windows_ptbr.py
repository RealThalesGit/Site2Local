import os
import platform

def is_windows() -> bool:
    """
    Detecta se o sistema atual é Windows.
    """
    return platform.system().lower() == "windows"

def is_android() -> bool:
    """
    Detecta se o sistema atual é Android.
    """
    # Android se identifica como 'Linux', mas geralmente possui o arquivo /system/build.prop
    try:
        with open('/system/build.prop', 'r'):
            return True
    except Exception:
        pass
    return False

def is_linux() -> bool:
    """
    Detecta se o sistema atual é Linux (excluindo Android).
    """
    system = platform.system().lower()
    return system == "linux" and not is_android()

def is_macos() -> bool:
    """
    Detecta se o sistema atual é macOS (Darwin).
    """
    return platform.system().lower() == "darwin"

def safe_path(path: str) -> str:
    """
    Retorna um caminho absoluto seguro para leitura/escrita.
    No Windows adiciona o prefixo '\\?\' para suporte a caminhos longos.
    No Android, Linux e macOS retorna o caminho absoluto normal.
    """
    abs_path = os.path.abspath(path)
    if is_windows():
        if not abs_path.startswith(r"\\?\\"):
            abs_path = r"\\?\\" + abs_path
    # No Android, Linux e macOS não é necessário alterar o caminho
    return abs_path

def remove_long_path_prefix(path: str) -> str:
    """
    Remove o prefixo '\\?\' do Windows se presente.
    Para outros sistemas retorna o caminho sem alterações.
    """
    if is_windows():
        if path.startswith(r"\\?\\"):
            return path[4:]
    return path

def enable_windows_lib():
    """
    Função para ativar e informar o status da biblioteca.
    """
    if is_windows():
        print("[INFO] lib_windows_eng ativada no Windows")
    elif is_android():
        print("[INFO] lib_windows_eng detectou Android - sem tratamento especial de caminho")
    elif is_linux():
        print("[INFO] lib_windows_eng detectou Linux - sem tratamento especial de caminho")
    elif is_macos():
        print("[INFO] lib_windows_eng detectou macOS - sem tratamento especial de caminho")
    else:
        print("[INFO] lib_windows_eng rodando em sistema desconhecido - sem tratamento especial de caminho")
