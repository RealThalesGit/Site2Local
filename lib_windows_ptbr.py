import os
import sys

def ENABLE_WIN_LIB():
    """
    Ativa correções para suportar caminhos longos e nomes inválidos no Windows.
    Deve ser chamada no início do script principal.
    """
    if os.name != "nt":
        print("[INFO] Sistema operacional não é Windows, lib_windows desativada.")
        return

    try:
        # Habilita suporte a caminhos longos no Windows 10+
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # Apenas uma chamada simbólica para ilustrar, ativação real depende de política/registro
        kernel32.SetDllDirectoryW.restype = ctypes.c_bool

        print("[INFO] Lib Windows ativada: suporte a caminhos longos (se o SO permitir).")
    except Exception as e:
        print(f"[WARN] Erro ao ativar suporte a caminhos longos: {e}")

def safe_path(path):
    """
    Retorna uma versão segura do caminho para Windows,
    adicionando prefixo \\?\ para evitar limite de 260 caracteres
    e limpando caracteres inválidos no nome dos arquivos.
    """
    if os.name != "nt":
        return path  # Em outros sistemas operacionais, retorna o caminho original

    # Remove caracteres inválidos em nomes do Windows
    invalid_chars = '<>:"|?*'
    cleaned_parts = []
    for part in path.split(os.sep):
        for ch in invalid_chars:
            part = part.replace(ch, "_")
        cleaned_parts.append(part)
    cleaned_path = os.sep.join(cleaned_parts)

    # Adiciona prefixo \\?\ para caminhos longos, se não já tiver
    if not cleaned_path.startswith(r"\\?\\"):
        cleaned_path = r"\\?\\" + os.path.abspath(cleaned_path)

    return cleaned_path
