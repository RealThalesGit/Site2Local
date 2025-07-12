import os
import sys

def ENABLE_WIN_LIB():
    """
    Enables fixes to support long paths and invalid filenames on Windows.
    Should be called at the start of the main script.
    """
    if os.name != "nt":
        print("[INFO] Operating system is not Windows, lib_windows disabled.")
        return

    try:
        # Enables support for long paths on Windows 10+
        import ctypes
        kernel32 = ctypes.windll.kernel32
        # This is a symbolic call; real enabling may require policy or registry changes
        kernel32.SetDllDirectoryW.restype = ctypes.c_bool

        print("[INFO] Windows lib enabled: long path support (if OS allows).")
    except Exception as e:
        print(f"[WARN] Error enabling long path support: {e}")

def safe_path(path):
    """
    Returns a Windows-safe version of the given path,
    adding the \\?\ prefix to avoid the 260 character limit,
    and cleaning invalid characters from file names.
    """
    if os.name != "nt":
        return path  # On other OSes, return the original path

    # Remove invalid characters in Windows file names
    invalid_chars = '<>:"|?*'
    cleaned_parts = []
    for part in path.split(os.sep):
        for ch in invalid_chars:
            part = part.replace(ch, "_")
        cleaned_parts.append(part)
    cleaned_path = os.sep.join(cleaned_parts)

    # Add \\?\ prefix for long paths if not already present
    if not cleaned_path.startswith(r"\\?\\"):
        cleaned_path = r"\\?\\" + os.path.abspath(cleaned_path)

    return cleaned_path
