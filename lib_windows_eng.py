import os
import platform

def is_windows() -> bool:
    """
    Detect if the current system is Windows.
    """
    return platform.system().lower() == "windows"

def is_android() -> bool:
    """
    Detect if the current system is Android.
    """
    # Android identifies as 'Linux' but usually has /system/build.prop file
    try:
        with open('/system/build.prop', 'r'):
            return True
    except Exception:
        pass
    return False

def is_linux() -> bool:
    """
    Detect if the current system is Linux (excluding Android).
    """
    system = platform.system().lower()
    return system == "linux" and not is_android()

def is_macos() -> bool:
    """
    Detect if the current system is macOS (Darwin).
    """
    return platform.system().lower() == "darwin"

def safe_path(path: str) -> str:
    """
    Returns a safe absolute path for reading/writing.
    On Windows adds '\\?\' prefix for long path support.
    On Android, Linux and macOS returns the normal absolute path.
    """
    abs_path = os.path.abspath(path)
    if is_windows():
        if not abs_path.startswith(r"\\?\\"):
            abs_path = r"\\?\\" + abs_path
    # For Android, Linux and macOS no change needed
    return abs_path

def remove_long_path_prefix(path: str) -> str:
    """
    Removes the Windows '\\?\' prefix if present.
    For other systems returns path unchanged.
    """
    if is_windows():
        if path.startswith(r"\\?\\"):
            return path[4:]
    return path

def enable_windows_lib():
    """
    Function to activate and inform status of the lib.
    """
    if is_windows():
        print("[INFO] lib_windows_eng enabled on Windows")
    elif is_android():
        print("[INFO] lib_windows_eng detected Android - no special path handling")
    elif is_linux():
        print("[INFO] lib_windows_eng detected Linux - no special path handling")
    elif is_macos():
        print("[INFO] lib_windows_eng detected macOS - no special path handling")
    else:
        print("[INFO] lib_windows_eng running on unknown system - no special path handling")
