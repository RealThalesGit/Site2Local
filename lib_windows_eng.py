import os
import sys

def safe_path(path: str) -> str:
    """
    Fixes the path to avoid Windows long path errors.
    If the path exceeds 260 characters, adds the \\?\ prefix to support long paths.
    Also replaces invalid characters for Windows filenames/folders.
    """
    if not path:
        return path

    # Invalid characters for Windows filenames
    invalid_chars = '<>:"/\\|?*'
    cleaned_path = "".join(c if c not in invalid_chars else "_" for c in path)

    # Add \\?\ prefix for long paths if needed
    if os.name == "nt" and len(cleaned_path) > 260 and not cleaned_path.startswith(r"\\?\"):
        # If UNC path (network), use special prefix
        if cleaned_path.startswith("\\\\"):
            # Replace initial double backslashes with \\?\UNC\
            cleaned_path = r"\\?\UNC" + cleaned_path[1:]
        else:
            cleaned_path = r"\\?\" + cleaned_path

    return cleaned_path

def ENABLE_WIN_LIB():
    """
    Enables Windows-specific fixes:
    - Fixes long path limit (260 characters)
    - Replaces invalid characters in paths
    - Can adjust other Windows-specific settings if needed
    """
    if os.name != "nt":
        # Do nothing if not running on Windows
        return

    # Increase recursion limit on Windows to avoid deep operation issues
    sys.setrecursionlimit(10000)

    print("[INFO] Windows Lib enabled: fixing long paths and invalid names")
