import os
import sys

def ENABLE_WIN_LIB():
    """
    Enables Windows long path support if OS version allows it (Windows 10+).
    This tries to enable the process-wide flag for long paths support via WinAPI.
    Note: On Windows, this also requires registry and policy changes outside Python.
    Call this once at the start of your program.
    """
    if os.name != "nt":
        print("[INFO] Not running on Windows, skipping Windows-specific enable.")
        return

    try:
        import ctypes
        import platform

        # Windows 10 build 14352+ supports long paths if enabled in manifest and policy
        version = platform.version()
        major_version = int(version.split('.')[0])

        if major_version < 10:
            print("[WARN] Windows version < 10, long path support limited.")
            return

        # Try to enable long path awareness for current process
        kernel32 = ctypes.windll.kernel32

        # This flag: PROCESS_CREATION_MITIGATION_POLICY_BLOCK_NON_MICROSOFT_BINARIES_ALWAYS_ON = 0x1
        # Not directly for long paths, but we can set long path awareness via manifest or registry.
        # Here we just print info, because actual enabling requires app manifest or group policy.

        # So, practically this is a no-op but leave this for future use or other tweaks.
        print("[INFO] Windows long path support requires external config (manifest/registry).")
        print("[INFO] ENABLE_WIN_LIB() called, but no direct API change made.")

    except Exception as e:
        print(f"[WARN] Exception during ENABLE_WIN_LIB: {e}")

def safe_path(path: str) -> str:
    """
    Cleans a Windows file path by replacing invalid characters and
    adds the '\\\\?\\' prefix to allow paths longer than 260 characters.

    Args:
        path (str): Original file system path.

    Returns:
        str: Sanitized and prefixed Windows path safe for long paths.
    """
    if os.name != "nt":
        # No changes needed on non-Windows systems
        return path

    invalid_chars = '<>:"|?*'
    parts = path.split(os.sep)

    cleaned_parts = []
    for part in parts:
        for ch in invalid_chars:
            part = part.replace(ch, "_")
        # Windows names cannot end with a space or dot
        part = part.rstrip(' .')
        cleaned_parts.append(part)

    cleaned_path = os.sep.join(cleaned_parts)

    # Convert relative path to absolute to avoid issues with prefix
    if not os.path.isabs(cleaned_path):
        cleaned_path = os.path.abspath(cleaned_path)

    # Add \\?\ prefix if not already present
    if not cleaned_path.startswith(r"\\?\"):
        cleaned_path = r"\\?\\" + cleaned_path

    return cleaned_path

def remove_long_path_prefix(path: str) -> str:
    """
    Removes the '\\\\?\\' prefix from a Windows path for compatibility
    with libraries that don't support long path prefix.

    Args:
        path (str): Windows path possibly with long path prefix.

    Returns:
        str: Path without the '\\\\?\\' prefix.
    """
    if os.name != "nt":
        return path
    if path.startswith(r"\\?\"):
        return path[4:]
    return path
