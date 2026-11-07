"""Site2Local: pure byte/hex/codec helpers for the hook GUI (Qt-free).

Extracted verbatim from the GUI closure so they can be unit-tested and
reused without importing PyQt6. Match: s2l.py inside _launch_hook_gui.
"""
from __future__ import annotations

import codecs
import re


def _bytes_to_hexdump(data: bytes) -> str:
    """Format editable bytes without offsets or length fields."""
    if not data:
        return ""
    return "\n".join(
        " ".join(f"{b:02x}" for b in data[off:off + 16])
        for off in range(0, len(data), 16)
    )


def _hexdump_to_bytes(text: str):
    """Parse raw hex bytes or legacy offset-prefixed dumps."""
    out = bytearray()
    for line in text.splitlines():
        stripped = re.sub(
            r"^[0-9a-fA-F]{8}\s{2,}", "", line).strip()
        if not stripped:
            continue
        for tok in stripped.split():
            if not re.fullmatch(r"[0-9a-fA-F]+", tok):
                return None
            if len(tok) % 2 != 0:
                return None
            out.extend(bytes.fromhex(tok))
    return bytes(out)


def _ws_auto_encoding(data: bytes) -> str:
    """Detect common Unicode encodings; arbitrary legacy encodings are ambiguous."""
    for marker, encoding in (
        (codecs.BOM_UTF32_LE, "utf-32"),
        (codecs.BOM_UTF32_BE, "utf-32"),
        (codecs.BOM_UTF8, "utf-8-sig"),
        (codecs.BOM_UTF16_LE, "utf-16"),
        (codecs.BOM_UTF16_BE, "utf-16"),
    ):
        if data.startswith(marker):
            return encoding
    sample = data[:256]
    if len(sample) >= 4:
        even_nuls = sum(sample[i] == 0 for i in range(0, len(sample), 2))
        odd_nuls = sum(sample[i] == 0 for i in range(1, len(sample), 2))
        pairs = len(sample) // 2
        if even_nuls > pairs * 0.3 and odd_nuls == 0:
            return "utf-16-be"
        if odd_nuls > pairs * 0.3 and even_nuls == 0:
            return "utf-16-le"
    try:
        data.decode("utf-8", errors="strict")
        return "utf-8"
    except UnicodeDecodeError:
        return "cp1252"
