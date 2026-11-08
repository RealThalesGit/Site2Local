"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations

import time

from ... import Runtime as _s2l_state


def set_last_platform(name: str) -> None:
    """Update the WAF detection result shared by logs, banner, and viewer."""
    with _s2l_state._last_platform_lock:
        _s2l_state._last_platform["name"] = name or "Unknown"
        _s2l_state._last_platform["ts"] = time.time()


def get_last_platform() -> dict:
    with _s2l_state._last_platform_lock:
        return dict(_s2l_state._last_platform)


def detect_platform(target_url: str) -> str:
    """Actively fingerprint the upstream with WAFW00F."""
    if _s2l_state._WAFW00F is None:
        return "Unavailable"

    scanner = _s2l_state._WAFW00F(target_url, timeout=min(_s2l_state.TIMEOUT_READ, 10))
    if scanner.rq is None:
        raise RuntimeError(f"WAFW00F could not reach {target_url}")

    detected, _trigger_url = scanner.identwaf()
    if detected:
        return ", ".join(name.split(" (", 1)[0] for name in detected)
    if scanner.genericdetect():
        return "Generic WAF"
    return "No WAF detected"
