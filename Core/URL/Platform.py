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

    class GuardedScanner(_s2l_state._WAFW00F):
        def Request(self, headers=None, path=None, params=None, delay=0):
            # WAFW00F's own transport bypasses our sessions and forces
            # verify=False. Keep its detection logic, replace only I/O.
            time.sleep(delay)
            with _s2l_state._make_requests_session() as session:
                response = session.get(
                    self.target, headers=headers or self.headers, params=params or {},
                    timeout=self.timeout, allow_redirects=self.allowredir,
                    verify=_s2l_state.TLS_VERIFY, stream=True)
                chunks, total, started = [], 0, time.monotonic()
                try:
                    for chunk in response.iter_content(chunk_size=8192):
                        chunks.append(chunk)
                        total += len(chunk)
                        if total >= 2 * 1024 * 1024 or time.monotonic() - started >= self.timeout:
                            break
                    response._content = b"".join(chunks)
                finally:
                    response.close()
                self.requestnumber += 1
                return response

    scanner = GuardedScanner(target_url, timeout=min(_s2l_state.TIMEOUT_READ, 10))
    if scanner.rq is None:
        raise RuntimeError(f"WAFW00F could not reach {target_url}")

    detected, _trigger_url = scanner.identwaf()
    if detected:
        return ", ".join(name.split(" (", 1)[0] for name in detected)
    if scanner.genericdetect():
        return "Generic WAF"
    return "No WAF detected"
