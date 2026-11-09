"""Site2Local: upstream HTTP session management."""
from __future__ import annotations

import requests
import requests.adapters

from ... import Runtime as _s2l_state


def _make_cffi_session(device: str, base_headers: dict):
    """Build a curl_cffi Session, trying impersonation targets in order until
    one is accepted by the installed curl_cffi build. Raises the last error
    only if every candidate (including the chrome136 catch-all) fails."""
    cached     = _s2l_state._cffi_impersonate_cache.get(device)
    candidates = (cached,) + _s2l_state._IMPERSONATE_BY_DEVICE.get(device, ("chrome136",)) if cached \
                 else _s2l_state._IMPERSONATE_BY_DEVICE.get(device, ("chrome136",))
    last_exc: Exception | None = None
    for target in candidates:
        try:
            s = _s2l_state._cffi_requests.Session(impersonate=target, verify=_s2l_state.TLS_VERIFY)
            s.headers.update(base_headers)
            _s2l_state._cffi_impersonate_cache[device] = target
            return _s2l_state._secure_session(s)
        except Exception as e:
            last_exc = e
            continue
    raise last_exc or RuntimeError("no usable curl_cffi impersonation target")


def _make_session(device: str | None = None):
    """Return a requests-compatible session (curl_cffi → cloudscraper → requests fallback)."""
    d = device or _s2l_state._effective_device()
    mobile = d in _s2l_state._MOBILE_DEVICES
    ua     = _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES.get(d, _s2l_state.UA_PROFILES["macintosh"]))
    ch     = _s2l_state._SEC_CH_UA.get(d, _s2l_state._SEC_CH_UA.get("desktop", {}))
    base_headers = {
        "User-Agent":                ua,
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br" if _s2l_state._BROTLI_OK else "gzip, deflate",
        "DNT":                       "1",
        "Upgrade-Insecure-Requests": "1",
        **ch,
    }

    # This is the only reliable method against Cloudflare Bot Management v2+.
    # Install: pip install curl-cffi --break-system-packages
    if _s2l_state._CURL_CFFI_OK:
        try:
            return _s2l_state._make_cffi_session(d, base_headers)
        except Exception as e:
            _s2l_state.log(f"curl_cffi init failed — falling back: {e}", "WARN")

    cfg = _s2l_state._next_cf_config(mobile)
    if _s2l_state._CLOUDSCRAPER_OK:
        try:
            s = _s2l_state.cloudscraper.create_scraper(browser=cfg, delay=0)
            # MUST override UA after create_scraper — cloudscraper injects old UAs
            # (often Chrome 80-83) that fail browser version checks on modern sites.
            s.headers.update(base_headers)
            s.keep_alive = True
            s.verify     = _s2l_state.TLS_VERIFY
            adapter = requests.adapters.HTTPAdapter(
                max_retries      = _s2l_state._RETRY_POLICY,
                pool_connections = _s2l_state.POOL_CONNECTIONS,
                pool_maxsize     = _s2l_state.POOL_MAXSIZE,
                pool_block       = False,
            )
            s.mount("https://", adapter)
            s.mount("http://",  adapter)
            return _s2l_state._secure_session(s)
        except Exception as e:
            _s2l_state.log(f"cloudscraper init failed — falling back to requests: {e}", "WARN")
    else:
        _s2l_state.log("ai-cloudscraper not installed (pip install ai-cloudscraper) "
            "— falling back to plain requests", "WARN")

    s = requests.Session()
    s.headers.update(base_headers)
    s.verify = _s2l_state.TLS_VERIFY
    adapter = requests.adapters.HTTPAdapter(
        max_retries      = _s2l_state._RETRY_POLICY,
        pool_connections = _s2l_state.POOL_CONNECTIONS,
        pool_maxsize     = _s2l_state.POOL_MAXSIZE,
        pool_block       = False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return _s2l_state._secure_session(s)


def _make_cloudscraper_session(device: str | None = None) -> object:
    """Build a cloudscraper session directly (bypassing curl_cffi).

    Used as a fallback when curl_cffi fails on specific networks/hosts.
    """
    d = device or _s2l_state._effective_device()
    mobile = d in _s2l_state._MOBILE_DEVICES
    ua     = _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES.get(d, _s2l_state.UA_PROFILES["macintosh"]))
    ch     = _s2l_state._SEC_CH_UA.get(d, _s2l_state._SEC_CH_UA.get("desktop", {}))
    base_headers = {
        "User-Agent":                ua,
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br" if _s2l_state._BROTLI_OK else "gzip, deflate",
        "DNT":                       "1",
        "Upgrade-Insecure-Requests": "1",
        **ch,
    }
    cfg = _s2l_state._next_cf_config(mobile)
    if not _s2l_state._CLOUDSCRAPER_OK:
        raise RuntimeError("ai-cloudscraper not installed — pip install ai-cloudscraper")
    s = _s2l_state.cloudscraper.create_scraper(browser=cfg, delay=0)
    s.headers.update(base_headers)
    s.keep_alive = True
    s.verify     = _s2l_state.TLS_VERIFY
    adapter = requests.adapters.HTTPAdapter(
        max_retries=_s2l_state._RETRY_POLICY,
        pool_connections=_s2l_state.POOL_CONNECTIONS,
        pool_maxsize=_s2l_state.POOL_MAXSIZE,
        pool_block=False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return _s2l_state._secure_session(s)


def _make_requests_session(device: str | None = None) -> object:
    """Build a plain requests session (last-resort fallback, no CF bypass)."""
    d = device or _s2l_state._effective_device()
    ua = _s2l_state._sanitize_ua(_s2l_state.UA_PROFILES.get(d, _s2l_state.UA_PROFILES["macintosh"]))
    ch = _s2l_state._SEC_CH_UA.get(d, _s2l_state._SEC_CH_UA.get("desktop", {}))
    base_headers = {
        "User-Agent":                ua,
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br" if _s2l_state._BROTLI_OK else "gzip, deflate",
        "DNT":                       "1",
        "Upgrade-Insecure-Requests": "1",
        **ch,
    }
    s = requests.Session()
    s.headers.update(base_headers)
    s.verify = _s2l_state.TLS_VERIFY
    adapter = requests.adapters.HTTPAdapter(
        max_retries=_s2l_state._RETRY_POLICY,
        pool_connections=_s2l_state.POOL_CONNECTIONS,
        pool_maxsize=_s2l_state.POOL_MAXSIZE,
        pool_block=False,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    return _s2l_state._secure_session(s)
