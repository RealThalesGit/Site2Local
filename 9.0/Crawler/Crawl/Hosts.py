"""Site2Local: crawler queues, workers and link discovery."""
from __future__ import annotations

from bs4 import BeautifulSoup
from colorama import Fore
from colorama import Style
from urllib.parse import urljoin
from urllib.parse import urlparse
import hashlib
import os
import queue
import threading
import time

from ... import Runtime as _s2l_state


def _mark_host_dead(netloc: str) -> None:
    """Mark a host as dead for DEAD_HOST_TTL seconds."""
    with _s2l_state._dead_hosts_lock:
        _s2l_state._dead_hosts[netloc] = time.time() + _s2l_state.DEAD_HOST_TTL
        # Prune expired entries periodically to prevent unbounded growth.
        # Only prune when the dict gets large (>500 entries) to avoid
        # scanning on every call.
        if len(_s2l_state._dead_hosts) > 500:
            now = time.time()
            expired = [k for k, v in _s2l_state._dead_hosts.items() if now > v]
            for k in expired:
                _s2l_state._dead_hosts.pop(k, None)


def _is_host_dead(netloc: str) -> bool:
    """Check if a host is currently marked dead (with TTL expiry)."""
    with _s2l_state._dead_hosts_lock:
        expiry = _s2l_state._dead_hosts.get(netloc)
        if expiry is None:
            return False
        if time.time() > expiry:
            # TTL expired — host is no longer considered dead, allow retry.
            _s2l_state._dead_hosts.pop(netloc, None)
            return False
        return True
