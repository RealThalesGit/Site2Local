"""Site2Local: CDN origin registry."""
from __future__ import annotations

from flask import Flask
from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
from werkzeug.serving import make_server
import logging
import os
import threading

from ... import Runtime as _s2l_state


def _register_cdn_host(netloc: str) -> None:

    if not _s2l_state.MULTIPORT:
        with _s2l_state._cdn_port_lock:
            if netloc not in _s2l_state._cdn_host_port:
                _s2l_state._cdn_host_port[netloc] = 0  # registered, routes via /__s2l_ext__/
        return
    with _s2l_state._cdn_port_lock:
        if netloc in _s2l_state._cdn_host_port:
            return
        port = _s2l_state._cdn_next_port
        _s2l_state._cdn_next_port += 1
        _s2l_state._cdn_host_port[netloc] = -1  # sentinel: server starting, not ready yet
    _s2l_state._start_cdn_server(netloc, port)


def _preregister_cdn_hosts_from_disk() -> int:
    """OFFLINE-mode safety: bring up MULTIPORT servers for every host we
    already have cached assets for, so URL rewrites have a live destination.

    In OFFLINE mode, _register_cdn_host is only called from upstream-fetch
    success paths — which never run. Without this pre-registration, every
    CDN asset 404s at the connection level because no mini-server is
    listening on the rewritten URL's port.
    """
    if not os.path.isdir(_s2l_state.SRC_FOLDER):
        return 0
    main_host_dir = _s2l_state.MAIN_HOST.removeprefix("www.")
    n = 0
    try:
        for host_dir in os.listdir(_s2l_state.SRC_FOLDER):
            full = os.path.join(_s2l_state.SRC_FOLDER, host_dir)
            if not os.path.isdir(full):
                continue
            if host_dir == main_host_dir:
                continue
            with _s2l_state._cdn_port_lock:
                already = host_dir in _s2l_state._cdn_host_port
            if not already:
                _s2l_state._register_cdn_host(host_dir)
                n += 1
    except Exception as e:
        _s2l_state.log(f"CDN pre-registration error: {_s2l_state._short_exc(e)}", "WARN")
    return n
