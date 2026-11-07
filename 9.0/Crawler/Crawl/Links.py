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


def _discovery_worker() -> None:
    """Long-lived crawl worker — processes the url_queue forever.

    Started after the initial crawl finishes. The initial crawl's WORKERS
    threads all exit (via None sentinels); these workers take over and keep
    draining the queue so enqueue() calls from the proxy handler are honored.
    """
    while True:
        u = _s2l_state.url_queue.get()
        if u is None:
            _s2l_state.url_queue.task_done()
            return
        try:
            _s2l_state._crawl(u)
        except Exception as e:
            _s2l_state.log(f"Discovery crawl error: {u} — {_s2l_state._short_exc(e)}", "WARN")
        finally:
            _s2l_state.url_queue.task_done()


def _start_discovery_workers() -> None:
    if _s2l_state._discovery_started.is_set():
        return
    _s2l_state._discovery_started.set()
    for i in range(_s2l_state._DISCOVERY_WORKERS):
        threading.Thread(target=_s2l_state._discovery_worker, daemon=True,
                         name=f"discovery-{i}").start()


def _extract_links_async(html_body: bytes, base_url: str) -> None:
    """Extract links from an HTML body (served by the proxy) and enqueue any
    same-domain URLs for background crawling. Runs in its own thread so the
    proxy response is never delayed by link parsing.

    This is the key fix for the "reload more than once" bug: without it, the
    first navigation to a page serves it live but never discovers its
    sub-resources, so the browser has to fetch each one on demand (and some
    JS may fail if a dependency isn't ready). With it, the discovery workers
    pre-cache linked pages and assets in the background.
    """
    try:
        soup = BeautifulSoup(html_body.decode("utf-8", "ignore"), "lxml")
        for tag in soup.find_all(["a", "script", "img", "link", "iframe", "source"]):
            v = tag.get("href") or tag.get("src")
            if not v:
                continue
            abs_url = _s2l_state.normalize_url(urljoin(base_url, v))
            parsed  = urlparse(abs_url)
            if parsed.hostname in _s2l_state._LOCAL_HOSTS:
                continue
            if _s2l_state.is_allowed_domain(parsed.netloc):
                _s2l_state.enqueue(abs_url)
            elif _s2l_state.PROXY_CDN and _s2l_state.is_external_domain(parsed.netloc) and tag.name in _s2l_state._ASSET_TAGS:
                # Use bounded pool instead of spawning a thread per asset —
                # prevents "can't start new thread" on pages with many links.
                try:
                    _s2l_state._ext_asset_queue.put_nowait(abs_url)
                except queue.Full:
                    pass  # queue full — skip this asset, not critical
    except Exception:
        pass
