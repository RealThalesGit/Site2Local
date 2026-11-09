"""Site2Local: crawler queues, workers and link discovery."""
from __future__ import annotations

from urllib.parse import urlparse
import queue
import threading

from ... import Runtime as _s2l_state
from .Extract import extract_links


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
    _discover_links(html_body, base_url, "text/html")


def _discover_links(data: bytes, base_url: str, content_type: str) -> None:
    """Shared discovery policy for initial crawling and proxy-served HTML."""
    try:
        for abs_url, is_asset in extract_links(
                data, base_url, content_type,
                scan_text=_s2l_state.DUMP_ALL, scan_limit=_s2l_state.SCAN_LIMIT):
            parsed  = urlparse(abs_url)
            if parsed.hostname in _s2l_state._LOCAL_HOSTS:
                continue
            if _s2l_state.is_allowed_domain(parsed.netloc):
                _s2l_state.enqueue(abs_url)
            elif _s2l_state.PROXY_CDN and is_asset and _s2l_state.is_external_domain(parsed.netloc):
                # Use bounded pool instead of spawning a thread per asset —
                # prevents "can't start new thread" on pages with many links.
                try:
                    _s2l_state._ext_asset_queue.put_nowait(abs_url)
                except queue.Full:
                    pass  # queue full — skip this asset, not critical
    except Exception as exc:
        _s2l_state.log(f"Link discovery failed: {base_url} — {_s2l_state._short_exc(exc)}", "WARN")
