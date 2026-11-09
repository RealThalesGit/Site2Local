"""Site2Local: crawler queues, workers and link discovery."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from urllib.parse import urlparse
import os
import threading

from ... import Runtime as _s2l_state


def enqueue(u: str) -> None:
    # Feed initial workers or persistent discovery workers, never a queue
    # without consumers (procedural/offline mode).
    if (not _s2l_state.OFFLINE and (
            not _s2l_state._crawl_done.is_set()
            or (_s2l_state.CRAWL and _s2l_state._discovery_started.is_set()))):
        _s2l_state.url_queue.put(_s2l_state.normalize_url(u))


def _cap_visited() -> None:
    """Evict the OLDEST entries of ``visited`` once it exceeds ``_VISITED_MAX``.

    Called with ``visited_lock`` held. ``visited`` is a plain set (hash
    order) — the insertion-order deque ``_visited_order`` (appended at
    every add) is what makes "oldest" actually mean oldest. Evicting in
    hash order dropped ~random URLs and caused duplicate re-fetch storms
    on crawls past 100k URLs. Popped entries that are already gone (or
    were re-added later) are harmless no-ops in the set.
    """
    order = _s2l_state._visited_order
    target = _s2l_state._VISITED_MAX // 2
    while order and len(_s2l_state.visited) > target:
        _s2l_state.visited.discard(order.popleft())


def _crawl(u: str) -> None:
    with _s2l_state.visited_lock:
        if u in _s2l_state.visited:
            return
        _s2l_state.visited.add(u)
        _s2l_state._visited_order.append(u)
        # Cap memory — if visited grows too large, evict the oldest half
        if len(_s2l_state.visited) > _s2l_state._VISITED_MAX:
            _cap_visited()

    if _s2l_state.url_depth(u) > _s2l_state.CRAWL_DEPTH:
        return
    if any(x in u for x in _s2l_state.BLOCK_PATHS):
        return

    netloc = urlparse(u).netloc
    if not _s2l_state.is_allowed_domain(netloc):
        return
    path_short = urlparse(u).path or "/"
    host_disp = _s2l_state._fmt_host(netloc)
    device = _s2l_state._device_variant(_s2l_state._effective_device())
    cache_path = _s2l_state.local_path(u, device)
    cached = os.path.isfile(cache_path)
    discovery_base = u

    if cached:
        try:
            with open(cache_path, "rb") as f:
                data = f.read()
        except OSError as e:
            _s2l_state.log(f"Crawl cache read error: {cache_path} — {_s2l_state._short_exc(e)}", "ERROR")
            return
        ct = _s2l_state._resolve_mime_with_sniff(cache_path, data)
        _s2l_state.log(f"Cached  {host_disp}{path_short} — skipping download", "CRAWL")
    else:
        if _s2l_state._is_host_dead(netloc):
            return
        try:
            r = _s2l_state._get_proxy_session().get(u, timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ), verify=False)
        except _s2l_state._CONN_ERRORS as e:
            _s2l_state.log(f"Skipping {netloc} — {_s2l_state._short_exc(e)}", "WARN")
            _s2l_state._mark_host_dead(netloc)
            _s2l_state.stats.inc("conn_errors")
            return
        except Exception as e:
            _s2l_state.log(f"Crawl error: {u} — {_s2l_state._short_exc(e)}", "WARN")
            _s2l_state.stats.inc("conn_errors")
            return

        _s2l_state.stats.inc("crawled")
        # Always decompress before processing
        data = _s2l_state.decompress_body(r.content, r.headers.get("Content-Encoding", ""))
        ct   = r.headers.get("Content-Type", "")
        discovery_base = getattr(r, "url", None) or u

        sc_c = (Fore.GREEN if r.status_code < 300
                else Fore.YELLOW if r.status_code < 400
                else Fore.RED)
        _s2l_state.log(f"GET {sc_c}{r.status_code}{Style.RESET_ALL}  {host_disp}{path_short}  {_s2l_state._fmt_size(len(data))}", "CRAWL")
        # Push crawl fetches to the GUI traffic log so they appear in the Hook
        # Inspector alongside proxied requests — the user needs to see what the
        # crawler is fetching, not just the terminal.
        _s2l_state._gui_push_raw("GET", path_short, r.status_code, ct, data,
                      display_tag="[CRAWL]", origin=host_disp,
                      _skip_log=True)   # caller already logged

        if r.status_code >= 400:
            _s2l_state.stats.inc("http_errors")
            if _s2l_state.SAVE_ERRORS:
                _s2l_state.save_queue.put((u, data, ct, device))
            return

    if _s2l_state.SHOW_HIDDEN and _s2l_state.is_html(data, ct):
        data = _s2l_state._reveal_hidden(data)
        _s2l_state.stats.inc("revealed")

    # Never cache RSC payloads
    if _s2l_state._is_wire_payload(data):
        _s2l_state.log(f"RSC payload skipped (not cached): {path_short}", "WARN")
        return

    # Save original (never rewritten) so cached files stay clean. Existing
    # cache entries are read above only for link discovery and must not be
    # queued for writing back to disk.
    if not cached:
        _s2l_state.save_queue.put((u, data, ct, device))

    discovery_ct = "text/html" if _s2l_state.is_html(data, ct) else ct
    _s2l_state._discover_links(data, discovery_base, discovery_ct)


def _crawl_worker() -> None:
    # Use a sentinel value to signal shutdown. A 3s timeout caused workers to
    # exit prematurely when the queue was temporarily empty between batches of
    # enqueued URLs — url_queue.join() would then hang forever because
    # unprocessed items never got task_done().
    #
    # CRITICAL: the try/except+finally mirrors _discovery_worker. Without it,
    # any exception from _crawl() (e.g. lxml FeatureNotFound, parsing error)
    # kills the worker WITHOUT calling task_done(), so url_queue.join() in
    # crawl_parallel() hangs forever, blocking startup entirely.
    while True:
        u = _s2l_state.url_queue.get()
        if u is None:
            _s2l_state.url_queue.task_done()
            return
        try:
            _s2l_state._crawl(u)
        except Exception as e:
            _s2l_state.log(f"Crawl error: {u} — {_s2l_state._short_exc(e)}", "WARN")
        finally:
            _s2l_state.url_queue.task_done()


def crawl_parallel() -> None:
    workers = [threading.Thread(target=_s2l_state._crawl_worker, daemon=True, name=f"crawl-{i}")
               for i in range(_s2l_state.WORKERS)]
    for w in workers:
        w.start()
    _s2l_state.url_queue.join()
    # Enqueue sentinels so all INITIAL workers exit cleanly (one per worker).
    # The persistent discovery workers are started AFTER, so they never see
    # these sentinels.
    for _ in range(_s2l_state.WORKERS):
        _s2l_state.url_queue.put(None)
    _s2l_state.save_queue.join()
    _s2l_state._crawl_done.set()   # signals initial crawl is complete
    # Start persistent discovery workers so newly-discovered URLs keep flowing.
    _s2l_state._start_discovery_workers()
