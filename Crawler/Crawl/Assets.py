"""Site2Local: crawler queues, workers and link discovery."""
from __future__ import annotations

from urllib.parse import urlparse
import threading

from ... import Runtime as _s2l_state


def _fetch_external_asset(u: str) -> None:
    """Fetch a CDN asset; discover CSS dependencies through the bounded queue."""
    netloc = urlparse(u).netloc
    # Skip domains that require user auth / real browser cookies.
    # Background sessions lack these cookies so the response is always a bot page.
    if any(d in netloc for d in _s2l_state._NO_BG_FETCH_DOMAINS):
        return
    if not _s2l_state._cdn_thread_sem.acquire(blocking=False):  # pylint: disable=consider-using-with
        return   # too many concurrent CDN fetches, skip silently
    try:
        with _s2l_state.visited_lock:
            if u in _s2l_state.visited:
                return
            _s2l_state.visited.add(u)
            _s2l_state._visited_order.append(u)
        if _s2l_state._is_host_dead(netloc):
            return
        try:
            # Use TIMEOUT_STREAM_READ (120s) — background CDN fetches may
            # download large WASM/asset bundles that timeout at 12s.
            r    = _s2l_state._get_proxy_session().get(u,
                                             timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_STREAM_READ),
                                             verify=_s2l_state.TLS_VERIFY)
            body = _s2l_state.decompress_body(r.content, r.headers.get("Content-Encoding", ""))
            # Never cache bot/CAPTCHA pages — they would poison the asset cache
            if _s2l_state._is_bot_page(body, r.status_code, urlparse(u).path):
                _s2l_state.log(f"Bot page from CDN {netloc} — skipping cache", "WARN")
                _s2l_state._mark_host_dead(netloc)
                return
            if r.status_code < 400:
                if _s2l_state.CACHE_CDN:
                    _s2l_state.save_queue.put((u, body, r.headers.get("Content-Type", ""), _s2l_state._effective_device()))
                _s2l_state._register_cdn_host(netloc)
                _s2l_state.stats.inc("cdn_fetched")
                # Stylesheets have their own relative base and can reference
                # further imports/fonts/images. The bounded asset queue and
                # visited set handle cycles without spawning more threads.
                if "text/css" in r.headers.get("Content-Type", "").lower():
                    _s2l_state._discover_links(body, getattr(r, "url", None) or u, "text/css")
                if _s2l_state.CAPTURE and _s2l_state.CAPTURE_CDN:
                    parsed_u = urlparse(u)
                    ctx_cdn  = _s2l_state.HookContext(
                        method="GET", url=u, path=parsed_u.path,
                        query=parsed_u.query, req_headers={}, req_body=b"",
                        resp_status=r.status_code, resp_headers=_s2l_state.filter_resp(_s2l_state._resp_headers_dict(r)),
                        resp_body=body,
                        resp_ct=r.headers.get("Content-Type", "application/octet-stream"),
                    )
                    _s2l_state._maybe_capture(ctx_cdn)
        except _s2l_state._CONN_ERRORS as e:
            _s2l_state._mark_host_dead(netloc)
            _s2l_state.log(f"CDN unreachable: {netloc} — {_s2l_state._short_exc(e)}", "WARN")
        except Exception as e:
            _s2l_state.log(f"CDN error: {u} — {_s2l_state._short_exc(e)}", "WARN")
    finally:
        _s2l_state._cdn_thread_sem.release()


def _start_ext_asset_workers() -> None:
    """Start a small pool of workers for external asset prefetch."""
    if _s2l_state._ext_asset_started.is_set():
        return
    _s2l_state._ext_asset_started.set()
    _n = min(8, _s2l_state._DISCOVERY_WORKERS)
    for i in range(_n):
        threading.Thread(target=_s2l_state._ext_asset_worker, daemon=True,
                         name=f"ext-asset-{i}").start()


def _ext_asset_worker() -> None:
    """Worker that fetches external assets from the queue."""
    while True:
        try:
            u = _s2l_state._ext_asset_queue.get()
            if u is None:
                return
            _s2l_state._fetch_external_asset(u)
        except Exception:
            pass
        finally:
            _s2l_state._ext_asset_queue.task_done()
