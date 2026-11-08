"""Site2Local: crawler queues, workers and link discovery."""
from __future__ import annotations

from urllib.parse import urlparse
import threading

from ... import Runtime as _s2l_state


def _clean_url_match(m: bytes) -> bytes:
    """Strip trailing punctuation that the greedy path class captures.

    These characters are valid inside URLs (e.g. a comma in a query string)
    but almost never appear at the very end of a real URL — they're sentence
    punctuation, JS operators, or closing delimiters that the regex hoovers
    up because they're in the path character class.

    Also strips a SINGLE trailing closing-paren when the match contains an
    UNMATCHED opening paren — this is the common case of `url(//host/path)`
    in CSS, where the `)` is the CSS function close, not part of the URL.
    The regex can't express "balanced parens" so we approximate here: count
    opens vs closes; if there's one extra close, drop it.
    """
    while m and m[-1] in _s2l_state._URL_TRAILING:
        m = m[:-1]
    # Unbalanced-paren fixup: if the trimmed match has more `)` than `(`,
    # drop exactly one trailing `)`. This is the standard CSS `url(/path)`
    # case and costs almost nothing for non-CSS matches (which are balanced
    # or have no parens at all).
    if m and m[-1] == ord(b")"):
        opens  = m.count(b"(")
        closes = m.count(b")")
        if closes > opens:
            m = m[:-1]
    return m


def _fetch_external_asset(u: str) -> None:
    """PROXY_CDN: fetch a single CDN asset and cache it. No recursion."""
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
                                             verify=False)
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
