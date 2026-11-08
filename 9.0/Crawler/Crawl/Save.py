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


def _save_worker() -> None:
    batch:      list  = []
    last_flush: float = time.time()
    while True:
        try:
            # Items are (url, body, content-type) or (url, body, content-type,
            # device_class). The optional 4th element carries the device class
            # of the request that fetched the body — the worker maps it to an
            # HTML cache-variant tag so a phone's fetch lands in
            # index__s2l_mobile.html instead of overwriting/occupying the
            # canonical desktop file (where the user's hand edits live).
            # Content-type is currently unused here — the cache derives MIME
            # from the URL via guess_mime() — but producers still push it so
            # the tuple contract stays stable. Bound with _ to flag intent.
            u, data, _unused_ctype, *rest = _s2l_state.save_queue.get(timeout=_s2l_state.SAVE_INTERVAL)
        except queue.Empty:
            pass
        else:
            # CRITICAL: wrap the per-item body in try/except+finally so any
            # exception (local_path, hashlib, lock contention, etc.) doesn't
            # kill the single save-worker thread — without task_done() the
            # save_queue.join() in crawl_parallel() hangs forever, blocking
            # startup entirely.
            try:
                # Safety gate: never cache bot-detection / CAPTCHA pages.
                # These can arrive from background CDN fetches or upstream retries.
                if _s2l_state._is_bot_page(data):
                    _s2l_state.log(f"Bot page detected — NOT caching {urlparse(u).netloc}{urlparse(u).path}", "WARN")
                    continue
                _dev_tag = _s2l_state._device_variant(rest[0]) if rest and rest[0] else ""
                _qs = urlparse(u).query or ""
                p = _s2l_state.local_path(u, _dev_tag)
                h = hashlib.sha1(data).hexdigest()
                _link_src: str | None = None
                with _s2l_state.content_lock:
                    _first = _s2l_state.content_hashes.get(h)
                    if _first is not None:
                        # Identical bytes were already saved for another path.
                        # DON'T drop this file — every fetched URL must
                        # materialize at its own cache path, otherwise:
                        #   (a) a device variant whose content equals the
                        #       canonical copy would never be written → the
                        #       device would re-fetch this URL on EVERY
                        #       request (cache can never hit), and
                        #   (b) OFFLINE mode would 404 any second URL whose
                        #       content duplicates the first.
                        # Hardlink to the first copy: zero extra disk. The
                        # flush falls back to a plain write if linking is
                        # impossible (cross-device, exotic filesystem).
                        _link_src = _first
                    else:
                        _s2l_state.content_hashes[h] = p
                    # Cap memory — clear if too large (prevents OOM on long runs)
                    if len(_s2l_state.content_hashes) > _s2l_state._CONTENT_HASHES_MAX:
                        _s2l_state.content_hashes.clear()
                        _s2l_state.content_hashes[h] = p
                # NOTE: we track saved_paths by the ORIGINAL path here. If the
                # write later remaps p to p/index.html (directory collision),
                # _save_worker updates saved_paths with the final write path
                # after a successful write. This ensures a failed write does NOT
                # permanently mark the URL as saved (which would make it
                # uncacheable for the rest of the run).
                original_p = p
                with _s2l_state.save_lock:
                    # Cache self-heal: the path is tracked as already
                    # saved, but the FILE ITSELF is gone from disk (the user
                    # deleted it while the server runs). Forget the dedup
                    # entry so the freshly fetched body re-materializes the
                    # file instead of being silently dropped — otherwise a
                    # deleted-then-re-fetched file would serve live ONCE and
                    # never return to the disk mirror.
                    if original_p in _s2l_state.saved_paths and not os.path.isfile(original_p):
                        _s2l_state.saved_paths.discard(original_p)
                    if original_p in _s2l_state.saved_paths:
                        continue
                    # Tentatively mark as in-progress; removed on write failure.
                    _s2l_state.saved_paths.add(original_p)
                    # Cap memory — clear if too large (prevents OOM on long runs)
                    if len(_s2l_state.saved_paths) > _s2l_state._SAVED_PATHS_MAX:
                        _s2l_state.saved_paths.clear()
                        _s2l_state.saved_paths.add(original_p)
                batch.append((original_p, p, data, _link_src, _qs))
            except Exception as e:
                _s2l_state.log(f"Save-worker item error: {u} — {e}", "ERROR")
            finally:
                _s2l_state.save_queue.task_done()
        now = time.time()
        if batch and (len(batch) >= _s2l_state.SAVE_BATCH or now - last_flush >= _s2l_state.SAVE_INTERVAL):
            for original_p, p, data, link_src, qs in batch:
                try:
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    if os.path.isdir(p):
                        p = os.path.join(p, "index.html")
                    # Duplicate-content materialization: prefer a hardlink to
                    # the first copy (zero extra disk). Fall back to writing
                    # the bytes when the source is gone (purged) or the
                    # filesystem refuses links (cross-device, FAT/exFAT).
                    if link_src and not os.path.isfile(p):
                        try:
                            if os.path.isfile(link_src):
                                os.link(link_src, p)
                                if qs:
                                    _s2l_state._write_query_sidecar(p, qs)
                                if p != original_p:
                                    with _s2l_state.save_lock:
                                        _s2l_state.saved_paths.discard(original_p)
                                        _s2l_state.saved_paths.add(p)
                                _s2l_state.stats.inc("saved")
                                continue
                        except OSError:
                            pass   # fall through to a plain byte write
                    with open(p, "wb") as f:
                        f.write(data)
                    # Query files record their owning RAW query in the .q
                    # sidecar — this is what makes the beautified names
                    # collision-safe (see local_path).
                    if qs:
                        _s2l_state._write_query_sidecar(p, qs)
                    # Write succeeded — record the FINAL write path in saved_paths
                    # (may differ from original_p due to directory-collision remap).
                    if p != original_p:
                        with _s2l_state.save_lock:
                            _s2l_state.saved_paths.discard(original_p)
                            _s2l_state.saved_paths.add(p)
                    _s2l_state.stats.inc("saved")
                except Exception as e:
                    _s2l_state.log(f"Save error: {p} — {e}", "ERROR")
                    # Write FAILED — remove from saved_paths so a future request
                    # for the same URL can retry instead of being permanently
                    # skipped (which would make it uncacheable for the run).
                    with _s2l_state.save_lock:
                        _s2l_state.saved_paths.discard(original_p)
                        _s2l_state.saved_paths.discard(p)
            batch.clear()
            last_flush = now
