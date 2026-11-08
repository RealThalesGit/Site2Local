"""Site2Local: background save worker — batched, atomic cache writes."""
from __future__ import annotations

from urllib.parse import urlparse
import hashlib
import os
import queue
import time

from ... import Runtime as _s2l_state


def _atomic_write(p: str, data: bytes) -> None:
    """Write ``data`` to ``p`` atomically: temp file + ``os.replace``.

    A concurrent reader can never observe a truncated or half-written
    cache file — it sees either the previous content or the complete new
    content. The temp lives in the same directory (same volume), which is
    what keeps the rename atomic on POSIX and Windows alike.
    """
    tmp = p + _s2l_state._CACHE_TMP_SUFFIX
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, p)


def _save_worker() -> None:
    batch:        list = []
    batch_keys:   set  = set()   # (url, dev) keys pending in `batch`
    last_flush:   float = time.time()
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
        except Exception as _e:
            # Malformed item (unpack failure, wrong shape): log and keep
            # going. This is the ONLY consumer of save_queue — if it dies,
            # caching stops silently, the queue grows forever and the
            # graceful-shutdown drain (save_queue.join) hangs.
            _s2l_state.log(f"Save-worker: dropped malformed item — {_e}", "ERROR")
            _s2l_state.save_queue.task_done()
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
                h = hashlib.sha1(data).hexdigest()

                # Dedup key is the (url, device-variant) — NOT an on-disk
                # path. The path only becomes final when the flush resolves
                # the .q collision guard against everything written before
                # it; keying the dedup on a dequeue-time path let two
                # colliding queries both claim the same pretty name inside
                # one batch window and clobber each other's bytes/sidecar.
                key = (u, _dev_tag)
                with _s2l_state.save_lock:
                    if key in _s2l_state.saved_keys:
                        # Self-heal, batch-aware: the marker exists but the
                        # file is gone from disk (the user deleted it while
                        # the server runs) → forget the marker so this body
                        # re-materializes the file. A key still pending in
                        # THIS batch has no file yet — that is not a deletion
                        # (this misfire is what made colliding same-batch
                        # queries overwrite each other).
                        if key not in batch_keys:
                            _lp = _s2l_state.local_path(u, _dev_tag)
                            if not os.path.isfile(_lp):
                                _s2l_state.saved_keys.discard(key)
                        if key in _s2l_state.saved_keys:
                            continue
                    _s2l_state.saved_keys.add(key)
                    # Cap memory — clear if too large (prevents OOM on long runs)
                    if len(_s2l_state.saved_keys) > _s2l_state._SAVED_KEYS_MAX:
                        _s2l_state.saved_keys.clear()
                        _s2l_state.saved_keys.add(key)
                batch.append((u, _dev_tag, data, h, _qs))
                batch_keys.add(key)
            except Exception as e:
                _s2l_state.log(f"Save-worker item error: {u} — {e}", "ERROR")
            finally:
                _s2l_state.save_queue.task_done()
        now = time.time()
        if batch and (len(batch) >= _s2l_state.SAVE_BATCH or now - last_flush >= _s2l_state.SAVE_INTERVAL):
            for u, dev_tag, data, h, qs in batch:
                try:
                    # Resolve the cache path AT FLUSH TIME. Every earlier
                    # write of this batch is already on disk, so the .q
                    # collision guard sees the truth: two colliding queries
                    # fetched within one batch window split into pretty +
                    # legacy fallback instead of sharing one file.
                    p = _s2l_state.local_path(u, dev_tag)
                    os.makedirs(os.path.dirname(p), exist_ok=True)
                    if os.path.isdir(p):
                        p = os.path.join(p, "index.html")
                    # Duplicate-content materialization: prefer a hardlink to
                    # the first copy (zero extra disk). The mapping is
                    # registered at flush (paths are only real then) and
                    # re-validated with isfile so a purged source falls back
                    # to a plain write; os.link also fails loudly on
                    # cross-device / exotic filesystems → plain write.
                    _link_src = None
                    with _s2l_state.content_lock:
                        _first = _s2l_state.content_hashes.get(h)
                        if _first is not None and os.path.isfile(_first):
                            _link_src = _first
                    if _link_src and not os.path.isfile(p):
                        try:
                            os.link(_link_src, p)
                            if qs:
                                _s2l_state._write_query_sidecar(p, qs)
                            _s2l_state.stats.inc("saved")
                            continue
                        except OSError:
                            pass   # fall through to a plain byte write
                    _atomic_write(p, data)
                    # Query files record their owning RAW query in the .q
                    # sidecar — this is what makes the beautified names
                    # collision-safe (see local_path).
                    if qs:
                        _s2l_state._write_query_sidecar(p, qs)
                    # Register/refresh hash→path now that the path is real
                    # (only when the current mapping is missing or dead).
                    with _s2l_state.content_lock:
                        _first = _s2l_state.content_hashes.get(h)
                        if _first is None or not os.path.isfile(_first):
                            _s2l_state.content_hashes[h] = p
                        # Cap memory — clear if too large (prevents OOM on long runs)
                        if len(_s2l_state.content_hashes) > _s2l_state._CONTENT_HASHES_MAX:
                            _s2l_state.content_hashes.clear()
                            _s2l_state.content_hashes[h] = p
                    _s2l_state.stats.inc("saved")
                except Exception as e:
                    _s2l_state.log(f"Save error: {u} — {e}", "ERROR")
                    # Write FAILED — drop the dedup key so a future request
                    # for the same URL can retry instead of being permanently
                    # skipped (which would make it uncacheable for the run).
                    with _s2l_state.save_lock:
                        _s2l_state.saved_keys.discard((u, dev_tag))
            batch.clear()
            batch_keys.clear()
            last_flush = now
