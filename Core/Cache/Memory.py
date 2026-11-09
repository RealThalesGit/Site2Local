"""Bounded raw-file cache for ATOMIC_SERVE; rewrites remain request-local."""
from collections import OrderedDict
import os
import threading

from ...Config import ATOMIC_SERVE_MAX_BYTES, parse_size

_MAX_BYTES = parse_size(ATOMIC_SERVE_MAX_BYTES)
if _MAX_BYTES < 0:
    raise ValueError("ATOMIC_SERVE_MAX_BYTES must be non-negative")
_MAX_FILE_BYTES = 16 * 1024 * 1024
_MAX_ENTRIES = 4096
_entries = OrderedDict()
_bytes = 0
_lock = threading.RLock()


def _signature(st):
    # On Windows stat() and fstat() can expose different ctime semantics.
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def read_cached_file(path, limit=None):
    """Reuse raw bytes only while the file's current disk signature matches.

    Large full bodies bypass RAM; head-only MIME probes can still be cached.
    Reads happen outside the lock so unrelated requests run concurrently.
    """
    global _bytes
    path = os.path.normcase(os.path.abspath(os.fspath(path)))
    key = (path, limit)
    try:
        signature = _signature(os.stat(path))
    except OSError:
        with _lock:
            old = _entries.pop(key, None)
            if old:
                _bytes -= len(old[1])
        raise
    with _lock:
        old = _entries.get(key)
        if old and old[0] == signature:
            _entries.move_to_end(key)
            return old[1]
        if old:
            _entries.pop(key)
            _bytes -= len(old[1])
    with open(path, "rb") as stream:
        before = _signature(os.fstat(stream.fileno()))
        data = stream.read() if limit is None else stream.read(limit)
        after = _signature(os.fstat(stream.fileno()))
    # Never pin a body modified while it was being read.
    if before == after == signature and len(data) <= min(_MAX_FILE_BYTES, _MAX_BYTES) and _MAX_BYTES:
        with _lock:
            old = _entries.pop(key, None)
            if old:
                _bytes -= len(old[1])
            _entries[key] = (signature, data)
            _bytes += len(data)
            while _bytes > _MAX_BYTES or len(_entries) > _MAX_ENTRIES:
                _, (_, evicted) = _entries.popitem(last=False)
                _bytes -= len(evicted)
    return data
