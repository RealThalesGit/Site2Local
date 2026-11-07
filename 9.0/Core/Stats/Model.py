"""Site2Local: statistics and CDN health tracking."""
from __future__ import annotations

import threading
import time

from ... import Runtime as _s2l_state


class _Stats:
    """Running counters with optional rolling-window snapshots.

    Total counters (crawled, saved, proxied, ...) accumulate for the entire
    run. The rolling-window variant (snapshot_window) returns counts for the
    last STATS_WINDOW seconds — useful for rate calculations and detecting
    when activity has stalled.
    """
    __slots__ = ("_lock", "crawled", "saved", "proxied",
                 "conn_errors", "http_errors", "hooks_run", "revealed", "cdn_fetched", "captured",
                 "ws_captured", "cache_304",
                 "_events")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        for s in self.__slots__[1:-1]:   # skip _lock and _events
            setattr(self, s, 0)
        self._events: list = []   # list of (timestamp, field) for rolling window

    def inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            setattr(self, key, getattr(self, key) + n)
            # Record event timestamp for rolling-window snapshot.
            now = time.time()
            for _ in range(n):
                self._events.append((now, key))
            # Prune events older than STATS_WINDOW to bound memory.
            cutoff = now - _s2l_state.STATS_WINDOW
            while self._events and self._events[0][0] < cutoff:
                self._events.pop(0)

    def snapshot(self) -> dict:
        with self._lock:
            return {s: getattr(self, s) for s in self.__slots__
                    if s not in ("_lock", "_events")}

    def snapshot_window(self) -> dict:
        """Return per-field counts for the last STATS_WINDOW seconds."""
        with self._lock:
            now = time.time()
            cutoff = now - _s2l_state.STATS_WINDOW
            counts: dict = {}
            for ts, key in self._events:
                if ts >= cutoff:
                    counts[key] = counts.get(key, 0) + 1
            return counts

    def reset(self) -> None:
        """Zero every counter + clear the rolling-window event log.

        Used by the viewer's "Reset stats" button so a developer can start a
        clean benchmark window without restarting the server. Thread-safe.
        """
        with self._lock:
            for s in self.__slots__[1:-1]:   # skip _lock and _events
                setattr(self, s, 0)
            self._events.clear()
