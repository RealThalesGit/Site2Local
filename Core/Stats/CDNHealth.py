"""Site2Local: statistics and CDN health tracking."""
from __future__ import annotations

import threading
import time

from ... import Runtime as _s2l_state


def record_cdn_health(host: str, status: int, rtt_ms: int,
                      is_error: bool = False) -> None:
    """Update the health record for a CDN host (thread-safe, O(1)).

    Keeps a bounded rolling RTT history (`rtt_history`, last 20 samples) so the
    web viewer can render a mini sparkline per host without the browser having
    to track history itself. The history is append-only and capped, so memory
    stays O(hosts × 20) regardless of session length.
    """
    if not host:
        return
    now = time.time()
    with _s2l_state._cdn_health_lock:
        rec = _s2l_state._cdn_health.get(host)
        if rec is None:
            rec = {"last_seen": now, "requests": 0, "errors": 0,
                   "last_status": status, "last_rtt_ms": rtt_ms,
                   "rtt_history": [],
                   "first_seen": now}
            _s2l_state._cdn_health[host] = rec
        rec["last_seen"] = now
        rec["requests"] += 1
        rec["last_status"] = status
        rec["last_rtt_ms"] = rtt_ms
        # Append + cap the RTT history (keep last 20 samples for the sparkline).
        hist = rec.get("rtt_history")
        if hist is None:
            hist = rec["rtt_history"] = []
        hist.append(rtt_ms)
        if len(hist) > 20:
            del hist[:len(hist) - 20]
        if is_error or status >= 500:
            rec["errors"] += 1


def snapshot_cdn_health() -> dict:
    """Return a copy of all CDN host health records (thread-safe)."""
    # Snapshot the port registry under ITS lock: writers mutate
    # _cdn_host_port while holding _cdn_port_lock, so iterating it under
    # _cdn_health_lock alone let a concurrent registration crash the
    # viewer with "dictionary changed size during iteration". The two
    # locks are taken sequentially, never nested — no ordering hazard.
    with _s2l_state._cdn_port_lock:
        active = {h for h, p in _s2l_state._cdn_host_port.items() if p > 0}
    with _s2l_state._cdn_health_lock:
        # Only return hosts still in the active registry — stale entries from
        # hosts that were deregistered are dropped here so the viewer never
        # shows a health row for a CDN port that no longer exists. The
        # rtt_history list is copied too: a shallow dict() would share the
        # live list the recorders append to (garbled sparklines under load).
        return {h: dict(r, rtt_history=list(r.get("rtt_history", ())))
                for h, r in _s2l_state._cdn_health.items() if h in active}


def prune_cdn_health() -> int:
    """Drop CDN health records for hosts no longer registered OR idle too long.

    Runs periodically from a background thread (see _start_health_pruner) so the
    `_cdn_health` dict stays bounded in long-running sessions that register
    many transient CDN hosts. Returns the number of entries pruned.

    A record is pruned when EITHER:
      - its host is no longer in _cdn_host_port (the CDN server died / was
        deregistered), OR
      - its last_seen is older than _CDN_HEALTH_MAX_AGE (the host is still
        registered but hasn't seen traffic in a while — keeping its stale
        RTT/error counts would mislead the viewer).
    """
    now = time.time()
    cutoff = now - _s2l_state._CDN_HEALTH_MAX_AGE
    # Same lock discipline as snapshot_cdn_health: the registry is iterated
    # under _cdn_port_lock (its writers' lock), NOT under _cdn_health_lock.
    with _s2l_state._cdn_port_lock:
        active = set(_s2l_state._cdn_host_port.keys())
    with _s2l_state._cdn_health_lock:
        victims = [h for h, r in _s2l_state._cdn_health.items()
                   if h not in active or r.get("last_seen", 0) < cutoff]
        for h in victims:
            _s2l_state._cdn_health.pop(h, None)
        return len(victims)


def _start_health_pruner() -> None:
    """Start the periodic CDN-health prune daemon (idempotent)."""

    if _s2l_state._health_pruner_started:
        return
    _s2l_state._health_pruner_started = True
    def _loop():
        while True:
            time.sleep(60)   # prune once a minute
            try:
                n = _s2l_state.prune_cdn_health()
                if n:
                    _s2l_state.log(f"Pruned {n} stale CDN health record(s)", "DEBUG")
            except Exception:
                pass   # never let the pruner die
    threading.Thread(target=_loop, daemon=True, name="cdn-health-pruner").start()
