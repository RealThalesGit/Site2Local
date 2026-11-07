"""Site2Local: hidden-path scanner."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from urllib.parse import urlparse
import json
import os
import sys
import threading
import time

from ... import Runtime as _s2l_state


def _run_path_scanner() -> None:
    """Hidden-path scanner. Iterates wordlists (one path per line), probes
    each path on MAIN_HOST with HEAD (falls back to GET on 405), classifies
    the result by status code, and writes the hits to hidden_paths.json.

    Log behavior is governed by the module-level filter state set by
    _prompt_scan_status_filter(): only statuses the user wants to see are
    logged, everything else is silently skipped (still recorded in
    hidden_paths.json — the filter only affects the terminal log, not the
    on-disk record).

    Mode-specific opening lines (per the user's spec):
      SCAN_PATHS = "<dir>/<file>"   → "Loading wordlist..."
      SCAN_PATHS = "all"            → "Loading first wordlist..."
      SCAN_PATHS = "all-in-dir"     → "Loading first wordlist..."
    Then for each wordlist:
      "Scanning paths of <dir>/<file>... [X/X]"
    """
    wordlists = _s2l_state._resolve_wordlists()
    if not wordlists:
        _s2l_state.log(f"SCAN_PATHS={_s2l_state.SCAN_PATHS!r} but no wordlists found in {_s2l_state.WORDLISTS_DIR}/ — "
            f"drop .txt files there (one path per line).", "WARN")
        return

    sess   = _s2l_state._make_session()
    found: list = []
    origin = f"{urlparse(_s2l_state.SITE_URL).scheme}://{_s2l_state.MAIN_HOST}"
    total_paths = sum(len(p) for _, p in wordlists)
    scanned = 0

    # ── Rate-limit setup (SCANS_PER_SECOND) ────────────────────────────
    # Two effects from the same knob:
    #   1. REAL RATE LIMIT — caps total probes/sec to MAIN_HOST across ALL
    #      worker threads. Implemented as a thread-safe sliding window: each
    #      worker acquires a "slot" by appending its timestamp; if the window
    #      already has _rate_n entries in the last 1s, the worker sleeps until
    #      the oldest slot ages out. The lock is held only for the check+
    #      append (microseconds); sleeping happens OUTSIDE the lock so other
    #      workers can proceed in parallel.
    #   2. PROGRESS REFRESH CADENCE — when tqdm isn't installed, decides how
    #      often to re-log "Scanning paths... [X/Y]".
    # SCANS_PER_SECOND <= 0 disables the rate limit — scanner runs as fast
    # as the worker pool allows.
    _rate_n = max(0, int(_s2l_state.SCANS_PER_SECOND)) if _s2l_state.SCANS_PER_SECOND else 0
    _rate_enabled = _rate_n > 0
    _rate_window: list[float] = []   # timestamps of recent scans (sliding window)
    _rate_lock = threading.Lock()
    # Progress refresh interval (in paths). Falls back to 25 if the rate
    # is disabled, so users who set SCANS_PER_SECOND=0 still get periodic
    # progress lines — they just don't get the rate limit.
    _progress_every = _rate_n if _rate_n > 0 else 25

    # ── Concurrency setup ──────────────────────────────────────────────
    # The rate limit caps TOTAL probes/sec, but each probe takes network
    # RTT (typically 50-200ms). A single-threaded scanner can only do
    # 1/RTT probes/sec — so SCANS_PER_SECOND=86 with 67ms probes maxes out
    # at ~15/s (exactly what the user observed). The fix: run N worker
    # threads in parallel, each rate-limited by the shared sliding window.
    # With 12 workers and 67ms probes: 12 / 0.067 = 179/s theoretical,
    # capped by the rate limiter to 86/s. Even slow servers (200ms) work:
    # 12 / 0.2 = 60/s, which is close enough that the rate limiter can
    # still reach 50-60/s.
    # Formula: ~15% of SCANS_PER_SECOND, clamped to [4, 24]. Generous
    # enough for slow servers, not so many threads that we overwhelm the
    # connection pool or the OS scheduler.
    if _rate_enabled:
        _scan_workers = min(max(int(_rate_n * 0.15), 4), 24)
    else:
        _scan_workers = 8   # no rate limit, use reasonable concurrency

    def _rate_limit_sleep():
        """Thread-safe rate limiter. Blocks until a "slot" is available.

        Called by EACH worker thread BEFORE its probe. The sliding window
        holds the timestamps of the last _rate_n probes; if it's full
        (all within the last 1s), the caller sleeps until the oldest
        timestamp exits the window, then retries.
        """
        if not _rate_enabled:
            return
        while True:
            with _rate_lock:
                now = time.time()
                # Trim timestamps older than 1s — they're outside the window.
                while _rate_window and (now - _rate_window[0]) >= 1.0:
                    _rate_window.pop(0)
                # If there's room in the window, claim a slot and return.
                if len(_rate_window) < _rate_n:
                    _rate_window.append(now)
                    return
                # Window is full — calculate how long to sleep.
                sleep_for = 1.0 - (now - _rate_window[0])
            # Sleep OUTSIDE the lock so other workers can check the window
            # in parallel. Cap at 1s to avoid pathological stalls.
            if sleep_for > 0:
                time.sleep(min(sleep_for, 1.0))
            # Loop and retry — the window will have aged by now.

    # ── Mode-specific opening line ─────────────────────────────────────
    mode = (_s2l_state.SCAN_PATHS or "").strip()
    if mode in ("all", "all-in-dir"):
        # Multi-wordlist mode — announce we're picking up the first one.
        _s2l_state.log("Loading first wordlist...", "SCAN")
    elif mode and mode not in ("all", "all-in-dir"):
        # Single-wordlist mode — announce we're loading that specific file.
        _s2l_state.log("Loading wordlist...", "SCAN")

    _s2l_state.log(f"Path scanner — {len(wordlists)} wordlists, {total_paths} paths for {_s2l_state.MAIN_HOST}", "SCAN")

    # Snapshot the filter settings so a mid-scan prompt update can't change
    # behavior out from under us. Read once, use for the whole scan.
    block_set = set(_s2l_state._SCAN_BLOCK_STATUSES)
    only_status = _s2l_state._SCAN_ONLY_STATUS

    def _should_log_status(sc: int) -> bool:
        """Apply the user's filter to decide whether to print a log line."""
        if only_status is not None:
            return sc == only_status
        if sc in block_set:
            return False
        return True

    # ── Per-wordlist scan loop ─────────────────────────────────────────

    for wl_name, paths in wordlists:
        wl_len = len(paths)
        host_disp = _s2l_state._fmt_host(_s2l_state.MAIN_HOST)
        # Short display name for the tqdm bar — the full path is too long
        # and makes the bar unreadable. Use basename; if that's still too
        # long, truncate.
        wl_short = os.path.basename(wl_name)
        if len(wl_short) > 30:
            wl_short = wl_short[:27] + "..."

        if _s2l_state._TQDM_OK:
            # ── tqdm + concurrent path ───────────────────────────────────
            # Worker threads probe paths in parallel; the rate limiter caps
            # total throughput at SCANS_PER_SECOND. The main thread consumes
            # completed futures via as_completed() and updates the bar.
            # All log output (from workers AND from other threads like Flask)
            # is routed through tqdm.write() via the _active_scan_pbar global
            # so nothing corrupts the bar.
            _wl_found = [0]
            _wl_notfound = [0]
            _counters_lock = threading.Lock()

            pbar = _s2l_state._tqdm(
                total=wl_len,
                desc=f"  {wl_short}",
                unit="path",
                unit_scale=False,
                dynamic_ncols=True,
                leave=True,          # keep the bar visible at 100% when done
                file=sys.stderr,     # don't pollute stdout (used by log())
                colour="cyan",
                bar_format="{desc} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{rate_fmt}] {postfix}",
            )
            _s2l_state._active_scan_pbar = pbar   # module-level: log() checks this
            try:
                def _probe_one(p, _wl=wl_name, _host=host_disp):
                    """Worker function: probe one path. Thread-safe.

                    Uses closures over sess, origin, found, _counters_lock,
                    _wl_found, _wl_notfound, _rate_limit_sleep,
                    _should_log_status, and log(). All of these are either
                    thread-safe by design (Session, log via tqdm.write) or
                    explicitly locked (_counters_lock, _rate_lock).
                    """
                    _rate_limit_sleep()
                    url = f"{origin}/{p}"
                    try:
                        r = sess.head(url, timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ),
                                      allow_redirects=False, verify=False)
                        if r.status_code == 405:
                            r = sess.get(url, timeout=(_s2l_state.TIMEOUT_CONN, min(_s2l_state.TIMEOUT_READ, 6)),
                                         allow_redirects=False, verify=False)
                        sc = r.status_code
                        ct = r.headers.get("Content-Type", "")
                        with _counters_lock:
                            if sc not in (404, 410):
                                found.append({"path": p, "url": url, "status": sc,
                                              "wordlist": _wl,
                                              "content_type": ct})
                                _wl_found[0] += 1
                            else:
                                _wl_notfound[0] += 1
                        # Per-status log reactions — log() is tqdm-aware
                        # (uses _active_scan_pbar to route through tqdm.write)
                        if _should_log_status(sc):
                            if sc == 200:
                                c = Fore.GREEN + Style.BRIGHT
                                _s2l_state.log(f"{c}Found path: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 404:
                                c = Fore.RED + Style.DIM
                                _s2l_state.log(f"{c}Path not found: {_host}/{p}{Style.RESET_ALL}", "SCAN")
                            elif sc in (301, 302, 303, 307, 308):
                                c = Fore.YELLOW
                                loc = r.headers.get("Location", "")
                                extra = f" → {loc}" if loc else ""
                                _s2l_state.log(f"{c}Redirect: {_host}/{p} [{sc}]{extra}{Style.RESET_ALL}", "SCAN")
                            elif sc == 403:
                                c = Fore.YELLOW + Style.BRIGHT
                                _s2l_state.log(f"{c}Forbidden: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 401:
                                c = Fore.YELLOW + Style.BRIGHT
                                _s2l_state.log(f"{c}Auth required: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif sc == 500:
                                c = Fore.MAGENTA + Style.BRIGHT
                                _s2l_state.log(f"{c}Server error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 200 <= sc < 300:
                                c = Fore.GREEN
                                _s2l_state.log(f"{c}Found path: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 300 <= sc < 400:
                                c = Fore.YELLOW
                                _s2l_state.log(f"{c}Redirect: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            elif 400 <= sc < 500:
                                c = Fore.RED
                                _s2l_state.log(f"{c}Client error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                            else:
                                c = Fore.MAGENTA + Style.BRIGHT
                                _s2l_state.log(f"{c}Server error: {_host}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    except _s2l_state._CONN_ERRORS:
                        # Network errors: silent — the bar's postfix still
                        # shows progress. Logging every timeout would drown
                        # the scan output on flaky targets.
                        pass
                    except Exception as e:
                        _s2l_state.log(f"Scan error: {url} — {_s2l_state._short_exc(e)}", "WARN")

                with ThreadPoolExecutor(max_workers=_scan_workers) as pool:
                    futures = {pool.submit(_probe_one, p): p for p in paths}
                    for fut in as_completed(futures):
                        try:
                            fut.result()
                        except Exception:
                            pass   # already logged inside _probe_one
                        scanned += 1
                        with _counters_lock:
                            pf, pm = _wl_found[0], _wl_notfound[0]
                        pbar.set_postfix_str(
                            f"{Fore.GREEN}found={pf}{Style.RESET_ALL} "
                            f"{Fore.RED}miss={pm}{Style.RESET_ALL}",
                            refresh=True,
                        )
                        pbar.update(1)
            finally:
                pbar.close()
                _s2l_state._active_scan_pbar = None   # log() reverts to print()
        else:
            # ── No-tqdm path: periodic log-line progress refresh ──
            # Logs "Scanning paths of <wl>... [X/Y]" every SCANS_PER_SECOND
            # paths. Used in minimal environments without tqdm installed.
            _s2l_state.log(f"Scanning paths of {wl_name}... [0/{wl_len}]", "SCAN")
            wl_scanned = 0
            for p in paths:
                _rate_limit_sleep()
                url = f"{origin}/{p}"
                try:
                    r = sess.head(url, timeout=(_s2l_state.TIMEOUT_CONN, _s2l_state.TIMEOUT_READ),
                                  allow_redirects=False, verify=False)
                    if r.status_code == 405:
                        r = sess.get(url, timeout=(_s2l_state.TIMEOUT_CONN, min(_s2l_state.TIMEOUT_READ, 6)),
                                     allow_redirects=False, verify=False)
                    sc = r.status_code
                    ct = r.headers.get("Content-Type", "")
                    if sc not in (404, 410):
                        found.append({"path": p, "url": url, "status": sc,
                                      "wordlist": wl_name,
                                      "content_type": ct})
                    if not _should_log_status(sc):
                        wl_scanned += 1
                        scanned += 1
                        if wl_scanned % _progress_every == 0 or wl_scanned == wl_len:
                            _s2l_state.log(f"Scanning paths of {wl_name}... [{wl_scanned}/{wl_len}]", "SCAN")
                        continue
                    if sc == 200:
                        c = Fore.GREEN + Style.BRIGHT
                        _s2l_state.log(f"{c}Found path: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 404:
                        c = Fore.RED + Style.DIM
                        _s2l_state.log(f"{c}Path not found: {host_disp}/{p}{Style.RESET_ALL}", "SCAN")
                    elif sc in (301, 302, 303, 307, 308):
                        c = Fore.YELLOW
                        loc = r.headers.get("Location", "")
                        extra = f" → {loc}" if loc else ""
                        _s2l_state.log(f"{c}Redirect: {host_disp}/{p} [{sc}]{extra}{Style.RESET_ALL}", "SCAN")
                    elif sc == 403:
                        c = Fore.YELLOW + Style.BRIGHT
                        _s2l_state.log(f"{c}Forbidden: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 401:
                        c = Fore.YELLOW + Style.BRIGHT
                        _s2l_state.log(f"{c}Auth required: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif sc == 500:
                        c = Fore.MAGENTA + Style.BRIGHT
                        _s2l_state.log(f"{c}Server error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 200 <= sc < 300:
                        c = Fore.GREEN
                        _s2l_state.log(f"{c}Found path: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 300 <= sc < 400:
                        c = Fore.YELLOW
                        _s2l_state.log(f"{c}Redirect: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    elif 400 <= sc < 500:
                        c = Fore.RED
                        _s2l_state.log(f"{c}Client error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                    else:
                        c = Fore.MAGENTA + Style.BRIGHT
                        _s2l_state.log(f"{c}Server error: {host_disp}/{p} [{sc}]{Style.RESET_ALL}", "SCAN")
                except _s2l_state._CONN_ERRORS:
                    if (wl_scanned + 1) % _progress_every == 0:
                        _s2l_state.log(f"  {Fore.YELLOW}unreachable: {p}{Style.RESET_ALL}", "WARN")
                except Exception as e:
                    _s2l_state.log(f"Scan error: {url} — {_s2l_state._short_exc(e)}", "WARN")
                wl_scanned += 1
                scanned += 1
                if wl_scanned % _progress_every == 0 or wl_scanned == wl_len:
                    _s2l_state.log(f"Scanning paths of {wl_name}... [{wl_scanned}/{wl_len}]", "SCAN")

    # ── Persist results ─────────────────────────────────────────────────
    out = os.path.join(_s2l_state.DATA_FOLDER, "hidden_paths.json")
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump({"scanned_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                       "target": _s2l_state.MAIN_HOST,
                       "mode": _s2l_state.SCAN_PATHS,
                       "scans_per_second": _s2l_state.SCANS_PER_SECOND,
                       "filter": {
                           "blocked": sorted(block_set),
                           "only": only_status,
                       },
                       "wordlists": [wl for wl, _ in wordlists],
                       "results": found}, f, indent=2)
    except Exception as e:
        _s2l_state.log(f"Scan save failed: {e}", "ERROR")
    _s2l_state.log(f"Scan done — {len(found)}/{scanned} found → {out}", "SCAN")
