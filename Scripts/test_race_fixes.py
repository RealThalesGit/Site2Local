#!/usr/bin/env python3
"""Race-condition reproduction battery for Site2Local 9.0 (Fix 8).

Each check reproduces one finding of the 2026-11-08 read-only concurrency
audit, at the exact code path where it lives:

  RF1  R1  batch-window sidecar collision — two colliding queries fetched
           within ONE save-worker batch window must BOTH materialize (one
           at the pretty name, one at the collision fallback), each with
           the right bytes and the right .q owner.
  RF2  R2  atomic cache writes — no reader may ever observe a truncated
           cache file while the save worker flushes a large batch.
  RF3  R5  _cdn_host_port iterated under the wrong lock — snapshot/prune
           must survive a concurrent registration churn (no RuntimeError).
  RF4  R6  SSE reader leak — abandoning the consumer generator must end
           the sse-reader thread (bounded-put escape hatch actually set).
  RF5  R9  visited eviction order — the OLDEST half is evicted (insertion
           order), not a hash-order random sample.
  RF6  R3  crawl lifecycle — after crawl_parallel() every crawl-* worker
           has exited via its sentinel and exactly _DISCOVERY_WORKERS
           discovery workers survive.
  RF7      worker resilience — a malformed save_queue item must not kill
           the single save worker (the next good item still lands).
  RF8      save accounting — one queued body == exactly +1 'saved' stat.
  RF9      source contracts — the structural fixes that only manifest at
           boot (R3 no early discovery start / R4 procedural _crawl_done /
           R7 pool overflow / R8 tunnel-handshake close / R10 no enqueue-
           side stats.inc).
"""
import hashlib
import inspect
import os
import shutil
import sys
import threading
import time

_here    = os.path.dirname(os.path.abspath(__file__))
SRC_TREE = os.path.join(os.path.dirname(_here), "download", "Site2Local")
WORK     = "/tmp/s2l_race"


def build_test_env():
    if os.path.isdir(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    shutil.copytree(SRC_TREE, os.path.join(WORK, "Site2Local"),
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    cfg = os.path.join(WORK, "Site2Local", "Config.py")
    txt = open(cfg, encoding="utf-8").read()
    txt = txt.replace('SITE = "jeskojets.com"', 'SITE = "aya.nodium.lol"')
    txt = txt.replace("OFFLINE = False", "OFFLINE = True")
    txt = txt.replace("CRAWL = True", "CRAWL = False")
    open(cfg, "w", encoding="utf-8").write(txt)


def main():
    build_test_env()
    os.chdir(WORK)
    sys.path.insert(0, WORK)
    from Site2Local import Runtime as R

    results = []

    def check(tag, ok, detail=""):
        results.append((tag, ok))
        print(f"[{'PASS' if ok else 'FAIL'}] {tag}" + (f" — {detail}" if detail else ""))

    # The save worker (normally started by main()) — started here directly.
    threading.Thread(target=R._save_worker, daemon=True, name="save-worker").start()

    # ── RF1: batch-window sidecar collision (R1) ───────────────────────────
    url_a = "https://example.com/user?q=a+b"       # prettifies to user_q_a_b
    url_b = "https://example.com/user?q=a%20b"     # SAME pretty name, diff query
    # both land in ONE batch window (SAVE_INTERVAL=0.15s between flushes)
    R.save_queue.put((url_a, b"content-A", "text/plain", None))
    R.save_queue.put((url_b, b"content-B", "text/plain", None))
    time.sleep(1.2)   # let the flush happen
    p_a = R.local_path(url_a)   # pretty name (owner: q=a+b)
    p_b = R.local_path(url_b)   # after A owns the pretty name → legacy fallback
    ok_a = os.path.isfile(p_a) and open(p_a, "rb").read() == b"content-A"
    ok_owner = R._query_sidecar_owner(p_a) == "q=a+b"
    ok_b = os.path.isfile(p_b) and open(p_b, "rb").read() == b"content-B"
    check("RF1 colliding queries in one batch BOTH survive (R1)",
          ok_a and ok_owner and ok_b,
          f"A={ok_a}/{ok_owner} B-file={ok_b} p_b={os.path.basename(p_b)}")

    # ── RF2: atomic writes — no truncated reads during flush (R2) ─────────
    n_files, size = 8, 8 * 1024 * 1024
    bodies, paths = [], []
    for i in range(n_files):
        body = (f"FILE-{i}-".encode() * (size // 8))
        u = f"https://example.com/big/blob{i}.bin"
        bodies.append(body)
        paths.append(R.local_path(u))
        R.save_queue.put((u, body, "application/octet-stream", None))
    partial = [None]
    stop = threading.Event()

    def _reader():
        expected = {p: len(b) for p, b in zip(paths, bodies)}
        deadline = time.time() + 15
        while not stop.is_set() and time.time() < deadline:
            done = 0
            for p, want in expected.items():
                try:
                    got = os.path.getsize(p)
                except OSError:
                    continue
                if got != want:
                    if 0 < got < want:
                        partial[0] = f"{os.path.basename(p)}: {got}/{want}"
                        return
                    continue   # temp file or replaced-not-yet
                done += 1
            if done == n_files:
                return
        return

    t_reader = threading.Thread(target=_reader, daemon=True)
    t_reader.start()
    t_reader.join(16)
    stop.set()
    ok = (partial[0] is None
          and all(os.path.isfile(p) for p in paths)
          and all(open(p, "rb").read() == b for p, b in zip(paths, bodies)))
    check("RF2 no truncated cache file observed mid-flush (R2)",
          ok, partial[0] or "all reads complete")

    # ── RF3: cdn_host_port churn vs snapshot/prune (R5) ────────────────────
    for i in range(6):
        R.record_cdn_health(f"cdn{i}.example.net", 200, 12)
    errs = []
    # Force frequent GIL switches so the unlocked iteration actually
    # overlaps a mutation window (default 5ms slices rarely collide).
    sys.setswitchinterval(0.0001)

    def _churn():
        for i in range(400):
            with R._cdn_port_lock:
                for k in range(300):
                    R._cdn_host_port[f"churn{k}.example.net"] = 9000 + k
                for k in range(0, 300, 2):
                    R._cdn_host_port.pop(f"churn{k}.example.net", None)

    churners = [threading.Thread(target=_churn, daemon=True) for _ in range(2)]
    for t in churners:
        t.start()
    for i in range(4000):
        try:
            R.snapshot_cdn_health()
            if i % 500 == 0:
                R.prune_cdn_health()
        except Exception as e:
            errs.append(f"{type(e).__name__}: {e}")
            break
    for t in churners:
        t.join(10)
    sys.setswitchinterval(0.005)
    check("RF3 snapshot/prune survive registry churn (R5)",
          not errs, errs[0] if errs else "4000 snapshots clean")

    # ── RF4: SSE reader escape hatch (R6) ──────────────────────────────────
    class _FakeSSEUpstream:
        status_code = 200
        headers = {"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}

        def iter_content(self, chunk_size=4096, decode_unicode=False):
            while True:
                yield b"event: tick\ndata: 1\n\n"
                time.sleep(0.01)

        def close(self):
            pass

    def _sse_readers():
        return sum(1 for t in threading.enumerate() if t.name == "sse-reader")

    before = _sse_readers()
    fake = _FakeSSEUpstream()
    resp = R._stream_sse_resp(fake, "GET", "https://example.com/stream")
    gen = resp.response
    first = next(gen)                       # starts the sse-reader thread
    time.sleep(3.0)                         # let buf_q (256 slots) fill up
    gen.close()                             # abandon the consumer (GeneratorExit)
    time.sleep(7.0)                         # > put-retry timeout of 5s
    leaked = _sse_readers() - before
    check("RF4 abandoned SSE stream does not leak the reader (R6)",
          leaked == 0 and first == b"event: tick\ndata: 1\n\n",
          f"leaked threads: {leaked}")

    # ── RF5: visited eviction order (R9) ───────────────────────────────────
    R._VISITED_MAX = 10
    with R.visited_lock:
        R.visited.clear()
    for i in range(20):
        try:
            R._crawl(f"https://example.com/u{i}")   # visited.add fires first
        except Exception:
            pass                                     # domain check rejects — fine
    newest = [f"https://example.com/u{i}" for i in range(12, 20)]
    oldest = [f"https://example.com/u{i}" for i in range(0, 12)]
    ok_new = all(u in R.visited for u in newest)
    ok_old = all(u not in R.visited for u in oldest)
    check("RF5 visited evicts the OLDEST half in order (R9)",
          ok_new and ok_old,
          f"newest kept={ok_new} oldest evicted={ok_old} size={len(R.visited)}")

    # ── RF6: crawl lifecycle — sentinels & discovery pool (R3) ─────────────
    R._crawl = lambda u: None                    # no network in this battery
    R.WORKERS = 4
    for i in range(20):
        R.url_queue.put(f"https://example.com/page{i}")
    t_crawl = threading.Thread(target=R.crawl_parallel, daemon=True)
    t_crawl.start()
    t_crawl.join(20)
    time.sleep(0.3)
    alive = {t.name.split("-")[0] for t in threading.enumerate()
             if t.name.startswith(("crawl-", "discovery-"))}
    n_disc = sum(1 for t in threading.enumerate() if t.name.startswith("discovery-"))
    n_crawl = sum(1 for t in threading.enumerate()
                  if t.name.startswith("crawl-") and not t.name.startswith("crawl-main"))
    check("RF6 crawl workers exit via sentinels; discovery pool exact (R3)",
          "crawl" not in alive and n_disc == R._DISCOVERY_WORKERS and n_crawl == 0,
          f"discovery={n_disc}/{R._DISCOVERY_WORKERS} crawl-alive={n_crawl}")

    # ── RF7: malformed item must not kill the save worker (T3) ────────────
    R.save_queue.put(())                         # unpack ValueError
    u_good = "https://example.com/resilient/ok.txt"
    R.save_queue.put((u_good, b"still-alive", "text/plain", None))
    time.sleep(1.2)
    p_good = R.local_path(u_good)
    check("RF7 save worker survives a malformed queue item (T3)",
          os.path.isfile(p_good) and open(p_good, "rb").read() == b"still-alive",
          "worker alive + good item written")

    # ── RF8: one queued body == exactly +1 saved stat (R10 worker side) ───
    s0 = R.stats.snapshot().get("saved", 0)
    u_stat = "https://example.com/stats/one.txt"
    R.save_queue.put((u_stat, b"x", "text/plain", None))
    time.sleep(1.0)
    delta = R.stats.snapshot().get("saved", 0) - s0
    check("RF8 one save-queue item increments 'saved' exactly once",
          delta == 1, f"delta={delta}")

    # ── RF9: source contracts for boot-time-only fixes ─────────────────────
    main_src = inspect.getsource(R.main)
    wsgi_src = open(os.path.join(WORK, "Site2Local", "Network", "Server", "WSGI.py"),
                    encoding="utf-8").read()
    resp_src = open(os.path.join(WORK, "Site2Local", "HTTP", "Streaming", "Response.py"),
                    encoding="utf-8").read()
    hdlr_src = open(os.path.join(WORK, "Site2Local", "Network", "Server", "Handler.py"),
                    encoding="utf-8").read()
    jobs_src = open(os.path.join(WORK, "Site2Local", "Crawler", "Crawl", "Jobs.py"),
                    encoding="utf-8").read()
    ok_r3 = "_start_discovery_workers()" not in main_src   # only crawl_parallel starts them
    ok_r4 = "_crawl_done.set()" in main_src                # procedural mode marks done
    ok_r7 = "acquire(blocking=False)" in wsgi_src          # pool overflow path exists
    ok_r8 = "Tunnel handshake send failed" in hdlr_src        # tunnel 101 fd leak closed
    ok_r10 = 'stats.inc("saved")' not in resp_src          # no enqueue-side double count
    ok_order = "_visited_order" in jobs_src                # insertion-order eviction
    check("RF9 source contracts (R3/R4/R7/R8/R10/evict-order)",
          ok_r3 and ok_r4 and ok_r7 and ok_r8 and ok_r10 and ok_order,
          f"R3={ok_r3} R4={ok_r4} R7={ok_r7} R8={ok_r8} R10={ok_r10} ord={ok_order}")

    # ── summary ─────────────────────────────────────────────────────────────
    passed = sum(1 for _, ok in results if ok)
    print(f"\n=== SUMMARY ===\n{passed}/{len(results)} passed")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
