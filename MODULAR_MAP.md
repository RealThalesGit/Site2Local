# Site2Local — Modular Map (9.0)

Living documentation of the modular tree. The monolith `s2l.py` is kept
frozen as the parity reference; **the modular tree is the live code**.

## Layout

```
Site2Local/
├── __main__.py          python -m Site2Local entrypoint
├── Application.py       Flask app assembly (routes + before/after hooks)
├── Runtime.py           shared process state, compat re-exports, main()
├── Config.py            user settings (VERSION, SITE, flags)
│
├── Core/                framework-agnostic building blocks
│   ├── Cache/           URL → disk-path mapping
│   │   ├── Paths.py       local_path() + .q sidecar collision guard
│   │   ├── Naming.py      _safe_seg, _beautify_query_name (every param
│   │   │                  joins NAME_VALUE), _cap_query_fname (post-join
│   │   │                  FS cap), _encode_query_for_filename (the
│   │   │                  reversible collision-fallback encoding)
│   │   └── Sweep.py       startup garbage sweep (9.0): deletes cache
│   │                       files no URL can resolve to (pre-9.1 / 9.1a /
│   │                       09/10 layouts, orphan .q sidecars), liveness
│   │                       proven via local_path()
│   ├── MIME/
│   │   ├── Guess.py       guess_mime + UNIFIED content resolver (Fix 6R, 9.1)
│   │   ├── ContentType.py _ensure_wasm_content_type, _ct_base
│   │   └── Assets.py      is_static_asset / is_subresource
│   ├── Compression.py   gzip/deflate/br/zstd (incl. streaming bomb guard)
│   ├── Logging/         log(), lolcat rainbow
│   ├── Stats/           _Stats counters + CDN health tracking
│   └── URL/             normalize/rewrite/detect-platform helpers
│
├── HTTP/                request/response plumbing (no Flask routes)
│   ├── Headers/
│   │   ├── Isolation.py  COOP/COEP logic (Fix 7, 9.1):
│   │   │                  · documents AND iframes establish isolation
│   │   │                  · workers + worklets get COEP
│   │   │                  · COEP credentialless (Chromium, Firefox ≥119)
│   │   │                  · body-signal fallback (_needs_isolation)
│   │   ├── CORS.py        credentialed CORS + CORP + PNA
│   │   ├── Response.py    filter_resp() + _finalize_security_headers()
│   │   ├── Security.py    nosniff + Referrer-Policy
│   │   ├── CI.py          case-insensitive header container shims
│   │   ├── Cookies.py     Set-Cookie extraction/flattening (the
│   │   │                  SameSite/Secure rewrite itself lives in
│   │   │                  Headers/Response.py _rewrite_cookie)
│   │   └── Forwarding.py  request-header filtering for upstream
│   ├── Sessions/        curl_cffi → cloudscraper → requests session tiers
│   └── Streaming/       _stream_resp (decompress+cache) + SSE proxy
│
├── Proxy/               the main reverse proxy
│   ├── Routes.py        proxy() — the /<path> view (live pipeline)
│   ├── Assets.py        /__s2l_ext__/<host>/<path> CDN fallback route
│   ├── Upstream/        _do_upstream, _make_flask_resp, redirect policy
│   ├── Cache/           Serve (_serve_cached/_cached_response),
│   │                    Validators (ETag w/ CT crc32 — 9.1 auto-heal,
│   │                    304s), Range (206 from disk)
│   └── TunnelRoutes.py  ws/tcp/udp tunnel views (socket-layer handles them)
│
├── CDN/
│   ├── Server.py        per-host Werkzeug mini-servers (MULTIPORT)
│   └── Registry/        host→port registry, real-origin rewrite
│
├── Crawler/
│   ├── Crawl/           queues, workers, link extraction, SAVE WORKER
│   │                    (writes bodies + .q sidecars — 9.1)
│   └── Scanner/         SCAN_PATHS wordlist scanner
│
├── Rewriting/           HTML/CSS/JSON URL rewriting + S2L JS injector
├── WebSockets/          frame codec, WS proxy, tunnels
├── Network/             pooled WSGI server + raw TCP/UDP pumps
├── Hooks/               traffic hooks + PyQt6 inspector GUI
├── Capture/             request/response/WS JSON capture workers
├── Viewers/             /__s2l_viewer__ + CDN host table page
├── CLI/                 startup banner
└── s2l.py               FROZEN monolith (parity reference, pre-9.1)
```

## Response pipeline (who sets what)

| Stage | Function | Sets |
|---|---|---|
| upstream headers in | `filter_fwd` | strips hop-by-hop / client hints |
| upstream headers out | `filter_resp` | CORS+CORP, COOP/COEP, nosniff, cookie rewrite |
| final Flask response | `_make_flask_resp` / `_finalize_security_headers` | same trio again (safety net) |
| cache hit (main) | `_cached_response` → `_serve_cached` | CT via **unified resolver**, Cache-Control, ETag(+CT crc32) |
| cache hit (CDN/ext) | `_cdn_serve` / `ext_asset` | + 206 via `_serve_range_from_disk` (9.1) |
| 304 | `_conditional_304` | ETag folds CT crc32 → MIME auto-heal |

## Content-Type resolution (Fix 6R, one resolver everywhere)

`_resolve_mime_with_sniff(lp, data=None, live_ct="")` — authority order:

1. magic bytes at 0 (`\0asm`, PNG, JPEG, GIF, RIFF/WEBP, woff2, ftyp/mp4…)
2. SVG prologue
3. HTML doctype (not RSC wire payload)
4. extension via `guess_mime` — **trusted** unless the extension belongs to
   the lying family (`.js/.css/.json`) or maps to an obscure type outside
   `_TRUSTED_CT_BASES` (the `.gl` → `video/gl` globe.gl case)
5. content: JSON (prefix-completing parser) → JS (token consensus ≥4) →
   CSS (@-rules / property shapes)
6. keep extension answer → live_ct → `application/octet-stream`

One 2048-byte window everywhere (200 / 304-probe / 206 agree).

## __query__ cache names (9.2 corrected beautify)

```
/runtime/Aya.App.js?v=26b38…  → __query__/runtime/Aya.App_v_26b38….js
/css2?family=Montserrat:wght@400&display=swap
                               → __query__/css2_family_Montserrat_wght@400_N_display_swap
/v1/asset?id=7                 → __query__/v1/asset_id_7
```

The user's sketch: every parameter joins its NAME with its value —
1 param → `[ARQUIVO]_[NOMEQUERY]_[VALOR]`, 2+ params →
`[ARQUIVO]_[K1]_[V1]_N_[K2]_[V2]` (`N` stands for "&").

Every query cache write records the owning raw query in `<file>.q`. When a
different query would beautify to the same name, `local_path()` detects the
conflict via the sidecar and the losing URL falls back to the collision-free
reversible encoding. Joined names that would still overflow the filesystem
limit get a head+sha1 cap (`_cap_query_fname`).

**9.0 (the "new generation" cut):** the migration bridges are GONE — old-
layout caches (pre-9.1 `_= encodings, 9.1a value-only names, 09/10 host-tree
files) are no longer renamed on request; such a URL is a plain MISS (online
re-fetches into the current name, offline 404s). The startup sweep
(`Sweep.py`, unconditional) deletes those unreachable files
— a file goes only when NO candidate URL maps back to it via `local_path()`,
so the sweep can never delete anything servable.

## Fix history (this release)

- **Fix 6R** — content-based MIME resolver restored + unified (was lost in a
  sandbox recycle; cured the Google-Fonts-as-octet-stream marketplace break
  and the globe.gl `video/gl` black-globe break).
- **Fix 7** — cross-origin isolation hardened: iframes + fencedframes join
  documents, worklets join workers, XHTML joins HTML, COEP `credentialless`
  on Chromium AND Firefox ≥ 119, live-HTML from CDN mini-servers and the
  ext route now pass through the isolation gate, streamed worker scripts
  get COEP.
- **Fix 8** — beautify design correction: the query NAME always joins the
  value (`Aya.App_v_26b38….js`, `asset_id_7`); post-join filename caps keep
  overlong no-extension/collision-fallback names writable.
- **Fix 9** — race-condition hardening (2026-11-08 audit): atomic cache
  writes (temp + `os.replace`, `.s2l-tmp` temps skipped by sweep/purge);
  the save worker resolves `local_path()` at FLUSH time and dedups by
  (url, device) key, so same-batch colliding queries split into pretty +
  fallback instead of clobbering each other; malformed queue items can
  no longer kill the save/capture workers; discovery workers start only
  after the crawl sentinels (main() no longer starts them early);
  procedural/OFFLINE boots mark `_crawl_done` so `url_queue` can't grow
  without a consumer; `snapshot/prune_cdn_health` snapshot the port
  registry under `_cdn_port_lock` (no more viewer 500s); the SSE consumer
  sets `upstream_done` on abandonment (no more leaked sse-reader
  threads); visited eviction follows insertion order (deque) instead of
  hash order; WSGI pool gains an overflow path so long-lived WS/SSE
  connections can't starve all HTTP serving; the tunnel 101-send failure
  closes the upstream socket (fd leak); streamed saves count 'saved'
  once (worker side only). Plus 324 dead import bindings stripped from
  61 files.
- **Fix 10** — hop-by-hop correctness (RFC 7230): header names listed in a
  `Connection` field are now stripped dynamically on BOTH legs (request
  and response) — the peer itself declares what is hop-by-hop, so custom
  names work with zero list maintenance; `upgrade` stays exempt (the
  WebSocket handler owns it); the static list's ancestral "trailers"
  typo (inherited verbatim from the frozen monolith) is fixed to the
  RFC 7230 §4.4 name `Trailer`.
- **Fix 11** — CDN port-sentinel leak (the "localhost:-1" report): the
  browser injector serialized the RAW host→port registry, so a page
  served during a mini-server's registration race window carried
  `HP[host] = -1`; the JS rewriter's truthiness checks then emitted
  `http://localhost:-1/…` (and the WS twin). The injector now ships
  only live ports (`> 0`, same filter as `.__s2l_hp`), the rw()/WS
  guards are explicit `> 0`, and the live HP poll REPLACES a changed
  port instead of skipping existing entries — a server that re-registered
  on a new port now heals without a page reload.
- **Fix 12** — human byte-size literals: `parse_size()` in Config accepts
  `"512"`, `"512BYTE"`, `"256KB"`, `"8MB"`, `"1.5GB"` (case-insensitive,
  1024-based, plain ints still pass through, garbage raises a loud
  ValueError at boot). All 7 size flags (`SCAN_LIMIT`, `WS_MAX_MSG_BYTES`,
  `MAX_BODY_BYTES`, `MAX_DECOMPRESSED_BYTES`, `STREAM_CACHE_MAX_BYTES`,
  `_EDITOR_MAX_BYTES`, `_STREAM_MIN_BYTES`) are now human strings,
  normalized to ints once at import — zero behavior change (battery
  asserts bit-for-bit parity with the old `256*1024`-era values).
- **Fix 13** — streamed-response TypeError (live 500 on `/api/cdn/dl/…zip`):
  `HTTP/Streaming/Response.py` has passed `is_worker_script=` to
  `filter_resp()` since the Worker hardening landed, but the signature
  never accepted the keyword — EVERY streamed response (large downloads,
  media, 206 ranges) died with `TypeError: filter_resp() got an
  unexpected keyword argument 'is_worker_script'`. No battery exercised
  `_stream_resp`, so it survived until real usage. `filter_resp()` now
  takes `is_worker_script: bool | None = None` — `None` auto-detects via
  Sec-Fetch-Dest (all other callers keep byte-identical behavior), an
  explicit bool overrides. The battery also audits the WHOLE tree for
  the same bug class (caller keywords vs. callee signatures): 124 files,
  zero remaining mismatches.
- **Fix 14** — optional-dependency audit (the "_brotli/_zstd/_ai_cloudscraper
  are missing" report): the three are NOT missing — they are optional-dep
  tiers with graceful fallback, and every requirements.txt claim matches
  PyPI reality (cloudscraper frozen at 1.2.71/2023-04-25; ai-cloudscraper
  3.8.4 active, installs module `cloudscraper`, so the first import in the
  tier chain is a future-proofing no-op and the second is what binds;
  curl-cffi 0.17.0 alive with cp314 + abi3 Windows wheels). The audit found
  REAL garbage instead: (1) the brotli "tolerant streaming fallback" called
  `Decompressor.finish()`, a method the official brotli package NEVER
  exposed (verified 1.1.0 and 1.2.0) — dead code since the monolith that
  silently returned raw compressed bytes; finish() is now only called when
  the binding actually has it (brotlipy-style APIs). (2) The zstd
  size-less-frame fallback ITERATED stream_reader(), which modern
  zstandard (0.25.0) rejects with io.UnsupportedOperation — size-less
  frames came back as raw compressed bytes; now a bounded read() loop
  with the same bomb cap/log as gzip, plus a frame-header pre-check
  (zstandard's one-shot decompress() ignores max_output_size when the
  frame declares its size — verified empirically). (3) requirements.txt
  never listed zstandard although the code supports Content-Encoding:
  zstd. (4) The WAFW00F not-installed notice logged at ERROR while every
  other optional dep logs WARN — now WARN, with a clearer message.
- **Fix 15** — lint-polish pass (the "any more garbage?" sweep): pyflakes
  over the live tree reported 260 warnings — 251 are the three documented
  re-export surfaces (Runtime.py's compat hub, 261 imports all
  re-referenced via `_s2l_state`; `Proxy/Routes.py`'s noqa'd TunnelRoutes
  compat line; `__init__.py`'s public `app`/`main`), 9 were real dead
  imports that had survived `strip_dead_imports.py` because their names
  appear inside docstrings ("time a WSGI request reaches…", "socket,
  CLOSE frame…"): `time` ×3, `json`, `field`, `socket`,
  `requests.adapters` ×3 — and dropping the last exposed `import
  requests` as dead in the same three Proxy files (pyflakes had been
  folding the duplicate binding into one warning). 12 bindings removed
  across 8 files, zero behavior change (py_compile + full 8-battery
  rerun, 90/90). The wider sweep found nothing else: no junk files
  (`.bak`/`__pycache__`/`*.orig`/…), no TODO/FIXME markers in the live
  tree, all 41 dead-code-scan candidates verified alive (including the
  three newest: `_SIZE_RE`/`_SIZE_UNITS` from Fix 12 and `_pool_slots`),
  and the frozen monolith's own legacy code stays untouched by design.
- **Fix 16** — universal-default pass (the "remove everything site-specific"
  request): the whole-tree audit found exactly ONE functional site-specific
  line — `Config.py`'s `SITE = "jeskojets.com"`. Now `SITE = "example.com"`
  (RFC 2606 §3 reserved domain; identical to the frozen s2l.py monolith's
  own default, so both flavors ship the same neutral target). Everything
  else that mentions real domains is universal machinery, not site config:
  WAF/bot-gate markers (`ddos-guard.net`, `challenges.cloudflare.com`,
  hcaptcha/reCAPTCHA paths), DONT_REWRITE suffix-match examples in
  comments, and aya.nodium.lol citations kept as fix-provenance docs.
  The dev batteries no longer string-replace the old default — each
  injects its test host with a regex over the SITE assignment itself, so
  a future default change can never silently turn the patch into a no-op.
- **9.0 cleanup (the "new generation" cut)** — migration machinery removed
  (`Legacy.py`, `Migration.py`, `_migration_checked` state, frozen 9.1a
  formula, 09/10 lossy sanitizer); unreachable legacy caches swept at
  startup; query strings in every access-log line; version line held at
  9.0 per user decision.
- **9.x extras** — ETag folds a crc32 of the CT base (MIME auto-heal);
  Range requests on CDN/ext cache-hits return real 206s; beautified
  `__query__` names with `.q` sidecar collision guard; purges never leave
  orphan sidecars.

## Test batteries

Dev-side batteries (kept in the maintainer's `scripts/` workspace — NOT
shipped inside the Site2Local tree):

- `test_aya_e2e.py` — the 9.0 contract battery (42 checks): isolation
  matrix, NO-migration semantics + startup sweep (deletes pre-9.1/9.1a/
  09/10, keeps live pretty/fallback/sidecar files, idempotent), sidecar
  collision (percent-decode pairs), lying/obscure extensions, truncated
  JSON, ETag auto-heal, 206/200 CT parity, COEP-by-UA, beautify unit
  matrix, filename caps. Uses the real aya.nodium.lol files + Google
  Fonts css2.
- `test_log_query.py` — access-log query-string battery (HIT/304/206/
  EXT/no-ext lines, `_fmt_path_qs` unit matrix).
- `test_hopbyhop.py` — Fix 10 battery (6 checks): RFC 7230 §6.1 dynamic
  `Connection` tokens on both legs, the `upgrade`/WS exemption, the
  `Trailer` (§4.4) name fix, list-valued Connection fields (curl_cffi
  shape), keyword-only no-op guard.
- `test_cdn_port.py` — Fix 11 battery (5 checks): injector HP excludes
  -1/0 sentinels, `>0` JS guards, live-poll replace-on-change,
  `.__s2l_hp` filtering, `_proxy_target` ext fallback.
- `test_size_flags.py` — Fix 12 battery (5 checks): `parse_size` unit
  matrix, bit-for-bit parity with the pre-Fix-12 values, int consumer
  surface, pure string literals in Config, re-export.
- `test_stream_worker.py` — Fix 13 battery (6 checks): kwarg + auto-detect
  semantics, full `_stream_resp` happy path (the user's zip-download
  scenario), streamed worker script gets COEP, explicit override, Fix 10
  rules preserved inside the streaming path (dynamic Connection tokens,
  gzip + Content-Length drop), and a whole-tree keyword/signature audit
  (124 files, package `_s2l_state.*` calls + local by-name calls in
  s2l.py) so this bug class can never ship silently again.
- `test_optdeps.py` — Fix 14 battery (7 checks): brotli one-shot
  regression, revived fallback (brotlipy-shape and official-shape shims
  with the one-shot forced to fail), zstd sized + size-less frames +
  bomb guard on both, Accept-Encoding negotiation tracks _BROTLI_OK,
  cloudscraper tier chain semantics (the exact source block exec'd under
  fake modules: fork shape / future rename / neither), WAFW00F WARN
  source contract (ast-based), zstd streamed end-to-end through
  _stream_resp.
- `test_race_fixes.py` — Fix 9 battery (9 checks): same-batch sidecar
  collision, atomic-write torn reads, CDN-registry churn, SSE reader
  leak, visited eviction order, crawl sentinel lifecycle, malformed-item
  worker resilience, save accounting, source contracts.
- Field check — headless Chromium against the live mirror:
  `crossOriginIsolated: true`, zero console errors, hero renders.
