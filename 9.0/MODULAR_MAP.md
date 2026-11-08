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
│   │   ├── Cookies.py     Set-Cookie rewriting (SameSite/Secure)
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
├── State/               runtime state holders
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

- `scripts/test_aya_e2e.py` — the 9.0 contract battery: isolation matrix,
  NO-migration semantics + startup sweep (deletes pre-9.1/9.1a/09/10,
  keeps live pretty/fallback/sidecar files, idempotent), sidecar collision
  (percent-decode pairs), lying/obscure extensions, truncated JSON, ETag
  auto-heal, 206/200 CT parity, COEP-by-UA, beautify unit matrix, filename
  caps. Uses the real aya.nodium.lol files + Google Fonts css2.
- `scripts/test_log_query.py` — access-log query-string battery (HIT/304/
  206/EXT/no-ext lines, `_fmt_path_qs` unit matrix).
- Field check — headless Chromium against the live mirror:
  `crossOriginIsolated: true`, zero console errors, hero renders.
