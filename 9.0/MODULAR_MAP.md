# MODULAR_MAP — The Match-Based Modularization Guide

> **Site2Local** · **version 9.0** · this document is the map between the
> modular tree and the `s2l.py` monolith (13,705 lines), kept **in the same
> folder as the package** as a reference. **Every symbol in the modular tree
> points to the monolith line it came from** — it is your compass to keep
> doing the *match* work (comparing module ↔ monolith during the
> transition).

---

## 0. Release 9.0 (Nov 8, 2026) — the modular line gets its own number

The version now lives in **one place only**: `Config.VERSION` (the boot
banner reads it from there). The **9.x line is the modular tree**; the
frozen monolith `s2l.py` rests at 8.1 as the historical match reference —
the only deliberate divergences are the post-match fixes (section 6) and
the version banner.

**What 9.0 inaugurates** (everything verified by its own battery):
- the complete monolith→modular migration (final audit: config 27/27 ·
  routes 10/10 · smoke 9/9 · symbols 193/0 lost · constants 196/0 lost ·
  injector +241 bytes = only fixes 3b/4);
- fixes 1-2: cache revalidation per device profile + coherent client
  hints (19/19);
- fixes 3a-3b: COOP/COEP isolation per engine (the end of
  `NS_ERROR_DOM_CORP_FAILED` on Firefox) + the `setProperty` patch
  (15/15);
- fix 4: the end of `localhost:-1` in the summary/injector (18/18);
- fix 5: content-based MIME rescue — the globe's black ball
  (46/46 + E2E with pixel-perfect visual parity);
- fix 6: content-based MIME for **extensionless** cache files — the end of
  `NS_ERROR_CORRUPTED_CONTENT` on reloaded stylesheets (css2 class) and of
  the JSON-API purge-refetch loop (36/36 unit + 16/16 E2E against the live
  site that reported it, headless browser included);
- the **Universality Principle** codified: no fix tied to a specific
  site; the target site is INPUT, a field bug is a CLASS detector.

---

## 1. Architecture

| Decision | How it ended up |
|---|---|
| **`__init__.py`** | **Exactly 1** in the whole project: `Site2Local/__init__.py`. Every subfolder is a *namespace package* (PEP 420) — they work without an `__init__`. |
| **State hub** | `Runtime.py` keeps all shared state and imports every leaf module **directly** (`from .Core.URL.Format import ...`), without going through an intermediate `__init__`. |
| **Compatibility** | `import s2l` / `Site2Local.runtime` / `Site2Local.config` keep working through aliases in the root `__init__.py`. |
| **Layout** | **Single folder**: the repository root IS the `Site2Local/` package folder — modules, `s2l.py`, `Boot.py`, `requirements.txt` and docs live in the same path. |
| **Monolith** | `s2l.py` stays inside the folder, **untouched and runnable** (`python Site2Local/s2l.py`, or `python s2l.py` from inside), for side-by-side comparison. |
| **Execution** | `python3 Site2Local/Boot.py` (from the parent folder) or `python3 Boot.py` (from inside) · equivalents: `python -m Site2Local` (from the parent folder) and `python s2l.py` (monolith). Runtime data (`site_src/`, `site_data/`) always lands **inside** the folder. |

### Why just 1 `__init__.py`?

The old 33 `__init__.py` files did two things: 32 were empty markers or
mechanical re-exports (X imports from Y just so the Runtime could re-import
it). With *namespace packages*, the folder exists as a package without a
single file, and the Runtime imports straight from the leaf — one less
level of indirection, zero magic, and `git grep` finds any symbol in
exactly one place.

### The golden rule to keep doing the match

> When you move/create code, update this map: the new symbol gets the
> monolith line it came from; a symbol with no origin is marked **new**.

---

## 2. Fragmentation of this revision (the 3 giants)

| File before | After | Reason for the seam |
|---|---|---|
| `Hooks/GUI.py` (1,790) | `Hooks/GUI/App.py` (1,720) + `Codecs.py` + `Find.py` | The Qt part is a single *closure* (it mirrors the monolith's single `_launch_hook_gui` function) — only the pure helpers (hex/codec/find) can be extracted without risk. **App.py stays big on purpose**: taking the closure apart would mean rewriting 1.7k lines of Qt with no way to test it. *Evidence (AST scan)*: of the 40 nested functions of `_gui_main`, **0** have zero closure variables — they all touch widgets (`tree`, `body_box`, …) or local Qt imports; there is nothing left to extract at a zero diff. |
| `Viewers/CDN.py` (1,082) | `Viewers/CDN/Page.py` (template) + `Endpoint.py` | 1,058 lines were a single HTML string. The template came out **byte-identical** (verified against the monolith) and the logic became 24 lines. |
| `Proxy/Routes.py` (896) | `Proxy/Routes/{Main, Guards, Fetch, Recovery, Fallbacks}.py` | `proxy()` was an 875-line function. Now `Main.py` is a ~250-line pipeline with **12 numbered phases**; each phase became a testable helper. |
| `Rewriting/Injector.py` (588) | `Rewriting/Injector/Build.py` (34) + `Template.py` (573) | The 554-line `js = ( ... )` came out **verbatim** into `Template.py` — only the function header changed (`hp`/`_dr_regex_js` became parameters). **Verified: byte-identical injector** (11,604 bytes at match close; 11,815 today — post-match fix #3b added the `setProperty` patch, see section 6). |

**Verifications run** (all green):
- `py_compile` on 97 modules
- full import of the tree (with real Flask)
- OFFLINE smoke test: `/` 404, `/.__s2l_hp` 200, `/__s2l_viewer__` 200 (63 KB), `/__s2l_viewer_data__` 200, `/__s2l_reset_stats__` 200, OPTIONS 204
- ONLINE smoke test: `GET /` → 200, 140 KB downloaded from upstream, `__s2l__` injector applied, cache saved to `site_src/`
- roundtrip of the extracted Codecs (`_bytes_to_hexdump` ↔ `_hexdump_to_bytes`, encoding detection)
- **the monolith ≡ modular parity battery** (AST symbols + routes + side-by-side smoke — section 6)
- **byte-identical `__s2l__` injector**: `_build_s2l_injector()` from the monolith × from the modular tree → the same 11,604 bytes (after the `Injector/` fragmentation; 11,815 in the modular tree today — post-match fix #3b, see section 6)
- **single-folder layout**: real boots of the 3 entry points (`Site2Local/Boot.py` from the parent folder, `Boot.py` from inside, `python -m Site2Local`) — server up and `site_src/`/`site_data/` always **inside** the folder; the historical `import s2l` works from inside

### Where the new seams came from in the monolith

The helpers created by the fragmentation are **verbatim** code from chunks
of the monolith's `proxy()` — each one keeps the match with the line range
it came from:

| New symbol | Came from `s2l.py` (inside `def proxy()`, line 12499) |
|---|---|
| `_guard_preflight` | lines ~12520-12581 (prefetch / SW / blocked / OPTIONS) |
| `_offline_response` | lines ~12583-12641 (the whole OFFLINE branch) |
| `_decide_streaming` | lines ~12689-12713 (stream/SSE heuristics) |
| `_fetch_with_retries` | lines ~12715-12801 (retry ladder + cache fallback) |
| `_push_redirect_history` | lines ~12802-12824 (redirect history in the GUI) |
| `_maybe_stream` | lines ~12826-12876 (SSE / streaming hand-off) |
| `_consume_upstream_body` | lines ~12878-12909 (decompress + empty-stream recovery) |
| `_waf_block_retry` | lines ~12911-12965 (WAF block detection + retry) |
| `_handle_304` | lines ~12966-12997 (304 without validators) |
| `_recover_empty_body` | lines ~12998-13100 (empty bodies A/B/C) |
| `_cdn_fallback` | lines ~13101-13171 (CDN host fallback) |
| `_rsc_refetch` | lines ~13186-13217 (RSC payload → full HTML) |
| `_bot_page_response` | lines ~13236-13262 (bot page → 503) |
| `_save_and_enqueue` | lines ~13264-13299 (save to cache + discovery) |
| `_rewrite_resp_body` | lines ~13301-13321 (HTML/CSS/JSON post-processing) |
| `_final_blank_page_recovery` | lines ~13328-13388 (blank-page guard) |
| `_bytes_to_hexdump` / `_hexdump_to_bytes` / `_ws_auto_encoding` | inside `_launch_hook_gui` (line 1196) |
| `_find_all` | inside `_launch_hook_gui` (line 1196) — it gained the `highlight_hex` param (it used to be the closure's `_YLW_HI` constant) |
| `_CDN_VIEWER_PAGE` (Page.py) | inline string of `_s2l_viewer_endpoint` (line ~11020), byte-identical |

---

## 3. Complete tree (single folder: 98 modules · 15,750 lines · 1 `__init__.py`)

The repository root **is** the package folder — code, the reference
monolith, launcher, requirements and docs all live in the same path:

```
Site2Local/
├── Capture/
│   ├── HTTP/
│   │   ├── Record.py
│   │   └── Worker.py
│   └── WebSocket/
│       ├── Record.py
│       └── Worker.py
├── CDN/
│   ├── Registry/
│   │   ├── Hosts.py
│   │   └── Origins.py
│   └── Server.py
├── CLI/
│   └── Banner.py
├── Core/
│   ├── Cache/
│   │   ├── Legacy.py
│   │   ├── Migration.py
│   │   ├── Naming.py
│   │   └── Paths.py
│   ├── Logging/
│   │   ├── Logger.py
│   │   └── Lolcat.py
│   ├── MIME/
│   │   ├── Assets.py
│   │   ├── ContentType.py
│   │   └── Guess.py
│   ├── Stats/
│   │   ├── CDNHealth.py
│   │   └── Model.py
│   ├── URL/
│   │   ├── Format.py
│   │   ├── Network.py
│   │   ├── Platform.py
│   │   ├── Policy.py
│   │   ├── Request.py
│   │   └── Rewrite.py
│   └── Compression.py
├── Crawler/
│   ├── Crawl/
│   │   ├── Assets.py
│   │   ├── Hosts.py
│   │   ├── Jobs.py
│   │   ├── Links.py
│   │   └── Save.py
│   └── Scanner/
│       ├── Runner.py
│       ├── Status.py
│       └── Wordlists.py
├── Hooks/
│   ├── GUI/
│   │   ├── App.py
│   │   ├── Codecs.py
│   │   └── Find.py
│   └── Hooks/
│       ├── Context.py
│       ├── GUIHooks.py
│       ├── GUITraffic.py
│       ├── GUIWS.py
│       ├── Registry.py
│       └── Store.py
├── HTTP/
│   ├── Headers/
│   │   ├── CI.py
│   │   ├── Cookies.py
│   │   ├── CORS.py
│   │   ├── Forwarding.py
│   │   ├── Isolation.py
│   │   ├── Response.py
│   │   └── Security.py
│   ├── Sessions/
│   │   ├── Clients.py
│   │   ├── Cloudflare.py
│   │   ├── Devices.py
│   │   └── Factory.py
│   └── Streaming/
│       ├── Detect.py
│       ├── Response.py
│       └── SSE.py
├── Network/
│   ├── Server/
│   │   ├── Handler.py
│   │   └── WSGI.py
│   └── Tunnels.py
├── Proxy/
│   ├── Cache/
│   │   ├── Range.py
│   │   ├── Serve.py
│   │   └── Validators.py
│   ├── Routes/
│   │   ├── Fallbacks.py
│   │   ├── Fetch.py
│   │   ├── Guards.py
│   │   ├── Main.py
│   │   └── Recovery.py
│   ├── Upstream/
│   │   ├── Headers.py
│   │   ├── Request.py
│   │   └── Response.py
│   ├── Assets.py
│   └── TunnelRoutes.py
├── Rewriting/
│   ├── HTML/
│   │   ├── Policy.py
│   │   ├── Reveal.py
│   │   ├── ServiceWorker.py
│   │   └── URLs.py
│   ├── Injector/
│   │   ├── Build.py
│   │   └── Template.py
│   └── JSON.py
├── State/
├── Viewers/
│   ├── CDN/
│   │   ├── Endpoint.py
│   │   └── Page.py
│   ├── Common.py
│   ├── Data.py
│   ├── Hosts.py
│   └── Reset.py
├── WebSockets/
│   ├── Protocol/
│   │   ├── Compression.py
│   │   ├── Frames.py
│   │   └── Hooks.py
│   ├── Upstream/
│   │   ├── CFFI.py
│   │   ├── Connect.py
│   │   └── Raw.py
│   └── Tunnel.py
├── Application.py
├── Boot.py
├── Config.py
├── MODULAR_MAP.md
├── README.md
├── requirements.txt
├── Runtime.py
├── s2l.py
├── .gitignore
├── __init__.py
└── __main__.py
```

---

## 4. Match table — symbol → line in `s2l.py`

> Auto-generated: 215 module-level symbols, **193 with a direct match** in
> the monolith, 22 marked **new** (seams created by this revision's
> fragmentation: `proxy()` phases, GUI helpers and `create_app`). Symbols
> inside closures (e.g. nested functions of `_launch_hook_gui`) don't show
> up — their match is the outer function's.

### `Application` (37 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def create_app` | **new** (fragmentation seam) |

### `CDN.Registry.Hosts` (59 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _register_cdn_host` | line 6831 (`def _register_cdn_host`) |
| `def _preregister_cdn_hosts_from_disk` | line 6846 (`def _preregister_cdn_hosts_from_disk`) |

### `CDN.Registry.Origins` (133 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _real_origin_for` | line 6479 (`def _real_origin_for`) |
| `def _proxy_target` | line 6875 (`def _proxy_target`) |

### `CDN.Server` (307 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _start_cdn_server` | line 6540 (`def _start_cdn_server`) |

### `CLI.Banner` (138 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _banner` | line 13380 (`def _banner`) |

### `Capture.HTTP.Record` (71 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _CaptureRecord` | line 7654 (`class _CaptureRecord`) |
| `def _capture_path` | line 7668 (`def _capture_path`) |
| `def _encode_body` | line 7702 (`def _encode_body`) |

### `Capture.HTTP.Worker` (62 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _capture_worker` | line 7708 (`def _capture_worker`) |
| `def _maybe_capture` | line 7739 (`def _maybe_capture`) |

### `Capture.WebSocket.Record` (33 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _WSCaptureRecord` | line 7766 (`class _WSCaptureRecord`) |
| `def _ws_capture_dir` | line 7778 (`def _ws_capture_dir`) |

### `Capture.WebSocket.Worker` (63 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _ws_capture_worker` | line 7783 (`def _ws_capture_worker`) |
| `def _maybe_capture_ws` | line 7814 (`def _maybe_capture_ws`) |

### `Core.Cache.Legacy` (97 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _legacy_cache_paths` | line 5326 (`def _legacy_cache_paths`) |

### `Core.Cache.Migration` (82 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _migrate_legacy_cache` | line 5428 (`def _migrate_legacy_cache`) |

### `Core.Cache.Naming` (90 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _safe_seg` | line 5230 (`def _safe_seg`) |
| `def _sanitize_query_for_filename` | line 5243 (`def _sanitize_query_for_filename`) |
| `def _encode_query_for_filename` | line 5274 (`def _encode_query_for_filename`) |

### `Core.Cache.Paths` (103 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def local_path` | line 5503 (`def local_path`) |

### `Core.Compression` (201 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def decompress_body` | line 3466 (`def decompress_body`) |
| `def _decompress_body_impl` | line 3482 (`def _decompress_body_impl`) |

### `Core.Logging.Logger` (30 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def log` | line 250 (`def log`) |

### `Core.Logging.Lolcat` (79 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _lolcat_internal` | line 175 (`def _lolcat_internal`) |
| `def _probe_lolcat` | line 197 (`def _probe_lolcat`) |
| `def _lolcat_real` | line 227 (`def _lolcat_real`) |

### `Core.MIME.Assets` (36 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def is_static_asset` | line 3752 (`def is_static_asset`) |
| `def is_subresource` | line 3769 (`def is_subresource`) |

### `Core.MIME.ContentType` (41 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _ensure_wasm_content_type` | line 3677 (`def _ensure_wasm_content_type`) |
| `def _ct_base` | line 3720 (`def _ct_base`) |

### `Core.MIME.Guess` (53 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def guess_mime` | line 3660 (`def guess_mime`) |
| `def _resolve_mime_with_sniff` | line 3723 (`def _resolve_mime_with_sniff`) |

### `Core.Stats.CDNHealth` (95 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def record_cdn_health` | line 6375 (`def record_cdn_health`) |
| `def snapshot_cdn_health` | line 6409 (`def snapshot_cdn_health`) |
| `def prune_cdn_health` | line 6424 (`def prune_cdn_health`) |
| `def _start_health_pruner` | line 6449 (`def _start_health_pruner`) |

### `Core.Stats.Model` (66 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _Stats` | line 6295 (`class _Stats`) |

### `Core.URL.Format` (50 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _fmt_size` | line 4519 (`def _fmt_size`) |
| `def _fmt_host` | line 4526 (`def _fmt_host`) |
| `def log_req` | line 4545 (`def log_req`) |

### `Core.URL.Network` (79 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _is_ip_literal` | line 476 (`def _is_ip_literal`) |
| `def _is_ipv6_literal` | line 494 (`def _is_ipv6_literal`) |
| `def _is_wire_payload` | line 513 (`def _is_wire_payload`) |
| `def _is_valid_html` | line 521 (`def _is_valid_html`) |
| `def resolve_ip` | line 4504 (`def resolve_ip`) |
| `def url_depth` | line 4510 (`def url_depth`) |
| `def is_html` | line 4513 (`def is_html`) |

### `Core.URL.Platform` (40 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def set_last_platform` | line 548 (`def set_last_platform`) |
| `def get_last_platform` | line 554 (`def get_last_platform`) |
| `def detect_platform` | line 558 (`def detect_platform`) |

### `Core.URL.Policy` (29 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def is_allowed_domain` | line 6275 (`def is_allowed_domain`) |
| `def is_external_domain` | line 6284 (`def is_external_domain`) |

### `Core.URL.Request` (76 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _raw_path_after` | line 445 (`def _raw_path_after`) |
| `def _request_host_header` | line 4416 (`def _request_host_header`) |
| `def _request_hostname` | line 4433 (`def _request_hostname`) |
| `def _proxy_base` | line 4442 (`def _proxy_base`) |

### `Core.URL.Rewrite` (118 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def rewrite_abs_urls` | line 4447 (`def rewrite_abs_urls`) |
| `def strip_fragment` | line 4497 (`def strip_fragment`) |
| `def normalize_url` | line 4500 (`def normalize_url`) |
| `def build_base_url` | line 5601 (`def build_base_url`) |

### `Crawler.Crawl.Assets` (120 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _clean_url_match` | line 7433 (`def _clean_url_match`) |
| `def _fetch_external_asset` | line 7468 (`def _fetch_external_asset`) |
| `def _start_ext_asset_workers` | line 8000 (`def _start_ext_asset_workers`) |
| `def _ext_asset_worker` | line 8010 (`def _ext_asset_worker`) |

### `Crawler.Crawl.Hosts` (42 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _mark_host_dead` | line 7360 (`def _mark_host_dead`) |
| `def _is_host_dead` | line 7373 (`def _is_host_dead`) |

### `Crawler.Crawl.Jobs` (187 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def enqueue` | line 7833 (`def enqueue`) |
| `def _crawl` | line 7841 (`def _crawl`) |
| `def _crawl_worker` | line 7963 (`def _crawl_worker`) |
| `def crawl_parallel` | line 8083 (`def crawl_parallel`) |

### `Crawler.Crawl.Links` (78 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _discovery_worker` | line 8023 (`def _discovery_worker`) |
| `def _start_discovery_workers` | line 8042 (`def _start_discovery_workers`) |
| `def _extract_links_async` | line 8050 (`def _extract_links_async`) |

### `Crawler.Crawl.Save` (144 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _save_worker` | line 7519 (`def _save_worker`) |

### `Crawler.Scanner.Runner` (351 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _run_path_scanner` | line 5936 (`def _run_path_scanner`) |

### `Crawler.Scanner.Status` (187 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _scan_paths_summary` | line 5736 (`def _scan_paths_summary`) |
| `def _parse_status_csv` | line 5771 (`def _parse_status_csv`) |
| `def _prompt_scan_status_filter` | line 5796 (`def _prompt_scan_status_filter`) |

### `Crawler.Scanner.Wordlists` (83 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _load_wordlist` | line 5670 (`def _load_wordlist`) |
| `def _resolve_wordlists` | line 5688 (`def _resolve_wordlists`) |

### `HTTP.Headers.CI` (61 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _ci_header_keys` | line 4009 (`def _ci_header_keys`) |
| `def _ci_get_header` | line 4028 (`def _ci_get_header`) |
| `def _ci_remove_header` | line 4050 (`def _ci_remove_header`) |

### `HTTP.Headers.CORS` (110 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _cors_origin_pair` | line 3946 (`def _cors_origin_pair`) |
| `def _pna_allow_header` | line 3969 (`def _pna_allow_header`) |
| `def _apply_corp_header` | line 4068 (`def _apply_corp_header`) |
| `def _apply_cors_headers` | line 4074 (`def _apply_cors_headers`) |

### `HTTP.Headers.Cookies` (132 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _all_set_cookies` | line 3788 (`def _all_set_cookies`) |
| `def _resp_headers_dict` | line 3833 (`def _resp_headers_dict`) |
| `def _flatten_cookiejar` | line 3859 (`def _flatten_cookiejar`) |

### `HTTP.Headers.Forwarding` (65 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def filter_fwd` | line 3784 (`def filter_fwd`) |
| `def inject_csrf_headers` | line 4354 (`def inject_csrf_headers`) |
| `def rewrite_origin` | line 4362 (`def rewrite_origin`) |

### `HTTP.Headers.Isolation` (137 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _needs_isolation` | line 3913 (`def _needs_isolation`) |
| `def _is_worker_request` | line 4161 (`def _is_worker_request`) |
| `def _is_document_request` | line 4183 (`def _is_document_request`) |
| `def _coep_value` | line 4198 (`def _coep_value`) |
| `def _apply_isolation_headers` | line 4209 (`def _apply_isolation_headers`) |

### `HTTP.Headers.Response` (102 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def filter_resp` | line 4257 (`def filter_resp`) |
| `def _finalize_security_headers` | line 4331 (`def _finalize_security_headers`) |

### `HTTP.Headers.Security` (43 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _apply_security_headers` | line 4122 (`def _apply_security_headers`) |

### `HTTP.Sessions.Clients` (63 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _get_proxy_session` | line 3407 (`def _get_proxy_session`) |
| `def _client_id` | line 3419 (`def _client_id`) |
| `def _get_client_session` | line 3428 (`def _get_client_session`) |
| `def _short_exc` | line 3451 (`def _short_exc`) |

### `HTTP.Sessions.Cloudflare` (131 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _next_cf_config` | line 3085 (`def _next_cf_config`) |
| `def _is_cf_block` | line 3255 (`def _is_cf_block`) |
| `def _is_raw_block_text` | line 3343 (`def _is_raw_block_text`) |
| `def _is_bot_page` | line 3383 (`def _is_bot_page`) |

### `HTTP.Sessions.Devices` (61 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _sanitize_ua` | line 3016 (`def _sanitize_ua`) |
| `def _detect_device_from_ua` | line 3019 (`def _detect_device_from_ua`) |
| `def _effective_device` | line 3030 (`def _effective_device`) |
| `def _device_variant` | line 3038 (`def _device_variant`) |

### `HTTP.Sessions.Factory` (154 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _make_cffi_session` | line 3116 (`def _make_cffi_session`) |
| `def _make_session` | line 3135 (`def _make_session`) |
| `def _make_cloudscraper_session` | line 3195 (`def _make_cloudscraper_session`) |
| `def _make_requests_session` | line 3229 (`def _make_requests_session`) |

### `HTTP.Streaming.Detect` (46 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _should_stream` | line 8115 (`def _should_stream`) |
| `def _is_sse_response` | line 9436 (`def _is_sse_response`) |

### `HTTP.Streaming.Response` (193 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _stream_resp` | line 8139 (`def _stream_resp`) |

### `HTTP.Streaming.SSE` (176 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _sse_iter` | line 9449 (`def _sse_iter`) |
| `def _sse_stream_generator` | line 9490 (`def _sse_stream_generator`) |
| `def _stream_sse_resp` | line 9527 (`def _stream_sse_resp`) |

### `Hooks.GUI.App` (1720 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _launch_hook_gui` | line 1196 (`def _launch_hook_gui`) |

### `Hooks.GUI.Codecs` (63 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _bytes_to_hexdump` | **new** (fragmentation seam) |
| `def _hexdump_to_bytes` | **new** (fragmentation seam) |
| `def _ws_auto_encoding` | **new** (fragmentation seam) |

### `Hooks.GUI.Find` (35 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _find_all` | **new** (fragmentation seam) |

### `Hooks.Hooks.Context` (45 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class HookContext` | line 579 (`class HookContext`) |

### `Hooks.Hooks.GUIHooks` (98 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _origin_matches` | line 976 (`def _origin_matches`) |
| `def _apply_gui_hooks` | line 1007 (`def _apply_gui_hooks`) |

### `Hooks.Hooks.GUITraffic` (147 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _gui_push` | line 847 (`def _gui_push`) |
| `def _gui_push_raw` | line 905 (`def _gui_push_raw`) |

### `Hooks.Hooks.GUIWS` (126 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _gui_ws_push` | line 739 (`def _gui_ws_push`) |
| `def _apply_gui_ws_hooks` | line 764 (`def _apply_gui_ws_hooks`) |

### `Hooks.Hooks.Registry` (63 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _norm_methods` | line 609 (`def _norm_methods`) |
| `def on_request` | line 616 (`def on_request`) |
| `def on_response` | line 625 (`def on_response`) |
| `def _run_hooks` | line 634 (`def _run_hooks`) |

### `Hooks.Hooks.Store` (146 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _make_hook_store` | line 1056 (`def _make_hook_store`) |
| `def _editor_preview` | line 1164 (`def _editor_preview`) |

### `Network.Server.Handler` (412 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _S2LWSGIRequestHandler` | line 9805 (`class _S2LWSGIRequestHandler`) |

### `Network.Server.WSGI` (59 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _PooledWSGIServer` | line 9757 (`class _PooledWSGIServer`) |

### `Network.Tunnels` (120 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _pump_tcp_over_ws` | line 9632 (`def _pump_tcp_over_ws`) |

### `Proxy.Assets` (204 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def ext_asset` | line 12277 (`def ext_asset`) |

### `Proxy.Cache.Range` (104 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _parse_range_header` | line 10628 (`def _parse_range_header`) |
| `def _serve_range_from_disk` | line 10768 (`def _serve_range_from_disk`) |

### `Proxy.Cache.Serve` (188 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _serve_cached` | line 10823 (`def _serve_cached`) |
| `def _cached_response` | line 10906 (`def _cached_response`) |

### `Proxy.Cache.Validators` (126 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _disk_validators` | line 10659 (`def _disk_validators`) |
| `def _conditional_304` | line 10676 (`def _conditional_304`) |
| `def _apply_freshness` | line 10734 (`def _apply_freshness`) |

### `Proxy.Routes.Fallbacks` (162 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _cdn_fallback` | **new** (fragmentation seam) |
| `def _rsc_refetch` | **new** (fragmentation seam) |
| `def _bot_page_response` | **new** (fragmentation seam) |

### `Proxy.Routes.Fetch` (241 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _decide_streaming` | **new** (fragmentation seam) |
| `def _fetch_with_retries` | **new** (fragmentation seam) |
| `def _push_redirect_history` | **new** (fragmentation seam) |
| `def _maybe_stream` | **new** (fragmentation seam) |

### `Proxy.Routes.Guards` (137 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _guard_preflight` | **new** (fragmentation seam) |
| `def _offline_response` | **new** (fragmentation seam) |

### `Proxy.Routes.Main` (239 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _save_and_enqueue` | **new** (fragmentation seam) |
| `def _rewrite_resp_body` | **new** (fragmentation seam) |
| `def proxy` | line 12499 (`def proxy`) |

### `Proxy.Routes.Recovery` (335 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _consume_upstream_body` | **new** (fragmentation seam) |
| `def _waf_block_retry` | **new** (fragmentation seam) |
| `def _handle_304` | **new** (fragmentation seam) |
| `def _recover_empty_body` | **new** (fragmentation seam) |
| `def _final_blank_page_recovery` | **new** (fragmentation seam) |

### `Proxy.TunnelRoutes` (27 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def ws_ext` | line 12478 (`def ws_ext`) |
| `def tcp_ext` | line 12486 (`def tcp_ext`) |
| `def udp_ext` | line 12491 (`def udp_ext`) |

### `Proxy.Upstream.Headers` (88 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _main_security_headers` | line 10212 (`def _main_security_headers`) |
| `def _normalize_request_origin` | line 10216 (`def _normalize_request_origin`) |
| `def _cors_allow_headers` | line 10270 (`def _cors_allow_headers`) |

### `Proxy.Upstream.Request` (261 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _upstream_url` | line 10300 (`def _upstream_url`) |
| `def _build_ctx` | line 10306 (`def _build_ctx`) |
| `def _is_sw_path` | line 10321 (`def _is_sw_path`) |
| `def _do_upstream` | line 10376 (`def _do_upstream`) |

### `Proxy.Upstream.Response` (63 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _make_flask_resp` | line 10583 (`def _make_flask_resp`) |

### `Rewriting.HTML.Policy` (41 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _is_dont_rewrite_host` | line 6962 (`def _is_dont_rewrite_host`) |

### `Rewriting.HTML.Reveal` (32 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _reveal_hidden` | line 4573 (`def _reveal_hidden`) |

### `Rewriting.HTML.ServiceWorker` (59 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _inject_sw_clear` | line 5177 (`def _inject_sw_clear`) |

### `Rewriting.HTML.URLs` (340 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _rewrite_ext_urls` | line 6993 (`def _rewrite_ext_urls`) |

### `Rewriting.Injector.Build` (34 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _build_s2l_injector` | line 4595 (`def _build_s2l_injector`) — the Python logic (lock, `hp`, DONT_REWRITE → `_dr_regex_js`) verbatim; the tail calls `_injector_js(hp, _dr_regex_js).encode("utf-8")` |

### `Rewriting.Injector.Template` (573 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _injector_js` | **new** (fragmentation seam) — the core is the monolith's `js = ( ... )` from lines 4618-5172, **verbatim**; `hp`/`_dr_regex_js` were closure locals that became parameters |

### `Rewriting.JSON` (42 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _rewrite_json_urls` | line 6932 (`def _rewrite_json_urls`) |

### `Runtime` (1719 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def main` | **new** (fragmentation seam) |

### `Viewers.CDN.Endpoint` (24 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _s2l_viewer_endpoint` | line 11202 (`def _s2l_viewer_endpoint`) |

### `Viewers.Common` (76 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _viewer_cors_headers` | line 11025 (`def _viewer_cors_headers`) |
| `def _viewer_disabled_response` | line 11050 (`def _viewer_disabled_response`) |
| `def _viewer_ip_denied_response` | line 11059 (`def _viewer_ip_denied_response`) |
| `def _viewer_ip_allowed` | line 11065 (`def _viewer_ip_allowed`) |

### `Viewers.Data` (104 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _s2l_viewer_data_endpoint` | line 11110 (`def _s2l_viewer_data_endpoint`) |

### `Viewers.Hosts` (26 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _s2l_hp_endpoint` | line 10995 (`def _s2l_hp_endpoint`) |

### `Viewers.Reset` (30 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _s2l_reset_stats_endpoint` | line 11090 (`def _s2l_reset_stats_endpoint`) |

### `WebSockets.Protocol.Compression` (67 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _ws_deflate_init` | line 8405 (`def _ws_deflate_init`) |
| `def _ws_deflate_msg` | line 8420 (`def _ws_deflate_msg`) |
| `def _ws_inflate_msg` | line 8431 (`def _ws_inflate_msg`) |
| `def _ws_deflate_cleanup` | line 8445 (`def _ws_deflate_cleanup`) |

### `WebSockets.Protocol.Frames` (190 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _ws_unmask` | line 8369 (`def _ws_unmask`) |
| `def _ws_handshake_accept` | line 8854 (`def _ws_handshake_accept`) |
| `def _recv_exact` | line 8860 (`def _recv_exact`) |
| `def _ws_read_frame` | line 8889 (`def _ws_read_frame`) |
| `def _ws_make_frame` | line 8958 (`def _ws_make_frame`) |

### `WebSockets.Protocol.Hooks` (34 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def on_ws_message` | line 8456 (`def on_ws_message`) |
| `def _run_ws_msg_hooks` | line 8462 (`def _run_ws_msg_hooks`) |

### `WebSockets.Tunnel` (405 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _WSTunnel` | line 8471 (`class _WSTunnel`) |
| `def _ws_keepalive_loop` | line 8521 (`def _ws_keepalive_loop`) |
| `def _pump_ws_frames_v2` | line 8573 (`def _pump_ws_frames_v2`) |

### `WebSockets.Upstream.CFFI` (164 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _CffiUpstreamWS` | line 9249 (`class _CffiUpstreamWS`) |

### `WebSockets.Upstream.Connect` (68 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `def _get_session_for_ws` | line 9221 (`def _get_session_for_ws`) |
| `def _ws_connect_upstream` | line 9395 (`def _ws_connect_upstream`) |

### `WebSockets.Upstream.Raw` (245 lines)

| Symbol | Match in `s2l.py` |
|---|---|
| `class _RawSocketUpstreamWS` | line 8994 (`class _RawSocketUpstreamWS`) |

---

## 5. How to continue the modularization work

0. **Starting point**: `python3 Site2Local/Boot.py` brings the modular proxy
   up; `python Site2Local/s2l.py` brings the monolith up — run both to
   compare behavior during the match.
1. **Pick a target** in the map above (by size: `Hooks/GUI/App.py` is the
   next natural giant; `Rewriting/Injector.py` has 588 lines of JS template
   that would fit in an `Injector/Template.py`).
2. **Open the monolith at the indicated line** and compare section by
   section.
3. **Move the code without changing it** — the value of the match is the
   zero diff.
4. **Update the Runtime** (leaf import) and this map.
5. **Run the battery**: `python -m compileall Site2Local`, import, and the
   two smoke tests (OFFLINE and ONLINE) described in section 2.

---

## 6. Monolith ≡ modular parity (executed battery)

Side-by-side test: the `s2l.py` monolith and the `Site2Local` package
booted in separate folders, both with `OFFLINE=True`, same `SITE`, through
the WSGI *test client* (no sockets). **Verdict: MONOLITH ≡ MODULAR.**
*(Re-executed after the single-folder restructuring — identical result.)*

| Test | Result |
|---|---|
| **Symbols (AST)** | 193 top-level defs/classes in the monolith → **0 lost** in the modular tree. The 29 extras: the 21 seams of section 2 + `create_app` (`Application.py`) + `main` (`Runtime.py`) + `_coep_credentialless_supported` (post-match fix #3a) + `_looks_like_js`/`_js_mime_rescue` (fix #5) + `_looks_like_css`/`_looks_like_json`/`_sniff_nonhtml_type` (fix #6). |
| **Bodies (symbol-level match)** | **186/193 bodies identical** to the monolith in the post-normalization AST comparison (new docstrings, route decorators, `Runtime` aliases and the `global`→hub-attribute swap dissolved — see the rules below). The remaining 4 are the documented seams (`proxy`, `_launch_hook_gui`, `_s2l_viewer_endpoint`, `_build_s2l_injector`), whose fidelity is proven by behavior (smoke hashes + byte-identical injector) and by the verbatim fragments. The 3 that diverge **on purpose** are post-match fixes: `_do_upstream` (#2) and `_coep_value` + `_apply_isolation_headers` (#3a — see the subsection below). |
| **Module constants (state-level match)** | 196 top-level constants in the monolith → **194/194 stable ones identical** in the Runtime (0 missing, 0 different). The remaining 2 (`_START_TS`, `_last_platform`) float between two boots of the monolith itself and are present in the Runtime. |
| **Config** | 27 knobs (`SITE`…`SCAN_PATHS`) identical on both sides. |
| **Flask routes** | 10/10 identical rules: same URL, same methods, same endpoint. |
| **OFFLINE smoke** | 9 endpoints with the same status, same `Content-Length` and same body `sha256` — including the 63,533-byte viewer, byte-identical. |
| **`__s2l__` injector** | `_build_s2l_injector()` byte-identical at match close: 11,604 bytes on both sides. Today 11,815 in the modular tree — post-match fix #3b added the `setProperty` patch (the monolith stays at 11,604, frozen as the reference). |

### Body-match normalization rules

The AST comparison dissolves (on BOTH sides) only the differences that the
modularization itself demands — any other difference would count as a
divergence:

| Normalization | Why it is legitimate |
|---|---|
| docstring removed | seams gained new docstrings ("Extracted from…") |
| decorators removed | in the modular tree routes are registered via `add_url_rule` in `Application.py` (url_map parity proven separately) |
| `ALIAS.Name` → `Name` (when ALIAS imports the Runtime) | the monolith uses the direct name; leaves use `_s2l_state.Name` |
| import aliases resolved (e.g. `flask_request` → `request`) | same object, different local name |
| `global X` removed | in the modular tree the module's mutable state became a hub attribute (`_s2l_state.X`) — a semantically mandatory transformation |

With these 5 rules, **187 of the 193 bodies collapse into identical**
(before post-match fixes #2 and #3a; after them, 186) — meaning the logic
was migrated unchanged; what changed is only the state addressing, exactly
as the architecture demands.

### Deliberately replicated quirk (bug-for-bug compatibility)

In the monolith, the three tunnel routes register the rule with one name
and the view signs with another (`<path:wspath>` ↔ `def ws_ext(_wspath)`,
lines 12477-12493 of `s2l.py`). Flask passes `wspath=…` as a *keyword* to
a function that only accepts `_wspath` → **TypeError → 500** instead of
the didactic 426 the code's own comment describes. Minimal impact: these
views only run when someone opens the tunnel URL directly in the browser
(real WS traffic is intercepted at the socket layer, before Flask).

The modular seam had aligned the names (rule `_wspath` ↔ view `_wspath` →
426). **Reverted to the monolith's behavior** during the match phase: the
rules in `Application.py` are `wspath`/`tcppath`/`udppath` verbatim again.
To fix it for real once match mode is over, it's one line per route:
align the rule's variable name with the view's signature.

### The `SITE` divergence (pre-existing in the project, not from the seam)

The monolith embeds `SITE = "example.com"` (line 272), but the project's
historical `Config.py` already had `SITE = "jeskojets.com"`. The modular
tree preserves the user's `Config.py` (the right knob). In the parity
battery `SITE` was equalized in the test sandboxes to isolate the variable
— the 2 bytes of difference that show up in `viewer_data` are just the
`"target"` field.

### Post-match fixes (phase 2 — deliberate functional divergences)

Real field tests (the user browsing a game portal with server-side device
detection and cross-origin game iframes, `poki.com`) exposed three holes
the monolith also has — **they are not seam regressions**, they are
original behaviors the match phase preserved faithfully. Fixed **only in
the modular package**; the `s2l.py` monolith stays frozen as the match
reference. Each fix is a small, localized block if you ever want to
backport it.

**Reported symptom (fixes 1-2)**: with `DEVICE` on `auto`, `desktop` or
`macintosh`, the site served the mobile HTML version
(`<body class="mobileVersion">`), identical across the three profiles.

**Field diagnosis (reproduced)**: the target site picks the variant
**exclusively from the User-Agent** — A/B cookies and client hints don't
influence it (tested one by one); all desktop-class UAs (Windows, Mac,
Linux, ChromeOS, Safari, IE11, Googlebot and even a missing UA) get
`desktopVersion`. The proxy's live path was correct (verified end to end:
`auto`+desktop → desktop, `auto`+mobile → mobile in its own variant, cache
hit preserving the canonical). What remained was the **canonical cache
serving previously recorded mobile HTML** (from an old profile or a
pre-variant build): `auto`/`desktop`/`macintosh` all map to the SAME
canonical file, and nothing ever revalidated that file when the profile
changed.

**Reported symptom (fix 3)**: on Firefox, DevTools (Network) shows two
requests to `a.poki-cdn.com/.../rotation.svg` with
`NS_ERROR_DOM_CORP_FAILED` (0 B, initiator `img`) when opening a game —
the resource is blocked by the page's CORP policy.

**Field diagnosis (reproduced)**: the monolith (and the modular tree, until
this fix) applies COOP `same-origin` + COEP on **every** document
navigation (`_is_document_request()`). On Chromium the COEP is
`credentialless`, which loads anonymous cross-origin subresources without
CORP — but **Firefox and Safari don't implement `credentialless`** (MDN,
Dec 2025): for them what's left is `require-corp`, which **hard-blocks**
any no-CORS cross-origin subresource whose response doesn't carry CORP.
Now, the proxy rewrites and proxies the overwhelming majority of URLs, but
a URL built at runtime can escape all the patches (template literals like
`https://cdn/icons/${name}.svg` in the Poki SDK, CSS custom properties
with `url()` — `--icon-src`, imgs injected via innerHTML before the
MutationObserver microtask). On Chrome these escapees load; on Firefox
they die with `NS_ERROR_DOM_CORP_FAILED`. The real site, it's worth
noting, **sends no COOP/COEP/CORP at all** — the blanket was a restriction
added by the proxy, not by the mirrored site. (Full context: the realm
chain poki.com → games.poki.com (wrapper) → gdn.poki.com (game), all
served by local CDN ports with the injector; the game chunks load from
`localhost:8081`; only the runtime-built URLs from the SDK escape.)

| # | Fix | Where | New behavior |
|---|---|---|---|
| 1 | Cache revalidation when `DEVICE` changes | `Runtime.py` (`_DEVICE_CHANGED`, `_DEVICE_REFRESHED` + `_note_device_change()` nested in `main()`) and `Proxy/Routes/Main.py` (Phase 3 of `proxy()`) | The active profile is written to `site_data/<site>/device_profile.json` on every boot. When it changes between boots: WARN in the log + every cached HTML document is re-fetched upstream **once** (per file, per session) and rewritten with the current profile's variant — assets keep coming from cache (desktop-class profiles share the same ones). Heals mirrors whose canonical was recorded under another profile/build. |
| 2 | Client hints coherent with the profile | `Proxy/Upstream/Request.py` (`_do_upstream`) | Explicit profiles (`DEVICE != "auto"`) strip the real browser's `Sec-CH-UA*` headers and install the profile's (`_SEC_CH_UA`); `auto` keeps mirroring the browser (backfill included), and `Sec-Fetch-*` pass through untouched in both modes. Reason: someone testing under mobile emulation (DevTools) was sending `Sec-CH-UA-Mobile: ?1` along with the profile's desktop UA — hint-first backends would serve mobile even with `DEVICE=macintosh`. |
| 3a | Browser-aware COOP/COEP isolation | `HTTP/Headers/Isolation.py` (new `_coep_credentialless_supported()` + gate in `_apply_isolation_headers`) | The COOP+COEP blanket on document navigation becomes **Chromium-only** (the only engine family with `credentialless`). Firefox/Safari only get isolation when the page itself asks for it (the `_needs_isolation()` heuristic — SAB/WASM markers in the body) or when it's a Worker script (path unchanged, `require-corp`). Mirrors the real site's posture (most sites send no COEP) and kills Firefox's `NS_ERROR_DOM_CORP_FAILED`: the escapees now load straight from the real CDN (online) instead of being blocked. `_coep_value()` was rebuilt on top of the new helper — same regex, same result. |
| 3b | `setProperty` patch in the injector | `Rewriting/Injector/Template.py` (CSS block, after the `cssText` patch) | `CSSStyleDeclaration.prototype.setProperty` now passes values with `url(...)` through `_cssRw` — covers CSS custom properties (`--icon-src: url(https://cdn/x.svg)`, Poki's icon system uses exactly that through React style props), which the `backgroundImage`/`cssText` setters never saw. These URLs are now rewritten by `rw()` and load through the local CDN port instead of going straight to the real CDN. Injector: 11,604 → 11,815 bytes. |
| 4 | The end of `localhost:-1` (the MULTIPORT bind sentinel) | `Runtime.py` (`_bg_crawl` summary loop), `Rewriting/Injector/Build.py` (`p > 0` filter in the injector's initial bake) and `Rewriting/Injector/Template.py` (the HP poll mirrors the server's map) | **Symptom**: on the `jeskojets.com` mirror boot the post-crawl summary printed `CDN cdn.prod.website-files.com → http://localhost:-1`. **Diagnosis**: `-1` is the "mini-server still binding" sentinel (`CDN/Registry/Hosts.py`) — the real port only enters the map when werkzeug's `make_server` finishes (`CDN/Server.py`), and the `host → :8081` line is logged only AFTER that; the crawl summary snapshotted at the instant `crawl_parallel()` returned (the 2-page crawl finished within the same second as the registration) and printed the stale copy. The map itself was never wrong — nothing ever routed to port -1. **Fix**: the summary now re-reads the live port at print time, with a bounded grace window (≤2s in 50ms steps) for hosts still binding, skips entries that vanished (bind failed — ERROR already logged by `_start_cdn_server`) and labels `0` as the `/__s2l_ext__/` route. **Audit of every consumer of the map**: the server-side rewriter (`_proxy_target`) already guarded `port > 0`; `/__s2l_hp`, the viewer and health already filtered `p > 0`; `Guards.py`/`Fallbacks.py` only used the keys. The only real leak was the **injector's initial bake**, which didn't filter: a page served in the millisecond window between sentinel and bind carried `HP[host] = -1` (truthy in JS → `rw()` emitted `http://localhost:-1/...`), and the add-only poll (`if(!HP[h])`) would never repair an existing value. The bake now filters `p > 0` (the same filter as `/__s2l_hp`) — in that window the host falls back to the `/__s2l_ext__/` route on the main port until the poll (first tick 1.5s) brings the real port — and the poll now **mirrors** the map: adds new hosts, updates changed ports (crash + re-register picks up the NEW port; add-only would keep the dead one forever) and removes hosts whose server died (URLs go back to the `/__s2l_ext__/` route, which is what re-registers the host). Injector: +30 bytes. |
| 5 | Content-based MIME rescue (scripts behind an exotic extension) | `Core/MIME/Guess.py` (new `_looks_like_js` + `_js_mime_rescue`), applied in `Proxy/Cache/Serve.py` (`_serve_cached` + the 304 probe of `_cached_response`), `CDN/Server.py` (304 probe + live path), `Proxy/Assets.py` (304 probe + live path), `Proxy/Cache/Range.py` (206), `Proxy/Routes/Fallbacks.py` (disk fallback + live) and `Proxy/Cache/Validators.py` (`_disk_validators` now doubles the CT into the ETag); seam in `Runtime.py` | **Symptom**: on the `jeskojets.com` mirror the Global section rendered a **black ball** instead of the textured 3D globe with arcs (side-by-side comparison with the real site). **Diagnosis (reproduced with headless Chromium)**: the page loads `<script src="https://cdn.jsdelivr.net/npm/globe.gl">` — the package NAME ends in `.gl`, and the stdlib maps `.gl` → `video/gl` (the 1993 Graphics Language format, still in the IANA registry). The disk cache stores only the body (the upstream's `application/javascript` isn't persisted), so every cache-hit serve re-derives the Content-Type from the extension and delivers `video/gl`. With `X-Content-Type-Options: nosniff` (both we and jsdelivr send it), Chromium **refuses to execute** a classic script with a non-JS MIME: `window.Globe` never exists, the Slater chunk's `typeof Globe !== 'function'` guard returns early, and the styled container (a dark circle) shows up in place of the WebGL. The texture (`globe-map.svg`) wasn't even requested. The first visit (live fetch, upstream CT) worked; subsequent visits (cache hit) broke — the classic "worked once, then never again". **Fix**: `_js_mime_rescue(ct, lp, head)` — when the derived type is NOT JavaScript and is NOT in the never-touch list (html/css/xml/json/svg/wasm, where a JS-looking head would be a false positive — CSS `/*!` banners are nearly identical to JS ones), it sniffs the first 512 bytes with `_looks_like_js` (strips up to 3 leading comment blocks and matches high-precision token prefixes; English prose like "various"/"constant" doesn't match because a space after the token is required) and serves it as `text/javascript; charset=utf-8`. Identical bytes, correct label; real binaries (PNG/MP4/WASM) never match and keep the extension's type. Applied at EVERY point that derives a CT from disk (main-host cache-hit 200/304/206, CDN mini-server, the `/__s2l_ext__/` route, fallbacks) and also on the live paths (CDN + ext + fallback), covering a missing/lying upstream CT. **Healing poisoned caches**: the browser keeps the STORED response's Content-Type across matching 304s — anyone who already cached `video/gl` would never see the corrected type; the ETag now folds a crc32 of the base CT (`s2l:size:mtime:xxxxxxxx`), so the derivation change invalidates the validator and forces one fresh 200 per asset per client (and any future MIME fix self-heals the same way). Deduplicated WARN log (cap 256) per rescued file. |
| 6 | Content-based MIME for **extensionless** cache files | `Core/MIME/Guess.py` (new `_looks_like_css` + `_looks_like_json` + `_sniff_nonhtml_type` + `_SNIFF_HEAD = 2048`; the extensionless arm of `_resolve_mime_with_sniff` now sniffs CSS/JS/JSON), `Proxy/Cache/Serve.py` (`_serve_cached`: the no-extension branch, plus the 6b rescue for CSS/JS/JSON bodies stored at `index.html` paths; `_cached_response`: probe head 512→2048), seam in `Runtime.py`. The 304 probes of `CDN/Server.py`, `Proxy/Assets.py`, `Proxy/Cache/Range.py` and `Proxy/Routes/Fallbacks.py` inherit the fix through `_resolve_mime_with_sniff` | **Symptom**: on a Next.js storefront mirror (`e-play-inky.vercel.app`), Firefox loads the page fine on the **first visit**, but a **reload** makes the Google Fonts stylesheet request (`fonts.googleapis.com/css2?family=Montserrat…` on the dedicated CDN mini-port) fail with `NS_ERROR_CORRUPTED_CONTENT`, 0 B in the network panel — the page renders without the font. **Diagnosis (reproduced E2E)**: `css2` is a path with NO extension plus a query string, so the 10/10 cache layout stores it as `__query__/css2/family=Montserrat…` — extensionless. The cache-hit serve only sniffed for **HTML** in the no-extension branch and shipped everything else as `application/octet-stream`; combined with the universal `X-Content-Type-Options: nosniff` hardening, a stylesheet that is not `text/css` is refused by the browser (Firefox's `EnsureMIMEOfStyleOrScript` → `NS_ERROR_CORRUPTED_CONTENT`; Chrome logs a MIME refusal). First visit worked (live fetch ships the upstream's real CT), every reload failed — the same "worked once, then never again" shape as fix 5, on the extensionless side of the cache. **Second symptom of the same root**: JSON APIs cached at `index.html` paths (no-query, no-extension endpoints such as `/api/v1/listings`) fail the HTML check and were purged + re-fetched from upstream on EVERY request ("Stale RSC cache — purging" spam in the log; the response never stayed cached). **Fix**: `_sniff_nonhtml_type(data)` — JS via the fix-5 signatures (strictest, checked first), CSS via at-rule prefixes (`@font-face`, `@media`, `@charset`, vendor `@-webkit-`…) or a tight selector-then-brace shape, JSON via a structural gate (`{`/`[` … `}`/`]`) plus a full bounded parse (≤8 MB). The selector regex deliberately excludes `[`, `(`, `=` and `;` — ordinary JS lines (`document.x = {`) and, critically, **JSON arrays of objects** (`[{"id":…`, which the first draft matched as "`[` + block" and mislabeled a list API as `text/css`) can never match; an attribute selector as the very first rule of a stylesheet is the accepted, vanishingly rare miss. CSS/JSON sniffed bodies get their URL-rewrite passes, so the font URLs inside a cached css2 keep pointing at the local CDN ports. **6b**: at `index.html` paths, plain CSS/JS/JSON bodies are now served with their real content type instead of the purge+refetch loop — wire payloads (RSC) and unsniffable junk keep the old behavior, and OFFLINE mode keeps its never-destroy semantics. ETag self-healing comes free from fix 5's CT-folded validators (a browser that cached the poisoned octet-stream entry gets a fresh 200 with the healed type). The sniff head grew 512→2048 bytes (license banners on minified CSS/JS routinely exceed 512). **Verified**: unit battery 36/36 (including false-positive guards for JSONP/`var x = {`/`document.addEventListener(`/binary) and E2E 16/16 against the live site that reported it — first visit live `text/css`, reload via the mini-port `text/css`, nosniff still on, 304 revalidation, healing ETag, fonts fetched 200 through the local proxy, listings JSON served from cache with **0 purges**, and a headless browser confirming `document.fonts.check('16px Montserrat') === true` with **0** MIME/corrupted console errors. |

Verification (1-2): `device_fix_verify.sh` (project scripts) — **19/19
green**: the full canonical-poisoning scenario → profile switch → WARN →
single re-fetch → cache hit on the 2nd visit; hint coherence proven with
an upstream echo server; the `auto`-mode regression trio unchanged.

Verification (3): `corp_fix_verify.sh` (project scripts) — **15/15
green**: Firefox + document + ordinary page → **no** COEP/COOP (CORP
`cross-origin` stays as a universal safeguard); Chrome + document → COOP
`same-origin` + COEP `credentialless` (blanket kept); Firefox + Worker
request → COEP `require-corp` (unchanged); Firefox + page with SAB
markers → isolated even without the blanket (heuristic path);
non-Chromium UA → no COEP; the injector served on the page loads the
`setProperty` patch and the extracted `<script id="__s2l__">` passes
`node --check`; the regression endpoints (viewer/hp/reset) return 200.
Live against `poki.com`: `/br/g/subway-surfers` with a Firefox UA comes
out without COOP/COEP and with a Chrome UA comes out `credentialless` —
and the full parity battery (config 27/27, routes 10/10, smoke 9/9 with
identical hashes) stays green after the fix.

Verification (4): `fix_port_minus1_verify.py` (project scripts) — **18/18
green**: package `py_compile`/`compileall`; the injector bake with a fake
map `{-1, 0, 8085}` comes out with only the live port and no
`localhost:-1`/`localhost:0`; the extracted injector passes `node --check`
(11,835-char body); full injector: 11,865 bytes (+30 over the 3b add-only
expression, same measurement base); mirror-poll semantics proven in Node
(adds `fresh`, updates `stale`, removes `dead`); the summary loop
simulated in all five states — up (→ real port), comes-up-during-the-
grace-window (→ real port after ~120ms), bind-failed (→ no line), no
dedicated port (→ `/__s2l_ext__/` route), never-comes-up (→ "port
pending") — and `localhost:-1` shows up on no path.

Verification (5): `fix_mime_rescue_verify.py` (project scripts) — **46/46
green**, against the live proxy mirroring `jeskojets.com` (real crawl +
disk cache): package `compileall`; `_looks_like_js` battery with REAL
heads from the mirror (globe.gl `// banner → !function`, lenis
`function`, gsap's `/*!` banner) and closed false-positives (CSS with a
`/*!` banner, JSON object/array, HTML, PNG, WASM, "various/constant"
prose, unterminated banner); the rescue battery (video/gl+JS→JS,
video/gl+binary→untouched, octet-stream+JS→JS, text/plain+JS→JS,
text/plain+prose→untouched, never-touch on
css/html/json/svg/wasm/manifest+json/+xml/+json, JS→JS unchanged, empty
head/missing file→untouched); ETag (3-segment format, stable per CT,
differs between CTs, differs from the CT-less legacy); live —
`npm/globe.gl` served `text/javascript; charset=utf-8` with a
deduplicated "MIME rescue" WARN in the log, consistent 304 with ETag,
**the legacy CT-less ETag no longer matches → fresh 200** (poisoned-cache
heal), and zero regression (`gsap.min.js` text/javascript, `lenis.css`
text/css, `img_jet.webp` image/webp; the served HTML rewrites globe.gl to
`:8083` and injects `__s2l__`). End-to-end proof in headless Chromium
against the proxy: before the fix `window.Globe === 'undefined'` and no
canvas in `[globe-container]`; after it `typeof Globe === 'function'`, a
587×587 canvas with the texture loaded, and the capture of the #global
section confirmed by VLM: a 3D globe with visible continents + dashed
route arcs — visual parity with the real site. **E2E re-verification
(Nov 8, 2026, `scripts/globe_e2e_verify.sh` + `globe_luminance.sh`)**:
fresh boot with disk cache → `:8083/npm/globe.gl` cache-hit
`text/javascript; charset=utf-8` (ETag `s2l:size:mtime:crc32ct`),
consistent 304, the served HTML rewrites the script to `:8083`,
`typeof Globe === 'function'` on the mirror, 1 canvas, zero page errors;
mask/overlay/texture (`globe-mask.webp`, `globe-over.webp`,
`globe-map.svg`) all 200 through the CDN ports; the same
`scrollY`/DOM/528×528 canvas as the live site; and **pixel-identical
canvas luminance** (5 frames on each side, average 36.0 × 36.0, identical
temporal evolution) — the residual brightness differences in individual
captures are the entrance animation's phase, not a mirror defect. The
comparison was saved to `download/globe_mirror_vs_live.jpg`.

Verification (6): `fix_extless_mime_verify.py` + `fix6_e2e_verify.sh`
(project scripts) — **36/36 unit green** and **16/16 E2E green**. Unit: the
extensionless css2 cache hit ships `text/css` (RED proof first: the
pre-fix code shipped `application/octet-stream`), font URLs inside it
rewritten to the local proxy; extensionless worker JS →
`text/javascript`; extensionless JSON → `application/json`; PNG at
`__query__/` untouched (octet-stream, byte-identical body); the listings
JSON at an `index.html` path served (not purged — RED proof first),
body intact; RSC wire payloads still purged; probe/serve CT equality
(stable ETags); false-positive guards — `document.addEventListener(`,
JSONP, `var x = {`, CommonJS, binary, JSON object/array, HTML, and the
field regression `[{"id":…` (a JSON array that the first regex draft
mislabeled as CSS). E2E against the live site that reported the bug
(`e-play-inky.vercel.app`, fresh mirror): first visit serves css2 live
as `text/css` through `/__s2l_ext__/` and caches it extensionless under
`__query__/css2/`; the reload serves it from the dedicated CDN mini-port
as `text/css` (the exact request that used to die with
`NS_ERROR_CORRUPTED_CONTENT`), `nosniff` still on, `If-None-Match`
revalidation → 304, a legacy/foreign ETag → fresh 200 (poisoned-cache
heal), the font file itself fetched 200 through the local proxy, the
`/api/v1/listings` JSON served from cache with **zero** purge lines,
and a headless browser confirming `document.fonts.check('16px
Montserrat') === true` with **zero** MIME/corrupted console errors.

### The Universality Principle (audit of Nov 8, 2026)

> **No fix may be tied to a site.** The target site is INPUT
> (`Config.SITE`), not logic. A bug found on a real site (poki,
> jeskojets) is the *detector* of the problem CLASS — the fix attacks the
> class, never the case. The proxy's whole decision chain is relative:
> "external" = not `MAIN_HOST`; "CDN" = any host that successfully served
> an asset (dynamic registration, runtime map); MIME = sniffed from
> content; browser = engine family (Gecko/Chromium); device = UA class.

**Specificity audit (modular package, `s2l.py` frozen aside)**: a sweep
for site/CDN names and domain literals across every `.py` in the package.
Result — the only site-specific state in the project is the user's input
and the data derived from it:

| What exists in the code | Classification | Why it is universal |
|---|---|---|
| `Config.SITE` | **input** | the target chosen by the user — it is *supposed* to be specific |
| docstrings citing poki/jeskojets/jsdelivr | **documentation** | the history of the field bugs; the compiler ignores them |
| `_BOT_PAGE_SIGNATURES` (Google Sorry, DDoS-Guard, hCaptcha, reCAPTCHA) and `_is_cf_block` (cf-mitigated, jschl_vc) | **technology** | anti-bot challenge-page fingerprints — any site behind these WAFs benefits; they detect marks in the response BODY, they don't route by domain |
| `_coep_credentialless_supported()` | **technology** | engine family (Gecko vs Chromium), not site |
| `Devices.py` (UA profiles) | **technology** | device classes, not sites |
| path heuristics (`/api/`, `/assets/`, `/builds/`) | **convention** | universal web path patterns; getting one wrong only changes the fetch shape, not the result |
| `_js_mime_rescue` | **content** | a single question: "do these bytes look like JavaScript?" — zero host/URL references in executable code (verified by AST) |
| `_sniff_nonhtml_type` (+ `_looks_like_css`/`_looks_like_json`) | **content** | the extensionless sibling of fix 5: "are these bytes CSS? JS? JSON?" — pure byte sniffing, zero host/URL references in executable code (verified by AST, docstrings stripped) |

A line that does NOT exist anywhere: `if host == "site.tld"`. The
`CDN_BLOCK` list (host blocklist) is **empty by default** — it's user
config, not hardcode. The seven post-match fixes all follow the
principle: 1-2 device profile (UA class), 3a browser engine, 3b a
standard CSS API, 4 port lifecycle (internal architecture), 5 body
content, 6 body content for extensionless cache files.

### Final migration completeness audit (Nov 8, 2026, post-fix 5, re-executed post-fix 6)

Fresh re-staging of both trees (`/tmp/parity`, `SITE=example.com`,
deterministic empty cache) and the full battery re-executed against the
CURRENT state of the code: **config 27/27 identical · routes 10/10
identical (rule + methods + endpoint) · offline smoke 9/9 with identical
sha256** (`parity_driver` ×2 + `parity_compare`). **Symbols**: 193
top-level defs/classes of the monolith → **0 lost**; the 26 extras = the
21 seams documented in section 2 + the fixes' helpers
(`_coep_credentialless_supported` 3a, `_looks_like_js`/`_js_mime_rescue`
5) + `create_app`/`main`/`_injector_js` from the modular layout itself.
**Constants** (`parity_constants.py` — gap closed: the original battery
only looked at def/class): 196 module assignments of the monolith → **0
lost**; the 10 extras = the constants of fixes 1 (`_DEVICE_*`) and 5
(`_JS_*`, `_NEVER_JS_RESCUE`, `_js_rescue_logged`) + `_CDN_VIEWER_PAGE`
(extracted template) + Boot's `_HERE`/`_PARENT` + the `config` alias.
**Injector** (`injector_diff_verify.py`): +241 bytes over the monolith =
exactly +211 (fix 3b, the `setProperty` patch) and +30 (fix 4, the HP
poll with add/update/delete) — **zero additions outside the documented
fixes**. **Entry points** (`entry_points_verify.sh`): `python -m
Site2Local`, `python3 Site2Local/Boot.py` and `python3 s2l.py` boot and
listen; the historical `import s2l` resolves. The monolith is confirmed
frozen (the only difference vs staging: a comment on the `SITE` line).
**Verdict: migration complete — nothing pending; the only
monolith→modular divergences are the deliberate post-match fixes.**

**Re-audit after fix 6** (same day, same staging method): parity battery
re-executed — **config 27/27 · routes 10/10 · smoke 9/9, verdict
MONOLITH ≡ MODULAR**; symbols 193 → **0 lost**, extras now **29** (the
26 above + `_looks_like_css`/`_looks_like_json`/`_sniff_nonhtml_type`
from fix 6); constants 196 → **0 lost**, extras now **15** (the 10 above
+ fix 6's `_CSS_AT_RULES`, `_CSS_SELECTOR_RE`, `_JSON_SNIFF_MAX`,
`_SNIFF_HEAD` + the `VERSION` knob inaugurated by 9.0); the injector is
UNTOUCHED by fix 6 (+241 bytes, the same as post-fix 4 — the fix lives
entirely on the Python cache-serve side); entry points 3/3 green; the
universality audit extended to the fix-6 functions (AST with docstrings
stripped: zero host/URL references in executable code; behavioral
check: the sniffers fire for files of any fictitious host). Nothing
pending after fix 6 — the divergences remain the 7 documented rows of
the table above.
