# Site2Local — Technical Documentation

**Version:** modular 9.0 · **Reviewed:** 2026-11-09

This reference describes the maintained Python package launched by `Boot.py`. The root `s2l.py` and files under `Archive/` are historical references; their behavior can differ. Implementation links below point to the modular source.

Start with the [README](README.md) for installation, configuration examples, and ordinary troubleshooting. Use the [module map](MODULAR_MAP.md) to navigate the tree. This document focuses on request flow, persistence, extension points, and operational limits.

## Contents

1. [Architecture and startup](#1-architecture-and-startup)
2. [Configuration and operating modes](#2-configuration-and-operating-modes)
3. [HTTP response pipelines](#3-http-response-pipelines)
4. [Disk cache and filenames](#4-disk-cache-and-filenames)
5. [Validators, ranges, and HEAD](#5-validators-ranges-and-head)
6. [RAM cache](#6-ram-cache)
7. [CDN routing and browser rewriting](#7-cdn-routing-and-browser-rewriting)
8. [Headers and cross-origin isolation](#8-headers-and-cross-origin-isolation)
9. [Hooks and editor behavior](#9-hooks-and-editor-behavior)
10. [Link extraction, crawling, and scanning](#10-link-extraction-crawling-and-scanning)
11. [Streaming and realtime protocols](#11-streaming-and-realtime-protocols)
12. [Sessions and upstream recovery](#12-sessions-and-upstream-recovery)
13. [Captures and internal endpoints](#13-captures-and-internal-endpoints)
14. [Testing and contributing](#14-testing-and-contributing)
15. [Limits and diagnosis](#15-limits-and-diagnosis)

## 1. Architecture and startup

| Component | Role |
| --- | --- |
| [Boot.py](Boot.py) / [__main__.py](__main__.py) | Command-line entry points. `Boot.py` resolves paths against the project directory. |
| [Config.py](Config.py) | Target, modes, limits, and optional features. |
| [Runtime.py](Runtime.py) | Shared settings, sessions, queues, compatibility exports, and process startup. |
| [Application.py](Application.py) | Flask routes and before/after-request callbacks. |
| [Proxy/Routes.py](Proxy/Routes.py) | Main-host HTTP handling. |
| [Proxy/Assets.py](Proxy/Assets.py) / [CDN/Server.py](CDN/Server.py) | External-host requests on the main port or dedicated ports. |
| [Crawler/Crawl/Save.py](Crawler/Crawl/Save.py) | Background disk persistence. |

`Runtime` assembles shared state during import; some setup, including target resolution, can happen before `main()`. Importing the package is not a fully isolated, network-free operation. `Application.create_app()` assembles a Flask app without launching workers, but the app still uses shared Runtime settings, sessions, queues, and caches. It does not create an independent proxy instance.

At startup, `main()` prepares storage and the save worker, validates flag combinations, registers shutdown handling, schedules cache maintenance, and starts the requested features. Existing CDN hosts are preregistered in offline mode, or online when `PREMAP_CDN` is enabled.

When `CRAWL` is enabled online, the initial crawl is launched in a background thread. The proxy continues its startup rather than waiting for the crawl to finish. After the initial crawl's workers finish, persistent discovery workers take over. External asset prefetch has its own bounded queue and worker pool.

On graceful shutdown, the process waits for pending save/capture work before terminating. Save queue tasks are acknowledged after disk-write attempts, or after an item is deliberately skipped. A completed queue does not guarantee every write succeeded: inspect save-error logs, especially when storage is full or unavailable.

## 2. Configuration and operating modes

Settings are read from [Config.py](Config.py), with size literals converted to byte counts by `parse_size`. Restart after editing flags. Examples in documentation are not promises about the values currently committed to the repository.

| Area | Relevant settings |
| --- | --- |
| Listener and target | `SITE`, `HOST`, `PORT`, `DEVICE` |
| Discovery | `CRAWL`, `CRAWL_DEPTH`, `WORKERS`, `DUMP_ALL`, `SCAN_LIMIT` |
| Disk and RAM | `OFFLINE`, `SAVE_ERRORS`, `SAVE_BATCH`, `SAVE_INTERVAL`, `ATOMIC_SERVE`, `ATOMIC_SERVE_MAX_BYTES` |
| External hosts | `PROXY_CDN`, `CACHE_CDN`, `MULTIPORT`, `PREMAP_CDN`, `CDN_BLOCK`, `DONT_REWRITE` |
| Policies | `CROSS_ORIGIN_ISOLATION`, `BLOCK_PATHS`, `DUMP_TARGETS` |
| Protocols and limits | `TIMEOUT_CONN`, `TIMEOUT_READ`, `TIMEOUT_STREAM_READ`, `STREAM_CACHE_MAX_BYTES`, `MAX_BODY_BYTES`, `MAX_DECOMPRESSED_BYTES`, `WS_MAX_MSG_BYTES` |
| Inspection | `HOOK_GUI`, `CAPTURE`, `CAPTURE_CDN`, `CAPTURE_BODIES`, `CAPTURE_SKIP_STATIC`, `CAPTURE_WS` |
| Optional tools | `SCAN_PATHS`, `WORDLISTS_DIR`, `SCANS_PER_SECOND`, `WEB_CDNVIEWER`, `WEB_CDNVIEWER_IP_WHITELIST` |

- **On demand:** `CRAWL = False`, `OFFLINE = False`. Browser requests fetch missing resources.
- **Crawl + proxy:** `CRAWL = True`, `OFFLINE = False`. Background discovery adds work while browsing remains available.
- **Offline:** `OFFLINE = True`. Ordinary HTTP cache misses are unavailable; live application backends are not recreated by the mirror.

Byte-size settings accept integer byte counts and strings such as `"512KB"`, `"64MB"`, and `"1GB"`; units are binary. `"256"` is 256 bytes. Different settings bound different resources: request size, decompression output, raw stream accumulation, RAM file retention, and WebSocket reassembly are not one global memory budget.

## 3. HTTP response pipelines

### Main-host miss

[Proxy/Routes.py](Proxy/Routes.py) handles local policy checks, builds the upstream context, and runs applicable Python request hooks. The upstream client receives filtered headers with origin/referer adjustments. Depending on the response, the route uses recovery, buffered handling, binary streaming, or SSE handling.

For eligible buffered responses, raw upstream content is queued for disk storage before URL rewriting and response-hook replacement. HTML/CSS/JSON processing happens before Python response hooks and GUI hooks. The final context is sent to inspection/capture helpers and converted into a Flask response.

This ordering matters: a response hook edits what the browser receives without normally replacing the downloaded source on disk. Some explicitly requested transformations, such as `SHOW_HIDDEN`, have their own ordering; do not assume every feature is a pure display-only operation.

### Cache hit

The main proxy and both external-host routes use [Proxy/Cache/Serve.py](Proxy/Cache/Serve.py):

1. Resolve MIME and evaluate an eligible conditional request.
2. Load cached bytes and apply the HTML/CSS/JSON processing appropriate to the resource.
3. For external documents/stylesheets, resolve root-relative references against the upstream host.
4. Build a `HookContext` and run Python request hooks, Python response hooks, and GUI hooks.
5. Build the final response with current headers.

Request hooks on a cache hit operate on the cached-response context; they do not represent a new upstream request. Conditional `304` shortcuts are disabled when Python hooks or the GUI inspector are active, so those shortcuts do not skip replacement processing.

Range requests use a separate partial-file path. Binary streaming, SSE, errors, redirects, and socket-level traffic also have specialized paths; do not assume every response traverses the full buffered hook pipeline.

### Final headers

[Proxy/Upstream/Response.py](Proxy/Upstream/Response.py) builds normal context responses. [HTTP/Headers/Response.py](HTTP/Headers/Response.py) provides the shared finalizer registered on the main Flask app and CDN apps. This final pass normalizes security/resource policies even for responses created outside the main buffered route.

## 4. Disk cache and filenames

[Core/Cache/Paths.py](Core/Cache/Paths.py) maps URLs to mirror locations; [Core/Cache/Naming.py](Core/Cache/Naming.py) sanitizes components and builds query names.

```text
site_src/<site-name>/<upstream-host>/...
site_data/<site-name>/...
```

Typical mappings within a host folder:

| URL | Relative cache path |
| --- | --- |
| `/assets/app.js` | `assets/app.js` |
| `/page` or `/page/` | `page/index.html` |
| `/assets/app.js?v=123` | `__query__/assets/app_v_123.js` |
| `/v1/asset?id=7` | `__query__/v1/asset_id_7` |

Query names combine parameter names and values. Multiple parameters use `_N_` separators. Sanitization is lossy, so a `.q` sidecar records the owning raw query. A different query that would claim an existing pretty name gets a deterministic encoded fallback. Long components are capped; ordinary extensions are preserved where space permits, while oversized extensions cannot exceed the configured component limit.

Device tags can create separate cache variants for mobile/tablet resources. The canonical unsuffixed file remains the desktop-family copy. A request may therefore target a different file from the one a user edited. Hostnames, query variants, and device variants all matter during diagnosis.

The save worker batches writes and uses a temporary file plus `os.replace` for body replacement. Newly saved URLs own independent files: identical content is not materialized through hardlinks. This does not separate hardlinks already present in an older mirror.

Body and query-sidecar writes are separate operations. Do not treat them as a filesystem transaction covering every cache artifact. Keep sidecars alongside their owning files and back up important edits before changing cache layouts.

[Core/Cache/Sweep.py](Core/Cache/Sweep.py) handles startup cleanup of legacy entries and orphan sidecars. Older layouts are not migrated automatically. Files that no longer map to a reachable cache URL can be removed by maintenance; check logs when upgrading old mirrors.

## 5. Validators, ranges, and HEAD

[Proxy/Cache/Validators.py](Proxy/Cache/Validators.py) derives ETags from file size, nanosecond modification time, and a checksum of the MIME base. MIME changes therefore invalidate an old ETag, helping clients recover from previously incorrect types.

Static responses generally use browser revalidation. HTML/JSON use no-store policies. `If-None-Match` takes precedence over `If-Modified-Since`; enabled hooks prevent the disk-only `304` shortcut. These validators are metadata-based, not full-content hashes. An edit that preserves the relevant metadata can evade detection.

[Proxy/Cache/Range.py](Proxy/Cache/Range.py) serves supported single byte ranges from disk. The requested slice does not determine MIME; the resolver examines the file's beginning. An `If-Range` ETag must strongly match, or its supplied date must match the current last-modified value. Stale, weak, or invalid validators fall back to a complete response rather than combining versions.

A streamed `206` response is disk-cache eligible only when its declared range covers the entire resource and the actual received byte count matches the declared total. Starting at byte zero alone is insufficient. Interrupted or partial streams are not eligible as complete files.

For normal context-built `HEAD` responses, the wire body is empty while `Content-Length` describes the representation, or preserves supplied upstream length metadata. This is distinct from a zero-length resource. Status codes that prohibit bodies still have their own HTTP handling.

## 6. RAM cache

[Core/Cache/Memory.py](Core/Cache/Memory.py) implements raw-file retention for `ATOMIC_SERVE`.

| Bound | Meaning |
| --- | --- |
| `ATOMIC_SERVE_MAX_BYTES` | Total retained raw-file bytes. |
| 16 MiB | Maximum individual body eligible for retention. |
| 4096 entries | Raw-file entry cap. |
| 4096 entries | Separate MIME memoization cap in [Guess.py](Core/MIME/Guess.py). |

The file cache checks device/inode, size, and nanosecond mtime signatures. Ordinary changes invalidate retention; deletion cannot return a retained body without a successful current stat. Reads occur outside the retention lock, while lookup and eviction accounting are synchronized. Request-specific rewriting and hook results are not stored as shared raw file bytes.

A zero byte budget disables raw-file retention; MIME memoization remains available if `ATOMIC_SERVE` is enabled. Large files still read from disk. This cache does not bound all process memory, guarantee a speedup, or make every concurrent edit/read a transaction.

## 7. CDN routing and browser rewriting

External resources can use:

- Dedicated per-host servers from [CDN/Server.py](CDN/Server.py) with `MULTIPORT = True`.
- `/__s2l_ext__/<host>/<path>` on the main port, including fallback before a dedicated server is ready.

[CDN/Registry/](CDN/Registry/) manages host-to-port mappings and real-origin translations. HTML, CSS, and JSON rewriting use that registry. Offline startup prepares cached CDN hosts automatically when proxying and multiport are enabled; `PREMAP_CDN` also requests preparation online.

[Rewriting/Injector.py](Rewriting/Injector.py) builds the browser runtime for dynamic requests and resource attributes. [Rewriting/HTML/ServiceWorker.py](Rewriting/HTML/ServiceWorker.py) inserts the S2L script into documents and removes an old S2L script before reinsertion. [Rewriting/HTML/URLs.py](Rewriting/HTML/URLs.py) handles document/style URL passes; [Rewriting/JSON.py](Rewriting/JSON.py) handles JSON references.

The runtime includes fetch/XHR, WebSocket, worker, DOM-attribute, and iframe communication adaptations. Message origin translation checks the sending frame's URL before port-only fallback, since a cold-load frame may still be using the main-port external route. The live host-map endpoint lets browser mappings catch up with newly registered ports.

These adaptations cannot fully reproduce native remote origins. SDKs can inspect script URLs or enforce their own checks. `DONT_REWRITE` lets selected hosts load directly, trading proxy/offline coverage for compatibility.

## 8. Headers and cross-origin isolation

Header code is split under [HTTP/Headers/](HTTP/Headers/):

| Module | Responsibility |
| --- | --- |
| [Forwarding.py](HTTP/Headers/Forwarding.py) | Outgoing header filtering, origin/referer and CSRF-related handling. |
| [Response.py](HTTP/Headers/Response.py) | Upstream response filtering and final response callback. |
| [CI.py](HTTP/Headers/CI.py) | Case-insensitive access across dictionaries and Werkzeug headers. |
| [CORS.py](HTTP/Headers/CORS.py) | CORS, CORP, and private-network preflight handling. |
| [Security.py](HTTP/Headers/Security.py) | Baseline headers such as `nosniff` and referrer policy. |
| [Isolation.py](HTTP/Headers/Isolation.py) | COOP/COEP selection and automatic demand state. |
| [Cookies.py](HTTP/Headers/Cookies.py) | Repeated Set-Cookie extraction and cookie-jar handling. |

The project filters a static header set and additional names declared by `Connection`. The current `_HOP_BY_HOP` set also contains project-specific omissions, including cache validators and framework headers; it is not a pure list of standard hop-by-hop fields. WebSocket upgrades have their own socket-layer path.

Upstream restrictions such as CSP and X-Frame-Options are removed where needed for local rewriting/embedding. `nosniff` makes correct MIME handling important. CORP is normalized for proxied resources. Credentialed CORS uses a concrete request origin rather than combining credentials with a wildcard origin.

### Isolation modes

- `CROSS_ORIGIN_ISOLATION = "auto"`: document COEP headers or HTML signals can establish demand. A top-level detection remains active for the proxy process until restart, so later documents and workers share a policy.
- `True`: force isolation for document and worker responses.
- `False`: remove enforced COOP/COEP for compatibility.

The automatic detector does not execute external JavaScript and cannot retroactively change a loaded parent's response headers. A late iframe requirement does not establish isolation for its already-loaded ancestors. Use forced mode before loading a site whose requirements are not detected automatically.

When isolation is active, modern Firefox/Chromium user agents select `credentialless`; other supported request shapes fall back to `require-corp`. Both can restrict iframe embedding. Parent/child policy consistency and whether the content actually travels through the proxy matter more than adding headers indiscriminately.

## 9. Hooks and editor behavior

### HookContext and Python hooks

[Hooks/Hooks/Context.py](Hooks/Hooks/Context.py) defines the mutable context:

| Fields | Meaning |
| --- | --- |
| `method`, `url`, `path`, `query` | Request identity; query is stored separately from path. |
| `req_headers`, `req_body` | Outgoing request headers/body. |
| `resp_status`, `resp_headers`, `resp_body`, `resp_ct` | Response status, headers, bytes, and content type. |

Helpers include `json()`, `set_json()`, `req_json()`, and `set_req_json()`. Python decorators in [Registry.py](Hooks/Hooks/Registry.py) register request and response callbacks, filtered by method and a case-insensitive regex on `ctx.path`. Inspect `ctx.query` inside a Python callback when query-specific logic is needed.

For a custom launcher saved beside the `Site2Local/` package folder:

```python
from Site2Local import Runtime as S

@S.on_response(pattern=r"^/api/settings$", methods="GET")
def customize_settings(ctx):
    if ctx.resp_status == 200 and "json" in ctx.resp_ct:
        payload = ctx.json()
        if isinstance(payload, dict):
            payload["local_preview"] = True
            ctx.set_json(payload)
            for key in list(ctx.resp_headers):
                if key.lower() == "content-length":
                    del ctx.resp_headers[key]

if __name__ == "__main__":
    S.main()
```

Run that launcher with the project directory as the working directory so its generated paths stay where intended. Hook exceptions are logged and do not intentionally terminate the entire request worker.

### GUI replacements

[Hooks/GUI.py](Hooks/GUI.py) provides the PyQt6 editor. [GUIHooks.py](Hooks/Hooks/GUIHooks.py) applies the first enabled matching HTTP replacement. GUI patterns test path plus query and retain path-only matching for compatibility; this differs from the Python registry's path-only matcher.

Newly selected traffic entries generate exact patterns, such as `^/$` or `^/_astro/Search\.e75879d4\.js$`. A bare `/` is broad enough to match stylesheets and scripts. GUI origin, method, and status filters are separate from that regex.

The editor strips S2L's own script from HTML before preview/truncation. Saving a replacement strips it too. When applying an HTML replacement, the proxy reruns rewriting and injects one fresh runtime. Site scripts are retained; ordinary CSS/JavaScript replacements retain their MIME. Invalid replacement code can still break the page.

[Store.py](Hooks/Hooks/Store.py) persists hook bodies and metadata below `site_data`, grouped by hook type and main host. Each body/metadata file is replaced atomically, but the pair is not a transactional unit. GUI replacements survive restart; Python hooks require their launcher or registration code to run again.

WebSocket editing uses separate message hooks and binary/text controls. It is not the same mechanism as an HTTP response replacement. Arbitrary binary HTTP bodies should not be round-tripped through a UTF-8 text editor.

## 10. Link extraction, crawling, and scanning

[Crawler/Crawl/Extract.py](Crawler/Crawl/Extract.py) is a pure discovery layer: it returns deduplicated `(url, is_asset)` pairs and does not download files. [Links.py](Crawler/Crawl/Links.py) applies domain/local-host filtering and queues discovered work. [Jobs.py](Crawler/Crawl/Jobs.py) uses that same layer for downloaded or cached crawl responses.

| Input | Handling |
| --- | --- |
| HTML | BeautifulSoup parser, first base href, links/scripts/images, media/poster/track/embed/object references, lazy image attributes, srcset, inline styles. |
| CSS | Quoted/unquoted `url()` and quoted `@import`, with comment/string exclusion and escape handling. |
| JavaScript/JSON with `DUMP_ALL` | Complete quoted URL candidates within `SCAN_LIMIT`; known concatenated fragments are skipped. |

Relative paths resolve against the document or stylesheet URL; a live crawl uses the final response URL after redirects. Fragments and non-HTTP(S) downloads are skipped. Queries remain intact. Srcset handling preserves commas inside URL tokens rather than splitting every comma indiscriminately.

This is conservative extraction, not a full JS evaluator or a complete browser parser for every possible syntax. Dynamically assembled URLs, some lazy-loading conventions, and application-driven resource discovery may require real browsing.

Initial workers consume `url_queue`; persistent workers start after the initial phase and its sentinels. `enqueue()` accepts later work when discovery workers are active and avoids filling a queue without consumers in ordinary procedural/offline operation.

External assets use a bounded queue and a visited set. Downloaded external CSS can add imports, fonts, and images through that same bounded queue. Failures and saturation can leave resources undiscovered or skipped; discovery is not a guarantee of an exhaustive offline mirror.

`SCAN_PATHS` is separate: [Crawler/Scanner/](Crawler/Scanner/) probes paths from wordlists. `SCANS_PER_SECOND` controls its rate/progress behavior; status filtering can be prompted interactively. Offline mode skips upstream path scanning.

## 11. Streaming and realtime protocols

[HTTP/Streaming/Response.py](HTTP/Streaming/Response.py) forwards binary streams and accumulates a bounded raw buffer for optional disk saving. External streams respect `CACHE_CDN`; main-host streams can still cache when that external-only flag is disabled.

CDN/external responses selected for streaming by path heuristics are reclassified when real headers arrive. Rewritable HTML/CSS/JSON are consumed through `iter_content` and passed through their normal rewrite/hook pipeline. This avoids depending on a streamed response's `.content` behavior. Actual binary bodies, partial ranges, and event streams remain on specialized paths.

Supported streaming encodings depend on installed decompression packages. When content is decompressed, original compressed-length headers cannot describe the bytes sent to the browser. Large buffered documents and decoded data can use memory beyond the raw stream-retention budget.

SSE uses [HTTP/Streaming/](HTTP/Streaming/) event-oriented forwarding and configurable heartbeats. It is not a finite response to edit or cache like ordinary HTML.

WebSocket upgrades are intercepted by the socket handling in [Network/Server/](Network/Server/) before normal Flask response handling. [WebSockets/](WebSockets/) handles framing, upstream connections, message hooks, and optional captures. Ping, reconnect, deflate, and reassembly limits have independent configuration.

Optional TCP/UDP tunnels carry traffic over WebSocket. Their flags are separate from ordinary HTTP proxying. Neither HTTP cache files nor captured JSON messages implement offline replay of a live protocol backend.

## 12. Sessions and upstream recovery

[HTTP/Sessions/](HTTP/Sessions/) contains upstream client selection, device profiles, and session/block helpers. Client choices include `curl_cffi`, cloudscraper-compatible implementations, and requests depending on availability.

Main request sessions use a random, signed `HttpOnly` browser cookie rather than an IP-derived identifier. Browsers sharing an IP have separate upstream cookie jars. The identity is shared across CDN ports, excluded from upstream requests, and protected against replacement by upstream `Set-Cookie` headers. In-memory sessions expire and are capped at 512 entries; restarting invalidates browser identities. Background fetches use separate sessions and do not inherit interactive authentication state.

`Network/Outbound.py` applies a shared outbound destination policy. HTTP backends use an authenticated loopback proxy so every connection, including redirects, connects to a checked numeric address rather than resolving the hostname again. Native WebSockets and raw tunnels use the same resolver and connector. Non-global, multicast, reserved and private addresses are denied unless an exact `host:port` is configured in `OUTBOUND_PRIVATE_ALLOWLIST` (bracket IPv6 hosts). Environment proxy settings are ignored. Optional WAF detection uses the guarded requests transport too.

`TLS_VERIFY` controls certificate and hostname validation for HTTPS/WSS, including startup probes and retry paths. Enabling it prevents startup from silently falling back to HTTP. It is independent of destination restrictions: disabling certificate validation does not permit private network access. These controls do not authenticate incoming clients; restrict network exposure or add an authenticated gateway before publishing the service.

Cookie/header rewriting, TLS impersonation, retries, and block-page checks improve compatibility but do not reproduce all browser state or defeat every upstream restriction. Cookie-domain/path handling and origin-specific SDK checks remain important boundaries to investigate for login problems. Do not interpret a successful transport connection as successful authentication or application startup.

## 13. Captures and internal endpoints

HTTP and WebSocket capture writers run asynchronously under [Capture/](Capture/). Typical directories are `site_data/<site-name>/captures/` and `site_data/<site-name>/captures_ws/`. Hook stores use separate subdirectories under `site_data`.

`CAPTURE_CDN` includes external traffic, `CAPTURE_BODIES` controls HTTP request-body capture, and `CAPTURE_SKIP_STATIC` filters common static resources. Captured headers can contain credentials even when request-body capture is disabled. Inspect and redact complete records before sharing.

The main Flask routes are assembled in [Application.py](Application.py):

| Path | Purpose |
| --- | --- |
| `/` and `/<path>` | Main configured target. |
| `/__s2l_ext__/<host>/<path>` | External HTTP resource route. |
| `/.__s2l_hp` | Live CDN host-to-port map used by the injector. |
| `/__s2l_viewer__` | Optional CDN viewer. |
| `/__s2l_viewer_data__` | Viewer data. |
| `/__s2l_reset_stats__` | Statistics reset. |
| `/__s2l_ws_ext__/<host>/<path>` | External WebSocket entry path; upgrades use socket-layer handling. |
| `/__s2l_tcp__/<host:port>` / `/__s2l_udp__/<host:port>` | Optional tunnel entry paths. |

Viewer-related routes are controlled by their feature flag and access settings. Dedicated CDN apps serve their own host routes on additional ports. Check the actual registered methods and socket handlers before integrating against these internal endpoints.

## 14. Testing and contributing

The README lists the focused offline regression commands. These cover cached MIME/HTTP handling, streaming cache eligibility, isolation policies, shared extraction, save integrity, hook editing, and cached CDN hooks. They load selected helpers without importing a complete live proxy.

[Scripts/test_injector_origin.py](Scripts/test_injector_origin.py) needs Node.js on PATH to execute its JavaScript mock; it skips otherwise. Older scripts can require a live server, browser automation, or an environment-specific layout. Do not assume blanket test discovery over `Scripts/` is offline-safe.

A passing mocked request matrix is not browser validation. For protocol changes, check real Firefox and Chromium when possible, including cold cache, warm cache, iframe nesting, revalidation, and offline behavior. For cache changes, test same-size edits, deletion, failed writes, duplicates, and interruption in addition to successful downloads.

Documentation examples should be checked against Config names, route registration, and current helper signatures. The README explains usage; this reference explains mechanics; MODULAR_MAP is a navigation aid. Update the relevant documents together when behavior changes.

## 15. Limits and diagnosis

- A saved frontend cannot replace remote login, database, advertising, multiplayer, or API services.
- HTML rewriting, MIME sniffing, automatic isolation, and quoted-URL discovery use heuristics; they are not full browser execution.
- RAM/file limits are separate from document buffering, session pools, captures, and decoded content sizes.
- Files copied from an older mirror can retain old layouts or hardlinks. New behavior does not automatically repair all historical data.
- Ordinary full-file cache hits, ranges, binary streams, SSE, and WebSockets have different extension points.
- Disabling upstream security policies for local compatibility does not make the proxy a production security boundary.

When reporting a failure, include the upstream URL, relevant flags, server log, browser version, Console errors, and the failing Network request's status, MIME, and policy headers. State whether it happens before or after refresh, with hooks enabled or disabled, and with a fresh or existing disk mirror. Preserve useful caches and manual edits while investigating; clearing everything should not be the first diagnostic step.

Return to the [README](README.md) for setup and a concise troubleshooting table.
