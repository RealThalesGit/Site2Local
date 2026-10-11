# Site2Local — Technical Documentation

Documented version: V8.1 · Source: `8.0/s2l.py` · Updated: 2026-10-11

> This document describes the source code in `8.0/s2l.py`. It combines source-level documentation with operational guidance. Runtime behavior can vary by browser, operating system, dependency versions, network, and upstream application.

## Contents

1. [Overview](#1-overview)
2. [Installation and first run](#2-installation-and-first-run)
3. [Configuration reference](#3-configuration-reference)
4. [Architecture](#4-architecture)
5. [HTTP request lifecycle](#5-http-request-lifecycle)
6. [Disk cache](#6-disk-cache)
7. [Device-specific cache variants](#7-device-specific-cache-variants)
8. [CDNs and external hosts](#8-cdns-and-external-hosts)
9. [Protocols and WebSocket handling](#9-protocols-and-websocket-handling)
10. [Hooks and the traffic inspector](#10-hooks-and-the-traffic-inspector)
11. [Browser rewriting and response headers](#11-browser-rewriting-and-response-headers)
12. [Upstream sessions and block detection](#12-upstream-sessions-and-block-detection)
13. [Crawler and path scanner](#13-crawler-and-path-scanner)
14. [Traffic capture](#14-traffic-capture)
15. [Internal endpoints](#15-internal-endpoints)
16. [Source-code map](#16-source-code-map)
17. [Limitations and operational considerations](#17-limitations-and-operational-considerations)
18. [Troubleshooting](#18-troubleshooting)

## 1. Overview

Site2Local (S2L) is a Python reverse proxy and disk-backed site mirror built around Flask and Werkzeug. It listens locally, forwards eligible requests to a configured upstream site, rewrites selected URLs so the browser continues to use the proxy, and saves eligible upstream responses to disk. Saved files can be edited and served later, including in offline mode.

Major features in this source version include:

- HTTP proxying to HTTP or HTTPS upstream sites.
- On-demand fetching or a background crawl.
- Disk caching and offline serving.
- CDN/external-host proxying, including optional per-host ports.
- Browser-side URL rewriting for dynamically generated requests and resources.
- WebSocket, Server-Sent Events (SSE), and optional TCP/UDP tunnels carried over WebSocket.
- Python request/response hooks and an optional PyQt6 traffic inspector with HTTP and WebSocket hook editors.
- Optional HTTP/WebSocket capture, hidden-path scanning, and live CDN statistics.
- Upstream session choices including `curl_cffi`, cloudscraper-compatible packages, and `requests`.

The main operating modes are:

- **On-demand proxy:** set `CRAWL = False`; content is fetched as the browser requests it.
- **Background crawl:** set `CRAWL = True`; a crawler starts from the configured site URL while the proxy is running.
- **Offline serving:** set `OFFLINE = True`; regular HTTP content is served from the local cache only. WebSocket connections cannot be replayed from the HTTP cache.

Site2Local is a mirroring and development tool, not a general-purpose production web server or a security boundary.

## 2. Installation and first run

### Requirements

The source uses Python 3.10 or newer and requires these packages:

- `requests`
- `urllib3`
- `beautifulsoup4`
- `flask` (which uses Werkzeug)
- `colorama`

Optional packages and tools:

- `curl-cffi`: browser-like TLS fingerprints and the preferred upstream session when available.
- `ai-cloudscraper` or `cloudscraper`: alternate upstream session implementation.
- `brotli`: Brotli response decompression.
- `tqdm`: progress display for path scanning.
- `PyQt6`: required only when `HOOK_GUI = True`.
- `lolcat`: optional rainbow log formatting; an internal fallback is used if unavailable.

Optional packages enable or improve specific features; they do not guarantee that a particular upstream site will work.

### Create an isolated environment

On Windows, from the directory containing `s2l.py`:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install requests urllib3 beautifulsoup4 flask colorama
```

On macOS or Linux:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install requests urllib3 beautifulsoup4 flask colorama
```

Install optional packages in the same environment as needed. For example:

```sh
python -m pip install curl-cffi
```

### Configure and launch

1. Edit the constants in the `CONFIG` section near the top of `s2l.py`. In this source version, configuration is by editing the file; there is no documented command-line configuration interface.
2. Set `SITE` to the upstream site. Set `CRAWL = False` for browser-driven, on-demand fetching, or leave it enabled for background crawling.
3. Run the script:

   ```sh
   python s2l.py
   ```

   On Windows, you can invoke the virtual-environment interpreter directly:

   ```powershell
   .\.venv\Scripts\python.exe s2l.py
   ```

4. Open `http://localhost:8080` in a browser, or use the configured `PORT`.

The startup banner reports the target, listening address, device profile, selected upstream engine, timeouts, retry settings, hooks, and enabled flags. If a port is already occupied, change `PORT` and restart. CDN multiport mode also needs additional available ports.

At startup, `CACHE_CDN = True` is disabled with a warning if `PROXY_CDN = False`. `MULTIPORT` has no effect when CDN proxying is disabled.

When `GRACEFUL_SHUTDOWN = True`, the Ctrl+C handler waits for the save and enabled capture queues to drain before calling `os._exit`. This is queue draining, not a guarantee that every in-flight network request completes.

### Common operating profiles

These are configuration recipes, not separate modes or command-line flags:

| Goal                                | Suggested settings                                                    | Result                                                                                           |
| ----------------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| Browse and cache as you go          | `CRAWL = False`, `OFFLINE = False`                                    | The browser drives upstream requests; eligible responses are cached.                             |
| Crawl and then inspect offline      | `CRAWL = True`, `OFFLINE = False`, then stop and set `OFFLINE = True` | Starts a background crawl, then a later run can serve the saved files without upstream requests. |
| Use only an existing mirror         | `OFFLINE = True`                                                      | Safe HTTP methods use cached files; uncached paths return an offline response.                   |
| Diagnose external-resource failures | `PROXY_CDN = True`, then compare `MULTIPORT = True` and `False`       | Tests dedicated CDN ports against the main-port `/__s2l_ext__/` route.                           |
| Inspect or edit HTTP traffic        | `HOOK_GUI = True`                                                     | Starts the optional PyQt6 traffic inspector; it requires a graphical session.                    |
| Capture traffic for analysis        | `CAPTURE = True` and optionally `CAPTURE_WS = True`                   | Writes HTTP and/or WebSocket records under `site_data`. Review capture data before sharing it.   |

Configuration is stored in the Python source, so changing an option normally requires restarting S2L. Cache and auxiliary data paths are relative to the process working directory.

## 3. Configuration reference

Values below are the defaults in the documented source. Some options interact; check the corresponding sections before changing them.

### 3.1 Target, listener, and upstream timeouts

| Setting               |         Default | Purpose                                                                                                                             |
| --------------------- | --------------: | ----------------------------------------------------------------------------------------------------------------------------------- |
| `SITE`                | `"example.com"` | Upstream domain or URL to mirror.                                                                                                   |
| `HOST`                |     `"0.0.0.0"` | Local listen address. `0.0.0.0` binds to all IPv4 interfaces.                                                                       |
| `PORT`                |          `8080` | Main proxy port.                                                                                                                    |
| `DEVICE`              |        `"auto"` | User-Agent profile. `auto` follows the requesting browser; fixed profiles include mobile/device variants, Macintosh, IE11, and bot. |
| `TIMEOUT_CONN`        |             `6` | Upstream connection timeout in seconds.                                                                                             |
| `TIMEOUT_READ`        |            `12` | Ordinary upstream read timeout in seconds.                                                                                          |
| `TIMEOUT_STREAM_READ` |           `120` | Read timeout used for long-running streams such as media.                                                                           |
| `RETRIES`             |             `2` | Retry count configured for selected upstream operations/statuses.                                                                   |
| `BACKOFF`             |           `0.4` | Base delay used by retry/backoff logic.                                                                                             |
| `POOL_MAXSIZE`        |            `64` | Per-session connection-pool maximum size.                                                                                           |
| `POOL_CONNECTIONS`    |            `32` | Per-session connection-pool slot count.                                                                                             |
| `DEAD_HOST_TTL`       |            `60` | Seconds to remember an upstream host as unavailable.                                                                                |

The main listener default is reachable from other devices on the same reachable network. For loopback-only use, consider changing `HOST` to `127.0.0.1`.

`DEVICE = "auto"` forwards the incoming browser User-Agent after sanitizing control characters and detects a broad device family for cache/session decisions. The documented profile names include `auto`, `mobile`, `macintosh`, `ie11`, `iphone`, `ipad`, and `bot`; the implementation may also recognize UA-derived classes such as `desktop`, `tablet`, and `symbian`. Cache variants collapse these profiles into canonical, mobile, and tablet families; see section 7.

The connection pool limits apply per session. They are separate from the WSGI HTTP request thread-pool cap and the worker counts used by the crawler, scanner, and background discovery tasks.

### 3.2 Crawling and response handling

| Setting       |      Default | Purpose                                                                 |
| ------------- | -----------: | ----------------------------------------------------------------------- |
| `CRAWL`       |       `True` | Start a background crawl. `False` leaves fetching to incoming requests. |
| `OFFLINE`     |      `False` | Serve the disk cache without fetching from the upstream.                |
| `DUMP_ALL`    |      `False` | Extract and follow URLs found in response bodies.                       |
| `SCAN_LIMIT`  | `256 * 1024` | Maximum body bytes examined by `DUMP_ALL`.                              |
| `CRAWL_DEPTH` |          `7` | Maximum URL path depth followed by the crawler.                         |
| `WORKERS`     |         `60` | Crawler worker-pool size.                                               |
| `SAVE_ERRORS` |      `False` | Whether eligible 4xx/5xx GET responses may be saved to the main cache.  |
| `SHOW_HIDDEN` |      `False` | Reveal selected hidden or disabled HTML elements.                       |

`CRAWL` is ignored when offline. The crawler and discovery workers are separate from the request-serving thread pool. A crawl improves coverage but cannot guarantee that every resource, client-side route, authenticated view, or dynamically generated URL is discovered.

### 3.3 Disk cache and streams

| Setting                  |             Default | Purpose                                                                              |
| ------------------------ | ------------------: | ------------------------------------------------------------------------------------ |
| `MAX_FNAME`              |               `180` | Maximum on-disk path-segment length used by the cache mapper.                        |
| `SAVE_BATCH`             |                `32` | Number of files processed per save batch.                                            |
| `SAVE_INTERVAL`          |              `0.15` | Maximum wait interval between save flushes, in seconds.                              |
| `STREAM_CACHE_MAX_BYTES` | `256 * 1024 * 1024` | Maximum streamed response body accumulated for caching. `0` disables stream caching. |

Large streams can still be relayed to the browser after the cache accumulation limit is reached; the limit is intended to bound memory use, not to impose a maximum browser download size. Cache writes use a background queue and worker, with `SAVE_BATCH` and `SAVE_INTERVAL` controlling flush batching.

### 3.4 CDN and external-host behavior

| Setting        | Default | Purpose                                                      |
| -------------- | ------: | ------------------------------------------------------------ |
| `PROXY_CDN`    |  `True` | Proxy supported external/CDN resources.                      |
| `CACHE_CDN`    |  `True` | Save proxied CDN resources to disk.                          |
| `MULTIPORT`    |  `True` | Start a dedicated local port for each registered CDN host.   |
| `CDN_BLOCK`    |    `()` | Hostname substrings excluded from CDN handling/discovery.    |
| `BLOCK_PATHS`  |    `()` | URL substrings blocked with `403` on applicable proxy paths. |
| `DONT_REWRITE` |    `()` | Hosts or host/path patterns whose URLs should remain direct. |
| `DUMP_TARGETS` |    `{}` | Additional targets used by discovery/extraction logic.       |

When `MULTIPORT = False`, external resources use `/__s2l_ext__/` on the main port. `DONT_REWRITE` can be useful for third-party widgets that reject a local proxy origin; those requests then go directly from the browser to the external host. Host entries can match subdomains; the source comments specify bare hostnames without a scheme or port.

`DUMP_TARGETS` is an initially empty mapping used by URL-dump/discovery logic. When `DUMP_ALL` is enabled and this mapping is non-empty, only discovered URLs whose `netloc` is a key in `DUMP_TARGETS` are accepted, in addition to the configured main host. Keys therefore need to match the URL network location, including any explicit port.

### 3.5 WebSocket, SSE, and tunnels

| Setting             |           Default | Purpose                                                           |
| ------------------- | ----------------: | ----------------------------------------------------------------- |
| `WS_PING_INTERVAL`  |               `0` | WebSocket keepalive ping interval in seconds; `0` disables pings. |
| `WS_PONG_TIMEOUT`   |              `15` | Pong wait timeout when ping keepalive is enabled.                 |
| `WS_AUTO_RECONNECT` |           `False` | Attempt to reconnect a dropped upstream WebSocket.                |
| `WS_DEFLATE`        |           `False` | Enable `permessage-deflate` negotiation/handling where supported. |
| `WS_LOG_FRAMES`     |           `False` | Log individual WebSocket frames.                                  |
| `WS_MAX_MSG_BYTES`  | `8 * 1024 * 1024` | Maximum assembled WebSocket message size.                         |
| `SSE_PROXY`         |            `True` | Enable handling for Server-Sent Events streams.                   |
| `SSE_HEARTBEAT`     |              `15` | Interval between injected SSE keepalives; `0` disables them.      |
| `TCP_TUNNEL`        |           `False` | Enable the raw TCP-over-WebSocket tunnel route.                   |
| `UDP_TUNNEL`        |           `False` | Enable the raw UDP-over-WebSocket tunnel route.                   |
| `TUNNEL_TIMEOUT`    |             `300` | Idle timeout for raw tunnels, in seconds.                         |

WebSocket ping is off by default because concurrent reads/writes on some upstream handles can race. Enable optional protocol features only when the target application requires them and test them with that application.

### 3.6 Discovery and capture

| Setting               |       Default | Purpose                                                                                      |
| --------------------- | ------------: | -------------------------------------------------------------------------------------------- |
| `SCAN_PATHS`          |       `False` | Disable scanning, or choose `"all"`, `"all-in-dir"`, or a wordlist path.                     |
| `SCANS_PER_SECOND`    |          `10` | Aggregate scanner rate limit and progress cadence; non-positive values disable the rate cap. |
| `WORDLISTS_DIR`       | `"Wordlists"` | Directory containing path wordlists.                                                         |
| `CAPTURE`             |       `False` | Capture HTTP request/response metadata and configured bodies.                                |
| `CAPTURE_CDN`         |       `False` | Include CDN responses in HTTP capture.                                                       |
| `CAPTURE_BODIES`      |       `False` | Include request bodies in HTTP capture.                                                      |
| `CAPTURE_SKIP_STATIC` |        `True` | Skip selected static asset types in HTTP capture.                                            |
| `CAPTURE_WS`          |       `False` | Capture WebSocket messages.                                                                  |

See sections 13 and 14 for usage and data-handling considerations.

### 3.7 UI, logging, and statistics

| Setting             | Default | Purpose                                                        |
| ------------------- | ------: | -------------------------------------------------------------- |
| `HOOK_GUI`          | `False` | Enable the optional PyQt6 traffic inspector and hook editor.   |
| `WEB_CDNVIEWER`     | `False` | Enable the built-in CDN/statistics viewer routes.              |
| `RAINBOW_LOGS`      | `False` | Use rainbow-colored log output.                                |
| `STATS_WINDOW`      |    `60` | Rolling statistics window in seconds.                          |
| `GRACEFUL_SHUTDOWN` |  `True` | Drain selected background queues during signal-based shutdown. |

The web viewer is disabled by default in this source version. When disabled, its viewer routes return `404`.

## 4. Architecture

The application is implemented in a single Python file. Runtime state is divided between in-memory state (sessions, counters, queues, registered CDN ports, and active hooks) and filesystem state (cached responses, capture records, and persisted GUI hooks). Major components include:

- **Main Flask application:** serves normal HTTP requests through Werkzeug on `PORT`.
- **`_S2LWSGIRequestHandler`:** intercepts WebSocket upgrades and tunnel upgrades at the socket level before Flask routing.
- **`_PooledWSGIServer`:** customized threaded WSGI server used for the main application and CDN servers.
- **CDN mini-servers:** with `MULTIPORT = True`, `_start_cdn_server()` creates an app/server for each registered external host.
- **Background workers:** crawler/discovery, external asset fetching, disk saving, optional HTTP and WebSocket capture, CDN health pruning, and optional path scanning.
- **Hook GUI:** when enabled, Flask runs in a background thread while the Qt event loop uses the main thread.
- **Browser injector:** `_build_s2l_injector()` creates JavaScript inserted into eligible HTML responses.

The source also contains URL/cache mapping, content decoding and MIME inference, response-header policy, streaming, WebSocket framing/relays, SSE handling, capture, and statistics.

### Runtime directories

At startup, the configured site is normalized into `SITE_URL`, `MAIN_HOST`, and `SITE_NAME` (the main host with a leading `www.` removed and dots replaced by underscores). The principal directories are created relative to the current working directory:

```text
site_src/<site-name>/                 cached upstream content
site_data/<site-name>/                captures, scanner output, and site data
site_data/MyHooks/<host>/             persisted GUI HTTP hooks
site_data/MyWSHooks/<host>/           persisted GUI WebSocket hooks
```

Use the same working directory when restarting if you expect S2L to find the same cache and hook data. The folder name is derived from the configured main host; changing `SITE` can therefore select a different cache tree.

## 5. HTTP request lifecycle

`proxy()` handles ordinary HTTP methods. WebSocket upgrades and raw tunnel upgrades are intercepted before a Flask view is called; see section 9.

The main Flask catch-all route accepts `GET`, `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, and `OPTIONS`. `GET` and `HEAD` are treated as safe/cache lookup methods; `POST`, `PUT`, `PATCH`, and `DELETE` may carry a request body. `OPTIONS` is handled locally for preflight rather than being sent to the upstream.

1. **Resolve target and apply local shortcuts.** S2L combines the configured upstream with the request path. It can answer selected document prefetches with `204`, serve a no-op Service Worker script, block configured paths with `403`, and answer `OPTIONS` locally with CORS/Private Network Access headers.
2. **Handle offline requests.** In `OFFLINE` mode, S2L does not contact the upstream. For safe methods it looks for a device-specific cache variant, then a canonical file, and may also search known CDN caches. Missing content results in `404`. A body method against a path with only a cached GET may return `405`.
3. **Check online cache.** Online `GET` and `HEAD` requests can be served from disk. Device variants are considered. Range requests avoid returning a complete cached file as an incorrect `200`; they are sent upstream, while offline cached range support can return `206`.
4. **Build context and run request hooks.** On a cache miss, S2L reads request data when applicable, builds a `HookContext`, and runs registered request hooks. `_do_upstream()` selects a client session and forwards the prepared request.
5. **Retry selected connection failures.** The main proxy can rotate a client session and try fresh session implementations, including `curl_cffi`, cloudscraper-compatible sessions, and plain `requests`. If those attempts fail, a cached canonical copy may be used for a safe method; otherwise the client receives `502`.
6. **Choose streaming or buffered processing.** Range/media/large-body hints and SSE request signals can cause the upstream request to use streaming. SSE is handled by `_stream_sse_resp()`; eligible media, partial, or large responses use `_stream_resp()`. Stream accumulation for disk caching is bounded by `STREAM_CACHE_MAX_BYTES`.
7. **Normalize the upstream response.** Buffered response bodies are decompressed before applicable transformations. S2L adjusts MIME and response headers, detects selected WAF/bot pages and platforms, and handles special cases such as RSC-style payloads or unexpectedly empty bodies. A detected challenge/bot response is not saved as an ordinary cache entry.
8. **Queue an eligible cache copy.** Eligible `GET` responses with a body are queued for disk saving before response hooks and browser-specific rewrites. Error responses are excluded unless `SAVE_ERRORS` is enabled; bot/challenge and recognized wire-format responses may be excluded by their dedicated checks. This allows the cache to retain the upstream representation while delivery-time behavior is applied.
9. **Rewrite and return.** Applicable HTML/CSS/JSON/JavaScript URL transformations are applied, HTML may receive S2L browser scripts, response hooks are applied, and the final response includes the relevant CORS, security, isolation, and freshness headers.
10. **Reprocess cache hits for delivery.** `_cached_response()` applies relevant response transformations/hooks/headers when serving a cached body, instead of assuming a prior browser-specific transformation is appropriate for every later request.

CDN routes, errors, streams, and special response classes have their own paths; the sequence above describes the general proxy flow, not every branch. URLs rewritten for the browser use request-derived proxy addressing rather than always assuming `localhost`; this does not remove browser secure-context restrictions or make a network-exposed listener safe.

## 6. Disk cache

The main cache is stored below:

```text
site_src/<site-name>/<host>/<path>
```

For example, `https://www.example.test/catalog/item.js` maps under the site cache root to a host directory normalized from `example.test` and a path corresponding to `catalog/item.js`. Exact normalization and exceptional cases are handled by `local_path()`.

`local_path()` maps a URL to a file path:

- A path such as `/a/b.js` maps to the corresponding host directory and path.
- A directory-style path such as `/a/` or `/a` maps to an `index.html` entry.
- Query-string variants are separated under the host's `__query__/` directory so different queries do not overwrite one another.
- For a path with an extension, the encoded query is placed before the extension. For example, `/a/b.js?v=1.2` has a query-specific cache filename rather than sharing `b.js`.
- For extensionless endpoints, the path and query are represented in a query-specific directory/file layout. MIME inference may inspect the cached body where the URL itself provides no extension.
- Host ports and the `www.` prefix are normalized for filesystem use. Long path segments are shortened using a hash to respect filesystem limits.
- Legacy cache layouts are migrated to the current query-cache layout when encountered.

For a path with an extension and a query, the current layout places the entry under `__query__` while preserving the path hierarchy, with the encoded query inserted before the extension. For an extensionless path with a query, the URL path becomes directories and the query has its own final filename; filename encoding preserves query text while escaping filesystem-sensitive characters. Very long names are shortened to fit filesystem limits. The mapping is designed to keep query variants separate, but applications should use `local_path()` rather than constructing cache filenames themselves.

The exact filename encoding can change between revisions. Use `local_path()` and the matching source revision as the source of truth when scripting against the cache.

In offline mode, only cached content is available. Files edited manually in the cache are the local content S2L serves, subject to its response processing. The cache is associated with the configured site name; it is not automatically synchronized between installations.

### Conditional and range requests

Cached responses can be validated using generated `ETag` and `Last-Modified` values. Matching conditional requests may receive `304 Not Modified`. Range requests against supported cached files may receive `206 Partial Content` with `Content-Range` and `Accept-Ranges`. A range request is not equivalent to a full-file response and must not be answered with a misleading `200`.

## 7. Device-specific cache variants

Some sites return different bodies based on the User-Agent. S2L stores variants by broad device family to reduce cross-device cache contamination:

- **Canonical (no suffix):** desktop, Macintosh, and bot profiles. This is also the canonical location for manual edits.
- **`mobile`:** mobile/phone-like profiles.
- **`tablet`:** tablet profiles.

Variant filenames use a suffix such as `__s2l_mobile` before the extension (for example, `index__s2l_mobile.html`). With `DEVICE = "auto"`, the browser's User-Agent determines the profile. Online, if a device-specific file does not exist, S2L can fetch that device's representation instead of serving the canonical page as the first response. Offline, it tries the device variant and then the canonical fallback.

The code also contains cleanup behavior for some cached HTML when it detects a device change. Consult `_device_variant()`, the cache lookup in `proxy()`, and `_cached_response()` for the exact behavior in a particular source revision.

## 8. CDNs and external hosts

Pages frequently request fonts, scripts, images, APIs, and other resources from hosts other than the main site. When CDN proxying is enabled, S2L supports two local routing strategies:

- **Multiport (`MULTIPORT = True`):** each registered external host receives a dedicated local port. Rewritten URLs point to that port.
- **Main-port fallback (`MULTIPORT = False`):** external resources use `/__s2l_ext__/<host>/<path>` on the main proxy port.

The actual route prefix is `/__s2l_ext__/`. It supports range requests for media, including eligible cached range responses in offline mode. Hosts matching `DONT_REWRITE` remain direct browser requests.

### Host registration and health data

- When an external host is discovered, it is entered in `_cdn_host_port`. A multiport entry can be pending while its server starts; with multiport disabled, it routes through the main-port fallback.
- A CDN mini-server constructs an HTTPS upstream URL for its host, forwards the request through the applicable session path, processes the response, and records status/latency. Connection or processing errors can result in `502`.
- In offline multiport mode, S2L can pre-register hosts found in the existing cache so the rewritten URLs refer to started local endpoints.
- The browser injector polls `/.__s2l_hp` for an updated host-to-port map, allowing it to learn about hosts registered after the initial page load.
- The CDN viewer exposes host and health/statistics snapshots. These are diagnostic data, not a guarantee of availability or an automatic CDN failover mechanism.

Multiport mode may open many local ports. Ensure they are available, and do not publish them to untrusted networks.

## 9. Protocols and WebSocket handling

### Protocol coverage

- **Browser to S2L:** HTTP/1.x through the Werkzeug server.
- **S2L to upstream:** HTTP or HTTPS through the selected session implementation. HTTP/2 may be available upstream when `curl_cffi` is used; the local Werkzeug listener remains HTTP/1.x.
- **WebSocket (`ws`/`wss`):** supported by a socket-level handler and upstream relays.
- **Server-Sent Events:** supported when `SSE_PROXY` is enabled, detected by response content type and request/URL hints.
- **TCP/UDP tunnels:** optional routes use WebSocket as the browser-to-proxy transport and are disabled by default.
- **Not proxied by this source:** HTTP/3/QUIC, WebRTC, WebTransport, and FTP. Recognizing an `ftp://` string is not the same as implementing FTP proxying.

S2L does not run a local HTTPS listener by default. A secure upstream website can still be fetched by S2L while the browser connects to the local proxy using HTTP. Browsers generally treat `localhost` specially as a secure context; a LAN IP over plain HTTP may not have the same browser capabilities.

### WebSocket upgrade path

`_S2LWSGIRequestHandler.handle_one_request()` detects `Upgrade: websocket` before handing a request to Flask. This avoids relying on a Flask route for the actual upgrade. Consequently, the Flask routes for `/__s2l_ws_ext__/`, `/__s2l_tcp__/`, and `/__s2l_udp__/` only explain the endpoint when accessed as ordinary HTTP and return `426`; they are not the actual tunnel implementation.

For external WebSockets, the handler extracts the host, path, query, and relevant request headers, builds the upstream `ws://` or `wss://` URL, and establishes an upstream connection. It prefers the curl_cffi-based implementation when available and can fall back to a raw-socket implementation. The local handshake selects no more than one requested subprotocol. The relay forwards messages in both directions and contains handling for control frames, fragmented messages, configured size limits, and optional `permessage-deflate`.

Compression negotiation, keepalive, and reconnect behavior are conditional and can be disabled or constrained for compatibility. Test them against the target application rather than assuming all WebSocket servers implement extensions identically.

In offline mode, a WebSocket cannot be replayed from the HTTP cache; the upgrade is rejected with `503`. The GUI and `CAPTURE_WS` can inspect/capture message traffic. Binary payloads are not guaranteed to be meaningful UTF-8 text.

## 10. Hooks and the traffic inspector

S2L provides Python hooks and, optionally, GUI-managed HTTP and WebSocket hooks. These are separate mechanisms.

### 10.1 Python request and response hooks

`@on_request(pattern, methods)` and `@on_response(pattern, methods)` register functions receiving a `HookContext`.

- `pattern` is a case-insensitive regular expression searched against `ctx.path`, not the host. Use `ctx.url` when host-specific behavior is needed.
- `methods` accepts `"*"` or one/more HTTP method names.
- The context exposes method, URL, path, query, request headers/body, response status/headers/body, and content type.
- Helpers include `json()`, `set_json(obj)`, `req_json()`, and `set_req_json(obj)`.
- Exceptions raised by a hook are logged; an individual hook failure is intended not to terminate the request.
- The hook list is copied before iteration, allowing registrations to change while hooks are being executed.

Example:

```python
@on_response(pattern=r"/api/user/me")
def _patch_profile(ctx: HookContext) -> None:
    data = ctx.json()
    data["example_flag"] = True
    ctx.set_json(data)
```

Keep hooks narrowly scoped and test them against known responses. Hook code runs inside the proxy process and has the same privileges as that process.

### 10.2 HTTP traffic inspector and hooks

When `HOOK_GUI = True`, the optional PyQt6 GUI provides the **Traffic / Hooks** tab:

- **Traffic Log** lists observed traffic with time, method, origin, path, status, web/content type, and size. It has pause, auto-scroll, and clear controls. `Ctrl+F` filters by path, status, content type, or method; the visible row count is bounded.
- **Body Editor** displays a preview of the selected response body and fills in hook fields from that traffic entry. Search shortcuts navigate matches. Buttons can copy the request as cURL or copy its captured request headers.
- **Hook editor** configures the hook name, method, response status, path regex, origin/host, and replacement body.
- **Active Hooks** lists saved rules and supports enable/disable, edit, and delete.
- HTTP hooks can be imported/exported as JSON.

A GUI HTTP hook replaces the response body/status when its enabled filters match; it does not execute Python. GUI-managed hooks are stored under `site_data/MyHooks/<host>/`.

### 10.3 WebSocket messages and hooks

The **WebSocket Hooks** tab has a live message table with direction, opcode, origin, path, time, and size. Filters can narrow by origin/path, direction, and text/binary opcode; payload search can include a bounded UTF-8 preview. The tab also provides auto-scroll, pause, and clear controls.

The editor separates the **When** payload (the match target) from the **Then** payload (the replacement). Rules can filter by:

- Direction: incoming (`in`, upstream to browser) or outgoing (`out`, browser to upstream).
- Opcode: any, text, or binary.
- Origin/host and path regular expression.
- Payload mode: any payload, exact bytes, payload regex, or byte substring.

`Freeze match` keeps the original match payload while another message is selected for replacement editing. Replacement bytes can be edited as text using a selected encoding or as hexadecimal, which supports arbitrary binary messages. The path test checks the configured regex against a supplied WebSocket path.

The first enabled matching hook is applied to the reassembled message. Hook definitions are persisted below `site_data/MyWSHooks/<host>/`; their enabled state can be toggled without deleting them.

### 10.4 Persistence boundaries

Python hooks exist in the source code. GUI HTTP hooks and GUI WebSocket hooks are persisted separately under `site_data` for the configured target. GUI HTTP JSON import/export does not import Python functions, and GUI-managed hooks do not modify the upstream server.

## 11. Browser rewriting and response headers

### URL rewriting

S2L performs server-side URL rewriting for supported response bodies and injects a browser-side script into eligible HTML. The two layers complement one another: static response rewriting cannot see every URL created later by application JavaScript.

The injector is built by `_build_s2l_injector()` and inserted as a script with `id="__s2l__"`. Its shared `rw()` function:

- Leaves relative and already-local URLs unchanged.
- Converts protocol-relative URLs to HTTPS before deciding how to route them.
- Leaves hosts matching `DONT_REWRITE` direct.
- Routes main-site URLs through the main proxy port.
- Routes registered CDN hosts through their assigned local port.
- Routes other supported HTTPS external URLs through `/__s2l_ext__/`.
- Uses dedicated paths for proxied WebSocket URLs, including plain `ws://` and secure `wss://` upstreams.

The injector wraps browser APIs including `fetch`, `XMLHttpRequest`, `WebSocket`, and `EventSource` where available. It also handles `Worker`/`SharedWorker` with special logic for worker scripts and relative `importScripts()` URLs. DOM URL-bearing attributes such as `src`, `href`, `srcset`, `poster`, `data-src`, and `action` are scanned; a `MutationObserver` watches for relevant dynamic changes. `iframe.srcdoc` has dedicated handling. The injector polls `/.__s2l_hp` for newly registered CDN hosts.

S2L also attempts to remove prior Service Workers registered for the local origin, to reduce interference from stale workers/caches. This is best-effort browser-side cleanup, not a general guarantee that all browser caches or storage are cleared.

Sites can still bypass rewriting if they use unsupported APIs, retain native API references before injection, construct URLs in an unhandled way, or encounter browser policy restrictions. Validate the result in the target browser and inspect its developer console/network panel.

### CORS, CSP, and cross-origin isolation

- **CORS and Private Network Access:** S2L constructs CORS and PNA headers for the local proxy and relevant CDN responses.
- **Content Security Policy:** CSP response headers and matching HTML meta policies are removed in order to permit rewritten local resources. This removes an upstream security layer. Run mirrored sites in a separate test browser profile; do not assume the proxy is a sandbox.
- **COOP/COEP:** when `_needs_isolation()` detects signals such as `SharedArrayBuffer`, `Atomics.wait`, or `crossOriginIsolated`, S2L can add cross-origin isolation headers. Worker responses are also considered. COEP selection differs by browser path; Chromium may use `credentialless`, while other paths can use `require-corp`.
- **Secure contexts:** browser features such as Service Workers, `crypto.subtle`, and cross-origin isolation may require a secure context. `localhost` is treated specially by browsers; a plain-HTTP LAN IP may not be.

Header policy changes are compatibility behavior, not proof that the upstream security model remains intact when served locally.

## 12. Upstream sessions and block detection

S2L has optional upstream session implementations:

1. **`curl_cffi`** can use browser-like TLS fingerprints and may improve compatibility with some upstreams.
2. **`ai-cloudscraper`/`cloudscraper`** can be used as alternatives where installed.
3. **`requests`** is the final basic HTTP fallback.

Import-time availability determines which optional packages can be used. A successful import or selected fingerprint does not guarantee bypass of a WAF, CAPTCHA, waiting room, authentication check, or rate limit.

The source identifies some Cloudflare/block responses (including selected challenge pages that return HTTP 200) and includes platform/WAF fingerprinting based on response headers and body. Detection is heuristic: false positives, false negatives, and unknown platforms are possible. The code does not automatically solve every challenge; waiting rooms and systems such as Anubis are documented limitations.

Client sessions are separated by client identity for cookies/session state. Retry and session rotation logic is intended to recover from some connection failures; it does not make unsafe retrying of arbitrary application operations universally correct. A retry of a state-changing request can have different consequences if the upstream processed the first attempt before the connection failed.

## 13. Crawler and path scanner

### Background crawler

With `CRAWL = True` and `OFFLINE = False`, startup enqueues the configured site URL and starts background crawling/discovery workers. `WORKERS` controls the crawler pool and `CRAWL_DEPTH` bounds path traversal. Discovered links/resources are queued according to the source's discovery rules. With `CRAWL = False`, no startup bulk crawl is launched; the browser can still cause on-demand upstream requests.

Crawling cannot guarantee a complete mirror. Authentication, JavaScript-only routes, interaction-dependent content, rate limits, inaccessible pages, and unrecognized resource patterns can all reduce coverage.

### `DUMP_ALL`

When enabled, S2L extracts URLs from supported response bodies more aggressively, with inspection limited by `SCAN_LIMIT`. This increases discovery but can also increase requests substantially.

### `SCAN_PATHS`

The path scanner probes paths from files under `WORDLISTS_DIR` against the configured main host. Supported modes are:

- `"all"`: wordlists recursively under the directory.
- `"all-in-dir"`: top-level wordlists only.
- A relative wordlist path, such as `"common/admin.txt"`.

Blank lines and lines beginning with `#` are ignored; leading slashes are stripped. The scanner sends `HEAD` first and retries with `GET` when the server returns `405`; redirects are not followed. Responses other than `404` and `410` are recorded in `hidden_paths.json`, including statuses such as `401`, `403`, and redirects. The interactive status filter affects terminal logging, not which results are saved. The JSON output includes the scan time, target, mode, configured rate, filters, wordlist names, and result records containing path, URL, status, wordlist, and content type.

`SCANS_PER_SECOND` caps aggregate probe rate when positive. The scanner uses a bounded worker pool; setting the value to zero or less disables the explicit rate cap. `tqdm` is used for progress when available; otherwise progress is logged. If running interactively, the scanner can prompt for status filtering. In non-interactive sessions it skips the prompt. The scanner is ignored in offline mode.

Path scanning sends real HTTP requests to the upstream. Use a suitable wordlist and a rate limit appropriate to the network and server; non-positive `SCANS_PER_SECOND` values disable the scanner's explicit aggregate rate cap.

## 14. Traffic capture

### HTTP capture

With `CAPTURE = True`, eligible HTTP transactions are queued and written as JSON under:

```text
site_data/<site-name>/captures/
```

`CAPTURE_CDN` controls whether CDN traffic is included, `CAPTURE_BODIES` controls request-body capture, and `CAPTURE_SKIP_STATIC` can skip common static resources. Capture filenames include request metadata, a timestamp, and query disambiguation where applicable.

### WebSocket capture

With `CAPTURE_WS = True`, WebSocket messages are written as JSON under:

```text
site_data/<site-name>/captures_ws/
```

Capture writers use background queues/workers. Captures may contain cookies, authorization values, tokens, personal information, request bodies, or private site data. Treat them as sensitive; inspect and redact before sharing or committing.

## 15. Internal endpoints

| Path                                 | Purpose                                                                      |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| `/` and `/<path>`                    | Main proxy for configured HTTP methods.                                      |
| `/__s2l_ext__/<host>/<path>`         | External/CDN proxy route on the main port.                                   |
| `/__s2l_ws_ext__/<host:port>/<path>` | External WebSocket route; the actual upgrade is handled at the socket layer. |
| `/__s2l_tcp__/<host:port>`           | Optional TCP tunnel over WebSocket.                                          |
| `/__s2l_udp__/<host:port>`           | Optional UDP tunnel over WebSocket.                                          |
| `/.__s2l_hp`                         | JSON map of registered CDN hosts to local ports for the injector.            |
| `/__s2l_viewer__`                    | Web-based CDN/statistics viewer; requires `WEB_CDNVIEWER = True`.            |
| `/__s2l_viewer_data__`               | Viewer data endpoint; requires `WEB_CDNVIEWER = True`.                       |
| `/__s2l_reset_stats__`               | `POST` endpoint that resets statistics; requires `WEB_CDNVIEWER = True`.     |

The viewer reports operational statistics such as CDN hosts/health, counters, request rate, uptime, and the last detected platform. The reset endpoint is mutating and there is no user authentication on these viewer endpoints. Keep the listener on a trusted interface/network.

Opening a WebSocket/tunnel route as an ordinary HTTP URL does not create a tunnel; the Flask fallback responds `426 Upgrade Required`. Tunnel routes are disabled by default and respond with an explanatory error when an upgrade attempts to use a disabled tunnel.

## 16. Source-code map

Approximate line ranges for the documented V8.1 source; use symbol search because ranges shift as the file changes.

| Area                                             | Approximate lines / symbols                                     |
| ------------------------------------------------ | --------------------------------------------------------------- |
| Imports, logging, configuration                  | 1–420                                                           |
| Platform/WAF detection                           | 520–620; `detect_platform()`                                    |
| Hook system and PyQt6 GUI                        | 620–3050; `on_request()`, `on_response()`, `_launch_hook_gui()` |
| Device profiles and upstream sessions            | 3060–3500; `_make_session()`                                    |
| Decompression and MIME handling                  | 3500–3800                                                       |
| CORS, response security, isolation               | 3940–4430; `filter_resp()`                                      |
| Client-aware addressing and static URL rewriting | 4430–4530; `rewrite_abs_urls()`                                 |
| JavaScript injector                              | 4620–5250; `_build_s2l_injector()`                              |
| Disk cache mapping and migration                 | 5250–5630; `local_path()`                                       |
| Path scanner                                     | 5690–6300; `_run_path_scanner()`                                |
| CDN servers, host registration, health           | 6300–7550; `_start_cdn_server()`                                |
| Saving, capture, crawler                         | 7550–8150; `_save_worker()`, `crawl_parallel()`                 |
| Streaming                                        | 8150–8400; `_should_stream()`, `_stream_resp()`                 |
| WebSocket frames, compression, relays            | 8400–9450; `_RawSocketUpstreamWS`, `_CffiUpstreamWS`            |
| SSE and raw TCP/UDP tunnels                      | 9450–9800                                                       |
| Custom WSGI server and upgrade handler           | 9790–10250; `_PooledWSGIServer`, `_S2LWSGIRequestHandler`       |
| Upstream request and cache serving               | 10250–11020; `_do_upstream()`, `_cached_response()`             |
| Internal routes and web viewer                   | 11020–12500                                                     |
| Main proxy route                                 | 12500–13380; `proxy()`                                          |
| Banner and startup                               | 13380–end                                                       |

## 17. Limitations and operational considerations

- **Not a complete browser emulator:** compatibility varies by site, browser, dependency version, authentication model, and network environment.
- **Incomplete mirror is possible:** crawler discovery and runtime rewriting cannot guarantee that every route or resource is found.
- **Some challenges are not solved:** CAPTCHA, waiting rooms, Anubis, rate limits, and other access controls may still block requests.
- **Unsupported protocols:** HTTP/3/QUIC, WebRTC, WebTransport, and FTP are not proxied.
- **WebSockets are live connections:** they cannot be reconstructed from the ordinary HTTP disk cache in offline mode.
- **Single-file implementation:** the application is concentrated in a large `s2l.py`; changes should be tested carefully because components share state and runtime behavior.
- **Default listener is network-visible:** `HOST = "0.0.0.0"` binds to all IPv4 interfaces. For local-only use, consider `127.0.0.1`, and do not forward proxy/CDN ports from a router or expose them publicly.
- **Viewer endpoints have no authentication:** `WEB_CDNVIEWER` is off by default, but enabling it exposes statistics and a state-changing reset endpoint to clients that can reach the listener.
- **Mirrored content is not sandboxed:** CSP is removed to support rewriting, and proxied pages run in the browser. Use a separate test browser profile and avoid signing into sensitive accounts unless you understand the consequences.
- **Capture data can be sensitive:** do not commit or share raw captures without reviewing and redacting them.

## 18. Troubleshooting

| Symptom                                                   | Likely causes                                                                                                  | Checks and next steps                                                                                                                                                             |
| --------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ModuleNotFoundError` at startup                          | A required package is missing from the active Python environment.                                              | Activate/use the intended virtual environment and install the required packages there.                                                                                            |
| Main listener fails to bind                               | `PORT` is already in use or unavailable.                                                                       | Choose a free `PORT`. Multiport CDN servers also need free local ports.                                                                                                           |
| Browser cannot connect to S2L                             | Wrong address/port, stopped process, or local firewall rule.                                                   | Confirm the `Listening on ...` log and use the same host/port in the browser.                                                                                                     |
| Main page loads but external scripts/fonts/images fail    | CDN host not registered, host excluded by `DONT_REWRITE`, occupied/blocked CDN port, or browser origin policy. | Check CDN logs, `/.__s2l_hp`, and browser developer tools. Compare with `MULTIPORT = False` if appropriate.                                                                       |
| CAPTCHA, challenge, blank page, or upstream block appears | WAF, rate limit, unsupported challenge, or upstream returned an empty/challenge response.                      | Inspect status/logs and detected platform. `curl-cffi` may improve compatibility but is not a guarantee; reduce request volume and retry after the upstream's restriction window. |
| Offline request returns `404`                             | The URL/query was not cached, the cache belongs to another site name, or the expected variant is missing.      | Inspect `site_src/<site-name>/`, confirm `SITE`, and fetch the resource online before switching to offline mode.                                                                  |
| Video seeking or resuming fails                           | Upstream/client range behavior differs, the requested range is invalid, or the cached full file is missing.    | Inspect `206` and `Content-Range` in the browser/network log; verify the complete file is cached for offline playback.                                                            |
| WebSocket fails while offline                             | Expected limitation: a live socket cannot be served from the HTTP cache.                                       | Use online mode for pages requiring real-time connections.                                                                                                                        |
| Worker or cross-origin resource fails from a LAN IP       | Browser secure-context or CORS/COEP behavior differs from `localhost`.                                         | Test with `localhost`, inspect browser console/network errors, and verify relevant response headers.                                                                              |
| PyQt6 GUI does not open                                   | `HOOK_GUI` is off, PyQt6 is missing, or no graphical desktop is available.                                     | Enable `HOOK_GUI`, install PyQt6 in the active environment, and run in a graphical session. The proxy can run without the GUI.                                                    |

When diagnosing cache behavior, verify that a file actually exists under the expected `site_src` tree; a page rendering once does not prove that every resource was saved. An offline run is a useful coverage check but will expose missing resources as 404s. Use the browser developer tools and S2L logs together to identify the failing URL and status.
