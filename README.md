# Site2Local

> **Mildly Powerful and IT'S FREE!!**

Run a website through a local Python proxy, save the resources you visit, and edit your local copy. Site2Local can also crawl linked resources, serve downloaded files offline, and inspect or replace responses through a hook editor.

**Version 9.0 · Work in progress.** The modular Python package is the actively maintained implementation. Compatibility depends on the website, browser, and available dependencies; a saved frontend is not a replacement for the website's backend.

**Jump to:** [Quick start](#quick-start) · [Modes](#how-it-works) · [Configuration](#configuration) · [Editing and hooks](#edit-downloaded-files-and-use-hooks) · [Troubleshooting](#troubleshooting-and-limitations) · [Development](#development-and-tests)

## Quick start

1. Clone the repository:

   ```sh
   git clone https://github.com/RealThalesGit/Site2Local.git
   cd Site2Local
   ```

   Use a Python environment that supports modern syntax such as `str | None` (Python 3.10 or newer). Depending on your system, the command may be `python3` or `py` instead of `python`.

   Create a virtual environment:

   ```sh
   python -m venv .venv
   ```

   Activate it with `.venv\Scripts\Activate.ps1` in Windows PowerShell, or `source .venv/bin/activate` in a Linux/macOS shell. Then install the core dependencies:

   ```sh
   python -m pip install -r requirements.txt
   ```

2. Edit [Config.py](Config.py). For a simple first run:

   ```python
   SITE = "example.com"
   HOST = "127.0.0.1"
   PORT = 8080
   CRAWL = True
   OFFLINE = False
   HOOK_GUI = False
   ```

   These are example settings, not a statement of the checked-in defaults. Check `Config.py` before starting; it is also where you choose the target website.

3. Start the modular application from the project folder:

   ```sh
   python Boot.py
   ```

4. Open **http://localhost:8080**. Browse normally to fetch and save resources. Stop with **Ctrl+C** and allow pending saves to drain.

With the example's `CRAWL = True`, the initial crawl runs in the background while the proxy starts. You can browse while files are discovered and downloaded. Set `CRAWL = False` to skip that initial crawl.

Restart the application after changing configuration flags. `Boot.py` keeps generated paths relative to the project folder. The folder can have any name (for example `S2L` or `My Site Proxy`); launch with `python Boot.py` inside it, or `python /path/to/project/Boot.py` from elsewhere. You can also run `python -m Site2Local` from its parent directory when the folder is named `Site2Local`.

## How it works

The browser connects to Site2Local, which fetches missing resources from the target or serves an existing disk copy. URL rewriting and an injected browser runtime help route scripts, styles, images, workers, WebSockets, and other requests through the proxy.

| Mode | Settings | Behavior |
| --- | --- | --- |
| On demand | `CRAWL = False`, `OFFLINE = False` | Fetch resources as the browser requests them. |
| Crawl + proxy | `CRAWL = True`, `OFFLINE = False` | Crawl the target at startup and continue discovering links during browsing. Startup can take longer. |
| Offline | `OFFLINE = True` | Serve available cached resources without fetching missing ones upstream. Missing resources remain unavailable. |

For offline use, first browse or crawl the required pages online. Then stop the proxy, keep the same `SITE`, and change:

```python
OFFLINE = True
```

Restart and open the same local address. Offline mode automatically prepares existing CDN hosts when `PROXY_CDN` and `MULTIPORT` are enabled. `PREMAP_CDN = True` requests that preparation in online mode too.

### Where your files go

| Folder | Contents |
| --- | --- |
| `site_src/` | Downloaded resources, grouped by target site and upstream host. |
| `site_data/` | Hook stores, captures, and runtime metadata. |
| `__pycache__/` | Regenerable Python bytecode; safe to remove while the application is stopped. |

Generated mirrors and runtime data are excluded by `.gitignore`. Removing `site_src` removes the downloaded mirror and any edits inside it; removing `site_data` can remove saved hooks and captures.

## Configuration

All settings live in [Config.py](Config.py). The tables below describe their purpose; examples are not necessarily the current defaults.

### Target, discovery, and CDN routing

| Flag | Purpose |
| --- | --- |
| `SITE`, `HOST`, `PORT` | Target website and local listening address. Set `SITE = "http://example.com"` or `"https://example.com"`; a bare hostname means HTTPS. Failed probes and HTTPS-to-HTTP startup redirects stop startup. Same-protocol redirects and HTTP-to-HTTPS upgrades are supported. `127.0.0.1` is local-only; `0.0.0.0` listens on available interfaces. |
| `DEVICE` | `"auto"` follows the requesting browser's user agent; other supported profiles are listed in the configuration. |
| `CRAWL`, `CRAWL_DEPTH`, `WORKERS` | Enable discovery and control its depth and worker count. |
| `DUMP_ALL`, `SCAN_LIMIT` | Also look for complete quoted URLs in JavaScript/JSON, within the scan budget. This does not execute JavaScript. |
| `PROXY_CDN` | Route external resource requests through the proxy. |
| `CACHE_CDN` | Save fetched external resources to disk. Requires `PROXY_CDN`. |
| `MULTIPORT` | Use dedicated ports for CDN hosts; otherwise external resources use `/__s2l_ext__/`. |
| `DONT_REWRITE` | Hosts that the browser should access directly. These resources still need an internet connection. |
| `BLOCK_PATHS`, `CDN_BLOCK`, `DUMP_TARGETS` | Configure request blocking and discovery scope. |
| `TIMEOUT_CONN`, `TIMEOUT_READ`, `TIMEOUT_STREAM_READ` | Connection and response timeouts. Streaming has a separate read timeout. |
| `TLS_VERIFY` | Verify upstream HTTPS and WSS certificates (default `True`). Set `False` only for a trusted test server with an invalid certificate. This does not select HTTP: choose `http://` explicitly in `SITE` for a plain HTTP origin. Restart after changing it. |
| `OUTBOUND_PRIVATE_ALLOWLIST` | Private or non-global outbound destinations are blocked, including redirects, WebSockets and TCP/UDP tunnels. For a trusted local service, add its exact `"host:port"`, e.g. `["localhost:8443"]`. Restart after changing it. |

HTML discovery understands `<base href>`, `srcset`, media and poster attributes, lazy-image attributes, and inline styles. CSS discovery handles `url()` and `@import`, including dependencies of external stylesheets. URLs are deduplicated without discarding query strings; fragments and non-HTTP(S) schemes are excluded from downloads.

### Disk and RAM cache

| Flag | Purpose |
| --- | --- |
| `OFFLINE` | Use the downloaded mirror without upstream fetches. |
| `PREMAP_CDN` | Prepare CDN ports from existing cache folders. |
| `SAVE_ERRORS` | Allow error responses to be saved. |
| `ATOMIC_SERVE` | Reuse raw file bytes in a bounded RAM cache and memoize MIME detection. |
| `ATOMIC_SERVE_MAX_BYTES` | RAM budget for raw file bytes, for example `"64MB"` or `"256MB"`. |
| `STREAM_CACHE_MAX_BYTES` | Maximum accumulated raw stream size eligible for disk caching; `0` disables stream-to-disk caching. |

Size flags accept byte counts and strings such as `"512KB"`, `"64MB"`, and `"1GB"`, using binary units. **`"256"` means 256 bytes, not 256 MB.**

`ATOMIC_SERVE` accelerates repeated reads of already-downloaded resources. It checks file signatures so ordinary edits, replacements, and deletions invalidate stale entries. URL rewrites and hooks still run per request. Raw-file caching is also capped at 4096 entries, files over 16 MiB bypass the body cache, and MIME memoization has its own 4096-entry limit. The configured byte budget is not a cap on the entire Python process.

This option does not accelerate the upstream connection or the game's own JavaScript execution. Set `ATOMIC_SERVE_MAX_BYTES = 0` to disable raw-file RAM storage while retaining MIME memoization when `ATOMIC_SERVE` is enabled.

### Cross-origin isolation

```python
CROSS_ORIGIN_ISOLATION = "auto"
```

| Value | Behavior |
| --- | --- |
| `"auto"` | Detect isolation requirements from document headers or HTML signals such as SharedArrayBuffer/threaded WASM. |
| `True` | Force COOP/COEP for documents and workers. Useful when automatic detection misses a requirement. |
| `False` | Remove enforced COOP/COEP for compatibility with ordinary third-party frames. |

In automatic mode, a requirement detected on a top-level document stays active across proxy ports until restart, including cached documents and workers. Detection is heuristic: a requirement hidden in external JavaScript may need `True` before loading the main page.

Isolation enables certain browser features but can block external iframes that do not opt in. After changing this setting, restart the proxy and reload the main page. Different browsers can enforce these policies differently.

### Inspection and other tools

| Flag | Purpose |
| --- | --- |
| `HOOK_GUI` | Open the PyQt6 traffic inspector and response/WebSocket hook editors. |
| `CAPTURE`, `CAPTURE_CDN`, `CAPTURE_WS` | Record HTTP and/or WebSocket traffic under `site_data`. |
| `CAPTURE_BODIES`, `CAPTURE_SKIP_STATIC` | Control what HTTP captures include. |
| `WEB_CDNVIEWER` | Enable the CDN viewer at `http://localhost:8080/__s2l_viewer__`. |
| `WEB_CDNVIEWER_IP_WHITELIST` | Restrict access to that viewer when configured. |
| `WEB_CDNVIEWER_ALLOWED_ORIGINS` | Additional trusted browser origins allowed to read the viewer or reset counters. Empty permits same-origin requests and direct navigation only. This complements the IP allowlist; it is not authentication. |
| `SCAN_PATHS`, `WORDLISTS_DIR`, `SCANS_PER_SECOND` | Configure optional path discovery from wordlists. |
| `SSE_PROXY`, `SSE_HEARTBEAT` | Configure Server-Sent Events forwarding and keepalives. |
| `SHOW_HIDDEN`, `RAINBOW_LOGS` | Optional visibility changes and terminal coloring. |

See `Config.py` for WebSocket, tunnel, request-size, decompression, and connection-pool settings.

## Edit downloaded files and use hooks

### Edit the disk mirror

Downloaded resources are stored under `site_src/<site>/<host>/`. You can edit the local HTML, CSS, JavaScript, and other files. Reload the page to request the updated content.

An existing cache entry is generally served locally rather than checked for a newer upstream version on every request. Preserve your edits before removing a specific cached resource to fetch it again online.

Query variants use a `__query__/` layout with `.q` sidecars that identify the owning query. Device variants can have separate filenames. Keep sidecars with their files and avoid renaming cache entries casually: URL-to-file lookup depends on the naming rules.

New saves use independent files rather than hardlinks. Existing mirrors created by older versions can still contain hardlinked files; that change does not automatically separate old copies.

### Replace responses in the hook inspector

Install PyQt6 and enable the inspector:

```sh
python -m pip install PyQt6
```

```python
HOOK_GUI = True
```

Select a traffic entry, edit its replacement body, and save a hook. Hooks can filter by method, status, origin, and a regex path pattern. New selections generate exact path patterns:

```regex
^/$
^/_astro/Search\.e75879d4\.js$
```

`^` and `$` anchor the match; `\.` matches a literal dot. These patterns select requests; they do not change filenames. Query-specific patterns are supported, while path-only patterns remain compatible with requests carrying query parameters.

For example, `^/api/data\?id=1$` targets that query, while `^/api/data$` remains a path-only match. Paths and queries are matched case-insensitively. To handle a build hash that changes, a pattern such as `^/_astro/Search\.[a-zA-Z0-9_-]+\.js$` matches that bundle family instead of one exact filename.

The response editor hides Site2Local's injector from HTML. Edited HTML is rewritten and receives a fresh injector when served. CSS and JavaScript replacements preserve their existing MIME type. Editing binary resources as UTF-8 text can corrupt the replacement bytes.

Be careful with broad patterns: `/` matches many paths, whereas `^/$` matches only the root path. A hook that replaces CSS with HTML can remove the page's styling. Hooks replace local proxy responses; they do not modify the upstream website's source files. Normal browsing can still call the website's live APIs.

## Optional dependencies

The core requirements are Flask, requests, BeautifulSoup, and colorama. Additional packages enable specific features:

| Package | Feature |
| --- | --- |
| `curl-cffi` | Alternative HTTP/TLS client with browser impersonation. |
| `ai-cloudscraper` / `cloudscraper` | Additional upstream client options. |
| `brotli`, `zstandard` | Support for additional content encodings. |
| `PyQt6` | Desktop hook inspector. |
| `wafw00f` | WAF identification. |
| `tqdm` | Progress displays. |

For example:

```sh
python -m pip install curl-cffi brotli zstandard
```

See [requirements.txt](requirements.txt) for the package list. Optional dependencies do not guarantee access to websites that reject proxy traffic.

## Troubleshooting and limitations

The shared mirror bypasses authenticated requests (Authorization, upstream cookies or a nonempty server-side session jar). Responses marked `private` or `no-store`, responses setting cookies, and responses varying on Cookie/Authorization are not saved. This applies to the proxy, CDN, streams and crawler. Consequently, authenticated/private pages are not available from this shared offline mirror. Older mirrors can contain private data saved before this policy; review them before sharing a mirror with others.

Upstream sessions are separated by a signed `HttpOnly` browser cookie rather than by IP address. The cookie is shared across the proxy's CDN ports and is never sent upstream. Browsers must accept cookies to retain a session. Restarting the process invalidates these identities and clears the in-memory upstream sessions. This isolates cookie jars; it does not authenticate access to the proxy. Keep the proxy private or put authentication and network restrictions in front of it.

HTTP backends use an authenticated loopback outbound guard; WebSockets and raw tunnels use the same destination policy. The guard connects to the numeric address that it validated, including on redirected HTTP connections. Environment proxy settings are ignored by upstream sessions. `OUTBOUND_PRIVATE_ALLOWLIST` grants an explicit exception for a trusted `host:port` (`[::1]:8443` for IPv6). An allowed local service must itself be trusted; the exception permits access to that service.

| Symptom | What to check |
| --- | --- |
| Missing resources offline | Confirm they were downloaded online, including CDN resources. Offline mode cannot reproduce live APIs, authentication servers, or WebSocket backends. |
| A game loads but will not start | Check the browser Console and Network panels for failed assets, SDK errors, MIME mismatches, or iframe communication failures. A successful HTTP response alone does not prove the game initialized. |
| `NS_ERROR_DOM_COEP_FAILED` or a Chromium iframe policy error | Check `CROSS_ORIGIN_ISOLATION` and the parent/child response policies. Forcing isolation can block third-party content. |
| A script or stylesheet is rejected by `nosniff` | Inspect the response MIME and body: the resource may be an error page or an overly broad hook replacement. |
| Hook edits do not appear | Check that the hook is enabled and that method, status, origin, path, and query filters match. Restart after changing configuration flags. |
| A local file edit appears stale | Check whether the request targets a query/device variant or a different CDN host. Try a hard reload and inspect the requested URL. |
| Login, CAPTCHA, or an external SDK fails | Some services require their real origin or live browser state. `DONT_REWRITE` can allow selected hosts to load directly, but this also makes those resources network-dependent. |

The crawler does not execute JavaScript or discover every dynamically constructed URL. Browsing a page may fetch resources that crawling alone missed. Sites using authentication, ads, origin checks, DRM, or remote application state may work only partially.

Older cache naming layouts are not migrated automatically. A startup sweep removes entries it identifies as unreachable legacy files and orphan query sidecars. Back up a mirror containing important manual edits before upgrading across cache-layout changes.

Use Site2Local for websites and content you are allowed to access and modify. Do not use it for phishing or impersonation. Keep local experiments private unless you intend to expose the proxy and its tools on your network.

## Development and tests

The maintained code is modular. `s2l.py` remains a historical monolithic reference; fixes in the package are not automatically backported to it.

| Location | Responsibility |
| --- | --- |
| `Boot.py`, `Application.py`, `Runtime.py` | Startup, route assembly, and shared state. |
| `Core/`, `HTTP/`, `Proxy/`, `CDN/` | Cache paths, MIME detection, HTTP handling, and resource serving. |
| `Crawler/` | Crawling, shared link extraction, background saves, and path scanning. |
| `Rewriting/`, `Hooks/` | URL/runtime rewriting and traffic editing. |
| `Capture/`, `Viewers/`, `WebSockets/`, `Network/` | Captures, viewers, and connection handling. |
| `Scripts/` | Regression tests and diagnostic scripts. |

Run the focused offline regression suites from the project root:

```sh
python -B -m unittest discover -s Scripts -p "test_cached_*.py"
python -B -m unittest discover -s Scripts -p test_stream_cache.py
python -B -m unittest discover -s Scripts -p test_isolation_headers.py
python -B -m unittest discover -s Scripts -p test_link_extraction.py
python -B -m unittest discover -s Scripts -p test_save_integrity.py
python -B -m unittest discover -s Scripts -p test_hook_editor.py
python -B -m unittest discover -s Scripts -p test_cdn_cached_hooks.py
```

Run every local regression script, including the adapted historical batteries and the live suites, with:

```sh
python -X utf8 -B Scripts/run_local_tests.py
```

Each script runs in its own process. Historical batteries use temporary copies of the current checkout and bundled fixtures; they do not depend on `download/Site2Local`, fixed `/tmp` paths, your personal configuration, or external websites. Use `--legacy-only` for just these batteries, and `--jobs 1` to run sequentially. Failures produce a nonzero exit code; skipped tests are reported separately.

The JavaScript checks need Node.js on `PATH`. Browser isolation uses Playwright Chromium against a fresh local live proxy; it skips explicitly when the browser dependency is unavailable. The optional-dependency battery requires Brotli and Zstandard; the WSS certificate test requires `cryptography`. Missing optional prerequisites are reported as skips. `-B` avoids creating Python bytecode cache files. Unit and cache-fixture tests intentionally remain offline; the HTTP/browser/WSS integration tests make real loopback connections.

Run the real-network integration suite locally (no external website needed):

```sh
python -X utf8 -B -m unittest discover -s Scripts -p test_live_proxy.py
```

This starts an HTTP fixture and the real `Boot.py` in a temporary renamed checkout with `OFFLINE=False`. It checks live HTML/assets, POST bodies, browser-session isolation, redirects, blocked private destinations, disk caching, ranges and HEAD. It preserves your configuration and mirrors, stops the test processes, and deletes its temporary files. Optional WAF fingerprinting is disabled in this test; upstream requests are real. This suite does not replace browser or TLS/WebSocket tests.

For manual testing, start the fixture with `python Scripts/local_test_server.py --port 9090`. To proxy it, use `SITE = "http://127.0.0.1:9090"`, `OFFLINE = False`, `TLS_VERIFY = True` (the fixture uses plain HTTP, so no certificate is involved), `CRAWL = False`, and `OUTBOUND_PRIVATE_ALLOWLIST = ["127.0.0.1:9090"]`. Its routes include `/`, `/app.js`, `/style.css`, `/redirect`, `POST /login` (`user=alice`), and `POST /echo`. Keep this fixture bound to loopback.

For a bug report, include the target URL, relevant settings, whether the failure happens on the first load or a reload, server logs, and browser Console/Network errors. Remove credentials and session tokens from shared logs. Contributions and reproducible reports are welcome; this is an independently maintained work in progress.

## More documentation

- [Configuration reference in the source](Config.py)
- [Modular map and response pipeline](MODULAR_MAP.md)
- [Technical reference](DOCUMENTARY.MD) — current modular architecture, request pipelines, cache behavior, hooks, and operational limits.
- [Apache 2.0 license](LICENSE)
