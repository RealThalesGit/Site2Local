"""User settings. Edit before starting Site2Local."""

# Package version — single source of truth, shown in the boot banner.
# The 9.x line is the MODULAR tree; the frozen monolith reference (s2l.py)
# stays at its historical 8.1. Bump here and the banner follows.
VERSION = "9.0"

SITE = "jeskojets.com"   # target domain or URL
HOST = "0.0.0.0"               # listen address
PORT = 8080                    # listen port
DEVICE = "auto"                # UA profile: auto|mobile|macintosh|ie11|iphone|ipad|bot — auto mirrors the requesting browser's own UA
TIMEOUT_READ = 12              # upstream read timeout (s)
TIMEOUT_CONN = 6               # upstream connect timeout (s)
TIMEOUT_STREAM_READ = 120      # streaming read timeout (s) — longer for video/audio
WORKERS = 60                   # crawler thread-pool size
MAX_FNAME = 180                # max on-disk path-segment length
SAVE_BATCH = 32                # files per save-flush
SAVE_INTERVAL = 0.15           # max seconds between save flushes
CRAWL_DEPTH = 7                # max URL path depth to follow
SCAN_LIMIT = 256 * 1024        # body bytes scanned in DUMP_ALL mode
RETRIES = 2                    # retry count on 5xx / timeout
BACKOFF = 0.4                  # exponential backoff base (s)
CRAWL = True                   # crawl at startup; False = proxy-on-demand only
OFFLINE = False                # never hit upstream — serve disk only
PREMAP_CDN = False             # pre-start ports for CDN hosts already cached on disk
SAVE_ERRORS = False            # cache 4xx/5xx responses
DUMP_ALL = False               # extract + crawl every URL found in any response body
PROXY_CDN = True                # proxy external CDN/third-party assets
CACHE_CDN = True                # cache CDN assets to disk (False = live-proxy, no disk)
MULTIPORT = True                # each CDN host gets a dedicated port (False = /__s2l_ext__/)
HOOK_GUI = False                # PyQt6 traffic inspector + live hook editor (Fusion theme)
RAINBOW_LOGS = False            # lolcat-style terminal output
SHOW_HIDDEN = False             # un-hide display:none / disabled elements in HTML
SCAN_PATHS = False             # hidden-path scanner: False | "all" | "all-in-dir" | "<dir>/<file>"
                                #   "all"        — every wordlist under wordlists/ (recursive)
                                #   "all-in-dir" — every wordlist in wordlists/ top-level only
                                #   "dir/file"   — one specific wordlist (relative to wordlists/)
SCANS_PER_SECOND = 10           # SCAN_PATHS rate limit + progress refresh cadence.
                                #   Three effects, one knob:
                                #     1. REAL RATE LIMIT — caps total probes/sec to
                                #        MAIN_HOST across ALL worker threads. The
                                #        scanner runs concurrently (ThreadPoolExecutor)
                                #        so it can actually reach this rate even with
                                #        slow network probes. ~15% of this value
                                #        (clamped to [4, 24]) is used as the worker
                                #        thread count.
                                #     2. PROGRESS REFRESH (no-tqdm fallback) — the
                                #        "Scanning paths of <wl>... [X/Y]" line is
                                #        re-logged every SCANS_PER_SECOND paths.
                                #   Set to 0 (or a negative) to disable the rate
                                #   limit — the scanner then runs as fast as 8
                                #   worker threads allow (no cap).
WORDLISTS_DIR = "Wordlists"      # wordlist files for SCAN_PATHS (one path per line)
CAPTURE = False                  # record every request+response as JSON
CAPTURE_CDN = False              # include CDN responses in captures
CAPTURE_BODIES = False            # include request bodies in captures
CAPTURE_SKIP_STATIC = True       # skip images/fonts/JS/CSS from captures
CAPTURE_WS = False                # record every WebSocket message (text+binary) as JSON
# ── Web UI viewers (totally optional) ─────────────────────────────────────────
# When False, the matching /__s2l_*__ routes return 404 and their cross-links
# are hidden from the other page, so the proxy runs headless with zero web
# surface. Flip either on to expose the corresponding self-contained viewer.
WEB_CDNVIEWER    = True            # /__s2l_viewer__ + /__s2l_viewer_data__ + /__s2l_reset_stats__
WEB_CDNVIEWER_IP_WHITELIST = []      # empty = allow anyone; otherwise e.g. ["192.168.1.20", "2001:db8::10"]
# ──────────────────────────────────────────────────────────────────────────────
# LAYER 2 CONFIG
# ──────────────────────────────────────────────────────────────────────────────
WS_PING_INTERVAL    = 0        # seconds between WS keepalive pings (0 = off).
                                # Sending a ping can race with a blocked receive
                                # on the same curl_cffi handle and corrupt its
                                # internal buffers. Pong timeouts can also close
                                # tunnels when a pong is auto-consumed or merged
                                # with other frames. Keep disabled by default;
                                # enable only if the application has no heartbeat.
WS_PONG_TIMEOUT     = 15       # seconds to wait for a pong after a ping before
                                # declaring the upstream dead (only used when
                                # WS_PING_INTERVAL > 0; must be < WS_PING_INTERVAL
                                # to keep detection meaningful; clamped at runtime)
WS_AUTO_RECONNECT   = False     # transparently re-establish dropped WS upstreams
WS_DEFLATE          = False    # negotiate permessage-deflate (RFC 7692)
WS_LOG_FRAMES       = False    # log every WS frame (verbose; for debugging)
WS_MAX_MSG_BYTES    = 8 * 1024 * 1024   # hard cap on reassembled WS message size
SSE_PROXY           = True     # proxy Server-Sent Events streams (text/event-stream)
SSE_HEARTBEAT       = 15       # seconds between injected SSE keepalives (0 = off)
TCP_TUNNEL          = False    # /__s2l_tcp__/<host:port> raw TCP tunnel route
UDP_TUNNEL          = False    # /__s2l_udp__/<host:port> raw UDP tunnel route
TUNNEL_TIMEOUT      = 300      # idle timeout (s) for raw TCP/UDP tunnels
POOL_MAXSIZE        = 64       # per-session connection pool size
POOL_CONNECTIONS    = 32       # per-session connection pool slots
DEAD_HOST_TTL       = 60       # seconds to remember a dead upstream host
STATS_WINDOW        = 60       # seconds for rolling stats window
GRACEFUL_SHUTDOWN   = True     # drain in-flight requests on SIGTERM
MAX_BODY_BYTES       = 256 * 1024 * 1024  # request-body cap (Flask MAX_CONTENT_LENGTH) —
                                        # OOM guard for the threaded server
MAX_DECOMPRESSED_BYTES = 512 * 1024 * 1024  # BOMB GUARD: decompression output cap
                                        # (a 10MB gzip declaring gigabytes must not
                                        # OOM the process — see decompress_body)
# Maximum raw (compressed) body size (bytes) we'll accumulate in RAM while
# streaming a response so it can ALSO be cached to disk after the stream
# finishes. Bodies larger than this are still streamed to the browser, but
# NOT cached to disk — prevents OOM on huge video/download streams.
# Default: 256 MB. Set to 0 to disable stream-to-disk caching entirely
# (every stream is forwarded and forgotten, never written to site_src).
STREAM_CACHE_MAX_BYTES = 256 * 1024 * 1024   # 256 MB

# ──────────────────────────────────────────────────────────────────────────────
# Filters / constants
# ──────────────────────────────────────────────────────────────────────────────

BLOCK_PATHS = ()   # user-tunable: paths to block with 403 (e.g. vendor challenge URLs)
CDN_BLOCK    = ()
DUMP_TARGETS = {}

# Domains whose URLs must NEVER be rewritten by the S2L injector (neither the
# Python-side _rewrite_ext_urls nor the browser-side JS rw()). Captcha widgets
# (hcaptcha.com, recaptcha.net, etc.) break when their script/iframe URLs are
# rewritten to localhost — the captcha backend then sees "localhost" as the
# origin and either refuses to render ("localhost detected") or 404s the
# challenge asset ("NOT FOUND"). List the bare hostnames (no scheme, no port);
# subdomain suffix matches are allowed (e.g. "hcaptcha.com" also matches
# "api.hcaptcha.com"). These hosts always go DIRECT from the browser.
DONT_REWRITE = () # You will need to write the domains manually, this is effective with some websites and some captchas

