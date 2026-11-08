"""Site2Local: cli."""
from __future__ import annotations

from colorama import Fore
from colorama import Style

from .. import Runtime as _s2l_state


def _banner() -> None:
    W, C, G = Style.BRIGHT+Fore.WHITE, Style.BRIGHT+Fore.CYAN, Style.BRIGHT+Fore.GREEN
    Y, M    = Style.BRIGHT+Fore.YELLOW, Style.BRIGHT+Fore.MAGENTA
    DIM, R  = Style.DIM, Style.RESET_ALL

    ip      = _s2l_state.resolve_ip(_s2l_state.MAIN_HOST)
    n_hooks = len(_s2l_state._REQ_HOOKS) + len(_s2l_state._RESP_HOOKS)

    flags = []
    if _s2l_state.CRAWL:         flags.append(f"{G}crawl{R}")
    if _s2l_state.OFFLINE:       flags.append(f"{Y}offline{R}")
    if _s2l_state.PREMAP_CDN:    flags.append(f"{C}premap-cdn{R}")
    if _s2l_state.DUMP_ALL:      flags.append(f"{M}dump-all{R}")
    if _s2l_state.PROXY_CDN and _s2l_state.CACHE_CDN:   flags.append(f"{M}cdn:cache{R}")
    elif _s2l_state.PROXY_CDN:               flags.append(f"{Y}cdn:live{R}")
    if _s2l_state.MULTIPORT:     flags.append(f"{M}multiport{R}")
    if _s2l_state.SHOW_HIDDEN:   flags.append(f"{G}show-hidden{R}")
    if _s2l_state.SCAN_PATHS:    flags.append(f"{Style.BRIGHT+Fore.RED}scan:{_s2l_state.SCAN_PATHS}{R}")
    if _s2l_state.CAPTURE:       flags.append(f"{Style.BRIGHT+Fore.RED}capture{R}")
    if _s2l_state.CAPTURE_WS:    flags.append(f"{Style.BRIGHT+Fore.RED}capture-ws{R}")
    if _s2l_state.HOOK_GUI:      flags.append(f"{Y}hook-gui{R}")
    if _s2l_state.WS_PING_INTERVAL > 0:  flags.append(f"{G}ws-keepalive:{_s2l_state.WS_PING_INTERVAL}s{R}")
    if _s2l_state.WS_DEFLATE:            flags.append(f"{G}ws-deflate{R}")
    if _s2l_state.WS_AUTO_RECONNECT:     flags.append(f"{G}ws-reconnect{R}")
    if _s2l_state.SSE_PROXY:             flags.append(f"{C}sse{R}")
    if _s2l_state.SSE_HEARTBEAT > 0:     flags.append(f"{C}sse-hb:{_s2l_state.SSE_HEARTBEAT}s{R}")
    if _s2l_state.STREAM_CACHE_MAX_BYTES > 0:
        flags.append(f"{C}stream-cache:{_s2l_state._fmt_size(_s2l_state.STREAM_CACHE_MAX_BYTES)}{R}")
    else:
        flags.append(f"{Y}stream-cache:off{R}")
    if _s2l_state._CURL_CFFI_OK:         flags.append(f"{C}h2{R}")
    if _s2l_state.TCP_TUNNEL:            flags.append(f"{M}tcp-tunnel{R}")
    if _s2l_state.UDP_TUNNEL:            flags.append(f"{M}udp-tunnel{R}")
    flag_str   = f" {DIM}·{R} ".join(flags) if flags else f"{DIM}none{R}"

    hook_str   = (f"{Y}{n_hooks} hook{'s' if n_hooks != 1 else ''}{R}"
                  if n_hooks else f"{DIM}none{R}")
    device_str = (f"{W}{_s2l_state.DEVICE}{R} {DIM}(auto-mirrors browser UA){R}"
                  if _s2l_state.DEVICE == "auto" else f"{W}{_s2l_state.DEVICE}{R}")

    bypass_eng = (f"{G}curl_cffi{R} {DIM}(TLS profile auto-matched to device){R}" if _s2l_state._CURL_CFFI_OK
                  else f"{Y}cloudscraper{R} {DIM}(pip install curl-cffi for better CF bypass){R}")

    # Pretty box banner with dynamic right border. Body lines contain ANSI
    # color codes (zero visual width) so we must measure VISIBLE width by
    # stripping ANSI before padding. Each body line is padded to _BOX_W and
    # closed with a right ║ so the box is always perfectly rectangular.
    # The box auto-expands if any line is wider than the default _BOX_W.
    _title = "S I T E  2  L O C A L"
    # Letter-spaced version from Config.VERSION (was a stale hardcoded
    # "V 8 . 1" two generations back).
    _ver   = "V " + _s2l_state.VERSION.replace(".", " . ")
    _title_core = f"  {_title}    {_ver}  "

    # Build body lines first so we can measure their visible widths.
    _l_target  = f"  {DIM}Target:  {R}  {G}{_s2l_state.MAIN_HOST}{R} {DIM}({ip}){R}"
    _l_proxy   = f"  {DIM}Proxy:   {R}  {W}http://{_s2l_state.HOST}:{_s2l_state.PORT}{R}"
    _l_device  = f"  {DIM}Device:  {R}  {device_str}"
    _waf_name  = _s2l_state.get_last_platform().get("name", "Unknown")
    _l_waf     = f"  {DIM}WAF:     {R}  {Y}[{_waf_name}]{R}"
    _l_bypass  = f"  {DIM}Bypass:  {R}  {bypass_eng}"
    _l_workers = f"  {DIM}Workers: {R}  {W}{_s2l_state.WORKERS}{R}"
    _l_timeout = f"  {DIM}Timeout: {R}  {W}connect={_s2l_state.TIMEOUT_CONN}s  read={_s2l_state.TIMEOUT_READ}s{R}"
    _l_retries = f"  {DIM}Retries: {R}  {W}{_s2l_state.RETRIES}× backoff={_s2l_state.BACKOFF}s{R}"
    _l_hooks   = f"  {DIM}Hooks:   {R}  {hook_str}"
    _l_flags   = f"  {DIM}Flags:   {R}  {flag_str}"
    _body_lines = [_l_target, _l_proxy, _l_device, _l_waf, _l_bypass, _l_workers,
                   _l_timeout, _l_retries, _l_hooks, _l_flags]

    # Box width: max of default (54), title core, and all body visible widths
    # plus 1 col of right padding so content never touches the right border.
    _BOX_W = max(54, len(_title_core),
                 max(len(_s2l_state._ANSI_ESC.sub("", ln)) for ln in _body_lines) + 1)

    def _pad_r(content: str) -> str:
        """Pad a content string (with ANSI codes) to _BOX_W visible cols."""
        vw = len(_s2l_state._ANSI_ESC.sub("", content))
        if vw >= _BOX_W:
            return content
        return content + " " * (_BOX_W - vw)

    # Center the title within _BOX_W.
    _pad_left = (_BOX_W - len(_title_core)) // 2
    _pad_right = _BOX_W - len(_title_core) - _pad_left
    _title_line = " " * _pad_left + _title_core + " " * _pad_right

    def _row(content: str) -> str:
        return f"{C}  ║{R}{_pad_r(content)}{C}║{R}"

    print(
        f"\n"
        f"{C}  ╔{'═' * _BOX_W}╗{R}\n"
        f"{C}  ║{W}{_title_line}{R}{C}║{R}\n"
        f"{C}  ╠{'═' * _BOX_W}╣{R}\n"
        + "\n".join(_row(ln) for ln in _body_lines) + "\n"
        f"{C}  ╚{'═' * _BOX_W}╝{R}"
    )

    if _s2l_state.PROXY_CDN and _s2l_state.MULTIPORT:
        print(f"  {M}▶ MULTIPORT{R} {DIM}CDN hosts get a dedicated port starting at {_s2l_state.PORT+1}{R}")
        if _s2l_state.WEB_CDNVIEWER:
            print(f"  {M}▶ MULTIPORT viewer{R} {DIM}open {C}http://localhost:{_s2l_state.PORT}/__s2l_viewer__{R} {DIM}(live CDN table, refreshes every 50 ms){R}")
        else:
            print(f"  {DIM}▶ MULTIPORT viewer{R} {DIM}disabled (WEB_CDNVIEWER=False){R}")
    elif _s2l_state.PROXY_CDN:
        print(f"  {M}▶ CDN{R} {DIM}assets via {_s2l_state._EXT_PREFIX}/ (single port){R}")
    if _s2l_state.CAPTURE:
        skip   = "static skipped" if _s2l_state.CAPTURE_SKIP_STATIC else "all captured"
        body   = "req body on" if _s2l_state.CAPTURE_BODIES else "req body off"
        others = " · CDN included" if _s2l_state.CAPTURE_CDN else ""
        print(f"  {Style.BRIGHT+Fore.RED}▶ CAPTURE{R} {DIM}({skip} · {body}{others}) → {_s2l_state.DATA_FOLDER}/captures/{R}")
    if _s2l_state.CAPTURE_WS:
        print(f"  {Style.BRIGHT+Fore.RED}▶ CAPTURE_WS{R} {DIM}every WebSocket message (text+binary, both directions) → {_s2l_state.DATA_FOLDER}/captures_ws/{R}")
    if _s2l_state.SHOW_HIDDEN:
        print(f"  {G}▶ SHOW_HIDDEN{R} {DIM}hidden elements revealed in every HTML page{R}")
    if _s2l_state.SCAN_PATHS:
        n_wl, n_paths = _s2l_state._scan_paths_summary()
        _rate_disp = (f"{_s2l_state.SCANS_PER_SECOND}/s" if _s2l_state.SCANS_PER_SECOND and _s2l_state.SCANS_PER_SECOND > 0
                      else "unlimited")
        _bar_disp = f"{G}tqdm{R}{DIM}" if _s2l_state._TQDM_OK else f"{DIM}log"
        print(f"  {Style.BRIGHT+Fore.RED}▶ SCAN_PATHS{R} {DIM}mode={_s2l_state.SCAN_PATHS} · {n_wl} wordlist(s) · "
              f"{n_paths} paths · rate={_rate_disp} · progress={_bar_disp} → "
              f"{_s2l_state.DATA_FOLDER}/hidden_paths.json{R}")
    if _s2l_state.HOOK_GUI:
        print(f"  {Y}▶ HOOK_GUI{R} {DIM}PyQt6 traffic inspector + live hook editor (Fusion theme){R}")
    if n_hooks:
        print(f"  {Y}▶ HOOKS{R}")
        for pat, mset, fn in _s2l_state._REQ_HOOKS:
            print(f"  {DIM}  req  [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")
        for pat, mset, fn in _s2l_state._RESP_HOOKS:
            print(f"  {DIM}  resp [{','.join(sorted(mset))}] {pat.pattern} → {fn.__name__}{R}")
