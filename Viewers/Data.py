"""JSON snapshot consumed by the live CDN dashboard."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
import json
import time

from .. import Runtime as _s2l_state


def _s2l_viewer_data_endpoint() -> Response:
    """Snapshot of the live CDN host→port table + counts for the web viewer.

    Enriched with: detected upstream WAF/platform (from detect_platform),
    process uptime, capture-flag status, and a rolling-window request rate
    (events in the last STATS_WINDOW seconds) so the viewer can show a live
    throughput sparkline without keeping its own history.
    """
    if not _s2l_state.WEB_CDNVIEWER:
        return _s2l_state._viewer_disabled_response()
    if not _s2l_state._viewer_ip_allowed():
        return _s2l_state._viewer_ip_denied_response()
    if flask_request.method == "OPTIONS":
        r = Response(status=204)
        r.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
        return r

    with _s2l_state._cdn_port_lock:
        snap = sorted(((h, p) for h, p in _s2l_state._cdn_host_port.items() if p > 0),
                      key=lambda hp: hp[1])
        pending  = sum(1 for _, p in _s2l_state._cdn_host_port.items() if p < 0)
        unported = sum(1 for _, p in _s2l_state._cdn_host_port.items() if p == 0)
    s = _s2l_state.stats.snapshot()
    win = _s2l_state.stats.snapshot_window()
    uptime_s = int(time.time() - _s2l_state._START_TS)

    # Rolling-window totals (events in the last STATS_WINDOW seconds).
    win_total = sum(win.values())
    req_per_s = round(win_total / _s2l_state.STATS_WINDOW, 1) if _s2l_state.STATS_WINDOW > 0 else 0.0

    # Per-CDN-host health snapshot (last_seen, requests, errors, RTT, status).
    health = _s2l_state.snapshot_cdn_health()

    # Most recently detected upstream platform/WAF (Cloudflare, Akamai, ...).
    plat = _s2l_state.get_last_platform()
    plat_name = plat.get("name", "Unknown")
    plat_age = int(time.time() - plat.get("ts", 0)) if plat.get("ts") else 0

    payload = {
        "ts":          time.strftime("%H:%M:%S"),
        "epoch_ms":    int(time.time() * 1000),
        "refresh_ms":  _s2l_state._S2L_VIEWER_REFRESH_MS,
        "target":      _s2l_state.MAIN_HOST,
        "proxy":       f"http://{_s2l_state.HOST}:{_s2l_state.PORT}",
        "uptime_s":    uptime_s,
        # Client-aware URL: the viewer page may be opened from a phone/LAN
        # device — build each CDN URL on the host the browser actually used.
        "active":      [{"host": h, "port": p, "url": f"http://{_s2l_state._request_hostname()}:{p}",
                         "health": health.get(h)}
                        for h, p in snap],
        "pending":     pending,
        "shared":      unported,
        "total":       len(_s2l_state._cdn_host_port),
        "captured":    s.get("captured", 0),
        "ws_captured": s.get("ws_captured", 0),
        "proxied":     s.get("proxied", 0),
        "cdn_fetched": s.get("cdn_fetched", 0),
        # Rolling-window rate for the throughput sparkline.
        "req_per_s":   req_per_s,
        "win_events":  win_total,
        "win_window":  _s2l_state.STATS_WINDOW,
        "win_breakdown": {k: v for k, v in win.items()},
        # Most recently detected upstream platform/WAF, surfaced in the header.
        "platform":    {"name": plat_name, "age_s": plat_age},
        # Capture-flag status so the viewer can show what's being recorded.
        "flags": {
            "CAPTURE":           _s2l_state.CAPTURE,
            "CAPTURE_WS":        _s2l_state.CAPTURE_WS,
            "CAPTURE_CDN":       _s2l_state.CAPTURE_CDN,
            "CAPTURE_BODIES":    _s2l_state.CAPTURE_BODIES,
            "CAPTURE_SKIP_STATIC": _s2l_state.CAPTURE_SKIP_STATIC,
            "HOOK_GUI":          _s2l_state.HOOK_GUI,
            "MULTIPORT":         _s2l_state.MULTIPORT,
            "PROXY_CDN":         _s2l_state.PROXY_CDN,
            "CACHE_CDN":         _s2l_state.CACHE_CDN,
            "OFFLINE":           _s2l_state.OFFLINE,
            "PREMAP_CDN":        _s2l_state.PREMAP_CDN,
            "SSE_PROXY":         _s2l_state.SSE_PROXY,
            "TCP_TUNNEL":        _s2l_state.TCP_TUNNEL,
            "UDP_TUNNEL":        _s2l_state.UDP_TUNNEL,
            "WS_DEFLATE":        _s2l_state.WS_DEFLATE,
            "WS_AUTO_RECONNECT": _s2l_state.WS_AUTO_RECONNECT,
            # Web-UI master switch (lets the viewer be turned off server-side).
            "WEB_CDNVIEWER":     _s2l_state.WEB_CDNVIEWER,
        },
    }
    resp = Response(json.dumps(payload), content_type="application/json")
    resp.headers["Cache-Control"] = "no-store"
    resp.headers.update(_s2l_state._viewer_cors_headers(flask_request.headers.get("Origin", "")))
    return resp


