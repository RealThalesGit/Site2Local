"""Site2Local: hook context, registry, GUI bridge and editor store."""
from __future__ import annotations

from urllib.parse import urlparse
import json
import re

from ... import Runtime as _s2l_state
from ...Rewriting.HTML.ServiceWorker import _strip_s2l_injector


def _origin_matches(hook_origin: str, ctx: _s2l_state.HookContext) -> bool:
    """Check if a hook's origin filter matches the request's actual origin.

    hook_origin semantics:
      - "" / "*" / "any"  → matches ANY origin (the target site AND all CDNs)
      - "target" / "main" → matches only MAIN_HOST (the target site)
      - a hostname        → matches if the request's netloc ends with it
                            (e.g. "cdn.example.com" matches that exact host;
                             "example.com" also matches "cdn.example.com")
    This lets users write hooks that target a specific CDN without affecting
    the main site, or vice-versa.
    """
    ho = (hook_origin or "").strip().lower()
    if ho in ("", "*", "any"):
        return True
    try:
        netloc = urlparse(ctx.url).netloc.lower()
    except Exception:
        netloc = ""
    if not netloc:
        return False
    # Strip port
    host = netloc.split(":", 1)[0]
    # Strip www.
    if host.startswith("www."):
        host = host[4:]
    if ho in ("target", "main"):
        return host == _s2l_state.MAIN_HOST.lower() or host == ("www." + _s2l_state.MAIN_HOST).lower()
    # Hostname suffix match: "example.com" matches "cdn.example.com"
    return host == ho or host.endswith("." + ho)


def _apply_gui_hooks(ctx: _s2l_state.HookContext) -> None:
    """Apply the first matching enabled GUI hook. Thread-safe."""
    if not _s2l_state.HOOK_GUI:
        return
    with _s2l_state._gui_hooks_lock:
        hooks = list(_s2l_state._gui_hooks)

    for h in hooks:
        if not h["enabled"]:
            continue
        # Method filter
        mf = h["method"].strip().upper()
        if mf not in ("", "*") and ctx.method.upper() != mf:
            continue
        # Status filter — hook fires only when upstream status matches
        _rs = h.get("status", 0)
        hook_status = 0 if _rs in (0,"*","any","") else int(_rs)
        if hook_status not in (0, ctx.resp_status):
            continue
        # Origin filter — match by domain (target site vs specific CDN)
        if not _s2l_state._origin_matches(h.get("origin_url", ""), ctx):
            continue
        # Path + query pattern (match against full path?query so hooks can target specific params)
        try:
            if not re.search(h["pattern"], ctx.path, re.IGNORECASE):
                continue
        except re.error:
            continue

        # Apply hook — replace body, auto-detect content-type
        new_body = h["body_bytes"]
        ctx.resp_body = new_body
        # Only override HTTP status when hook specifies a real numeric code (>0)
        _hs = h.get("status", 0)
        if isinstance(_hs, int) and _hs > 0:
            ctx.resp_status = _hs
        ctx.resp_headers["_hooked"] = "1"
        try:
            json.loads(new_body)
            ctx.resp_ct = "application/json; charset=utf-8"
        except Exception:
            head = new_body[:64].lstrip()
            if head.startswith(b"<"):
                ctx.resp_ct = "text/html; charset=utf-8"
        if _s2l_state._is_document_ct(ctx.resp_ct):
            # Hooks store editable site HTML, not the proxy's implementation.
            # Re-run the HTML pipeline because GUI hooks run after rewriting.
            new_body = _strip_s2l_injector(new_body)
            if _s2l_state.PROXY_CDN:
                new_body = _s2l_state._rewrite_ext_urls(new_body, html_doc=True)
            new_body = _s2l_state.rewrite_abs_urls(new_body)
            new_body = _s2l_state._inject_sw_clear(new_body)
            host = urlparse(ctx.url).netloc
            if host != _s2l_state.MAIN_HOST:
                new_body = _s2l_state.rewrite_doc_relative(new_body, host, html_doc=True)
            ctx.resp_body = new_body
        for key in list(ctx.resp_headers):
            if key.lower() == "content-length":
                del ctx.resp_headers[key]
        ctx.resp_headers["Content-Length"] = str(len(ctx.resp_body))
        _s2l_state.log(f"Hook '{h['name']}' matched {ctx.method} {ctx.path} "
            f"[{ctx.resp_status}] {_s2l_state._fmt_size(len(new_body))}", "HOOK")
        break
