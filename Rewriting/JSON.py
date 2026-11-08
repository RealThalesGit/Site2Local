"""JSON and webmanifest URL rewriting."""
from __future__ import annotations

import re

from .. import Runtime as _s2l_state


def _rewrite_json_urls(data: bytes) -> bytes:
    """Rewrite absolute https?://host/path URLs embedded as JSON string values.

    Web App Manifests (.webmanifest) and a great many first-party API
    responses embed absolute asset URLs in JSON — icon lists, level data
    referencing textures, CMS responses with image fields. Unlike HTML/CSS,
    nothing about JSON content ever went through _rewrite_ext_urls (it's
    gated on text/html and text/css content types), so any such URL silently
    pointed straight at the real internet — invisible to JS-level interception
    too, since plenty of this is consumed by the BROWSER's own native code
    (e.g. PWA manifest icon prefetch for installability/"Add to Home Screen"),
    which page JavaScript never gets a chance to touch at all.
    """
    if not _s2l_state.PROXY_CDN or not data:
        return data
    try:
        text = data.decode("utf-8", "ignore")
    except Exception:
        return data

    def _rep(m: re.Match) -> str:
        host, tail = m.group(1), m.group(2) or ""
        r = _s2l_state._proxy_target(host, tail)
        return f'"{r}"' if r else m.group(0)

    text = re.sub(
        r'"https?://([a-zA-Z0-9\-._]+(?::\d+)?)((?:/[^"\\<>]*)?)"',
        _rep, text, flags=re.IGNORECASE)
    return text.encode("utf-8")


