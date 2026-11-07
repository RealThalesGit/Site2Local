"""Site2Local: HTML visibility, service-worker and URL rewriting."""
from __future__ import annotations

from bs4 import BeautifulSoup
from urllib.parse import urlparse
import json
import re

from ... import Runtime as _s2l_state


def _reveal_hidden(html_bytes: bytes) -> bytes:
    try:
        soup = BeautifulSoup(html_bytes.decode("utf-8", "ignore"), "lxml")
        style_tag = soup.new_tag("style")
        style_tag.string = _s2l_state._REVEAL_STYLE
        head = soup.find("head")
        (head or soup).insert(0, style_tag)
        for sel in _s2l_state._HIDDEN_SELECTORS:
            for el in soup.select(sel):
                style = el.attrs.get("style", "")
                style = re.sub(r"display\s*:\s*none\s*;?",      "display:block;",      style, flags=re.I)
                style = re.sub(r"visibility\s*:\s*hidden\s*;?", "visibility:visible;", style, flags=re.I)
                style = re.sub(r"opacity\s*:\s*0\s*;?",         "opacity:1;",          style, flags=re.I)
                if style.strip():
                    el.attrs["style"] = style
                for attr in ("hidden", "disabled", "readonly"):
                    el.attrs.pop(attr, None)
        return soup.encode("utf-8")
    except Exception as e:
        _s2l_state.log(f"reveal_hidden() failed: {e}", "WARN")
        return html_bytes
