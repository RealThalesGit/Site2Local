"""Site2Local: upstream HTTP session management."""
from __future__ import annotations

from flask import request as flask_request
import hashlib
import re
import requests
import requests.adapters
import time

from ... import Runtime as _s2l_state


def _sanitize_ua(ua: str) -> str:
    return re.sub(r"[\t\r\n]+", " ", ua).strip()


def _detect_device_from_ua(ua: str) -> str:
    ua = ua.lower()
    if "ipad"     in ua:                      return "ipad"
    if "iphone"   in ua or "ipod" in ua:      return "iphone"
    if "android"  in ua and "mobile" in ua:   return "mobile"
    if "android"  in ua:                      return "tablet"
    if "macintosh" in ua or "mac os x" in ua: return "macintosh"
    if "windows"  in ua:                      return "desktop"
    if "bot"      in ua or "crawl" in ua:     return "bot"
    return "desktop"


def _effective_device() -> str:
    if _s2l_state.DEVICE != "auto":
        return _s2l_state.DEVICE
    try:
        return _s2l_state._detect_device_from_ua(flask_request.headers.get("User-Agent", ""))
    except RuntimeError:
        return "macintosh"


def _device_variant(device: str) -> str:
    """Map a device class → cache-variant tag ('' = canonical/desktop).

    Sites that server-side sniff the User-Agent ship different files per
    device family — HTML documents, CSS/JS bundles, JSON APIs, media
    manifests, even image formats. The cache keeps ONE file per URL, so
    whichever device fetched first dictated the bytes for everyone (phone
    sees desktop, desktop sees phone). Variants split that single file
    into device families instead:
      • ""          — desktop-class (desktop, macintosh, bot): the CANONICAL
                      file, unchanged paths, and the home of the user's
                      hand edits (mods) — device fetches never overwrite it
      • "mobile"    — phones (mobile / iphone / symbian)
      • "tablet"    — tablets (ipad / android tablet)
    Phones and tablets are collapsed per family: UA-sniffed sites almost
    never differentiate deeper than this, and one file per family keeps
    fetch/disk duplication near zero for responsive (identical-HTML) sites.
    """
    if device in ("mobile", "iphone", "symbian"):
        return "mobile"
    if device in ("ipad", "tablet"):
        return "tablet"
    return ""
