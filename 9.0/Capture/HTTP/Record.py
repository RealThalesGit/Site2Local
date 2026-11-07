"""Site2Local: HTTP capture records and writer worker."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from urllib.parse import urlparse
import base64
import hashlib
import json
import os
import queue
import re
import time

from ... import Runtime as _s2l_state


@dataclass
class _CaptureRecord:
    method:       str
    url:          str
    query:        str
    req_headers:  dict
    req_body:     bytes
    resp_status:  int
    resp_headers: dict
    resp_body:    bytes
    resp_ct:      str
    ts:           str = field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))


def _capture_path(method: str, url: str, qs: str) -> str:
    """Build a HUMAN-READABLE capture file path.

    Layout: captures/<host>/<parent-dirs>/METHOD.path.slug.YYYYmmdd-HHMMSS-mmm[-q<hash>].json
    e.g.    captures/api.example.com/games/save/POST.games.save.20261003-143012-456.json

    The old scheme named files "METHOD_<ms-epoch>.json" (e.g.
    POST_1699999999999.json) — opaque when browsing the captures folder. The
    last two path segments now identify the endpoint in the FILENAME itself;
    the full URL and query still live inside the JSON document, and the short
    qs hash only keeps different query variants from colliding.
    """
    parsed = urlparse(url)
    host = re.sub(r"[^\w.\-]", "_",
                  parsed.netloc.split("@")[-1].split(":")[0]) or "_nohost_"
    segs = [re.sub(r"[^\w.\-]", "_", s) for s in parsed.path.strip("/").split("/") if s]
    # Last 2 segments identify the endpoint (they go into the filename); the
    # remaining ones become directory levels.
    tail = segs[-2:] if len(segs) >= 2 else (segs or ["_root_"])
    dirs = segs[:-2] if len(segs) >= 2 else []
    slug = ".".join(t for t in tail if t)[:80] or "_root_"
    ts = time.strftime("%Y%m%d-%H%M%S") + f"-{int(time.time() * 1000) % 1000:03d}"
    qtag = f"-q{hashlib.sha1(qs.encode()).hexdigest()[:6]}" if qs else ""
    base = os.path.join(_s2l_state.DATA_FOLDER, "captures", host, *dirs)
    path = os.path.join(base, f"{method}.{slug}.{ts}{qtag}.json")
    # The capture queue has a single writer thread — the existence loop below
    # is race-free and keeps same-millisecond captures of the same endpoint
    # from overwriting each other.
    final, n = path, 1
    while os.path.exists(final):
        n += 1
        final = f"{path[:-5]}-{n}.json"
    return final


def _encode_body(data: bytes) -> tuple[str, str]:
    try:
        return data.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(data).decode(), "base64"
