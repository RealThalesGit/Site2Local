"""Site2Local: disk cache serving, ranges and validators."""
from __future__ import annotations

from flask import Response
from flask import request as flask_request
from urllib.parse import urlparse
import os
import re
import email.utils

from ... import Runtime as _s2l_state


def _parse_range_header(range_hdr: str, file_size: int) -> tuple[int, int] | None:
    """Parse a Range: bytes=X-Y header. Returns (start, end_inclusive) or None."""
    if not range_hdr or file_size <= 0:
        return None
    try:
        m = re.match(r"bytes=(\d*)-(\d*)", range_hdr.strip())
        if not m:
            return None
        start_s, end_s = m.group(1), m.group(2)
        if not start_s and not end_s:
            return None
        if not start_s:
            # Suffix range: bytes=-N → last N bytes
            n = int(end_s)
            if n <= 0:
                return None
            start = max(0, file_size - n)
            end = file_size - 1
        elif not end_s:
            start = int(start_s)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s)
        if start < 0 or start >= file_size or end < start:
            return None
        end = min(end, file_size - 1)
        return (start, end)
    except (ValueError, AttributeError):
        return None


def _serve_range_from_disk(lp: str, target: str, method: str, req_path: str) -> Response | None:
    """206 Partial Content served straight from a disk-cached file.

    Shared by the main proxy, external assets and CDN servers. Return None
    for missing/unsupported ranges, failed reads, or a stale If-Range
    validator so callers can serve the complete current representation.
    """
    rng = flask_request.headers.get("Range", "")
    if not rng:
        return None
    try:
        file_size = os.path.getsize(lp)
    except OSError:
        return None
    rng_parsed = _s2l_state._parse_range_header(rng, file_size)
    if rng_parsed is None:
        return None
    # Resolve the type from the file head, never from a mid-file slice.
    ct = _s2l_state._resolve_mime_with_sniff(lp)
    ct = _s2l_state._ensure_wasm_content_type(ct, lp)
    validators = _s2l_state._disk_validators(lp, ct)
    if_range = flask_request.headers.get("If-Range", "").strip()
    if if_range:
        if validators is None:
            return None
        etag, last_modified = validators
        if if_range.startswith(('"', 'W/')):
            # If-Range requires a strong comparison; weak tags never match.
            if if_range.startswith('W/') or if_range != etag:
                return None
        else:
            try:
                supplied = email.utils.parsedate_to_datetime(if_range)
                current = email.utils.parsedate_to_datetime(last_modified)
                if supplied.tzinfo is None or supplied != current:
                    return None
            except (ValueError, TypeError, OverflowError):
                return None
    start, end = rng_parsed
    try:
        with open(lp, "rb") as _rf:
            _rf.seek(start)
            _slice = _rf.read(end - start + 1)
    except OSError:
        return None
    # For no-extension cache files (the 09/10 "asset/?id=N" layout), the
    # path itself gives no MIME hint — sniff the leading bytes so a Range
    # request on a cached PNG/MP3/WASM asset gets the right Content-Type
    # and the browser can stream it correctly. ALWAYS sniff the file's real
    # head (a fresh 2 KB read) instead of the requested slice: the 200 and
    # the 206 of the same file must carry the SAME Content-Type, and a
    # short mid-file slice would starve the sniffer of evidence.
    resp = Response(_slice, status=206, content_type=ct)
    if validators:
        resp.headers["ETag"], resp.headers["Last-Modified"] = validators
    resp.headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
    resp.headers["Accept-Ranges"] = "bytes"
    resp.headers["Content-Length"] = str(len(_slice))
    _s2l_state._apply_cors_headers(resp.headers)
    _s2l_state._apply_isolation_headers(resp.headers,
                             is_worker_script=_s2l_state._is_worker_request())
    _s2l_state._apply_security_headers(resp.headers)
    _s2l_state.log(f"{method} HIT {_s2l_state._fmt_host(urlparse(target).netloc)}{_s2l_state._fmt_path_qs(req_path)} 206 [{_s2l_state._fmt_size(len(_slice))}]", "←")
    _s2l_state._gui_push_raw(method, req_path, 206, ct, _slice,
                  origin=_s2l_state.MAIN_HOST,
                  _skip_log=True)   # caller already logged
    return resp
