"""Site2Local: streamed and SSE responses."""
from __future__ import annotations

from flask import Response
from flask import stream_with_context
from urllib.parse import urlparse
import queue
import threading
import time
import zlib

from ... import Runtime as _s2l_state


def _should_stream(ct: str, cl: int = 0) -> bool:
    """Return True if this response should be streamed without full buffering.

    Dynamic detection: any video/audio/wasm/archive Content-Type streams,
    as does any response larger than _STREAM_MIN_BYTES (5 MB) regardless of
    Content-Type. application/octet-stream only streams when actually
    large — many APIs use it for small JSON, which we want to buffer so
    URL/hook rewriting still applies.
    """
    base = _s2l_state._ct_base(ct)
    if base == "application/octet-stream":
        return bool(cl) and cl > _s2l_state._STREAM_MIN_BYTES
    if base.startswith(_s2l_state._STREAM_PREFIXES):
        return True
    if cl and cl > _s2l_state._STREAM_MIN_BYTES:
        return True
    return False


def _is_sse_response(ct: str) -> bool:
    """Check whether a Content-Type header indicates an SSE stream.

    Dynamic check: anything starting with text/event-stream or
    application/x-event-stream or application/stream+json. No hardcoded
    set — just a prefix match on the base content type.
    """
    if not ct:
        return False
    base = _s2l_state._ct_base(ct)
    return (base.startswith("text/event-stream")
            or base.startswith("application/x-event-stream")
            or base.startswith("application/stream+json"))
