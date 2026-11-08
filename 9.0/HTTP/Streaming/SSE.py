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


def _sse_iter(upstream_r, chunk_size: int = 4096):
    """Yield raw bytes from an upstream response, regardless of whether
    it was fetched with stream=True or stream=False.

    When stream=True was used, response.iter_content() is the natural API
    and yields bytes lazily as they arrive on the wire.

    When stream=False was used (curl_cffi already loaded response.content
    into memory), iter_content() raises a "stream mode is not enabled"
    error on the first call. Detect that and fall back to yielding
    response.content as a single chunk so the SSE reassembler still
    gets the data instead of emitting a noisy WARN on every request.
    """
    try:
        # Calling iter_content() returns an iterator without raising —
        # the error is raised lazily on the first __next__. So we must
        # actually iterate once inside the try block to catch it.
        _it = upstream_r.iter_content(chunk_size=chunk_size, decode_unicode=False)
        for chunk in _it:
            yield chunk
        return
    except Exception as exc:
        _msg = str(exc).lower()
        # curl_cffi raises roughly: "stream mode is not enabled"
        # requests/urllib3 raise: "stream mode not enabled" or similar.
        # Match loosely so we don't swallow unrelated errors silently.
        if ("stream" in _msg and "not" in _msg
                and ("enabl" in _msg or "open" in _msg)):
            # Fall back to the already-buffered body. If that's also
            # empty, just stop — nothing else we can do.
            body = b""
            try:
                body = upstream_r.content or b""
            except Exception:
                body = b""
            if body:
                yield body
            return
        # Unrelated error — re-raise so the caller can log it.
        raise


def _sse_stream_generator(upstream_r, target: str):
    """Yield complete SSE events from upstream_r, reassembling chunks that
    cross event boundaries.

    SSE framing: events are separated by a blank line ("\n\n"). Each event
    is one or more lines of "field: value". We buffer until we see "\n\n"
    then yield the whole event as one chunk.
    """
    buf = b""
    try:
        for chunk in _s2l_state._sse_iter(upstream_r, chunk_size=4096):
            if not chunk:
                continue
            buf += chunk
            # Split on event boundaries. Per the SSE spec, an event is delimited
            # by a blank line. Servers may use either "\n\n" (LF) or "\r\n\r\n"
            # (CRLF) as the line ending — handle both so CRLF-delimited events
            # don't buffer until the 65KB flush.
            while b"\n\n" in buf or b"\r\n\r\n" in buf:
                if b"\r\n\r\n" in buf and (b"\n\n" not in buf or buf.find(b"\r\n\r\n") < buf.find(b"\n\n")):
                    event, buf = buf.split(b"\r\n\r\n", 1)
                    yield event + b"\r\n\r\n"
                else:
                    event, buf = buf.split(b"\n\n", 1)
                    yield event + b"\n\n"
            # Yield any partial final event if buffer grows large (10KB+)
            # to avoid unbounded memory on a stream that never sends \n\n.
            if len(buf) > 65536:
                yield buf
                buf = b""
    except Exception as exc:
        _s2l_state.log(f"SSE upstream stream error {urlparse(target).netloc}: {_s2l_state._short_exc(exc)}", "WARN")
        return
    # Flush any trailing partial event
    if buf:
        yield buf


def _stream_sse_resp(upstream_r, method: str, target: str) -> Response:
    """Build a Flask Response that streams SSE with proper buffering."""
    def _keepalive_injected():
        """Forward SSE events from upstream, injecting keepalive comments
        when the stream goes quiet for more than SSE_HEARTBEAT seconds.

        Uses a background thread to monitor idle time and inject keepalives
        (": ping\\n\\n" is a valid SSE comment per the spec — browsers ignore
        it but it keeps the connection alive through intermediate proxies/CDNs
        that would otherwise close idle connections).
        """
        try:
            if _s2l_state.SSE_HEARTBEAT <= 0:
                # Heartbeat disabled — just forward chunks unchanged.
                yield from _s2l_state._sse_stream_generator(upstream_r, target)
                return

            buf_q: queue.Queue = queue.Queue(maxsize=256)
            upstream_done = threading.Event()
            last_event_time = [time.time()]

            def _reader():
                try:
                    for chunk in _s2l_state._sse_stream_generator(upstream_r, target):
                        last_event_time[0] = time.time()
                        # Bounded put: if the consumer generator is abandoned
                        # (browser disconnected → GeneratorExit), a plain put()
                        # would block FOREVER here, leaking this thread and the
                        # upstream connection. Time out and re-check whether
                        # the consumer is still among us.
                        while True:
                            try:
                                buf_q.put(chunk, timeout=5.0)
                                break
                            except queue.Full:
                                if upstream_done.is_set():
                                    return   # consumer gone — stop feeding
                                continue
                except Exception as e:
                    try: buf_q.put_nowait(e)
                    except queue.Full: pass
                finally:
                    upstream_done.set()

            threading.Thread(target=_reader, daemon=True, name="sse-reader").start()

            while True:
                try:
                    item = buf_q.get(timeout=1.0)
                except queue.Empty:
                    # No event for 1s — check if we need to inject a keepalive.
                    if upstream_done.is_set() and buf_q.empty():
                        break
                    if time.time() - last_event_time[0] >= _s2l_state.SSE_HEARTBEAT:
                        yield b": ping\n\n"
                        last_event_time[0] = time.time()
                    continue
                if isinstance(item, Exception):
                    _s2l_state.log(f"SSE keepalive stream error: {_s2l_state._short_exc(item)}", "WARN")
                    break
                yield item
                if upstream_done.is_set() and buf_q.empty():
                    break
        finally:
            # Client disconnected (GeneratorExit) or stream ended — release the
            # upstream connection so curl_cffi doesn't leak the socket.
            try: upstream_r.close()
            except Exception: pass

    headers = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(upstream_r))
    # Force chunked: Werkzeug doesn't auto-chunk generators with no Content-Length
    headers.pop("content-length", None)
    headers["Cache-Control"] = "no-cache, no-transform"
    headers["X-Accel-Buffering"] = "no"   # disable nginx buffering if behind one
    ct = upstream_r.headers.get("Content-Type", "text/event-stream")
    _s2l_state.log_req(method, 200, urlparse(target).netloc, urlparse(target).path, -1,
                       tag="SSE", query=urlparse(target).query)
    return Response(
        _keepalive_injected(),
        status=upstream_r.status_code,
        headers=headers,
        content_type=ct,
    )
