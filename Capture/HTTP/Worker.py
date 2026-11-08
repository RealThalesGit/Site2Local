"""Site2Local: HTTP capture records and writer worker."""
from __future__ import annotations

from urllib.parse import urlparse
import json
import os
import queue

from ... import Runtime as _s2l_state


def _capture_worker() -> None:
    while True:
        try:
            rec: _s2l_state._CaptureRecord = _s2l_state._capture_queue.get(timeout=1)
        except queue.Empty:
            continue
        # Everything below is wrapped as ONE unit: an exception in the
        # pre-encode steps (path/encoding) used to escape the per-item try
        # and kill this — the only consumer — silently (unbounded queue,
        # graceful-shutdown drain hangs). task_done() in finally keeps the
        # join() contract even on the worst failure.
        try:
            path = _s2l_state._capture_path(rec.method, rec.url, rec.query)
            rb_enc, rb_how = _s2l_state._encode_body(rec.req_body) if rec.req_body else ("", "none")
            pb_enc, pb_how = _s2l_state._encode_body(rec.resp_body)
            doc = {
                "s2l": "capture", "ts": rec.ts,
                "request":  {"method": rec.method, "url": rec.url, "query": rec.query,
                             "headers": rec.req_headers,
                             "body":    rb_enc if _s2l_state.CAPTURE_BODIES else None,
                             "encoding": rb_how if _s2l_state.CAPTURE_BODIES else "omitted"},
                "response": {"status": rec.resp_status, "ct": rec.resp_ct,
                             "headers": rec.resp_headers, "body": pb_enc,
                             "encoding": pb_how, "size": len(rec.resp_body)},
            }
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            _s2l_state.stats.inc("captured")
            rel = os.path.relpath(path, _s2l_state.DATA_FOLDER)
            _s2l_state.log(f"{rec.method} {_s2l_state._fmt_host(urlparse(rec.url).netloc)}{urlparse(rec.url).path or '/'} {rec.resp_status}"
                f" {_s2l_state._fmt_size(len(rec.resp_body))} → {rel}", "CAPTURE")
        except Exception as e:
            _s2l_state.log(f"Capture write error: {e}", "ERROR")
        finally:
            _s2l_state._capture_queue.task_done()


def _maybe_capture(ctx: _s2l_state.HookContext) -> None:
    if not _s2l_state.CAPTURE:
        return
    if _s2l_state.CAPTURE_SKIP_STATIC and _s2l_state.is_static_asset(ctx.resp_ct):
        return
    # Ensure body is not None
    body = ctx.resp_body if ctx.resp_body is not None else b""
    _s2l_state._capture_queue.put(_s2l_state._CaptureRecord(
        method=ctx.method, url=ctx.url, query=ctx.query,
        req_headers=ctx.req_headers, req_body=ctx.req_body,
        resp_status=ctx.resp_status, resp_headers=ctx.resp_headers,
        resp_body=body, resp_ct=ctx.resp_ct,
    ))
