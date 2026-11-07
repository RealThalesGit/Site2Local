"""Site2Local: WebSocket capture records and writer worker."""
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


def _ws_capture_worker() -> None:
    while True:
        try:
            rec: _s2l_state._WSCaptureRecord = _s2l_state._ws_capture_queue.get(timeout=1)
        except queue.Empty:
            continue
        try:
            out_dir = _s2l_state._ws_capture_dir(rec.ws_url, rec.tunnel_id)
            os.makedirs(out_dir, exist_ok=True)
            body_enc, how = _s2l_state._encode_body(rec.payload)
            doc = {
                "s2l": "ws_capture", "ts": rec.ts,
                "direction": rec.direction,
                "opcode": rec.opcode, "op_name": rec.op_name,
                "ws_url": rec.ws_url, "tunnel_id": rec.tunnel_id,
                "payload": body_enc, "encoding": how, "size": len(rec.payload),
            }
            # Legible filename: in.text.20261003-143012-456.t3.json — the old
            # scheme (in_1_1699999999999_3.json) encoded the opcode as a bare
            # number and the epoch made sorting/browsing by time pointless.
            fn = (f"{rec.direction}.{rec.op_name}."
                  f"{time.strftime('%Y%m%d-%H%M%S')}-{int(time.time() * 1000) % 1000:03d}"
                  f".t{rec.tunnel_id}.json")
            path = os.path.join(out_dir, fn)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            _s2l_state.stats.inc("ws_captured")
        except Exception as e:
            _s2l_state.log(f"WS capture write error: {e}", "ERROR")
        _s2l_state._ws_capture_queue.task_done()


def _maybe_capture_ws(direction: str, opcode: int, payload: bytes,
                      ws_url: str, tunnel_id: int) -> None:
    if not _s2l_state.CAPTURE_WS:
        return
    if not payload and opcode not in (8, 9, 10):
        return
    try:
        _s2l_state._ws_capture_queue.put_nowait(_s2l_state._WSCaptureRecord(
            direction=direction, opcode=opcode,
            op_name=_s2l_state._WS_OP_NAMES.get(opcode, f"?{opcode}"),
            ws_url=ws_url, tunnel_id=tunnel_id, payload=bytes(payload),
        ))
    except queue.Full:
        pass
