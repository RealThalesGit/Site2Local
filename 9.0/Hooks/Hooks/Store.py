"""Site2Local: hook context, registry, GUI bridge and editor store."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from urllib.parse import urlparse
import base64
import json
import os
import queue
import re
import tempfile
import time

from ... import Runtime as _s2l_state


def _make_hook_store(subdir: str, extra_binary_fields: tuple = ()):
    """Factory for a hook-list disk-persistence helper.

    Shared by every hook editor (response hooks, request-body-override hooks,
    and the WebSocket hooks) so the save/delete/load logic exists in exactly
    one place instead of being copy-pasted per tab.

    Storage layout: body is always stored as `{name}.body`, metadata as
    `{name}.meta.json`. Deletion constructs the exact filenames — no
    directory scan, no prefix match (which used to delete unrelated hooks
    sharing a name prefix). Writes are tmpfile+os.replace (atomic on POSIX),
    so a crash mid-save can never leave a half-written body/meta pair.

    `extra_binary_fields` lists hook dict keys OTHER than `body_bytes` that
    also hold raw bytes (e.g. the WS hook's `match_payload`). They're
    base64-encoded into the JSON meta on save and decoded back on load —
    JSON can't serialize bytes natively, and without this the WS hook save
    would silently fail every time `match_payload` was set.
    """
    hooks_dir = os.path.join("site_data", subdir, _s2l_state.MAIN_HOST)
    os.makedirs(hooks_dir, exist_ok=True)
    _extra_bin = tuple(extra_binary_fields)

    def _paths(name: str) -> tuple[str, str]:
        safe = re.sub(r"[^\w\-.]", "_", name)
        p = os.path.join(hooks_dir, f"{safe}.body")
        return p, p + ".meta.json"

    def _atomic_write(target: str, data: bytes) -> None:
        fd, tmp = tempfile.mkstemp(dir=hooks_dir, prefix=".tmp-")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, target)
        except Exception:
            try: os.remove(tmp)
            except OSError: pass
            raise

    def save(hook: dict) -> bool:
        try:
            p, meta_path = _paths(hook["name"])
            meta = {k: v for k, v in hook.items() if k != "body_bytes"}
            # Base64-encode any extra binary fields so they survive JSON
            # serialization. Stored under "<field>_b64" so the loader knows
            # exactly which keys to decode back to bytes.
            for f in _extra_bin:
                v = meta.get(f)
                if isinstance(v, (bytes, bytearray)):
                    meta[f + "_b64"] = base64.b64encode(bytes(v)).decode("ascii")
                    del meta[f]
            _atomic_write(p, hook["body_bytes"])
            _atomic_write(meta_path, json.dumps(meta, indent=2).encode("utf-8"))
            return True
        except Exception as e:
            _s2l_state.log(f"Hook disk save failed ({subdir}): {e}", "ERROR")
            return False

    def delete(name: str) -> None:
        for fp in _paths(name):
            try:
                os.remove(fp)
            except OSError:
                pass

    def load_all(defaults: dict) -> list:
        loaded = []
        try:
            for fn in sorted(os.listdir(hooks_dir)):
                if not fn.endswith(".body.meta.json"):
                    continue
                meta_path = os.path.join(hooks_dir, fn)
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    body_file = meta_path[:-len(".meta.json")]
                    if not os.path.isfile(body_file):
                        continue
                    with open(body_file, "rb") as f:
                        body_bytes = f.read()
                    # Decode any extra binary fields back from base64.
                    for f in _extra_bin:
                        b64_key = f + "_b64"
                        if b64_key in meta:
                            try:
                                meta[f] = base64.b64decode(meta.pop(b64_key))
                            except Exception:
                                meta[f] = b""
                    loaded.append({**defaults, **meta, "body_bytes": body_bytes})
                except Exception as e:
                    _s2l_state.log(f"Hook load error {fn} ({subdir}): {e}", "WARN")
        except Exception as e:
            _s2l_state.log(f"Hook store scan failed ({subdir}): {e}", "ERROR")
        return loaded

    return save, delete, load_all


def _editor_preview(body: bytes, ct: str = "") -> tuple[str, bool]:
    """Return (text, truncated) for the Body Editor.

    Decodes up to _EDITOR_MAX_BYTES of `body` as UTF-8 (errors replaced) and
    appends a visible truncation notice when the body was larger than the cap.
    JSON bodies are pretty-printed first (from the capped slice) so the editor
    stays readable for typical API responses.
    """
    if not body:
        return "", False
    truncated = len(body) > _s2l_state._EDITOR_MAX_BYTES
    if truncated:
        slice_b = body[:_s2l_state._EDITOR_MAX_BYTES]
    else:
        slice_b = body
    try:
        txt = slice_b.decode("utf-8", errors="replace")
    except Exception:
        txt = slice_b.hex()
    if "json" in (ct or "").lower():
        try:
            txt = json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
        except Exception:
            pass
    if truncated:
        txt += (
            f"\n\n…[truncated — editor cap {_s2l_state._fmt_size(_s2l_state._EDITOR_MAX_BYTES)} reached; "
            f"full body { _s2l_state._fmt_size(len(body)) } available in capture files]…"
        )
    return txt, truncated
