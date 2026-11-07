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


def _norm_methods(methods) -> frozenset:
    if methods in ("*", None):
        return frozenset({"*"})
    if isinstance(methods, str):
        methods = [methods]
    return frozenset(m.upper() for m in methods)


def on_request(pattern: str = r".*", methods="*") -> Callable:
    pat  = re.compile(pattern, re.IGNORECASE)
    mset = _s2l_state._norm_methods(methods)
    def decorator(fn: Callable) -> Callable:
        _s2l_state._REQ_HOOKS.append((pat, mset, fn))
        _s2l_state.log(f"Req hook [{','.join(sorted(mset))}] {pattern} → {fn.__name__}()", "HOOK")
        return fn
    return decorator


def on_response(pattern: str = r".*", methods="*") -> Callable:
    pat  = re.compile(pattern, re.IGNORECASE)
    mset = _s2l_state._norm_methods(methods)
    def decorator(fn: Callable) -> Callable:
        _s2l_state._RESP_HOOKS.append((pat, mset, fn))
        _s2l_state.log(f"Resp hook [{','.join(sorted(mset))}] {pattern} → {fn.__name__}()", "HOOK")
        return fn
    return decorator


def _run_hooks(hooks: list, ctx: _s2l_state.HookContext) -> int:
    n = 0
    # Iterate over a COPY — a hook may register new hooks at runtime, which
    # would mutate the list mid-iteration (RuntimeError: list changed size
    # during iteration). The GUI hooks already do this; apply the same
    # protection here.
    for pat, mset, fn in list(hooks):
        if "*" not in mset and ctx.method not in mset:
            continue
        if not pat.search(ctx.path):
            continue
        try:
            fn(ctx)
            n += 1
        except Exception as exc:
            _s2l_state.log(f"Hook {fn.__name__} raised: {exc}", "ERROR")
    return n
