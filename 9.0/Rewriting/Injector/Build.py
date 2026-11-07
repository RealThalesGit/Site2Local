"""Browser-side S2L runtime injector — Python side (state → JS values)."""
from __future__ import annotations

import json
import re

from ... import Runtime as _s2l_state
from .Template import _injector_js


def _build_s2l_injector() -> bytes:
    with _s2l_state._cdn_port_lock:
        # p > 0 — SAME filter as the /__s2l_hp endpoint. Never bake the -1
        # "mini-server still binding" sentinel (or the 0 "no dedicated port"
        # marker) into the page: -1 is truthy in JS, so rw() would emit
        # http://localhost:-1/... for that host, and the live HP poll only
        # adds MISSING hosts — it never repairs an existing value — so the
        # page would keep the broken port until a manual reload. A filtered
        # host falls through rw() to the /__s2l_ext__/ route on the main
        # port (which works — it's what non-MULTIPORT mode always does)
        # until the poll (first tick 1.5s) brings the real port.
        hp = {h: p for h, p in _s2l_state._cdn_host_port.items() if p > 0}
    # DONT_REWRITE — host patterns whose URLs must go DIRECT (never rewritten
    # to localhost). Compiled into one regex for the JS side so rw() can
    # short-circuit cheaply. Captcha backends (hcaptcha.com, recaptcha.net,
    # ...) reject requests whose Origin is "localhost", so their assets and
    # scripts must NOT be tunneled through S2L.
    _dr_patterns = []
    for _entry in _s2l_state.DONT_REWRITE:
        _e = _entry.strip().lower()
        if not _e:
            continue
        if "/" in _e:
            _h, _p = _e.split("/", 1)
            _dr_patterns.append(
                r'^(?:[^/]*\.)?' + re.escape(_h) + r'/' + re.escape(_p)
            )
        else:
            _dr_patterns.append(
                r'^(?:[^/]*\.)?' + re.escape(_e) + r'(?:[:/]|$)'
            )
    _dr_regex_js = json.dumps("|".join(_dr_patterns) if _dr_patterns else "")
    return _injector_js(hp, _dr_regex_js).encode("utf-8")
