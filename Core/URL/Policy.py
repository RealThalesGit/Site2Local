"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations


from ... import Runtime as _s2l_state


def is_allowed_domain(netloc: str) -> bool:
    if not netloc:
        return False
    if netloc == _s2l_state.MAIN_HOST:
        return True
    if any(bad in netloc for bad in _s2l_state.CDN_BLOCK):
        return False
    return _s2l_state.DUMP_ALL and (not _s2l_state.DUMP_TARGETS or netloc in _s2l_state.DUMP_TARGETS)


def is_external_domain(netloc: str) -> bool:
    host = netloc.split(":")[0]
    return (bool(netloc)
            and netloc != _s2l_state.MAIN_HOST
            and host not in _s2l_state._LOCAL_HOSTS
            and not any(bad in netloc for bad in _s2l_state.CDN_BLOCK))
