"""Site2Local: HTML visibility, service-worker and URL rewriting."""
from __future__ import annotations


from ... import Runtime as _s2l_state


def _is_dont_rewrite_host(host: str) -> bool:
    """Return True if `host` matches any entry in DONT_REWRITE.

    Match is case-insensitive and treats DONT_REWRITE entries as either:
      • bare hostnames (suffix match — "hcaptcha.com" matches "api.hcaptcha.com")
      • host/path patterns ("google.com/recaptcha" matches only when the path
        under google.com starts with /recaptcha; matched by inspecting the
        full URL string in the caller).
    """
    if not host or not _s2l_state.DONT_REWRITE:
        return False
    h = host.lower().strip()
    # Strip port suffix for matching purposes.
    if h.startswith("["):
        # IPv6 literal
        idx = h.find("]")
        if idx != -1:
            h = h[:idx + 1]
    else:
        h = h.split(":", 1)[0]
    for entry in _s2l_state.DONT_REWRITE:
        e = entry.lower().strip()
        if "/" in e:
            eh, _ep = e.split("/", 1)
            if h == eh or h.endswith("." + eh):
                return True
        else:
            if h == e or h.endswith("." + e):
                return True
    return False
