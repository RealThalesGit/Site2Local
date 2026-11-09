"""Site2Local: MIME type detection and asset classification."""
from __future__ import annotations


from ... import Runtime as _s2l_state


def is_static_asset(ct: str) -> bool:
    """Dynamic check: is this a static asset (CSS/JS/font/image/audio/video/wasm)?

    Uses the upstream-supplied content-type and falls back to extension
    guessing when the ct is generic. Kept for CAPTURE_SKIP_STATIC.
    """
    base = _s2l_state._ct_base(ct)
    if not base or base == "application/octet-stream":
        return False
    return (base.startswith("text/css")
            or base.startswith(("application/javascript", "text/javascript",
                                "application/x-javascript", "application/wasm"))
            or base.startswith("font/")
            or base.startswith(("application/font-woff", "application/font-woff2"))
            or base.startswith("image/")
            or base.startswith(("audio/", "video/")))
