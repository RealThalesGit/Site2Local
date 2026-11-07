"""Site2Local: case-insensitive header container helpers."""
from __future__ import annotations


def _ci_header_keys(out) -> set:
    """Return a set of lower-cased header key names from a plain dict OR a
    werkzeug Headers object.

    This is the CRITICAL compatibility shim: `for k in out` on a plain dict
    yields key strings, but on a werkzeug Headers object `__iter__` yields
    (key, value) TUPLES in many Werkzeug versions — so `{k.lower() for k in
    out}` crashes with `AttributeError: 'tuple' object has no attribute
    'lower'` when `out` is `resp.headers`. Using `.keys()` instead of bare
    `__iter__` is version-stable: both `dict.keys()` and `Headers.keys()`
    yield key strings.
    """
    try:
        return {k.lower() for k in out.keys()}
    except (AttributeError, TypeError):
        # Last-resort fallback for unusual containers / lists-of-tuples.
        return {(k[0] if isinstance(k, tuple) else k).lower() for k in out}


def _ci_get_header(out, name: str, default: str = "") -> str:
    """Case-insensitive header value lookup for a plain dict OR werkzeug
    Headers object.

    werkzeug's Headers.get() is already case-insensitive, but dict.get() is
    NOT — so a dict carrying "vary" (lowercase, as curl_cffi sometimes
    produces) would be missed by `out.get("Vary")`. This helper scans keys
    case-insensitively on both container types.
    """
    _name_lower = name.lower()
    try:
        for _k in out.keys():
            if _k.lower() == _name_lower:
                # out[_k] works on both dict (exact key match) and Headers
                # (case-insensitive). Use .get() to avoid KeyError on edge cases.
                _val = out.get(_k, "")
                return _val if _val is not None else default
    except (AttributeError, TypeError):
        pass
    return default


def _ci_remove_header(out, name: str) -> None:
    """Remove a header case-insensitively from a plain dict OR werkzeug
    Headers object. On a dict this removes the differently-cased key so a
    subsequent `out["Name"] = value` doesn't leave a stale duplicate. On
    werkzeug Headers, `del out[name]` is already case-insensitive.
    """
    if isinstance(out, dict):
        _name_lower = name.lower()
        for _k in list(out.keys()):
            if _k.lower() == _name_lower:
                del out[_k]
    else:
        try:
            del out[name]
        except (KeyError, TypeError):
            pass
