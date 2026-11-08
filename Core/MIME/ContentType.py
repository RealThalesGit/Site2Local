"""Site2Local: MIME type detection and asset classification."""
from __future__ import annotations




def _ensure_wasm_content_type(ct: str, path: str) -> str:
    """Override Content-Type to application/wasm for WASM resources.

    WebAssembly.instantiateStreaming() REQUIRES the response to have
    Content-Type: application/wasm — if the server sends application/octet-
    stream (or anything else), streaming compilation fails with a TypeError
    that manifests as "Failed to fetch" or "Decompression failed" in the
    client app. Many CDNs serve .wasm files as application/octet-stream
    because their MIME config predates the WASM spec; WASM-threaded games
    and apps hit this on every launch.

    We override when:
      • The path ends in .wasm (canonical case), OR
      • The path contains /wasm/ (generic CDN directory pattern for WASM
        bundles that have no file extension), OR
      • The upstream Content-Type is application/wasm already (passthrough)

    For any other path, we return the upstream Content-Type unchanged.
    """
    if not ct:
        ct = "application/octet-stream"
    ct_base = ct.split(";")[0].strip().lower()
    if ct_base == "application/wasm":
        return ct  # already correct
    p = (path or "").lower()
    if p.endswith(".wasm") or "/wasm/" in p:
        return "application/wasm"
    return ct


def _ct_base(ct: str) -> str:
    return ct.split(";")[0].strip().lower()
