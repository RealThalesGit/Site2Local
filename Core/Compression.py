"""Site2Local: compression."""
from __future__ import annotations

import zlib

from .. import Runtime as _s2l_state


def decompress_body(data: bytes, encoding: str) -> bytes:
    """Decompress an HTTP response body.

    curl_cffi with impersonate=True auto-decompresses internally but keeps the
    original Content-Encoding header in the response. We use magic-byte checks
    to detect whether data is still compressed or already plain text, preventing
    double-decompression bugs (which silently return garbled/empty data).

    Stacked encodings like "gzip, br" are handled by splitting on comma.
    """
    if not data:
        return data
    enc = (encoding or "").lower().strip()
    return _s2l_state._decompress_body_impl(data, enc)


def _decompress_body_impl(data: bytes, enc: str) -> bytes:
    """Guts of decompress_body — pure decode logic, no logging.

    `enc` is the lower-cased, stripped Content-Encoding value ("" or
    "identity" for plain, "gzip"/"deflate"/"br"/"zstd" or a comma-separated
    stack like "gzip, br"). Stacked encodings decode right-to-left.
    """
    # Stacked encodings: "gzip, br" → decode br first, then gzip
    if enc and "," in enc:
        parts = [e.strip() for e in enc.split(",") if e.strip()]
        for part in reversed(parts):
            data = _s2l_state.decompress_body(data, part)
        return data

    # Magic-byte helpers
    def _looks_gzip(d: bytes) -> bool:
        return len(d) >= 2 and d[:2] == b"\x1f\x8b"

    def _looks_zlib(d: bytes) -> bool:
        return len(d) >= 2 and d[0] == 0x78 and d[1] in (0x01, 0x9c, 0xda)

    def _looks_json_or_text(d: bytes) -> bool:
        """True if data looks like already-decoded UTF-8 text (JSON, HTML, etc).
        More reliable than byte-frequency heuristics:
        - starts with '{', '[', '<', or whitespace (JSON/HTML/XML)
        - OR starts with printable ASCII and has no null bytes in first 32 bytes
        This avoids false-positives with brotli/gzip compressed data that
        happen to have many high-bit bytes (>= 0x80).
        """
        if not d:
            return True
        # Fast path: common text starters
        if d[0:1] in (b'{', b'[', b'<', b'"', b' ', b'\n', b'\r', b'\t'):
            return True
        # Null bytes never appear in valid UTF-8 text responses
        if b'\x00' in d[:64]:
            return False
        # gzip/zlib magic = definitely NOT decoded yet
        if _looks_gzip(d) or _looks_zlib(d):
            return False
        # Brotli has no universal magic header, but brotli-compressed data
        # never starts with ASCII printable bytes (it starts with a bit-stream
        # header). If first byte is printable ASCII (0x20-0x7E), likely decoded.
        return 0x20 <= d[0] <= 0x7E

    try:
        if enc in ("", "identity"):
            # No compression declared — only decompress if gzip magic bytes present.
            # Do NOT try brotli speculatively: brotli has no magic and would corrupt
            # plain JSON bodies from curl_cffi that already auto-decoded.
            if _looks_gzip(data):
                try:
                    result = zlib.decompress(data, zlib.MAX_WBITS | 16)
                    if result:
                        return result
                except Exception:
                    pass
            return data

        if enc == "gzip":
            if not _looks_gzip(data):
                return data   # curl_cffi already decoded — body is plain text
            # MULTI-MEMBER FIX: concatenated gzip streams (legal per RFC 1952,
            # produced by streaming servers) were silently TRUNCATED to the
            # first member by the one-shot decompress — the tail of the asset
            # vanished without any error. Walk members via unused_data.
            # BOMB GUARD (round 2): bounded output — a small gzip expanding
            # past MAX_DECOMPRESSED_BYTES aborts instead of OOM-ing the proxy.
            try:
                out = bytearray()
                d = zlib.decompressobj(zlib.MAX_WBITS | 16)
                rest = data
                while rest:
                    _feed = rest
                    while _feed:
                        out += d.decompress(_feed, 64 * 1024 * 1024)
                        if len(out) > _s2l_state.MAX_DECOMPRESSED_BYTES:
                            raise MemoryError("gzip output exceeds decompression cap (bomb?)")
                        _feed = d.unconsumed_tail
                    rest = d.unused_data
                    if rest:
                        d = zlib.decompressobj(zlib.MAX_WBITS | 16)
                if out:
                    return bytes(out)
            except zlib.error:
                pass
            except MemoryError as _bomb:
                _s2l_state.log(f"{_bomb} — serving raw bytes", "ERROR")
                return data
            try:
                return zlib.decompress(data, zlib.MAX_WBITS | 16)
            except zlib.error:
                return data

        if enc == "deflate":
            if _looks_json_or_text(data):
                return data
            if _looks_zlib(data):
                try:
                    return zlib.decompress(data)
                except Exception:
                    pass
            if _looks_gzip(data):
                try:
                    return zlib.decompress(data, zlib.MAX_WBITS | 16)
                except Exception:
                    pass
            try:
                return zlib.decompress(data, -zlib.MAX_WBITS)
            except zlib.error:
                return data

        if enc in ("br", "brotli"):
            # Only skip decompression if data is already clearly decoded text.
            # Use _looks_json_or_text to avoid false-positives
            # on brotli-compressed data that has many high-bit bytes.
            if _looks_json_or_text(data):
                return data   # curl_cffi already decoded it
            if _s2l_state._BROTLI_OK:
                # Try the one-shot decompress first; if it fails (some
                # servers send brotli streams with trailing junk that confuses
                # the one-shot API), fall back to the streaming decompressor
                # which is more tolerant. We saw "Empty body (decompression
                # failure)" warnings on certain API endpoints where curl_cffi returned a brotli
                # body it couldn't auto-decompress; our one-shot _brotli.decompress
                # also failed silently and we returned the raw compressed bytes
                # as if they were the decoded body — which downstream code
                # correctly flagged as "len(body) == 0 but Content-Length > 0".
                try:
                    return _s2l_state._brotli.decompress(data)
                except Exception:
                    pass
                try:
                    # Streaming-decompressor fallback. NOTE: the historical
                    # `d.process(data) + d.finish()` here was DEAD CODE — the
                    # official `brotli` package's Decompressor has only ever
                    # exposed process()/is_finished() (verified on 1.1.0 and
                    # 1.2.0), so .finish() always raised AttributeError,
                    # silently swallowed below: this branch NEVER executed.
                    # Some alternative bindings (e.g. brotlipy) DO expose
                    # finish() — call it only when present. Under the official
                    # package this fallback is a harmless no-op: process()
                    # succeeds exactly when the one-shot decompress() above
                    # did (both reject trailing junk and partial streams).
                    d = _s2l_state._brotli.Decompressor()
                    out = d.process(data)
                    _fin = getattr(d, "finish", None)
                    if callable(_fin):
                        out += _fin()
                    if out:
                        return out
                except Exception:
                    pass
                # Both failed — data may already be decoded (curl_cffi) but
                # didn't pass _looks_json_or_text. Return as-is; the caller's
                # empty-body recovery will re-fetch with Accept-Encoding: identity.
                return data
            _s2l_state.log("brotli response received but 'brotli' library not installed "
                "(pip install brotli --break-system-packages) — body may be garbled", "WARN")
            return data

        if enc == "zstd":
            if _looks_json_or_text(data):
                return data
            if _s2l_state._ZSTD_OK:
                # Try one-shot first, then streaming. ZstdDecompressor.decompress
                # requires the frame to declare its size; some servers send
                # streaming frames without that, causing decompress() to raise
                # "could not determine content size in frame header". The
                # stream_reader() API handles both cases.
                # BOMB GUARD: zstandard's one-shot decompress() IGNORES
                # max_output_size when the frame declares its content size
                # (verified 0.25.0 — full output returned, cap disregarded),
                # so oversized DECLARED sizes are refused up front via the
                # frame header. Size-less frames report the 2**64-1 "unknown"
                # sentinel there and fall through to the bounded reader loop.
                _declared = 0
                try:
                    _declared = _s2l_state._zstd.get_frame_parameters(data).content_size or 0
                except Exception:
                    _declared = 0
                if (0 < _declared < 0xFFFFFFFFFFFFFFFF
                        and _declared > _s2l_state.MAX_DECOMPRESSED_BYTES):
                    _s2l_state.log("zstd frame declares output above decompression "
                        "cap (bomb?) — serving raw bytes", "ERROR")
                    return data
                try:
                    return _s2l_state._zstd.ZstdDecompressor().decompress(data)
                except Exception:
                    pass
                try:
                    # stream_reader().read() — NOT iteration. Modern zstandard
                    # (verified 0.25.0) returns a RawIOBase-style reader whose
                    # __iter__ raises io.UnsupportedOperation; the historical
                    # b"".join(reader) hit that head-on, the exception was
                    # swallowed, and size-less frames came back as RAW
                    # COMPRESSED bytes. Bounded read loop, same cap and bomb
                    # log as the gzip path.
                    _reader = _s2l_state._zstd.ZstdDecompressor().stream_reader(data)
                    out = bytearray()
                    while True:
                        _chunk = _reader.read(64 * 1024 * 1024)
                        if not _chunk:
                            break
                        out += _chunk
                        if len(out) > _s2l_state.MAX_DECOMPRESSED_BYTES:
                            raise MemoryError("zstd output exceeds decompression cap (bomb?)")
                    if out:
                        return bytes(out)
                except MemoryError as _bomb:
                    _s2l_state.log(f"{_bomb} — serving raw bytes", "ERROR")
                    return data
                except Exception:
                    pass
            else:
                _s2l_state.log("zstd response but 'zstandard' not installed "
                    "(pip install zstandard --break-system-packages)", "WARN")
            return data

    except Exception:
        pass

    return data
