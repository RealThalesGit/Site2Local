"""Site2Local: streamed and SSE responses."""
from __future__ import annotations

from flask import Response
from flask import stream_with_context
from urllib.parse import urlparse
import zlib

from ... import Runtime as _s2l_state


def _stream_resp(upstream_r, method: str, target: str) -> Response:
    """Stream a large upstream response (fetched with stream=True) to the browser.

    Body is not buffered in RAM (beyond a bounded cache buffer for disk writes).
    After the generator finishes the plain (decompressed) content is queued for
    disk caching — but only if the response completed normally AND fits within
    _STREAM_CACHE_MAX_BYTES, to avoid OOM on huge media files.

    Decompression is done per-chunk via a streaming decompressor so the browser
    receives plain bytes (Content-Encoding is stripped). This is critical:
    curl_cffi's impersonation can override Accept-Encoding at the libcurl level,
    so upstream may return gzip/br bytes even when we asked for identity.
    """
    ct    = upstream_r.headers.get("Content-Type", "application/octet-stream")
    # Ensure WASM resources are served with Content-Type: application/wasm —
    # WebAssembly.instantiateStreaming() requires it, and many CDNs serve .wasm
    # as application/octet-stream which causes "Failed to fetch" in WASM-threaded
    # clients.
    ct    = _s2l_state._ensure_wasm_content_type(ct, urlparse(target).path or "")
    sc    = upstream_r.status_code
    enc   = (upstream_r.headers.get("Content-Encoding", "") or "").lower().strip()
    out_h = _s2l_state.filter_resp(_s2l_state._resp_headers_dict(upstream_r),
                                    is_worker_script=_s2l_state._is_worker_request())
    # CORS + CORP are already set correctly by filter_resp() — do NOT re-set
    # them with a bare Access-Control-Expose-Headers: "*" here, that would
    # overwrite the concrete list filter_resp set for credentialed requests.
    out_h["Cache-Control"] = "no-cache, no-transform"
    # We decompress before yielding (see _gen below), so strip the header.
    # Also strip Content-Length when we're decompressing — the decompressed
    # size differs from the compressed Content-Length, and a mismatch causes
    # the browser to truncate or stall the stream.
    out_h.pop("content-encoding", None)
    out_h.pop("Content-Encoding", None)
    if enc and enc != "identity":
        out_h.pop("content-length", None)
        out_h.pop("Content-Length", None)

    # Build a streaming decompressor matching the upstream encoding.
    # Falls back to identity (pass-through) if the encoding is unknown/unsupported.
    # Handles stacked encodings (e.g. "gzip, br") by applying them in reverse order.
    _enc_chain: list = []   # list of decompressor specs applied in order
    for _enc_part in [e.strip() for e in enc.split(",") if e.strip()]:
        if _enc_part in ("gzip", "x-gzip"):
            _enc_chain.append(("gzip", zlib.decompressobj(zlib.MAX_WBITS | 16)))
        elif _enc_part == "deflate":
            # Try zlib-wrapped first; persistent raw-deflate fallback handled in _decompress_chunk.
            _enc_chain.append(("deflate", zlib.decompressobj(zlib.MAX_WBITS)))
        elif _enc_part in ("br", "brotli") and _s2l_state._BROTLI_OK:
            _enc_chain.append(("br", _s2l_state._brotli.Decompressor()))
        elif _enc_part == "zstd" and _s2l_state._ZSTD_OK:
            # ZstdDecompressionObj is the incremental API (not stream_reader,
            # which is read-only and raises on .write()).
            _enc_chain.append(("zstd", _s2l_state._zstd.ZstdDecompressor().decompressobj()))
        else:
            # Unknown/unsupported encoding — pass through (can't decompress).
            _s2l_state.log(f"Unsupported stream encoding {_enc_part!r} on {urlparse(target).path}", "WARN")

    _deflate_fallback: zlib.decompressobj | None = None  # persistent raw-deflate obj

    def _decompress_chunk(chunk: bytes) -> bytes:
        """Decompress one raw chunk through the encoding chain."""
        nonlocal _deflate_fallback
        if not _enc_chain:
            return chunk  # identity / no supported encoding → pass through
        data = chunk
        for _enc_name, _decomp in _enc_chain:
            try:
                if _enc_name == "br":
                    data = _decomp.process(data)
                elif _enc_name == "zstd":
                    data = _decomp.decompress(data)
                elif _enc_name == "deflate":
                    out = _decomp.decompress(data)
                    if not out and _deflate_fallback is None:
                        # First chunk produced nothing — maybe raw deflate (no zlib header).
                        # Create a PERSISTENT raw-deflate decompressor (NOT per-chunk:
                        # raw deflate is a stream, each chunk depends on prior state).
                        try:
                            _deflate_fallback = zlib.decompressobj(-zlib.MAX_WBITS)
                            out = _deflate_fallback.decompress(data)
                        except Exception:
                            _deflate_fallback = None
                    elif _deflate_fallback is not None:
                        out = _deflate_fallback.decompress(data)
                    data = out
                else:  # gzip
                    data = _decomp.decompress(data)
            except Exception:
                # On failure, pass current data through rather than killing the stream.
                return data
        return data

    def _gen():
        raw_chunks: list[bytes] = []
        total = 0
        completed = False
        # Capture the first few KB of the DECOMPRESSED body for the GUI traffic
        # log. Without this, streamed responses (audio/video 206, large files)
        # show "(empty response)" in the Hook Inspector body editor because the
        # body is never buffered — it goes straight to the browser. We grab a
        # bounded prefix so the user can at least see what kind of content it is.
        _gui_preview = bytearray()
        _GUI_PREVIEW_MAX = 4096
        try:
            for chunk in upstream_r.iter_content(chunk_size=65536):
                if chunk:
                    # Bound the cache buffer — if the response is huge, stop
                    # accumulating and skip disk caching entirely (avoids OOM).
                    if total < _s2l_state._STREAM_CACHE_MAX_BYTES:
                        raw_chunks.append(chunk)
                    total += len(chunk)
                    _dec = _decompress_chunk(chunk)
                    if len(_gui_preview) < _GUI_PREVIEW_MAX:
                        _gui_preview.extend(_dec[:_GUI_PREVIEW_MAX - len(_gui_preview)])
                    yield _dec
            completed = True
        except Exception as e:
            _s2l_state.log(f"Stream error: {urlparse(target).path} — {_s2l_state._short_exc(e)}", "WARN")
        finally:
            # Only cache if the stream completed normally AND we stayed within
            # the memory bound AND it's a full response (200, or 206 that
            # covers the ENTIRE file — detected via Content-Range starting at 0).
            # A 206 for "Range: bytes=1000-2000" is a partial slice and must
            # NOT be cached (would overwrite the full file with a slice).
            # But a 206 for "Range: bytes=0-" returns the whole file with
            # status 206 — that IS cacheable.
            _cr_header = ""
            try:
                _cr_header = upstream_r.headers.get("Content-Range", "") or ""
            except Exception:
                pass
            _is_full_content = (sc == 200) or (
                sc == 206 and _cr_header and _cr_header.startswith("bytes 0-"))
            if (method == "GET" and _is_full_content and completed
                    and raw_chunks and total <= _s2l_state._STREAM_CACHE_MAX_BYTES):
                full  = b"".join(raw_chunks)
                plain = _s2l_state.decompress_body(full, enc)
                if not _s2l_state._is_wire_payload(plain) and not _s2l_state._is_bot_page(plain, sc):
                    # The save worker counts 'saved' when the bytes actually
                    # hit disk — counting here too inflated the stat by 2 for
                    # every cached stream (and counted items the worker then
                    # dropped as bot pages / dedup misses).
                    _s2l_state.save_queue.put((target, plain, ct, _s2l_state._effective_device()))
            # Single consolidated stream log line. Include "not cached" note
            # when the stream was too large or didn't complete — previously
            # this was a second separate log line, creating duplicate entries.
            _not_cached_note = ""
            if total > _s2l_state._STREAM_CACHE_MAX_BYTES:
                _not_cached_note = " — too large, not cached"
            elif not completed:
                _not_cached_note = " — incomplete, not cached"
            _tgt = urlparse(target)
            _s2l_state.log(f"[STREAM] {method} {sc} {_s2l_state._fmt_host(_tgt.netloc)}{_s2l_state._fmt_path_qs(_tgt.path, _tgt.query)}"
                f" [{_s2l_state._fmt_size(total)}]{_not_cached_note}", "→")
            # Push to the GUI traffic log. For 206/204 and other non-2xx the
            # preview may be empty — that's fine, _gui_push_raw handles it.
            # For large streams we show the preview prefix with a note.
            _preview_bytes = bytes(_gui_preview)
            if _s2l_state.HOOK_GUI:
                _path = urlparse(target).path or "/"
                if total > _GUI_PREVIEW_MAX and _preview_bytes:
                    _gui_body = (_preview_bytes
                                 + f"\n... (streamed {_s2l_state._fmt_size(total)} total, showing first {_s2l_state._fmt_size(len(_preview_bytes))})"
                                   .encode("utf-8", "replace"))
                else:
                    _gui_body = _preview_bytes
                _s2l_state._gui_push_raw(method, _path, sc, ct, _gui_body,
                              display_tag="[STREAM]",
                              origin=_s2l_state._fmt_host(urlparse(target).netloc),
                              _skip_log=True)
            # CRITICAL: always close the upstream response, even on client
            # disconnect (GeneratorExit is a BaseException, not caught by
            # the `except Exception` above). Without this, the underlying
            # socket is never released back to the connection pool, leaking
            # file descriptors under sustained client disconnects —
            # especially bad on curl_cffi HTTP/2 where one leaked handle
            # can pin the entire connection.
            try:
                upstream_r.close()
            except Exception:
                pass

    return Response(stream_with_context(_gen()), status=sc,
                    headers=out_h, content_type=ct)
