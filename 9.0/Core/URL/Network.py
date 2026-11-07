"""Site2Local: URL, request and platform helpers."""
from __future__ import annotations

from flask import request as flask_request
from urllib.parse import urldefrag
from urllib.parse import urlparse
import re
import socket
import time

from ... import Runtime as _s2l_state


def _is_ip_literal(host: str) -> bool:
    """Return True if `host` is a literal IP address (v4 or v6), not a DNS name.

    Used to decide whether to send SNI in TLS handshakes: RFC 6066 says SNI
    is for DNS names only, and many servers/WAFs reject connections that
    send an IP literal as the SNI hostname.
    """
    if not host:
        return False
    h = host.strip("[]")
    # IPv6 contains ':', IPv4 contains '.', DNS names contain only [a-zA-Z0-9._-]
    if ":" in h:
        return True  # IPv6 literal
    if h.replace(".", "").isdigit() and h.count(".") == 3:
        return True  # IPv4 literal
    # Pure numeric (rare, but e.g. "1234" as a hostname) — treat as IP
    return h.isdigit()


def _is_ipv6_literal(host: str) -> bool:
    """Return True only if `host` is an IPv6 literal (not IPv4, not DNS name).

    curl_cffi has a known bug with IPv6 literals in ws_connect (TLS handshake
    fails with "invalid library"). IPv4 literals work fine — this function is
    used to skip curl_cffi ONLY for IPv6, so IPv4 hosts still benefit from
    curl_cffi's browser TLS fingerprinting.
    """
    if not host:
        return False
    h = host.strip("[]")
    return ":" in h


def _is_wire_payload(data: bytes) -> bool:
    if not data: return False
    # Strip a UTF-8 BOM before checking signatures — some Next.js RSC streams
    # are emitted with one, which would shift every signature off the front of
    # the scanned window and make this check silently miss them.
    head = data.lstrip(b"\xef\xbb\xbf")[:64]
    return any(head.startswith(sig) for sig in _s2l_state._WIRE_FORMAT_SIGNATURES)


def _is_valid_html(data: bytes) -> bool:
    """Quick check that cached data is real HTML, not RSC or other junk."""
    if not data:
        return False
    head = data[:512].lower()
    return (b"<html" in head or b"<!doctype" in head or b"<head" in head)


def resolve_ip(host: str) -> str:
    try:
        return socket.gethostbyname(host)
    except Exception:
        return "?"


def url_depth(u: str) -> int:
    return urlparse(u).path.count("/")


def is_html(data: bytes, ct: str) -> bool:
    if ct and "text/html" in ct.lower():
        return True
    head = data[:1024].lower()
    return b"<html" in head or b"<!doctype" in head
