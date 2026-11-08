"""Site2Local: URL, request and platform helpers."""
from .Request import (
    _raw_path_after,
    _request_host_header,
    _request_hostname,
    _proxy_base,
)
from .Network import (
    _is_ip_literal,
    _is_ipv6_literal,
    _is_wire_payload,
    _is_valid_html,
    resolve_ip,
    url_depth,
    is_html,
)
from .Platform import (
    set_last_platform,
    get_last_platform,
    detect_platform,
)
from .Rewrite import (
    rewrite_abs_urls,
    strip_fragment,
    normalize_url,
    build_base_url,
)
from .Format import (
    _fmt_size,
    _fmt_host,
    _fmt_path_qs,
    log_req,
)
from .Policy import (
    is_allowed_domain,
    is_external_domain,
)

__all__ = [
    "_raw_path_after",
    "_is_ip_literal",
    "_is_ipv6_literal",
    "_is_wire_payload",
    "_is_valid_html",
    "set_last_platform",
    "get_last_platform",
    "detect_platform",
    "_request_host_header",
    "_request_hostname",
    "_proxy_base",
    "rewrite_abs_urls",
    "strip_fragment",
    "normalize_url",
    "resolve_ip",
    "url_depth",
    "is_html",
    "_fmt_size",
    "_fmt_host",
    "_fmt_path_qs",
    "log_req",
    "build_base_url",
    "is_allowed_domain",
    "is_external_domain",
]
