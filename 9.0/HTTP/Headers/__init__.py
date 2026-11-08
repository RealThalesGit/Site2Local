"""Site2Local: HTTP header helpers (forwarding, CORS, isolation, cookies)."""
from .CI import (
    _ci_header_keys,
    _ci_get_header,
    _ci_remove_header,
)
from .Cookies import (
    _all_set_cookies,
    _resp_headers_dict,
    _flatten_cookiejar,
)
from .CORS import (
    _cors_origin_pair,
    _pna_allow_header,
    _apply_corp_header,
    _apply_cors_headers,
)
from .Isolation import (
    _needs_isolation,
    _is_worker_request,
    _is_document_request,
    _is_document_ct,
    _coep_value,
    _apply_isolation_headers,
)
from .Security import (
    _apply_security_headers,
)
from .Response import (
    filter_resp,
    _finalize_security_headers,
)
from .Forwarding import (
    filter_fwd,
    inject_csrf_headers,
    rewrite_origin,
)

__all__ = [
    "filter_fwd",
    "_all_set_cookies",
    "_resp_headers_dict",
    "_flatten_cookiejar",
    "_needs_isolation",
    "_cors_origin_pair",
    "_pna_allow_header",
    "_ci_header_keys",
    "_ci_get_header",
    "_ci_remove_header",
    "_apply_corp_header",
    "_apply_cors_headers",
    "_apply_security_headers",
    "_is_worker_request",
    "_is_document_request",
    "_is_document_ct",
    "_coep_value",
    "_apply_isolation_headers",
    "filter_resp",
    "_finalize_security_headers",
    "inject_csrf_headers",
    "rewrite_origin",
]
