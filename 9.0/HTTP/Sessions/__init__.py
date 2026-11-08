"""Site2Local: upstream HTTP session management."""
from .Devices import (
    _sanitize_ua,
    _detect_device_from_ua,
    _effective_device,
    _device_variant,
)
from .Cloudflare import (
    _next_cf_config,
    _is_cf_block,
    _is_raw_block_text,
    _is_bot_page,
)
from .Factory import (
    _make_cffi_session,
    _make_session,
    _make_cloudscraper_session,
    _make_requests_session,
)
from .Clients import (
    _get_proxy_session,
    _client_id,
    _get_client_session,
    _short_exc,
)

__all__ = [
    "_sanitize_ua",
    "_detect_device_from_ua",
    "_effective_device",
    "_device_variant",
    "_next_cf_config",
    "_make_cffi_session",
    "_make_session",
    "_make_cloudscraper_session",
    "_make_requests_session",
    "_is_cf_block",
    "_is_raw_block_text",
    "_is_bot_page",
    "_get_proxy_session",
    "_client_id",
    "_get_client_session",
    "_short_exc",
]
