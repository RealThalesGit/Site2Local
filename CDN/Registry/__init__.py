"""Site2Local: CDN origin registry."""
from .Origins import (
    _real_origin_for,
    _proxy_target,
)
from .Hosts import (
    _register_cdn_host,
    _preregister_cdn_hosts_from_disk,
)

__all__ = [
    "_real_origin_for",
    "_register_cdn_host",
    "_preregister_cdn_hosts_from_disk",
    "_proxy_target",
]
