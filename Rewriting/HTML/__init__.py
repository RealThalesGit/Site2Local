"""Site2Local: HTML visibility, service-worker and URL rewriting."""
from .Reveal import (
    _reveal_hidden,
)
from .ServiceWorker import (
    _inject_sw_clear,
)
from .Policy import (
    _is_dont_rewrite_host,
)
from .URLs import (
    _rewrite_ext_urls,
    rewrite_doc_relative,
)

__all__ = [
    "_reveal_hidden",
    "_inject_sw_clear",
    "_is_dont_rewrite_host",
    "_rewrite_ext_urls",
    "rewrite_doc_relative",
]
