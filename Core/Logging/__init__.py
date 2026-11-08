"""Site2Local: logging and lolcat output."""
from .Lolcat import (
    _lolcat_internal,
    _probe_lolcat,
    _lolcat_real,
)
from .Logger import (
    log,
)

__all__ = [
    "_lolcat_internal",
    "_probe_lolcat",
    "_lolcat_real",
    "log",
]
