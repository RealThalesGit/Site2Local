"""Site2Local proxy package."""

# Assemble shared state before exposing implementation modules.
from . import Runtime as runtime
from . import Config

# Preserve the historical lowercase module paths (`Site2Local.runtime` and
# `Site2Local.config`) while the files themselves use PascalCase.
import sys as _sys
config = Config
_sys.modules[__name__ + ".runtime"] = runtime
_sys.modules[__name__ + ".config"] = Config

from .Runtime import app, main
