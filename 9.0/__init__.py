"""Site2Local proxy package — the single __init__.py of the whole tree.

Every folder below this one is a PEP 420 *namespace package*: none of them
carries an ``__init__.py`` of its own, so exactly ONE __init__ exists in the
entire project. All public assembly happens in ``Runtime.py``, which imports
leaf modules directly (``from .Core.URL.Format import ...``). The historical
``import s2l`` API is preserved through the aliases and re-exports below.
"""

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
