"""quicksql's per-user directory: state files, user templates, the catalog
store, live sockets and the usage log all live under it.

Its own module, importing nothing of quicksql, so that anything may ask where
it is: an extension can't import ``state``, which knows the compiler's Project.
"""

from __future__ import annotations

import os
from pathlib import Path

HOME_ENV = "QSQL_HOME"  # not QUICKSQL_*: overrides.py reads those as config


def quicksql_home() -> Path:
    """``$QSQL_HOME``, else ``~/.quicksql``."""
    return Path(os.environ.get(HOME_ENV) or Path.home() / ".quicksql")
