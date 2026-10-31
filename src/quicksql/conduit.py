"""The DuckDB conduit: where duckdb cells run and every engine's result lands.

A run session holds one connection to it, to the database the header's
``input.duckdb`` names, in memory without one. Every duckdb cell runs there, so
a cell can't name a database of its own.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import input_spec


def conduit_target(config: Any, root: Path) -> str:
    """The database ``config``'s ``input.duckdb`` names: a path under ``root``,
    or ``:memory:``."""
    path = input_spec(config, "duckdb").get("path")
    if not path or path == ":memory:":
        return ":memory:"
    return str(root / path)
