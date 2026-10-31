"""The DuckDB conduit: where duckdb cells run and every engine's result lands.

A run session holds one connection to it, to the database the header's
``input.duckdb`` names, in memory without one. Every duckdb cell runs there, so
a cell can't name a database of its own.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .config import input_spec
from .executors.base import EngineSession
from .models import RunContext


def conduit_target(config: Any, root: Path) -> str:
    """The database ``config``'s ``input.duckdb`` names: a path under ``root``,
    or ``:memory:``."""
    path = input_spec(config, "duckdb").get("path")
    if not path or path == ":memory:":
        return ":memory:"
    return str(root / path)


class ConduitSession(EngineSession):
    """The conduit as the duckdb engine's session. A result never leaves it:
    extracting one is a lazy temp view, which the sink lands from."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn  # the run session's: it opens and closes it

    def execute(self, sql: str, *, scoped: bool = True) -> None:
        self.conn.execute(sql)

    def extract(self, sql: str, view: str, ctx: RunContext, *, scoped: bool = True) -> str:
        self.conn.execute(f'CREATE OR REPLACE TEMP VIEW "{view}" AS {sql}')
        return view

    def materialize(self, name: str, sql: str) -> str:
        # a table, not a view: each consumer would recompute a view
        self.conn.execute(f'CREATE OR REPLACE TEMP TABLE "{name}" AS {sql}')
        return f'SELECT * FROM "{name}"'

    def drop(self, name: str) -> None:
        self.conn.execute(f'DROP TABLE IF EXISTS temp."{name}"')
