"""The DuckDB conduit: where duckdb cells run and every engine's result lands.

A run session holds one connection to it, to the database the header's
``input.duckdb`` names, in memory without one. Every duckdb cell runs there, so
a cell can't name a database of its own.

Anything that reads through DuckDB uses the helpers here: loading extensions,
previewing a landed output, describing a relation's columns.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import duckdb

from .executors.base import EngineSession, engine_input, field_path_frame, unwrap_error_context
from .models import Preview, RunContext

if TYPE_CHECKING:
    import polars as pl

ENGINE = "duckdb"  # the engine whose cells run on the conduit itself


def conduit_target(config: Any, root: Path) -> str:
    """The database ``config``'s ``input.duckdb`` names: a path under ``root``,
    or ``:memory:``."""
    path = engine_input(config, ENGINE).get("path")
    if not path or path == ":memory:":
        return ":memory:"
    return str(root / path)


def load_extension(conn: Any, ext: str) -> None:
    """LOAD a duckdb extension on ``conn``, INSTALLing it first if need be."""
    try:
        conn.execute(f"LOAD {ext}")
    except duckdb.Error:
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")


def ensure_extension(ctx: RunContext, ext: str, cell: str | None = None) -> None:
    """Load ``ext`` on the run's conduit unless its run session already has."""
    if ext in ctx.ext_cache:
        return
    # first load per session — may INSTALL (the slow, network path)
    ctx.emit("cell_step", cell, f"loading duckdb extension {ext}")
    load_extension(ctx.conn, ext)
    ctx.ext_cache.add(ext)


def preview(conn: Any, relation: str, limit: int = 100) -> Preview:
    """The first ``limit`` rows of ``relation`` (a sink's ``ref_expr``, a
    ``read_parquet(...)``) as plain values, the way DuckDB fetches them."""
    rel = conn.sql(f"SELECT * FROM {relation} LIMIT {limit}")
    return Preview(tuple(rel.columns), tuple(rel.fetchall()))


def landed_relation(project: Any, name: str) -> str | None:
    """What cell ``name``'s landed output reads back as through DuckDB (its
    sink's ``ref_expr``), or None for an effect-only cell, which lands nothing.
    ``project`` is a compiled Project (untyped: extensions import this module,
    and they may not import the compiler)."""
    from .config import make_sink

    sink = make_sink(project.cells[name].config, project.root, project.registries)
    return sink.ref_expr(name) if sink.lands_output else None


def read_landed(conn: Any, relation: str | None, limit: int = 100) -> Preview | None:
    """The head of a landed output on ``conn``, or None when it can't be read
    there: never landed, or its database isn't attached on ``conn``. Never the
    sink's ``prepare()``: an ATTACH creates a missing database file or connects
    to a server, so a database output reads only where a run attached it."""
    if relation is None:
        return None
    try:
        return preview(conn, relation, limit)
    except duckdb.Error:
        return None


def duckdb_field_paths(columns: Sequence[str], types: Sequence[Any]) -> pl.DataFrame:
    """A relation's columns as a field-path frame, from DuckDBPyTypes
    (.id/.children), depth-first.

    ``.children`` raises on scalars, so recursion gates on ``.id``; fixed-size
    arrays carry a non-type child (the size), filtered by the ``.id`` check.
    LIMIT-0 relation types carry no comments, so descriptions stay empty.
    """
    rows: list[tuple[str, str, str, str, str]] = []

    def walk(name: str, type_: Any, column: str, prefix: str) -> None:
        path = f"{prefix}.{name}" if prefix else name
        mode = "REPEATED" if type_.id in ("list", "array", "map") else ""
        rows.append((column, path, str(type_), mode, ""))
        descend(type_, column, path)

    def descend(type_: Any, column: str, path: str) -> None:
        if type_.id == "struct":
            for sub_name, sub_type in type_.children:
                walk(sub_name, sub_type, column, path)
        elif type_.id in ("list", "array", "map"):
            # the collection wrapper adds no path segment: recurse into the
            # element type(s) under the collection's own path
            for _, sub_type in type_.children:
                if hasattr(sub_type, "id"):
                    descend(sub_type, column, path)

    for name, type_ in zip(columns, types):
        walk(name, type_, name, "")
    return field_path_frame(rows)


class ConduitSession(EngineSession):
    """The conduit as the duckdb engine's session. A result never leaves it:
    extracting one is a lazy temp view, which the sink lands from."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn  # the run session's: it opens and closes it

    def execute(self, sql: str, *, scoped: bool = True) -> None:
        self.conn.execute(sql)

    def extract(self, sql: str, view: str, ctx: RunContext, *, scoped: bool = True) -> str:
        self._wrapped(f'CREATE OR REPLACE TEMP VIEW "{view}" AS ', sql)
        return view

    def materialize(self, name: str, sql: str) -> str:
        # a table, not a view: each consumer would recompute a view
        self._wrapped(f'CREATE OR REPLACE TEMP TABLE "{name}" AS ', sql)
        return f'SELECT * FROM "{name}"'

    def check(self, sql: str) -> None:
        # runs the whole query and discards its rows; a count(*) over it would
        # let duckdb prune the very column that fails
        self._wrapped("EXPLAIN ANALYZE ", sql)

    def drop(self, name: str) -> None:
        self.conn.execute(f'DROP TABLE IF EXISTS temp."{name}"')

    def _wrapped(self, wrapper: str, sql: str) -> None:
        try:
            self.conn.execute(wrapper + sql)
        except duckdb.Error as exc:  # same type, so it still reads as duckdb's own
            exc.args = (unwrap_error_context(str(exc), wrapper),)
            raise
