"""Columns available to a cell, per relation — reusing the catalog introspection.

Three relation kinds, mirroring what shows up in a cell's FROM/JOIN:

* an engine-native table (``claims.medical_claims``) — the leaf the cell's
  executor finds in its own catalog (`Executor.catalog_table`), the tree the TUI
  browser walks, so duckdb/sqlite read locally and postgres/bigquery over their
  live connection;
* a ``ref('cell')`` — the upstream cell's landed output columns
  (`catalog.sink_output_node`);
* a ``source('file')`` — the file's columns via its DuckDB reader (`sources.py`).

Introspection is only ever run on a completion request (never per keystroke) and
memoized in a `SchemaCache`; remote engines are the reason the cache exists.
"""

from __future__ import annotations

import os
from typing import Any, Callable

import duckdb
import polars as pl

from dataclasses import dataclass

from .. import catalog
from ..conduit import load_extension
from ..sources import reader_for

Column = tuple[str, str]  # (name, type)


@dataclass(frozen=True)
class Projection:
    """A relation whose columns are known statically from a SELECT projection
    (a CTE's output), no introspection needed. Types are unknown."""

    columns: tuple[str, ...]


class SchemaCache:
    """Memoize columns by an opaque key; ``invalidate()`` drops everything."""

    def __init__(self) -> None:
        self._by_key: dict[Any, list[Column]] = {}

    def get(self, key: Any, compute: Callable[[], list[Column]]) -> list[Column]:
        if key not in self._by_key:
            self._by_key[key] = compute()
        return self._by_key[key]

    def invalidate(self) -> None:
        self._by_key.clear()


def _cols(frame: pl.DataFrame) -> list[Column]:
    """Extract (field_path, type) from a catalog field-path frame; field_path
    carries nested STRUCT/REPEATED paths too (e.g. BigQuery ``record.sub``)."""
    paths = frame["field_path"].to_list()
    types = frame["type"].to_list()
    return [(str(p), str(t)) for p, t in zip(paths, types)]


def ref_columns(project: Any, cell_name: str) -> list[Column]:
    """Columns of a ``ref('cell')`` from its landed sink; [] if not landed yet."""
    cell = project.cells.get(cell_name)
    if cell is None:
        return []
    try:
        frame = catalog.sink_output_node(
            cell.name, cell.config, project.root, project.registries
        ).load()
    except Exception:
        return []  # not landed / unreadable: best effort, no columns
    return _cols(frame)


def source_columns(project: Any, spec: str | None) -> list[Column]:
    """Columns of a ``source('file')`` via its DuckDB reader on a LIMIT-0 read."""
    if not spec:
        return []
    path = spec if os.path.isabs(spec) else str(project.root / spec)
    try:
        reader = reader_for(path, None, project.registries.readers)
        con = duckdb.connect()
        try:
            for ext in reader.requires:
                load_extension(con, ext)
            rel = con.sql(f"SELECT * FROM {reader.expr(path)} LIMIT 0")
            return [(str(c), str(t)) for c, t in zip(rel.columns, rel.types)]
        finally:
            con.close()
    except Exception:
        return []


def _table_frame(project: Any, cell: Any, name: str) -> pl.DataFrame | None:
    """The field-path frame of table ``name`` (maybe qualified) in the cell's
    context, from its engine's catalog; None when unknown or unreachable."""
    executor = project.registries.executors.get(cell.engine)
    parts = name.replace('"', "").split(".")
    try:
        leaf = executor.catalog_table(cell.config, project.root, parts)
        return leaf.load() if leaf is not None else None
    except Exception:
        return None


def table_columns(project: Any, cell: Any, name: str) -> list[Column]:
    frame = _table_frame(project, cell, name)
    return _cols(frame) if frame is not None else []


def relation_names(
    project: Any, cell: Any, parts: list[str], cache: SchemaCache | None = None
) -> list[Column]:
    """Completion candidates for the next segment of a partial dotted relation
    (``parts``: the segments typed so far) in the cell's context, as its
    engine's catalog offers them; [] when it offers none or can't be reached."""
    executor = project.registries.executors.get(cell.engine)
    key = ("relations", getattr(cell, "context", ""), tuple(parts))

    def compute() -> list[Column]:
        try:
            return executor.catalog_relations(cell.config, project.root, parts)
        except Exception:
            return []  # unreachable dataset/project: best effort, no names

    return cache.get(key, compute) if cache is not None else compute()


def columns_for(
    project: Any, cell: Any, relation: Any, cache: SchemaCache | None = None
) -> list[Column]:
    """Columns for a resolved relation: a mask ``Ref``/``Source`` tag, or a
    (possibly schema-qualified) engine-native table name string."""
    from .mask import Ref, Source

    if isinstance(relation, Projection):
        return [(c, "") for c in relation.columns]
    if isinstance(relation, Ref):
        key, fn = ("ref", relation.cell), lambda: ref_columns(project, relation.cell)
    elif isinstance(relation, Source):
        key, fn = ("source", relation.spec), lambda: source_columns(project, relation.spec)
    else:
        key = ("table", getattr(cell, "context", ""), relation)
        fn = lambda: table_columns(project, cell, relation)  # noqa: E731
    return cache.get(key, fn) if cache is not None else fn()
