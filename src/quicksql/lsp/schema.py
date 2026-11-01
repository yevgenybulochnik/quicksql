"""Columns available to a cell, per relation — reusing the catalog introspection.

Three relation kinds, mirroring what shows up in a cell's FROM/JOIN:

* an engine-native table (``claims.medical_claims``) — walked through the same
  per-engine catalog nodes the TUI browser uses (`catalog._CONTEXT_BUILDERS`),
  so duckdb/sqlite read locally and postgres/bigquery over their live connection;
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
            from ..runner import _load_extension

            for ext in reader.requires:
                _load_extension(con, ext)
            rel = con.sql(f"SELECT * FROM {reader.expr(path)} LIMIT 0")
            return [(str(c), str(t)) for c, t in zip(rel.columns, rel.types)]
        finally:
            con.close()
    except Exception:
        return []


def _table_frame(project: Any, cell: Any, name: str) -> pl.DataFrame | None:
    """Walk the cell engine's catalog to ``name``'s field-path frame. ``name``
    may be schema-qualified; unqualified falls back to a per-engine default."""
    engine = cell.engine
    builder = catalog._CONTEXT_BUILDERS.get(engine)
    if builder is None:
        return None
    registries = project.registries
    root = builder(cell.config, project.root, registries)
    if root is None or root.child is None:
        return None
    parts = name.replace('"', "").split(".")
    table = parts[-1]
    try:
        if engine == "postgres":
            schema = parts[-2] if len(parts) >= 2 else "public"
            mid = root.child({"schema": schema})
            leaf = mid.child({"table": table}) if mid and mid.child else None
        elif engine == "duckdb":
            schema = parts[-2] if len(parts) >= 2 else "main"
            leaf = root.child({"schema": schema, "table": table})
        elif engine == "sqlite":
            leaf = root.child({"table": table})
        elif engine == "bigquery":
            spec = registries.executors.get("bigquery").input_spec(cell.config)
            if len(parts) >= 3:
                # project.dataset.table — introspect that project, not the
                # spec's default (endpoint etc. carry over)
                root = catalog.bigquery_context_node({**spec, "project": parts[-3]}, registries)
            dataset = parts[-2] if len(parts) >= 2 else spec.get("dataset")
            mid = root.child({"dataset": dataset}) if dataset else None
            leaf = mid.child({"table": table}) if mid and mid.child else None
        else:
            return None
        return leaf.load() if leaf is not None else None
    except Exception:
        return None


def table_columns(project: Any, cell: Any, name: str) -> list[Column]:
    frame = _table_frame(project, cell, name)
    return _cols(frame) if frame is not None else []


def relation_names(
    cell: Any, parts: list[str], registries: Any, cache: SchemaCache | None = None
) -> list[Column]:
    """Completion candidates for the next segment of a partial dotted relation
    in a bigquery cell (v1: other engines return []): with no typed segments,
    the context's datasets; with one, that dataset's tables — falling back to
    the *project* of that name's datasets when it isn't a dataset; with two
    (project.dataset.), that project's dataset's tables."""
    if cell.engine != "bigquery" or len(parts) > 2:
        return []
    spec = registries.executors.get("bigquery").input_spec(cell.config)
    key = ("relations", getattr(cell, "context", ""), tuple(parts))

    def datasets(s: dict) -> list[Column]:
        frame = catalog.bigquery_context_node(s, registries).load()
        return [(d, "dataset") for d in frame["dataset"].to_list()]

    def tables(s: dict, ds: str) -> list[Column]:
        frame = catalog.bigquery_context_node(s, registries).child({"dataset": ds}).load()
        return [
            (row["table"], row["type"] or "table") for row in frame.iter_rows(named=True)
        ]

    def compute() -> list[Column]:
        try:
            if len(parts) == 0:
                return datasets(spec)
            if len(parts) == 2:
                return tables({**spec, "project": parts[0]}, parts[1])
            try:
                return tables(spec, parts[0])
            except Exception:
                return datasets({**spec, "project": parts[0]})
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
