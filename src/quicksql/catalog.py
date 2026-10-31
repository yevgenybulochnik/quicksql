"""Drillable schema catalog: nodes that load a Polars frame and resolve children.

Pure data + blocking loaders; the TUI runs ``load()`` in a thread worker and
pushes the frame as a Sheet whose Enter drills via ``child``. Field-path frames
mirror BigQuery's ``information_schema.column_field_paths``: one row per path
level, intermediate STRUCTs included; a LIST wrapper adds no path segment.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional, Sequence

import duckdb
import polars as pl

from .config import input_spec

if TYPE_CHECKING:
    from .compiler import Project

FIELD_PATH_SCHEMA = {
    "column": pl.Utf8,
    "field_path": pl.Utf8,
    "type": pl.Utf8,
    "mode": pl.Utf8,
    "description": pl.Utf8,
}


@dataclass(frozen=True)
class CatalogNode:
    """One drillable catalog level: a frame to show, and how to go deeper.

    ``load`` blocks (network/disk) — call it off the UI thread. ``child`` maps
    a named row of the loaded frame to the next level; None marks a leaf.
    """

    title: str
    load: Callable[[], pl.DataFrame]
    child: Optional[Callable[[dict[str, Any]], Optional["CatalogNode"]]] = None
    # stable cache identity across node re-creation (drilling rebuilds nodes);
    # set it wherever the display title isn't globally unique
    key: str | None = None

    @property
    def cache_key(self) -> str:
        return self.key if self.key is not None else self.title


class CatalogCache:
    """Loaded frames by node cache_key: browsing revisits levels, loads are
    remote. Invalidate one node (user refetch) or everything (a run, a
    recompile, or a notebook switch changed the world)."""

    def __init__(self) -> None:
        self._frames: dict[str, pl.DataFrame] = {}

    def __len__(self) -> int:
        return len(self._frames)

    def load(self, node: CatalogNode) -> tuple[pl.DataFrame, bool]:
        """The node's frame and whether it came from the cache."""
        key = node.cache_key
        if key in self._frames:
            return self._frames[key], True
        frame = node.load()
        self._frames[key] = frame
        return frame, False

    def invalidate(self, node: CatalogNode | None = None) -> None:
        if node is None:
            self._frames.clear()
        else:
            self._frames.pop(node.cache_key, None)


def _field_path_frame(rows: list[tuple[str, str, str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=FIELD_PATH_SCHEMA, orient="row")


def bq_field_paths(fields: Iterable[Any]) -> pl.DataFrame:
    """Flatten BigQuery SchemaFields (.name/.field_type/.mode/.fields/.description)
    depth-first."""
    rows: list[tuple[str, str, str, str, str]] = []

    def walk(field: Any, column: str, prefix: str) -> None:
        path = f"{prefix}.{field.name}" if prefix else field.name
        rows.append((column, path, field.field_type, field.mode or "", field.description or ""))
        for sub in field.fields or ():
            walk(sub, column, path)

    for field in fields:
        walk(field, field.name, "")
    return _field_path_frame(rows)


def duckdb_field_paths(columns: Sequence[str], types: Sequence[Any]) -> pl.DataFrame:
    """Flatten DuckDBPyTypes (.id/.children) depth-first.

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
    return _field_path_frame(rows)


# ---------- node builders ----------


def _info_node(title: str, message: str) -> CatalogNode:
    frame = pl.DataFrame({"info": [message]})
    return CatalogNode(title=title, load=lambda: frame)


def sink_output_node(
    cell_name: str, config: Any, root: Path, connect: Callable[[], Any] | None = None
) -> CatalogNode:
    """Leaf: field paths of a cell's landed sink, read back via its ref_expr.

    ``connect`` supplies the DuckDB connection; pass a cursor factory of the
    live conduit when it may already hold the sink's ATTACH (a throwaway
    connection cannot re-ATTACH a file another connection holds).
    """

    def load() -> pl.DataFrame:
        from .runner import _load_extension
        from .sinks import make_sink

        sink = make_sink(config, root)
        con = connect() if connect is not None else duckdb.connect()
        try:
            for ext in sink.requires:
                _load_extension(con, ext)
            sink.prepare(con)
            rel = con.sql(f"SELECT * FROM {sink.ref_expr(cell_name)} LIMIT 0")
            return duckdb_field_paths(rel.columns, rel.types)
        finally:
            con.close()

    return CatalogNode(title=f"schema({cell_name})", load=load)


def bigquery_context_node(spec: dict[str, Any], dataset: str | None = None) -> CatalogNode:
    """datasets -> tables -> field paths, on one client memoized per chain.
    With ``dataset``, the chain starts at that dataset's tables instead."""
    holder: dict[str, Any] = {}

    def client() -> Any:
        if "client" not in holder:
            from .registry import EXECUTORS

            holder["client"] = EXECUTORS.get("bigquery").make_client(spec)
        return holder["client"]

    title = f"bigquery:{spec.get('project')}"

    def dataset_node(ds: str) -> CatalogNode:
        def tables_load() -> pl.DataFrame:
            tables = list(client().list_tables(ds))
            return pl.DataFrame(
                {
                    "table": [t.table_id for t in tables],
                    "type": [getattr(t, "table_type", None) or "" for t in tables],
                },
                schema={"table": pl.Utf8, "type": pl.Utf8},
            )

        def tables_child(trow: dict[str, Any]) -> CatalogNode:
            table = trow["table"]
            return CatalogNode(
                title=f"{ds}.{table}",
                load=lambda: bq_field_paths(client().get_table(f"{ds}.{table}").schema),
                key=f"{title}/{ds}.{table}",
            )

        return CatalogNode(title=f"{title}/{ds}", load=tables_load, child=tables_child)

    if dataset is not None:
        return dataset_node(dataset)

    def load() -> pl.DataFrame:
        datasets = [d.dataset_id for d in client().list_datasets()]
        return pl.DataFrame({"dataset": datasets}, schema={"dataset": pl.Utf8})

    def child(row: dict[str, Any]) -> CatalogNode:
        return dataset_node(row["dataset"])

    return CatalogNode(title=title, load=load, child=child)


def postgres_context_node(dsn: str) -> CatalogNode:
    """schemas -> tables -> flat columns, a throwaway connection per load."""

    def query(sql: str, params: tuple | None = None) -> list[tuple]:
        from .registry import EXECUTORS

        con = EXECUTORS.get("postgres").make_connection(dsn)
        try:
            return con.execute(sql, params).fetchall()
        finally:
            con.close()

    def load() -> pl.DataFrame:
        rows = query(
            "SELECT schema_name FROM information_schema.schemata"
            " WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
            " ORDER BY schema_name"
        )
        return pl.DataFrame({"schema": [r[0] for r in rows]}, schema={"schema": pl.Utf8})

    def child(row: dict[str, Any]) -> CatalogNode:
        schema = row["schema"]

        def tables_load() -> pl.DataFrame:
            rows = query(
                "SELECT table_name FROM information_schema.tables"
                " WHERE table_schema = %s ORDER BY table_name",
                (schema,),
            )
            return pl.DataFrame({"table": [r[0] for r in rows]}, schema={"table": pl.Utf8})

        def tables_child(trow: dict[str, Any]) -> CatalogNode:
            table = trow["table"]

            def columns_load() -> pl.DataFrame:
                rows = query(
                    "SELECT column_name, data_type, is_nullable,"
                    " col_description(format('%%I.%%I', table_schema, table_name)::regclass,"
                    "                 ordinal_position)"
                    " FROM information_schema.columns"
                    " WHERE table_schema = %s AND table_name = %s"
                    " ORDER BY ordinal_position",
                    (schema, table),
                )
                return _field_path_frame(
                    [
                        (n, n, t, "REQUIRED" if nullable == "NO" else "", comment or "")
                        for n, t, nullable, comment in rows
                    ]
                )

            return CatalogNode(
                title=f"{schema}.{table}",
                load=columns_load,
                key=f"postgres:{dsn}/{schema}.{table}",
            )

        return CatalogNode(
            title=f"postgres/{schema}",
            load=tables_load,
            child=tables_child,
            key=f"postgres:{dsn}/{schema}",
        )

    return CatalogNode(title="postgres", load=load, child=child, key=f"postgres:{dsn}")


def _spec_path(spec: Any) -> str | None:
    return spec.get("path") if isinstance(spec, dict) else spec


def sqlite_context_node(spec: Any, root: Path) -> CatalogNode:
    """tables -> flat columns via PRAGMA table_info."""
    path = _spec_path(spec)
    if not path or path == ":memory:":
        # only the run session's shared connection holds it; don't race that
        return _info_node("sqlite::memory:", "in-memory sqlite context — not browsable")
    db = str(root / path)

    def load() -> pl.DataFrame:
        con = sqlite3.connect(db)
        try:
            rows = con.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
            ).fetchall()
        finally:
            con.close()
        return pl.DataFrame({"table": [r[0] for r in rows]}, schema={"table": pl.Utf8})

    def child(row: dict[str, Any]) -> CatalogNode:
        table = row["table"]

        def columns_load() -> pl.DataFrame:
            con = sqlite3.connect(db)
            try:
                quoted = table.replace('"', '""')
                info = con.execute(f'PRAGMA table_info("{quoted}")').fetchall()
            finally:
                con.close()
            return _field_path_frame(
                [
                    (name, name, type_ or "", "REQUIRED" if notnull else "", "")
                    for _, name, type_, notnull, *_ in info
                ]
            )

        return CatalogNode(title=f"sqlite:{path}/{table}", load=columns_load)

    return CatalogNode(title=f"sqlite:{path}", load=load, child=child)


def duckdb_context_node(spec: Any, root: Path) -> CatalogNode:
    """tables -> field paths on a duckdb database file.

    Connecting by path shares the in-process instance with a conduit that
    targets the same file, so no handle conflict arises here.
    """
    path = _spec_path(spec)
    if not path:
        return _info_node("duckdb::memory:", "in-memory duckdb context — not browsable")
    db = str(root / path)

    def load() -> pl.DataFrame:
        con = duckdb.connect(db)
        try:
            rows = con.execute(
                "SELECT table_schema, table_name FROM information_schema.tables"
                " ORDER BY table_schema, table_name"
            ).fetchall()
        finally:
            con.close()
        return pl.DataFrame(
            {"schema": [r[0] for r in rows], "table": [r[1] for r in rows]},
            schema={"schema": pl.Utf8, "table": pl.Utf8},
        )

    def child(row: dict[str, Any]) -> CatalogNode:
        schema, table = row["schema"], row["table"]

        def fields_load() -> pl.DataFrame:
            con = duckdb.connect(db)
            try:
                rel = con.sql(f'SELECT * FROM "{schema}"."{table}" LIMIT 0')
                return duckdb_field_paths(rel.columns, rel.types)
            finally:
                con.close()

        return CatalogNode(title=f"duckdb:{path}/{schema}.{table}", load=fields_load)

    return CatalogNode(title=f"duckdb:{path}", load=load, child=child)


def _postgres_from_config(config: Any) -> CatalogNode | None:
    from .executors.postgres_exec import _dsn

    dsn = _dsn(config)
    return postgres_context_node(dsn) if dsn else None


_CONTEXT_BUILDERS: dict[str, Callable[[Any, Path], CatalogNode | None]] = {
    "bigquery": lambda config, root: bigquery_context_node(input_spec(config, "bigquery")),
    "postgres": lambda config, root: _postgres_from_config(config),
    "sqlite": lambda config, root: sqlite_context_node(input_spec(config, "sqlite"), root),
    "duckdb": lambda config, root: duckdb_context_node(input_spec(config, "duckdb"), root),
}


def _bigquery_target(target: str) -> tuple[str, CatalogNode]:
    """A @catalog bigquery target: ``project`` or ``project.dataset``."""
    project, _, dataset = target.partition(".")
    if dataset:
        return f"bigquery:{project}/{dataset}", bigquery_context_node(
            {"project": project}, dataset=dataset
        )
    return f"bigquery:{project}", bigquery_context_node({"project": project})


# @catalog directive targets: engine -> (target string, root) -> (row name, node)
_CATALOG_TARGETS: dict[str, Callable[[str, Path], tuple[str, CatalogNode]]] = {
    "bigquery": lambda target, root: _bigquery_target(target),
    "postgres": lambda target, root: (f"postgres:{target}", postgres_context_node(target)),
    "sqlite": lambda target, root: (f"sqlite:{target}", sqlite_context_node(target, root)),
    "duckdb": lambda target, root: (f"duckdb:{target}", duckdb_context_node(target, root)),
}


def project_root_node(project: Project, connect: Callable[[], Any] | None = None) -> CatalogNode:
    """Top level: the project's engine contexts (deduped), @catalog-declared
    browse targets, and cell outputs."""
    from .registry import EXECUTORS

    contexts: list[tuple[str, str, Any]] = []  # (context_key, engine, config)
    seen: set[str] = set()
    for name in project.order:
        cell = project.cells[name]
        key = EXECUTORS.get(cell.engine).context_key(cell.config)
        if key not in seen:
            seen.add(key)
            contexts.append((key, cell.engine, cell.config))

    declared: dict[str, CatalogNode] = {}  # row name -> ready node
    rows = [("context", key, engine, "") for key, engine, _ in contexts]
    for engine, targets in (getattr(project.config, "catalog", None) or {}).items():
        make_target = _CATALOG_TARGETS.get(engine)
        if make_target is None:
            continue
        for target in targets:
            name, node = make_target(target, project.root)
            if name in seen:
                continue  # a cell's context already covers it
            seen.add(name)
            declared[name] = node
            rows.append(("context", name, engine, "@catalog"))

    for name in project.order:
        cell = project.cells[name]
        rows.append(("output", name, cell.engine, f"{cell.engine} → {cell.sink_type}"))
    frame = pl.DataFrame(
        rows,
        schema={"kind": pl.Utf8, "name": pl.Utf8, "engine": pl.Utf8, "detail": pl.Utf8},
        orient="row",
    )
    by_key = {key: (engine, config) for key, engine, config in contexts}

    def child(row: dict[str, Any]) -> CatalogNode | None:
        if row["kind"] == "output":
            cell = project.cells[row["name"]]
            return sink_output_node(cell.name, cell.config, project.root, connect=connect)
        if row["name"] in declared:
            return declared[row["name"]]
        engine, config = by_key[row["name"]]
        builder = _CONTEXT_BUILDERS.get(engine)
        return builder(config, project.root) if builder else None

    return CatalogNode(title="catalog", load=lambda: frame, child=child)
