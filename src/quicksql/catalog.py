"""The schema catalog the TUI browses: a project's contexts, its @catalog
targets and its cells' landed outputs, as drillable ``CatalogNode``s.

Pure data + blocking loaders; the TUI runs ``load()`` in a thread worker and
pushes the frame as a Sheet whose Enter drills via ``child``. Each engine's
executor builds its own contexts' trees (``Executor.catalog``); outputs are
read back through DuckDB. Leaves load field-path frames
(``executors.base.field_path_frame``).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import duckdb
import polars as pl

from .conduit import duckdb_field_paths, load_extension
from .config import make_sink
from .models import CatalogNode

if TYPE_CHECKING:
    from .compiler import Project
    from .registry import Registries

__all__ = ["CatalogCache", "CatalogNode", "project_root_node", "sink_output_node"]


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


# ---------- node builders ----------


def sink_output_node(
    cell_name: str,
    config: Any,
    root: Path,
    registries: Registries,
    connect: Callable[[], Any] | None = None,
) -> CatalogNode:
    """Leaf: field paths of a cell's landed sink, read back via its ref_expr.

    ``connect`` supplies the DuckDB connection; pass a cursor factory of the
    live conduit when it may already hold the sink's ATTACH (a throwaway
    connection cannot re-ATTACH a file another connection holds).
    """

    def load() -> pl.DataFrame:
        sink = make_sink(config, root, registries)
        con = connect() if connect is not None else duckdb.connect()
        try:
            for ext in sink.requires:
                load_extension(con, ext)
            sink.prepare(con)
            rel = con.sql(f"SELECT * FROM {sink.ref_expr(cell_name)} LIMIT 0")
            return duckdb_field_paths(rel.columns, rel.types)
        finally:
            con.close()

    return CatalogNode(title=f"schema({cell_name})", load=load)


def project_root_node(project: Project, connect: Callable[[], Any] | None = None) -> CatalogNode:
    """Top level: the project's engine contexts (deduped), @catalog-declared
    browse targets, and cell outputs."""
    registries = project.registries
    contexts: list[tuple[str, str, Any]] = []  # (context_key, engine, config)
    seen: set[str] = set()
    for name in project.order:
        cell = project.cells[name]
        key = registries.executors.get(cell.engine).context_key(cell.config)
        if key not in seen:
            seen.add(key)
            contexts.append((key, cell.engine, cell.config))

    declared: dict[str, CatalogNode] = {}  # row name -> ready node
    rows = [("context", key, engine, "") for key, engine, _ in contexts]
    for engine, targets in (getattr(project.config, "catalog", None) or {}).items():
        executor = registries.executors.get(engine)
        for target in targets:
            made = executor.catalog_target(target, project.root)
            if made is None:
                break  # the engine takes no @catalog targets
            name, node = made
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
            return sink_output_node(
                cell.name, cell.config, project.root, registries, connect=connect
            )
        if row["name"] in declared:
            return declared[row["name"]]
        engine, config = by_key[row["name"]]
        return registries.executors.get(engine).catalog(config, project.root)

    return CatalogNode(title="catalog", load=lambda: frame, child=child)
