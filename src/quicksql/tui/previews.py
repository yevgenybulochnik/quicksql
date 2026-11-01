"""What the Data tab shows for a cell: the preview its last run cached, else
its landed output read back through DuckDB, the head only."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from ..conduit import preview
from ..config import make_sink
from ..errors import QsqlError
from ..models import RunResult

if TYPE_CHECKING:
    from ..compiler import Project

PREVIEW_ROWS = 100


def preview_frame(
    project: Project | None, result: RunResult | None, name: str, connect: Callable[[], Any]
) -> pl.DataFrame:
    """The frame to show for cell ``name``. ``connect`` opens the DuckDB handle
    a landed output is read back through (a cursor of the live conduit)."""
    if result is not None and isinstance(result.preview, pl.DataFrame):
        return result.preview
    if result is not None and result.ok and result.target and result.target.endswith(".parquet"):
        return read_parquet_head(result.target)
    if project and name in project.cells:
        landed = read_landed(project, name, connect)
        if landed is not None:
            return landed
    return pl.DataFrame({"info": [f"no output for {name!r} yet — press r to run"]})


def read_parquet_head(target: str) -> pl.DataFrame:
    """Preview through duckdb's parquet reader, never polars' — the polars
    reader panics on duckdb-written INTERVAL columns, and duckdb only reads
    the head row groups for a LIMIT."""
    import duckdb

    con = duckdb.connect()
    try:
        return preview(con, f"read_parquet('{target}')", PREVIEW_ROWS)
    finally:
        con.close()


def read_landed(project: Project, name: str, connect: Callable[[], Any]) -> pl.DataFrame | None:
    """An earlier run's output, read back through the cell's own sink but
    never its prepare(): previews follow the cursor, and an ATTACH creates a
    missing database file or connects to a server. So a database sink
    previews only once a run this session has attached it to the conduit."""
    import duckdb

    sink = make_sink(project.cells[name].config, project.root, project.registries)
    if not sink.lands_output:
        return pl.DataFrame(
            {"info": [f"{name!r} is effect-only (output: {sink.name}): it lands nothing to preview"]}
        )
    con = connect()
    try:
        return preview(con, sink.ref_expr(name), PREVIEW_ROWS)
    except (duckdb.Error, QsqlError):
        return None  # never landed, or its database isn't attached this session
    finally:
        con.close()
