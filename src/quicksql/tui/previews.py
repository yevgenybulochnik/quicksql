"""What the Data tab shows for a cell: the preview its last run cached, else
its landed output read back through DuckDB, the head only."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import polars as pl

from ..conduit import landed_relation, preview
from ..conduit import read_landed as conduit_read_landed
from ..models import Preview, RunResult

if TYPE_CHECKING:
    from ..compiler import Project

PREVIEW_ROWS = 100


def preview_frame(
    project: Project | None, result: RunResult | None, name: str, connect: Callable[[], Any]
) -> pl.DataFrame:
    """The frame to show for cell ``name``. ``connect`` opens the DuckDB handle
    a landed output is read back through (a cursor of the live conduit)."""
    if result is not None and result.preview is not None:
        return frame_of(result.preview)
    if result is not None and result.ok and result.target and result.target.endswith(".parquet"):
        return read_parquet_head(result.target)
    if project and name in project.cells:
        landed = read_landed(project, name, connect)
        if landed is not None:
            return landed
    return pl.DataFrame({"info": [f"no output for {name!r} yet — press r to run"]})


def frame_of(head: Preview) -> pl.DataFrame:
    """A Polars frame of a run's plain preview, its dtypes inferred from the
    values (as the runner's own frame was before the core dropped Polars)."""
    return pl.DataFrame(list(head.rows), schema=list(head.columns), orient="row")


def read_parquet_head(target: str) -> pl.DataFrame:
    """Preview through duckdb's parquet reader, never polars' — the polars
    reader panics on duckdb-written INTERVAL columns, and duckdb only reads
    the head row groups for a LIMIT."""
    import duckdb

    con = duckdb.connect()
    try:
        return frame_of(preview(con, f"read_parquet('{target}')", PREVIEW_ROWS))
    finally:
        con.close()


def read_landed(project: Project, name: str, connect: Callable[[], Any]) -> pl.DataFrame | None:
    """An earlier run's output, read back through the cell's own sink but
    never its prepare() (``conduit.read_landed``): previews follow the cursor,
    so a database sink previews only once a run this session has attached it."""
    relation = landed_relation(project, name)
    if relation is None:
        sink = project.cells[name].sink_type
        return pl.DataFrame(
            {"info": [f"{name!r} is effect-only (output: {sink}): it lands nothing to preview"]}
        )
    con = connect()
    try:
        head = conduit_read_landed(con, relation, PREVIEW_ROWS)
        return None if head is None else frame_of(head)
    finally:
        con.close()
