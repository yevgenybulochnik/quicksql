"""Executor: run a cell's SQL on a backend, land the result as a view DuckDB can read."""

from __future__ import annotations

import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

import sqlglot
from sqlglot.tokens import TokenType

from ..models import RenderedCell, RunContext


def strip_trailing_semicolon(sql: str) -> str:
    return sql.strip().rstrip(";").strip()


def statement_spans(sql: str, dialect: str) -> list[tuple[int, int]]:
    """(start, end) char offsets of each top-level statement in ``sql``.

    Only the tokenizer is used to locate semicolons, so a ``;`` inside a string
    literal or comment is not a split point. Segments holding no real token — a
    run of comments/whitespace, e.g. a collapsed Jinja ``{% if %}`` or a
    trailing-comment tail after the final ``;`` — are dropped. Spans are the raw
    source segments between semicolons (leading/trailing whitespace kept), so
    they tile the source and a cursor offset always lands in one. Returns ``[]``
    on a tokenizer error (e.g. an unterminated literal); the caller decides the
    fallback.
    """
    try:
        tokens = sqlglot.Dialect.get_or_raise(dialect).tokenize(sql)
    except Exception:
        return []
    spans: list[tuple[int, int]] = []
    seg_start, has_token = 0, False
    for tok in tokens:
        if tok.token_type == TokenType.SEMICOLON:
            if has_token:
                spans.append((seg_start, tok.start))
            seg_start, has_token = tok.end + 1, False
        else:
            has_token = True
    if has_token:
        spans.append((seg_start, len(sql)))
    return spans


def split_statements(sql: str, dialect: str) -> list[str]:
    """Split a cell body into individual statements on top-level semicolons.

    Each statement is its *original* text (sqlglot's re-render would rewrite
    dialect-specific syntax, e.g. LIST_TRANSFORM/spacing), so we slice the source
    at the tokenizer's semicolon offsets. On a tokenizer error the whole body is
    treated as one statement so the engine reports the real syntax error, not us.
    """
    stmts = [sql[a:b].strip() for a, b in statement_spans(sql, dialect)]
    return [s for s in stmts if s] or [strip_trailing_semicolon(sql)]


def result_view(cell_name: str) -> str:
    return f"__qsql_{cell_name}"


def register_frame(ctx: RunContext, view: str, frame: Any) -> str:
    """Expose an extracted result on the conduit as `view`.

    Polars frames roundtrip through a temp parquet file (duckdb.register on a
    polars frame needs pyarrow, which only the bigquery extra ships); anything
    else (e.g. a real pyarrow Table) registers directly.
    """
    import polars as pl

    if isinstance(frame, pl.DataFrame):
        if ctx.tmpdir is None:
            ctx.tmpdir = tempfile.mkdtemp(prefix="qsql-run-")
        tmp = Path(ctx.tmpdir) / f"{view}.parquet"
        frame.write_parquet(tmp)
        ctx.conn.execute(
            f'CREATE OR REPLACE TEMP VIEW "{view}" AS SELECT * FROM read_parquet(\'{tmp}\')'
        )
    else:
        ctx.conn.register(view, _normalize_arrow(ctx, frame))
    return view


def _normalize_arrow(ctx: RunContext, frame: Any) -> Any:
    """duckdb cannot scan decimal256 (BigQuery BIGNUMERIC) at any precision:
    downcast to decimal128 when the precision fits DECIMAL(38), else cast to
    string — lossless text, since no duckdb decimal can hold the value."""
    try:
        import pyarrow as pa
    except ImportError:
        return frame
    if not isinstance(frame, pa.Table):
        return frame
    for index, fld in enumerate(frame.schema):
        if not isinstance(fld.type, pa.Decimal256Type):
            continue
        if fld.type.precision <= 38:
            target = pa.decimal128(fld.type.precision, fld.type.scale)
        else:
            target = pa.string()
            ctx.log.append(
                f"column {fld.name!r}: decimal256({fld.type.precision},{fld.type.scale}) "
                f"exceeds duckdb's DECIMAL(38); cast to string"
            )
        frame = frame.set_column(index, fld.name, frame.column(index).cast(target))
    return frame


class Executor(ABC):
    name: ClassVar[str] = ""
    # engines that keep a per-context session where same-context cells can
    # reference each other as temp tables (in the engine's own dialect)
    supports_context_refs: ClassVar[bool] = False
    # engines that can materialize a multi-statement cell as an in-context temp
    # (run its setup statements, then wrap the terminal statement in CREATE TEMP
    # TABLE AS). False for engines with native scripting that can't be split
    # client-side, so their whole body can't be wrapped (bigquery).
    supports_multistatement_materialization: ClassVar[bool] = True
    # the key a bare string in the cell's input means: {sqlite: x.db} is {path: x.db}
    input_shorthand: ClassVar[str] = "path"

    def context_key(self, config: Any) -> str:
        """Cells with equal keys share an execution context (engine + target)."""
        return f"{self.name}:"

    def split_statements(self, sql: str) -> list[str]:
        """Split the cell body into statements. The engine name doubles as the
        sqlglot dialect for all four engines (see lsp `_DIALECT`)."""
        return split_statements(sql, self.name)

    @abstractmethod
    def execute(self, cell: RenderedCell, ctx: RunContext) -> str:
        """Materialize the cell's result on ctx.conn; return the view name."""
