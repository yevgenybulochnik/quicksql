"""Executors and their sessions: where a cell's SQL runs.

An ``Executor`` names an engine and opens an ``EngineSession`` per execution
context (engine + connection target). The runner drives the session through the
cell protocol (``runner.execute_cell``): setup statements run for effect, then
the terminal statement is materialized as a temp for same-context consumers
and/or extracted onto the DuckDB conduit, where every sink lands from.
"""

from __future__ import annotations

import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

import sqlglot
from sqlglot.tokens import TokenType

from ..errors import ExecutorError
from ..models import RunContext


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


def unwrap_error_context(message: str, prefix: str) -> str:
    """Postgres and DuckDB quote a statement's first line under its error
    (``LINE 1: <sql>``, a caret beneath). For a cell's SQL that quicksql
    wrapped (``CREATE TEMP TABLE x AS <sql>``), cut ``prefix`` from that line and
    move the caret with it, so the quote is the cell's own SQL. A line the
    engine already trimmed (``LINE 1: ...``) has lost the prefix; it's kept."""
    lead = "LINE 1: "
    lines = message.split("\n")
    for i, (line, caret) in enumerate(zip(lines, lines[1:])):
        if (
            line.startswith(lead + prefix)
            and caret.strip() == "^"
            and caret.index("^") >= len(lead) + len(prefix)
        ):
            lines[i] = lead + line[len(lead) + len(prefix) :]
            lines[i + 1] = caret[len(prefix) :]
            return "\n".join(lines)
    return message


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
    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        """Connect to the context ``config`` names. The runner keeps the session
        for every cell sharing its ``context_key`` and closes it with the run
        session (or right after the cell, when there is no run session)."""


class EngineSession(ABC):
    """A live connection to one execution context, opened for the context's
    first cell and shared by the rest: their temp tables live here.

    ``scoped`` says the cell creates or reads this context's temps. A session
    whose scope costs something may run unscoped statements outside it:
    BigQuery runs them as plain jobs, so a cell's script temps don't leak into
    the shared session. Engines without that cost ignore it.
    """

    @abstractmethod
    def execute(self, sql: str, *, scoped: bool = True) -> None:
        """Run one statement for its effect."""

    def fetch(self, sql: str, *, scoped: bool = True) -> Any:
        """Run a query; return its rows as a Polars DataFrame or pyarrow Table."""
        raise NotImplementedError(f"{type(self).__name__} implements neither fetch nor extract")

    def extract(self, sql: str, view: str, ctx: RunContext, *, scoped: bool = True) -> str:
        """Make the rows of ``sql`` readable on the conduit (``ctx.conn``) as
        ``view``; return the name to read them from."""
        return register_frame(ctx, view, self.fetch(sql, scoped=scoped))

    def materialize(self, name: str, sql: str) -> str:
        """Replace the session temp table ``name`` with the rows of ``sql``;
        return the query that reads it back. Only engines with
        ``supports_context_refs`` are asked."""
        raise NotImplementedError(f"{type(self).__name__} has no temp tables")

    def drop(self, name: str) -> None:
        """Drop the session temp ``name`` if it exists."""

    def alive(self) -> bool:
        """False once the connection is known dead, so the runner reopens it.
        A cheap local check, never a round trip."""
        return True

    def close(self) -> None:
        """Release the connection."""


class DbApiSession(EngineSession):
    """An engine session over a DB-API connection. SQLite and Postgres differ
    only in how they connect, which schema holds temps, and how a cursor names
    a column."""

    engine: ClassVar[str] = ""  # prefixes driver errors: "sqlite: no such table: t"
    # qualifies a DROP so it can only reach a temp: unqualified, the name
    # resolves through the search path to a real table when no temp exists yet
    temp_schema: ClassVar[str] = "temp"

    def __init__(self, con: Any) -> None:
        self.con = con

    def execute(self, sql: str, *, scoped: bool = True) -> None:
        self._run(sql)

    def fetch(self, sql: str, *, scoped: bool = True) -> Any:
        import polars as pl

        cur = self._run(sql)
        if cur.description is None:
            raise ExecutorError(f"{self.engine} query returned no result set")
        cols = [self.column_name(d) for d in cur.description]
        return pl.DataFrame(cur.fetchall(), schema=cols, orient="row")

    def materialize(self, name: str, sql: str) -> str:
        self.drop(name)
        self._run(sql, wrapper=f'CREATE TEMP TABLE "{name}" AS ')
        return f'SELECT * FROM "{name}"'  # temps shadow real tables on reads

    def drop(self, name: str) -> None:
        self._run(f'DROP TABLE IF EXISTS {self.temp_schema}."{name}"')

    def close(self) -> None:
        self.con.close()

    def column_name(self, column: Any) -> str:
        return column[0]

    def _run(self, sql: str, wrapper: str = "") -> Any:
        try:
            return self.con.execute(wrapper + sql)
        except Exception as exc:  # the driver's own errors, whatever their type
            message = unwrap_error_context(str(exc), wrapper) if wrapper else str(exc)
            raise ExecutorError(f"{self.engine}: {message}") from exc
