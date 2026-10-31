"""Zero-dependency executor: proves cross-engine flow entirely offline."""

from __future__ import annotations

import sqlite3
from typing import Any

import polars as pl

from ..config import input_spec
from ..errors import ExecutorError
from ..models import RenderedCell, RunContext
from ..registry import SINKS, executor
from .base import Executor, register_frame, result_view


@executor("sqlite")
class SQLiteExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"sqlite:{input_spec(config, 'sqlite').get('path') or ':memory:'}"

    def _connect(self, cell: RenderedCell, ctx: RunContext) -> tuple[sqlite3.Connection, bool]:
        """The context session's shared connection (temp tables live there;
        :memory: contexts only exist there), or an ephemeral one without a session."""
        path = input_spec(cell.config, "sqlite").get("path")
        db = str(ctx.root / path) if path and path != ":memory:" else (path or ":memory:")
        if ctx.session is None:
            return sqlite3.connect(db), True
        key = self.context_key(cell.config)
        if key not in ctx.session.engine_sessions:
            # runs are serialized (exclusive workers); threads change between them
            ctx.session.engine_sessions[key] = sqlite3.connect(db, check_same_thread=False)
        return ctx.session.engine_sessions[key], False

    def execute(self, cell: RenderedCell, ctx: RunContext) -> str:
        con, ephemeral = self._connect(cell, ctx)
        *setup, final = self.split_statements(cell.sql)
        try:
            if not SINKS.get(cell.sink_type).lands_output:
                # effect-only: run every statement for its side effects, land nothing
                for stmt in [*setup, final]:
                    con.execute(stmt)
                return ""
            for stmt in setup:  # build-up statements run for effect on the connection
                con.execute(stmt)
            if cell.reffed_in_context:
                con.execute(f'DROP TABLE IF EXISTS temp."{cell.name}"')
                con.execute(f'CREATE TEMP TABLE "{cell.name}" AS {final}')
                cur = con.execute(f'SELECT * FROM temp."{cell.name}"')
            else:
                cur = con.execute(final)
            if cur.description is None:
                raise ExecutorError("sqlite query returned no result set")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        except sqlite3.Error as exc:
            raise ExecutorError(f"sqlite: {exc}") from exc
        finally:
            if ephemeral:
                con.close()
        frame = pl.DataFrame(rows, schema=cols, orient="row")
        return register_frame(ctx, result_view(cell.name), frame)
