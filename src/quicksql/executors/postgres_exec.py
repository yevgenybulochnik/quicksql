"""Postgres executor: runs cells next to the data via psycopg.

Needs the ``postgres`` extra (psycopg). The DSN may reference env vars ("$PG_DSN").
"""

from __future__ import annotations

import os
from typing import Any

import polars as pl

from ..errors import ExecutorError
from ..models import RenderedCell, RunContext
from ..registry import SINKS, executor
from .base import Executor, register_frame, result_view


def _dsn(config: Any) -> str | None:
    spec = (config.input or {}).get("postgres")
    dsn = spec.get("dsn") if isinstance(spec, dict) else spec
    return os.path.expandvars(dsn) if dsn else None


@executor("postgres")
class PostgresExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"postgres:{_dsn(config) or ''}"

    def make_connection(self, dsn: str) -> Any:
        """Split out so tests can substitute a fake connection."""
        try:
            import psycopg
        except ImportError as exc:
            raise ExecutorError(
                "postgres engine needs the 'postgres' extra: uv sync --extra postgres"
            ) from exc
        # autocommit: session temps stay visible across cells, and a failed
        # statement can't leave the shared connection idle-in-transaction
        return psycopg.connect(dsn, autocommit=True)

    def _connect(self, cell: RenderedCell, ctx: RunContext) -> tuple[Any, bool]:
        """The context session's shared connection (temp tables live there),
        or an ephemeral one without a session."""
        dsn = _dsn(cell.config)
        if not dsn:
            raise ExecutorError("postgres engine needs input.postgres.dsn")
        if ctx.session is None:
            return self.make_connection(dsn), True
        key = self.context_key(cell.config)
        if key not in ctx.session.engine_sessions:
            ctx.session.engine_sessions[key] = self.make_connection(dsn)
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
                # pg_temp: unqualified, the name resolves through the search_path to
                # a real table whenever this session has no temp of that name yet
                con.execute(f'DROP TABLE IF EXISTS pg_temp."{cell.name}"')
                con.execute(f'CREATE TEMP TABLE "{cell.name}" AS {final}')
                cur = con.execute(f'SELECT * FROM "{cell.name}"')
            else:
                cur = con.execute(final)
            if cur.description is None:
                raise ExecutorError("postgres query returned no result set")
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
        except ExecutorError:
            raise
        except Exception as exc:  # psycopg may be absent offline; wrap any driver failure
            raise ExecutorError(f"postgres: {exc}") from exc
        finally:
            if ephemeral:
                con.close()
        frame = pl.DataFrame(rows, schema=cols, orient="row")
        return register_frame(ctx, result_view(cell.name), frame)
