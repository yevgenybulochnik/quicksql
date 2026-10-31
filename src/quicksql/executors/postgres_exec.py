"""Postgres executor: runs cells next to the data via psycopg.

Needs the ``postgres`` extra (psycopg). The DSN may reference env vars ("$PG_DSN").
"""

from __future__ import annotations

import os
from typing import Any

from ..config import input_spec
from ..errors import ExecutorError
from ..models import RunContext
from ..registry import executor
from .base import DbApiSession, EngineSession, Executor


def _dsn(config: Any) -> str | None:
    dsn = input_spec(config, "postgres").get("dsn")
    return os.path.expandvars(dsn) if dsn else None


class PostgresSession(DbApiSession):
    engine = "postgres"
    temp_schema = "pg_temp"

    def column_name(self, column: Any) -> str:
        return column.name

    def alive(self) -> bool:
        # psycopg marks a connection broken once an operation finds it dead
        return not (self.con.closed or getattr(self.con, "broken", False))


@executor("postgres")
class PostgresExecutor(Executor):
    supports_context_refs = True
    input_shorthand = "dsn"

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

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        dsn = _dsn(config)
        if not dsn:
            raise ExecutorError("postgres engine needs input.postgres.dsn")
        return PostgresSession(self.make_connection(dsn))
