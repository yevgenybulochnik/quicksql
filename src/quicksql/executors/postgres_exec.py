"""Postgres executor: runs cells next to the data via psycopg.

Needs the ``postgres`` extra (psycopg). The DSN may reference env vars ("$PG_DSN").
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..errors import ExecutorError
from ..models import RunContext
from ..registry import executor
from .base import DbApiSession, EngineSession, Executor, engine_input


def _dsn(config: Any) -> str | None:
    dsn = engine_input(config, "postgres", PostgresExecutor.input_shorthand).get("dsn")
    return os.path.expandvars(dsn) if dsn else None


# password and sslpassword, as libpq keywords (host=h password=x) or URI params
_KEYWORD_SECRET = re.compile(r"\b((?:ssl)?password)\s*=\s*(?:'(?:[^'\\]|\\.)*'|\S+)", re.I)
_QUERY_SECRET = re.compile(r"(^|&)((?:ssl)?password)=[^&]*", re.I)


def _redacted(dsn: str) -> str:
    """The DSN with every password as ***, for showing: a context key appears in
    explain, the catalog and the TUI's log, and an env var DSN's secret isn't
    in the notebook."""
    if "://" not in dsn:
        return _KEYWORD_SECRET.sub(r"\1=***", dsn)
    parts = urlsplit(dsn)
    netloc = parts.netloc
    userinfo, at, host = netloc.rpartition("@")
    if at and ":" in userinfo:
        netloc = f"{userinfo.split(':', 1)[0]}:***@{host}"
    query = _QUERY_SECRET.sub(r"\1\2=***", parts.query)
    return urlunsplit(parts._replace(netloc=netloc, query=query))


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
        """The DSN, password hidden: DSNs that differ only in it share a context."""
        dsn = _dsn(config)
        return f"postgres:{_redacted(dsn) if dsn else ''}"

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
