"""Postgres executor: runs cells next to the data via psycopg.

Needs the ``postgres`` extra (psycopg). The DSN may reference env vars ("$PG_DSN").
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..errors import ExecutorError
from ..models import CatalogNode, RunContext
from ..registry import executor
from .base import DbApiSession, EngineSession, Executor, engine_input, field_path_frame


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


def _context_key(dsn: str | None) -> str:
    """The DSN, password hidden: DSNs that differ only in it share a context."""
    return f"postgres:{_redacted(dsn) if dsn else ''}"


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
    dialect = "postgres"
    supports_context_refs = True
    input_shorthand = "dsn"

    def context_key(self, config: Any) -> str:
        return _context_key(_dsn(config))

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

    def catalog(self, config: Any, root: Path) -> CatalogNode | None:
        dsn = _dsn(config)
        return self.context_node(dsn) if dsn else None

    def catalog_target(self, target: str, root: Path) -> tuple[str, CatalogNode]:
        """A DSN, read like an input's: env vars expanded, shown password hidden."""
        dsn = os.path.expandvars(target)
        return _context_key(dsn), self.context_node(dsn)

    def catalog_table(self, config: Any, root: Path, parts: list[str]) -> CatalogNode | None:
        node = self.catalog(config, root)
        if node is None or node.child is None:
            return None
        tables = node.child({"schema": parts[-2] if len(parts) >= 2 else "public"})
        return tables.child({"table": parts[-1]}) if tables and tables.child else None

    def context_node(self, dsn: str) -> CatalogNode:
        """schemas -> tables -> flat columns, a throwaway connection per load."""
        import polars as pl

        def query(sql: str, params: tuple | None = None) -> list[tuple]:
            con = self.make_connection(dsn)
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
                    return field_path_frame(
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
