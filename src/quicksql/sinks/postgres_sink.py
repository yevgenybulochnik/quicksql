"""Sink into Postgres through DuckDB's postgres extension (ATTACH ... TYPE postgres).

No Python Postgres driver needed; the DSN may reference env vars ("$PG_DSN").
"""

from __future__ import annotations

import os
from typing import Any

from ..errors import SinkError
from ..models import RenderedCell
from ..registry import sink
from .base import Sink, path_alias


@sink("postgres")
class PostgresSink(Sink):
    requires = ["postgres"]

    @property
    def dsn(self) -> str:
        dsn = self.cfg.get("dsn")
        if not dsn:
            raise SinkError("postgres sink needs output.dsn (e.g. \"$PG_DSN\")")
        return os.path.expandvars(dsn)

    @property
    def alias(self) -> str:
        return path_alias("qsql_pg", self.dsn)

    def _schema_table(self, cell_name: str) -> tuple[str, str]:
        table = self.cfg.get("table") or cell_name
        if "." in table:
            schema, table = table.split(".", 1)
        else:
            schema = self.cfg.get("schema") or "public"
        return schema, table

    def _target(self, cell_name: str) -> str:
        schema, table = self._schema_table(cell_name)
        return f'{self.alias}."{schema}"."{table}"'

    def prepare(self, conn: Any) -> None:
        conn.execute(f"ATTACH IF NOT EXISTS '{self.dsn}' AS {self.alias} (TYPE postgres)")

    def write(self, cell: RenderedCell, view: str, conn: Any) -> tuple[int, str]:
        target = self._target(cell.name)
        if self.cfg.get("mode", "replace") == "append":
            insert = f'INSERT INTO {target} SELECT * FROM "{view}"'
            (rows,) = conn.execute(insert).fetchone()  # this run's rows, not the table's
        else:
            conn.execute(f"DROP TABLE IF EXISTS {target}")
            (rows,) = conn.execute(f'CREATE TABLE {target} AS SELECT * FROM "{view}"').fetchone()
        schema, table = self._schema_table(cell.name)
        return int(rows), f"postgres::{schema}.{table}"

    def ref_expr(self, cell_name: str) -> str:
        return self._target(cell_name)
