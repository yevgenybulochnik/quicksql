"""Sink into a DuckDB database file, ATTACHed onto the conduit connection."""

from __future__ import annotations

from typing import Any

from ..models import RenderedCell
from ..registry import sink
from .base import Sink, path_alias


@sink("duckdb")
class DuckDBSink(Sink):
    @property
    def db_path(self) -> str:
        return str(self.root / self.cfg.get("path", "warehouse.db"))

    @property
    def alias(self) -> str:
        return path_alias("qsql_db", self.db_path)

    @property
    def schema(self) -> str:
        return self.cfg.get("schema") or "main"

    def _target(self, cell_name: str) -> str:
        table = self.cfg.get("table") or cell_name
        return f'{self.alias}."{self.schema}"."{table}"'

    def prepare(self, conn: Any) -> None:
        conn.execute(f"ATTACH IF NOT EXISTS '{self.db_path}' AS {self.alias}")
        if self.schema != "main":
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS {self.alias}."{self.schema}"')

    def write(self, cell: RenderedCell, view: str, conn: Any) -> tuple[int, str]:
        target = self._target(cell.name)
        if self.cfg.get("mode", "replace") == "append":
            conn.execute(
                f'CREATE TABLE IF NOT EXISTS {target} AS SELECT * FROM "{view}" WHERE 1=0'
            )
            insert = f'INSERT INTO {target} SELECT * FROM "{view}"'
            (rows,) = conn.execute(insert).fetchone()  # this run's rows, not the table's
        else:
            create = f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM "{view}"'
            (rows,) = conn.execute(create).fetchone()
        return int(rows), f"{self.db_path}::{self.schema}.{self.cfg.get('table') or cell.name}"

    def ref_expr(self, cell_name: str) -> str:
        return self._target(cell_name)
