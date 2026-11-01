"""The primary executor: duckdb cells run on the conduit itself."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from ..conduit import ConduitSession, duckdb_field_paths
from ..models import CatalogNode, RunContext
from ..registry import executor
from .base import EngineSession, Executor, info_node


@executor("duckdb")
class DuckDBExecutor(Executor):
    dialect = "duckdb"
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"duckdb:{self.input_spec(config).get('path') or ':memory:'}"

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        return ConduitSession(ctx.conn)

    def catalog(self, config: Any, root: Path) -> CatalogNode:
        return self.context_node(self.input_spec(config).get("path"), root)

    def catalog_target(self, target: str, root: Path) -> tuple[str, CatalogNode]:
        return f"duckdb:{target}", self.context_node(target, root)

    def catalog_table(self, config: Any, root: Path, parts: list[str]) -> CatalogNode | None:
        node = self.catalog(config, root)
        schema = parts[-2] if len(parts) >= 2 else "main"
        return node.child({"schema": schema, "table": parts[-1]}) if node.child else None

    def context_node(self, path: str | None, root: Path) -> CatalogNode:
        """tables -> field paths on a duckdb database file.

        Connecting by path shares the in-process instance with a conduit that
        targets the same file, so no handle conflict arises here.
        """
        if not path:
            return info_node("duckdb::memory:", "in-memory duckdb context — not browsable")
        db = str(root / path)

        def load() -> Any:
            import polars as pl

            con = duckdb.connect(db)
            try:
                rows = con.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables"
                    " ORDER BY table_schema, table_name"
                ).fetchall()
            finally:
                con.close()
            return pl.DataFrame(
                {"schema": [r[0] for r in rows], "table": [r[1] for r in rows]},
                schema={"schema": pl.Utf8, "table": pl.Utf8},
            )

        def child(row: dict[str, Any]) -> CatalogNode:
            schema, table = row["schema"], row["table"]

            def fields_load() -> Any:
                con = duckdb.connect(db)
                try:
                    rel = con.sql(f'SELECT * FROM "{schema}"."{table}" LIMIT 0')
                    return duckdb_field_paths(rel.columns, rel.types)
                finally:
                    con.close()

            return CatalogNode(title=f"duckdb:{path}/{schema}.{table}", load=fields_load)

        return CatalogNode(title=f"duckdb:{path}", load=load, child=child)
