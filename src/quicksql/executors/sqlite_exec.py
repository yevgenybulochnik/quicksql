"""Zero-dependency executor: proves cross-engine flow entirely offline."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from ..models import CatalogNode, RunContext
from ..registry import executor
from .base import DbApiSession, EngineSession, Executor, field_path_frame, info_node


class SQLiteSession(DbApiSession):
    engine = "sqlite"


@executor("sqlite")
class SQLiteExecutor(Executor):
    dialect = "sqlite"
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"sqlite:{self.input_spec(config).get('path') or ':memory:'}"

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        """One connection per context: temp tables live there, and a :memory:
        context exists only there."""
        path = self.input_spec(config).get("path")
        db = str(ctx.root / path) if path and path != ":memory:" else ":memory:"
        # runs are serialized (exclusive workers), but their threads change
        return SQLiteSession(sqlite3.connect(db, check_same_thread=False))

    def catalog(self, config: Any, root: Path) -> CatalogNode:
        return self.context_node(self.input_spec(config).get("path"), root)

    def catalog_target(self, target: str, root: Path) -> tuple[str, CatalogNode]:
        return f"sqlite:{target}", self.context_node(target, root)

    def catalog_table(self, config: Any, root: Path, parts: list[str]) -> CatalogNode | None:
        node = self.catalog(config, root)
        return node.child({"table": parts[-1]}) if node.child else None

    def context_node(self, path: str | None, root: Path) -> CatalogNode:
        """tables -> flat columns via PRAGMA table_info."""
        if not path or path == ":memory:":
            # only the run session's shared connection holds it; don't race that
            return info_node("sqlite::memory:", "in-memory sqlite context — not browsable")
        db = str(root / path)

        def load() -> Any:
            import polars as pl

            con = sqlite3.connect(db)
            try:
                rows = con.execute(
                    "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
                ).fetchall()
            finally:
                con.close()
            return pl.DataFrame({"table": [r[0] for r in rows]}, schema={"table": pl.Utf8})

        def child(row: dict[str, Any]) -> CatalogNode:
            table = row["table"]

            def columns_load() -> Any:
                con = sqlite3.connect(db)
                try:
                    quoted = table.replace('"', '""')
                    info = con.execute(f'PRAGMA table_info("{quoted}")').fetchall()
                finally:
                    con.close()
                return field_path_frame(
                    [
                        (name, name, type_ or "", "REQUIRED" if notnull else "", "")
                        for _, name, type_, notnull, *_ in info
                    ]
                )

            return CatalogNode(title=f"sqlite:{path}/{table}", load=columns_load)

        return CatalogNode(title=f"sqlite:{path}", load=load, child=child)
