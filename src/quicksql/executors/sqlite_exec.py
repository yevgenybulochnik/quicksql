"""Zero-dependency executor: proves cross-engine flow entirely offline."""

from __future__ import annotations

import sqlite3
from typing import Any

from ..config import input_spec
from ..models import RunContext
from ..registry import executor
from .base import DbApiSession, EngineSession, Executor


class SQLiteSession(DbApiSession):
    engine = "sqlite"


@executor("sqlite")
class SQLiteExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"sqlite:{input_spec(config, 'sqlite').get('path') or ':memory:'}"

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        """One connection per context: temp tables live there, and a :memory:
        context exists only there."""
        path = input_spec(config, "sqlite").get("path")
        db = str(ctx.root / path) if path and path != ":memory:" else ":memory:"
        # runs are serialized (exclusive workers), but their threads change
        return SQLiteSession(sqlite3.connect(db, check_same_thread=False))
