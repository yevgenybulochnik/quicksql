"""The primary executor: duckdb cells run on the conduit itself."""

from __future__ import annotations

from typing import Any

from ..conduit import ConduitSession
from ..models import RunContext
from ..registry import executor
from .base import EngineSession, Executor


@executor("duckdb")
class DuckDBExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"duckdb:{self.input_spec(config).get('path') or ':memory:'}"

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        return ConduitSession(ctx.conn)
