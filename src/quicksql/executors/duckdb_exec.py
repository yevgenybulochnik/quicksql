"""The primary executor: duckdb cells run on the conduit itself."""

from __future__ import annotations

from typing import Any

# the module, not the class: conduit imports executors.base, whose package
# __init__ imports this module mid-way (Phase 4 makes the __init__s lazy)
from .. import conduit
from ..config import input_spec
from ..models import RunContext
from ..registry import executor
from .base import EngineSession, Executor


@executor("duckdb")
class DuckDBExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"duckdb:{input_spec(config, 'duckdb').get('path') or ':memory:'}"

    def open_session(self, config: Any, ctx: RunContext) -> EngineSession:
        return conduit.ConduitSession(ctx.conn)
