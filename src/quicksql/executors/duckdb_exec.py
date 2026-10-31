"""The primary executor and cross-cell conduit: everything stays in DuckDB SQL."""

from __future__ import annotations

from typing import Any

from ..config import input_spec
from ..models import RenderedCell, RunContext
from ..registry import SINKS, executor
from .base import Executor, result_view


@executor("duckdb")
class DuckDBExecutor(Executor):
    supports_context_refs = True

    def context_key(self, config: Any) -> str:
        return f"duckdb:{input_spec(config, 'duckdb').get('path') or ':memory:'}"

    def execute(self, cell: RenderedCell, ctx: RunContext) -> str:
        *setup, final = self.split_statements(cell.sql)
        if not SINKS.get(cell.sink_type).lands_output:
            # effect-only: run every statement for its side effects, land nothing
            for stmt in [*setup, final]:
                ctx.conn.execute(stmt)
            return ""
        for stmt in setup:  # build-up statements run for effect on the conduit
            ctx.conn.execute(stmt)
        if cell.reffed_in_context:
            # same-context consumers reference the bare cell name: materialize
            # once as a temp table (a view would recompute per consumer)
            ctx.conn.execute(f'CREATE OR REPLACE TEMP TABLE "{cell.name}" AS {final}')
            return cell.name
        view = result_view(cell.name)
        ctx.conn.execute(f'CREATE OR REPLACE TEMP VIEW "{view}" AS {final}')
        return view
