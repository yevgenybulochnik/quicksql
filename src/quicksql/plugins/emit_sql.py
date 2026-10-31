"""EmitSql: demonstrator behavior plugin needing zero core edits.

Set ``render_dir`` (global directive or ``--set render_dir=build/sql``) and
every cell's rendered SQL is written there before the cell runs.
"""

from __future__ import annotations

from pydantic import BaseModel

from ..models import RenderedCell, RunContext, RunResult, Scope
from ..registry import plugin
from .base import Inner, Plugin, qfield


@plugin
class EmitSql(Plugin):
    scope = Scope.GLOBAL
    priority = -100  # outermost: emit even if inner behavior fails

    class Config(BaseModel):
        render_dir: str | None = qfield(None, affects_results=False)

    def run(self, cell: RenderedCell, ctx: RunContext, inner: Inner) -> RunResult:
        render_dir = getattr(ctx.config, "render_dir", None)
        if render_dir:
            out = ctx.root / render_dir
            out.mkdir(parents=True, exist_ok=True)
            (out / f"{cell.name}.sql").write_text(cell.sql + "\n")
        return inner(cell, ctx)
