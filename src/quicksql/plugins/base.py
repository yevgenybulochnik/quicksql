"""The Plugin base: config fields + validation, and optional run-time behavior.

A plugin bundles up to two capabilities:

* ``Config`` — a pydantic model whose fields become directives; validators run
  on the merged config. Field merge strategy rides on the field via ``qfield``.
* ``run`` — a decorator around cell execution: call ``inner`` zero or more
  times, do work before/after. Lower ``priority`` wraps outermore.
"""

from __future__ import annotations

from typing import Any, Callable, ClassVar

from pydantic import BaseModel, Field

from ..models import Merge, RenderedCell, RunContext, RunResult, Scope

Inner = Callable[[RenderedCell, RunContext], RunResult]


class EmptyConfig(BaseModel):
    """Default Config for behavior-only plugins: contributes no directives."""


def qfield(
    default: Any = None,
    *,
    merge: Merge = Merge.OVERRIDE,
    affects_results: bool = True,
    **kwargs: Any,
) -> Any:
    """A pydantic Field carrying quicksql's per-field merge strategy, and
    whether a change to it changes the cell's result: a field that doesn't
    (autorun, tags) stays out of the fingerprint, so editing it reruns nothing."""
    extra = dict(kwargs.pop("json_schema_extra", None) or {})
    extra["qsql_merge"] = merge.value
    extra["qsql_affects_results"] = affects_results
    return Field(default, json_schema_extra=extra, **kwargs)


class Plugin:
    name: ClassVar[str] = ""
    scope: ClassVar[Scope] = Scope.BOTH
    priority: ClassVar[int] = 0
    Config: ClassVar[type[BaseModel]] = EmptyConfig

    # -- execution wrapper (owns control flow: retry, skip, transform) --------
    def run(self, cell: RenderedCell, ctx: RunContext, inner: Inner) -> RunResult:
        """Default: delegate to the before/after sugar. Override for control
        flow (retries, caching); override the sugar for simple observation."""
        self.before_execute(cell, ctx)
        result = inner(cell, ctx)
        replaced = self.after_execute(cell, ctx, result)
        return result if replaced is None else replaced

    def before_execute(self, cell: RenderedCell, ctx: RunContext) -> None:
        """Observation hook before the cell executes."""

    def after_execute(
        self, cell: RenderedCell, ctx: RunContext, result: RunResult
    ) -> RunResult | None:
        """Observation hook after the cell executes; return a RunResult to
        replace it, or None to keep it."""
        return None

    # -- render seams (compile time) ------------------------------------------
    def render_context(self, rctx: Any) -> dict[str, Any] | None:
        """Contribute Jinja globals for a cell's SQL body: return a dict of
        name -> value. rctx is the RenderContext capability object (name,
        config, root, add_edge, producer_expr, require_extensions, ...).
        Two providers for one key is a compile error."""
        return None

    def after_render(self, rctx: Any, sql: str) -> str | None:
        """Transform a cell's rendered SQL; return the new SQL, or None to
        keep it. Transformers compose in (priority, registration) order."""
        return None

    # -- decision hooks (first non-None wins; core default applies last) -------
    def resolve_engine(self, config: Any) -> str | None:
        """Answer which engine a cell runs on, or None to pass."""
        return None

    def resolve_sink(self, config: Any) -> str | None:
        """Answer which sink type a cell lands in, or None to pass."""
        return None

    def should_rerun(self, cell: RenderedCell) -> bool | None:
        """Watch-mode filter: should this changed cell rerun? None to pass."""
        return None

    # -- aggregation/transform hooks -------------------------------------------
    def collect_edges(self, name: str, config: Any) -> list[str] | None:
        """Contribute dependency edges for a cell (unioned across plugins)."""
        return None

    def sink_config(self, config: Any, cfg: dict[str, Any]) -> dict[str, Any] | None:
        """Amend the sink's config dict before the sink is built; None keeps it."""
        return None

    # -- lifecycle notifications -----------------------------------------------
    def after_compile(self, project: Any) -> None:
        """Inspect/validate the compiled Project; raising rejects the compile."""

    def before_run(self, project: Any, ctx: RunContext) -> None:
        """Called once before the first cell of a run; failures are contained."""

    def after_run(self, project: Any, results: list[RunResult]) -> None:
        """Called once after the run completes; failures are contained."""


RUN_HOOKS = ("run", "before_execute", "after_execute")
