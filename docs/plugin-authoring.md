# Writing quicksql plugins

A plugin bundles up to two capabilities; ship either or both:

1. **Config** — a pydantic model whose fields become directives, validated on the
   merged config (global → cell → `--set`/env overrides).
2. **`run`** — a decorator around cell execution.

```python
from pydantic import BaseModel, field_validator

from quicksql.models import Merge, Scope
from quicksql.plugins import Plugin, qfield
from quicksql.registry import plugin


@plugin
class RowLimit(Plugin):
    scope = Scope.BOTH          # GLOBAL, CELL, or BOTH — where the directives may appear
    priority = 0                # run-chain order: lower wraps outermore

    class Config(BaseModel):
        row_limit: int | None = qfield(None)   # `-- @row_limit: 100` now parses + validates

        @field_validator("row_limit")
        @classmethod
        def positive(cls, v):
            if v is not None and v <= 0:
                raise ValueError("row_limit must be positive")
            return v

    def run(self, cell, ctx, inner):           # optional behavior hook
        result = inner(cell, ctx)              # call zero or more times
        return result
```

## Config fields

- Field names (or their pydantic alias) become directive keys. Two plugins may not
  claim the same key — registration fails fast rather than letting pydantic silently
  shadow one of them.
- `qfield(default, merge=...)` sets how layers combine: `Merge.OVERRIDE` (default),
  `Merge.DEEP` (dict merge), `Merge.EXTEND` (list concat).
- Read your value from `cell.config.<field>` (cell scope) or `ctx.config.<field>`
  (global scope) — validators have already run.

## The run chain

`runner` wraps the base cell-runner with every plugin participating in execution,
sorted by `(priority, registration order)`; lower priority = outermost. Convention:

- **retries** (outermost, ~-100) — re-invoke `inner` when the result isn't `ok`
- **caching** (~-50) — skip `inner` entirely and return a synthesized result
- **timing/logging** (~0) — observe around `inner`

Two ways in:

- **Sugar** — override `before_execute(cell, ctx)` / `after_execute(cell, ctx, result)`
  for observation; `after_execute` may return a replacement result (None keeps it).
- **Full control** — override `run(cell, ctx, inner)` to own control flow: call
  `inner` zero or more times (caching, retries), transform the result.

Cell failures arrive as `RunResult(status="failed", error=..., traceback=...)`, so
retry-style plugins can inspect them: `error` is the human message (the engine's own,
e.g. DuckDB's `Catalog Error: ...`, else prefixed with the exception type, `KeyError: 'x'`)
and `traceback` the full trace. `status` is `"ok"` / `"failed"` / `"skipped"`; `ok` is a
read-only property derived from it, so a result is built with `status=`. Exceptions your
plugin raises are caught outside the chain and degrade that cell to an error result
without killing the run.

A cell downstream of a failure is skipped: it bypasses the run chain entirely (`run`,
`before_execute` and `after_execute` aren't called), but `after_run` still receives its
result, with `status == "skipped"` and `error` naming the failed upstream.

Builtin `EmitSql` (`plugins/emit_sql.py`) is the reference implementation: global
`render_dir` config + a `run` hook that writes each cell's rendered SQL before
delegating — `quicksql run --set render_dir=build/sql`.

## Render seams (compile time)

- `render_context(rctx) -> dict | None` — **contribute** Jinja globals for SQL bodies:
  return `{"name": value}`. Two providers for one key is a compile error (mirroring
  the config-field collision guard), so contributions can't silently shadow each
  other. `rctx` is the `RenderContext` capability object: `name`, `config`, `root`,
  plus verbs `add_edge(cell)`, `producer_expr(cell)`, `require_extensions(exts)`,
  `mark_source_used()`, `resolve_path(path)`. The builtin globals are themselves
  plugin contributions — `Refs` (`ref`), `Sources` (`source`), `Vars` (`var`/`vars`),
  `Env` (`env`) — each bundled with the directive it consumes. Templates render in
  Jinja's sandbox: contributed globals (and their public methods) are callable, but
  their underscore attributes aren't reachable — anything public is, so don't contribute
  a module or a live connection. A pydantic model instance exposes only its fields (no
  `parse_file`, `model_validate`, …); contribute instances, not model classes.
- `after_render(rctx, sql) -> sql | None` — transform rendered SQL; return None to
  keep it. Transformers compose in `(priority, registration)` order. No builtin
  plugin uses this hook; the scratch `Header`/`Footer` plugins in
  `tests/test_hooks.py` show two transformers composing.

An exception either render hook raises fails that cell's compile as a template error
naming the plugin and hook (`template error: plugin 'boom' after_render: KeyError: 'x'`),
exit 3, and the LSP shows it as a diagnostic. Raise a `ConfigError` to word the message
yourself; it passes through unchanged.

## Decision & aggregation hooks

Every builtin directive owns its behavior through one of these:

- `resolve_engine(config) -> str | None` / `resolve_sink(config) -> str | None` —
  **first-result** decisions: plugins are asked in `(priority, registration)` order,
  the first non-None answer wins, and core defaults (duckdb / parquet) apply last.
  Engine answers for an explicit `@engine`; Input infers from its sole input key.
- `should_rerun(cell) -> bool | None` — first-result watch-mode filter (the Autorun
  plugin answers from `@autorun`; default when nobody answers: rerun).
- `collect_edges(name, config) -> list[str] | None` — dependency edges, unioned
  across plugins (DependsOn contributes `@depends_on`; `ref()` edges come from the
  render seam).
- `sink_config(config, cfg) -> dict | None` — amend the sink's config dict before the
  sink is built (Schema injects `@schema` into DB sinks).

## Lifecycle hooks

- `after_compile(project)` — inspect the compiled Project; **raising rejects the
  compile**, making this the seam for cross-DAG validation (naming rules, required
  tags, forbidden refs).
- `before_run(project, ctx)` / `after_run(project, results)` — fire-and-forget
  notifications around the run loop (setup, summaries, alerts); failures are logged
  to `ctx.log`, never fatal.

`quicksql explain [file]` prints the effective run chain and every hook's participants —
the runtime order is registry state, and this makes it readable.

## Other extension points

- `@executor("name")` — run a cell's SQL on a new backend; land the result as a view
  on the conduit (`executors/base.py`, use `register_frame` for extracted results).
  Override `context_key(config)` (engine + connection target) and set
  `supports_context_refs = True` to let same-context cells reference each other as
  session-scoped temp tables in your engine's dialect (see the sqlite/postgres/bigquery
  executors); reffed cells arrive with `reffed_in_context`/`context_refs` set.
  Keep connection/client construction behind a small seam (`make_connection` /
  `make_client`) so tests can fake the backend offline.
- `@sink("name")` — a new destination: `prepare` (ATTACH/mkdir), `write`, and
  `ref_expr` (how downstream DuckDB cells read it back).
- `@source_reader("name")` — map a file extension to a DuckDB reader expression;
  declare required extensions via `requires`.

## Loading

- `quicksql run --plugins my_plugins` (dotted module) or `--plugins ./my_plugins.py`
- a `quicksqlrc.py` next to the notebook file loads automatically
