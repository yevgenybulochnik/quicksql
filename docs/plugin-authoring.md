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

- Field names (or their pydantic alias) become directive keys. Two plugins in one
  notebook's registries may not claim the same key — adding the second fails fast
  rather than letting pydantic silently shadow one of them.
- `qfield(default, merge=...)` sets how layers combine: `Merge.OVERRIDE` (default),
  `Merge.DEEP` (dict merge), `Merge.EXTEND` (list concat).
- `qfield(..., affects_results=False)` keeps a field out of the cell's fingerprint, for a
  field that changes how quicksql treats a cell but not what it computes (like `autorun`
  or `tags`): editing it then reruns nothing under watch and leaves the cell's status
  alone. Fields default to `affects_results=True`, and so does a plain pydantic field.
- Read your value from `cell.config.<field>` (cell scope) or `ctx.config.<field>`
  (global scope) — validators have already run.
- A validator that needs the notebook's registries (to check an engine or sink name)
  takes pydantic's `info: ValidationInfo` and reads `config.registries_of(info)`; see
  the builtin `Engine`/`Output` validators.

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

- `@executor("name")` — run cells on a new backend (`executors/base.py`). An executor
  names its context with `context_key(config)` (engine + connection target) and
  connects with `open_session(config, ctx) -> EngineSession`. It reads its
  `input.<name>` directive with `self.input_spec(config)`, which expands a bare string
  to `{input_shorthand: value}` (`path` unless the executor says otherwise). The runner keeps one
  session per context, shared by its cells, and drives it through the cell protocol
  (`runner.execute_cell`), so an executor never sees a cell: setup statements go to
  `execute(sql)`, and the terminal statement to `fetch(sql)` (rows as a Polars frame or
  pyarrow Table, landed on the conduit for you), or every statement to `execute` when
  the cell's sink lands nothing. With `supports_context_refs = True`, same-context
  cells reference each other as session temps: `materialize(name, sql)` creates one
  and returns the query that reads it back, and `drop(name)` removes it. A session whose
  `extract` is lazy (the conduit's view) implements `check(sql)`: when landing fails, the
  runner runs the query alone to tell its error from the sink's. `alive()` is a
  cheap local liveness check, `close()` releases the connection, and `scoped=False`
  marks a statement that touches no temps (BigQuery runs those outside its session). A
  DB-API driver gets all of this from `DbApiSession` (see the sqlite/postgres
  executors). Keep connection/client construction behind a small seam
  (`make_connection` / `make_client`) so tests can fake the backend offline.

  An executor also declares what front-ends need to know, so a new engine works
  everywhere with no core edits:
  - `dialect` — the sqlglot dialect its SQL is written in (`"tsql"`, `"snowflake"`, ...;
    the engine's name needn't be one). Cell bodies split into statements in it, and the
    LSP checks syntax and offers keywords and functions in it. Left `None`, bodies split
    on generic SQL's tokens and the LSP checks no syntax.
  - a catalog, optionally. Each method returns lazy `quicksql.models.CatalogNode`s: a
    `title`, a `load()` returning a Polars frame, and a `child(row)` to drill into a row
    (None at a leaf). A table's leaf loads `executors.base.field_path_frame(rows)`, one
    `(column, field_path, type, mode, description)` row per column (nested fields get
    their own paths). Building a node must connect to nothing; only `load()` does.
    - `catalog(config, root)` — the tree of the context `config` names, which the TUI's
      catalog drills into. Without it, the context is listed but not browsable.
    - `catalog_target(target, root)` — a `@catalog: {<engine>: [target]}` entry as
      `(row name, node)`. Make the row name the `context_key` a cell in that context
      would have, so the two show once.
    - `catalog_table(config, root, parts)` — the leaf of a table a cell reads (`parts`:
      its dotted name, qualifiers first; fill in your engine's default schema), for the
      LSP's column completions.
    - `catalog_relations(config, root, parts)` — `(name, detail)` candidates for the
      next segment of a dotted name typed after `FROM`/`JOIN` (BigQuery's datasets and
      tables).

  `tests/conftest.py`'s `tsql_engine` fixture is a complete, offline example.
- `@sink("name")` — a new destination: `prepare` (ATTACH/mkdir), `write`, and
  `ref_expr` (how downstream DuckDB cells read it back).
- `@source_reader("name")` — map a file extension to a DuckDB reader expression;
  declare required extensions via `requires`.

## Loading

The decorators (`@plugin`, `@executor`, `@sink`, `@source_reader`) only mark a class;
nothing registers by being imported. Each notebook gets its own registries
(`quicksql.bootstrap.registries_for`): the builtins, then every decorated class its
plugin modules *define*, in definition order:

- a `quicksqlrc.py` next to the notebook file loads automatically;
- `quicksql run --plugins my_plugins` (dotted module) or `--plugins ./my_plugins.py`
  adds a module, after the rc.

So two notebooks in one process (the TUI, the LSP) never see each other's plugins,
and two rc files may each define a plugin of the same name. A class a module imports
(rather than defines) isn't registered from it, and a decorated class's undecorated
subclass isn't either.

A `.py` plugin file is keyed on its path and content: edit `quicksqlrc.py` and the
TUI's next recompile (or `watch`'s next save) loads the new version, no restart. It
runs from the file's current bytes, never a `__pycache__` entry. A file that fails to
run isn't cached, so fixing it is enough. Each edited version stays in `sys.modules`
for the life of the process.

From Python, build registries with `quicksql.bootstrap` and pass them on:
`bootstrap.compile_file(path)` compiles with the notebook's own registries, and
`builtin_registries().add(MyPlugin, MySink)` gives you a set to pass as
`registries=` to `compile_text`/`compile_file`. The compiled `Project` carries its
registries, so running, watching and browsing it need nothing else. The core
`quicksql.compiler.compile_text`/`compile_file` take `registries=` and have no default.

`Project.overrides` holds the run overrides (`--set`, `QUICKSQL_*`) the project was
compiled with, for an `after_compile` hook that wants them; quicksql itself doesn't
read them back.
