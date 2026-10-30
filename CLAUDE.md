# quicksql

quicksql is a CLI "notebook for SQL": one `.qsql`/`.sql` file of valid SQL, split into named
cells by `-- @cell <name>`. Config lives in `@`-comment YAML directives — line form
`-- @key: value`, block form `/*@ ... */` — with a global header (everything before the
first `@cell`) inherited by every cell.

## Core concepts

- **Cell shape = input → engine → output.** Cells form a DAG from `{{ ref('cell') }}` +
  `@depends_on`, topo-sorted with cycle detection (`graph.py`).
- **A cell body may be multiple statements.** `executors/base.split_statements` (sqlglot
  tokenizer, engine dialect, comment/string-aware, original text preserved) splits on
  top-level `;`; setup statements run for effect and the **terminal** statement is wrapped/
  landed as today. `@output: {type: none}` (`sinks/none_sink.py`, `lands_output=False`) is
  effect-only — runs every statement, lands nothing, can't be `ref()`'d (compiler rejects
  the ref). BigQuery overrides `split_statements` to *never* split (native scripting runs the
  whole body in one job); `supports_multistatement_materialization=False` makes the compiler
  reject a multi-statement BigQuery cell that is reffed in-context. Postgres/SQLite keep
  autocommit, so a mid-cell failure leaves earlier statements' effects (documented, by
  design). LSP column completion is statement-scoped: `_resolve_scope` slices the masked
  source to the cursor's statement (`statement_spans` offsets on the raw source) before
  `parse_one`, so a sibling statement's tables/aliases don't leak into scope.
- **Sinks are the cross-cell interchange.** Each cell lands via its sink (default
  `data/<cell>.parquet`); `Sink.ref_expr()` tells downstream cells how to read it back
  (read_parquet, ATTACHed duckdb/postgres table, ...). The `none` sink lands nothing.
- **Engine contexts.** Cells partition by (engine, connection target) via
  `Executor.context_key`. Same-context `ref()` resolves to a bare temp-table name in
  the engine's own dialect (session-scoped: duckdb conduit / shared sqlite connection /
  BigQuery session), executing the dependency next to the data; the cell still lands
  via its sink from that temp — one execution, two consumers.
- **DuckDB is the cross-context conduit.** Cells using *cross-context*
  `ref()`/`source()`/`@extensions` must run on duckdb (compile-time guardrail); other
  engines extract to Polars/Arrow and DuckDB lands it in any sink.
- **Everything is a plugin.** `@plugin` / `@executor` / `@source_reader` / `@sink`
  decorators register into registries (`registry.py`). A `Plugin` bundles a pydantic
  `Config` (its fields become directives; validators run on merged config; merge
  strategy rides on fields via `qfield`) with behavior hooks (lower `priority` =
  outermore): `run(cell, ctx, inner)` wraps execution (or the simpler
  `before_execute`/`after_execute`); `render_context(rctx)` *contributes* Jinja
  globals under a collision guard (the builtin ref/source/var/env globals are plugin
  contributions; `rctx` is the RenderContext capability: add_edge/producer_expr/
  require_extensions); `after_render(rctx, sql)` transforms SQL; first-result
  decision hooks `resolve_engine`/`resolve_sink`/`should_rerun` and aggregation hooks
  `collect_edges`/`sink_config` let directives own their behavior (core defaults:
  duckdb/parquet/rerun); `after_compile` may raise to reject a compile;
  `before_run`/`after_run` are contained notifications.
  `quicksql explain` prints the effective chain/hook order. `config.build_models`
  composes all plugin Configs into GlobalConfig/CellConfig via `create_model`.
- **Config resolution:** global → cell → run overrides (`--set k.v=x`, `QUICKSQL_K__V=x`),
  per-field merge strategy (OVERRIDE / DEEP / EXTEND). Config values are literal; Jinja
  applies to SQL bodies only.
- **Templates render sandboxed.** `render.py` (the only Jinja environment in `src/`) uses
  `jinja2.sandbox.SandboxedEnvironment`: templates call globals but can't reach
  underscore/internal attributes, so compiling (which the LSP does on every open and edit)
  can't run arbitrary Python; a blocked access is a `SecurityError` → `template error`,
  exit 3. Not covered: `run` executes the notebook's SQL, CLI commands import
  `quicksqlrc.py` (directory trust is Phase 7), `env()` reads environment variables by design.
- **Watch/TUI are reflect-only.** The file is edited elsewhere; change detection hashes
  SQL bodies only, so config-only edits don't trigger reruns (known limitation; the
  watch/TUI log flags them as `config changed (no rerun)` via
  `watcher.config_only_changes`).
  Third-party plugins load via `--plugins mod.or.path.py` or a `quicksqlrc.py` next to the file.

## Commands

- `uv run pytest` — markers `network` / `bigquery` / `postgres` are deselected by default.
  `docker compose up -d --wait` stands up Postgres (databases: `quicksql` for stress/manual
  data, `quicksql_test` for pytest, `quicksql_claims` for the synthetic-claims smoke test —
  seed per `demo/kitchen-sink/pg-init/10-claims-core.sql`, drive with
  `demo/kitchen-sink/kitchen-sink.qsql`) plus the goccy
  bigquery-emulator (REST :9050, project `quicksql-test` — no GCP account needed; cells
  reach it via `input.bigquery.endpoint`; it answers duplicate creates with a retryable
  500, so tests seed probe-then-create, never `create(exists_ok=True)`). Then
  `uv run --extra postgres pytest -m postgres` / `uv run --extra bigquery pytest -m bigquery`
  run the integration tests; they skip when their backend is down. Override endpoints with
  `QUICKSQL_TEST_PG_DSN` / `QUICKSQL_TEST_BQ_ENDPOINT`. Everything except the
  network-gated tests: `uv run --extra postgres --extra bigquery pytest -m "not network"`
  — an explicit `-m` overrides the default deselection. The standalone demo stack
  `demo/kitchen-sink/docker-compose.yml` runs on offset ports (5433/9051) and can
  stay up alongside this one.
- `uv run quicksql` (bare = init) | `init` | `run` | `watch` | `tui` | `list` | `show <cell>` |
  `compile` — default file `base.qsql`; `--set key.path=value` repeatable. `quicksql tui`
  with a missing file opens a notebook picker: existing .qsql/.qsql.sql files plus
  starter templates (builtin `base` + user templates from `~/.quicksql/templates/*.qsql`);
  `o` in the TUI switches notebooks.

## Architecture map (`src/quicksql/`)

- `parser.py` — text → RawBlocks (directive YAML + SQL body + body hash)
- `registry.py` — registries + decorators; field→plugin map with collision guard
- `plugins/` — `base.py` (Plugin, qfield), `builtin.py` (the directive vocabulary),
  `emit_sql.py` (behavior-plugin demonstrator)
- `config.py` — model composition, scope checks, layer merging, engine/sink resolution
- `overrides.py` — `--set` / `QUICKSQL_*` parsing
- `render.py` — Jinja on SQL bodies; ref/source/var/env; records edges + extensions
- `sources.py` — file reader registry (csv/parquet/json/excel)
- `graph.py` — topo sort, cycle detection, downstream sets
- `compiler.py` — parse → config → render → graph ⇒ `Project`
- `runner.py` — topo run; per-cell executor+sink; plugin run-chain; RunSession holds
  the conduit connection + extension cache across watch/TUI reruns
- `executors/`, `sinks/` — strategy leaves (duckdb/sqlite/postgres/bigquery;
  parquet/duckdb/postgres)
- `scaffold.py`, `cli.py`, `watcher.py`, `sheet.py` + `tui.py` — CLI, watch loop, TUI
- `lsp/` — `analysis.py` (diagnostics/completions on sqlglot + length-preserving
  Jinja mask), `server.py` (pygls stdio); bigquery cells prefer the googlesql
  `execute_query` binary when found (PATH or `QSQL_EXECUTE_QUERY` — deliberately
  not `QUICKSQL_*`, which overrides.py sweeps into config), fallback sqlglot with
  unsupported-pipe-operator errors downgraded to warnings

## Development methodology — red-green-refactor

Write the failing pytest first and watch it fail **for the expected reason**; write the
minimal code to green; refactor while green. Tests mirror `src/` layout
(`tests/test_<module>.py`); shared fixtures in `tests/conftest.py` (registry
snapshot/restore is autouse — tests may register scratch plugins freely).

## Gotchas

- **Never iterate `ctx.log` directly while emitting into it.** `RunContext.emit` handles a
  failing `on_event` callback by appending an error line to `ctx.log`; the end-of-run flush
  in `run_project` (`for line in ...: ctx.emit("note", ...)`) therefore fed its own appends
  back into the loop — with an always-raising callback
  (`test_broken_event_callback_never_kills_the_run`) it grew unboundedly (~7GB) and the
  kernel OOM killer took down the whole tmux pane scope, Claude session included, three
  times on 2026-07-18. Fixed in `runner.py` by iterating a snapshot:
  `for line in list(ctx.log)`. Keep that pattern for any loop that emits while reading the log.
- **This box has 7.5GiB RAM and no swap.** Run memory-risky commands (e.g. the full pytest
  suite) under `systemd-run --user --scope -p MemoryMax=3G <cmd>` so a runaway process is
  killed instead of the session. Don't use `ulimit -v`: polars/duckdb reserve large virtual
  address arenas and abort under address-space caps.

## Conventional Commits

`type(scope): summary` — scope = module (`parser`, `config`, `render`, `graph`, `sink`,
`executor`, `runner`, `cli`, `tui`, `watcher`); types in history: `feat`, `fix`, `test`,
`refactor`, `build`, `docs`, `chore`. Behavior-sized commits; `test(...)` red commit then
`feat/fix(...)` green commit when practical.
