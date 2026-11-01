# quicksql

quicksql is a CLI "notebook for SQL": one `.qsql`/`.sql` file of valid SQL, split into named
cells by `-- @cell <name>`. Config lives in `@`-comment YAML directives — line form
`-- @key: value`, block form `/*@ ... */` — with a global header (everything before the
first `@cell`) inherited by every cell.

## Core concepts

- **Cell shape = input → engine → output.** Cells form a DAG from `{{ ref('cell') }}` +
  `@depends_on`, topo-sorted with cycle detection (`graph.py`).
- **A cell body may be multiple statements.** `executors/base.split_statements` (sqlglot
  tokenizer in the executor's declared `dialect`, comment/string-aware, original text preserved) splits on
  top-level `;`; setup statements run for effect and the **terminal** statement is the
  result. The runner owns this cell protocol (`runner.execute_cell`): executors only open
  an `EngineSession` per context (`execute`/`fetch`/`extract`/`materialize`/`drop`/
  `alive`/`close`; DB-API engines share `DbApiSession`), and a failure records its
  statement (`RunResult.statement`, `statement 2 of 3:` in the message). A sink with
  `lands_output=False` (`@output: {type: none}`, `sinks/none_sink.py`) makes the cell
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
  via its sink from that temp — one execution, two consumers. Sessions live in the
  `RunSession` by context key; a session whose `alive()` is False is dropped at run
  start (its temps re-materialize), a failed cell's temp is dropped, and `SessionLost`
  (an expired BigQuery session) reopens, re-materializes, and retries the cell once;
  before each cell the runner re-creates any same-context temp it reads that's gone.
  Read an engine's input with its executor's `input_spec(config)`, never by hand (the
  conduit reads duckdb's with `executors.base.engine_input`).
- **DuckDB is the cross-context conduit.** Cells using *cross-context*
  `ref()`/`source()`/`@extensions` must run on duckdb (compile-time guardrail); other
  engines extract to Polars/Arrow and DuckDB lands it in any sink. Every duckdb cell runs
  on the conduit (`conduit.py`: `conduit_target`, `ConduitSession`), so a cell whose own
  `input.duckdb` differs from the header's is a compile error. Anything that reads through
  DuckDB uses the conduit's helpers (`load_extension`/`ensure_extension`, `preview`,
  `duckdb_field_paths`); `conduit.ENGINE` names the conduit's engine.
- **Everything is a plugin; registries are per notebook.** `@plugin` / `@executor` /
  `@source_reader` / `@sink` only *mark* a class (`registry.py`); nothing registers on
  import. `bootstrap.py` is the composition root: `BUILTIN_MODULES` is the one list of
  builtins, and `registries_for(notebook, plugins)` builds a notebook's `Registries`
  (plugins/executors/sinks/readers) from them plus the classes its `quicksqlrc.py` and
  `--plugins` modules *define* (`registry.marked`); a `.py` module is keyed on path +
  content, so an edited rc reloads. The core takes `registries=` explicitly and has no
  default (`compiler.compile_text/compile_file`); front-ends and tests call
  `bootstrap.compile_file/compile_text`, which supply them. `Project.registries` carries
  them to the runner (`RunContext.registries`), sinks, watcher, catalog, TUI and LSP;
  validators read them via `config.registries_of(info)` (pydantic validation context).
  Hook participant lists are cached per registry. A `Plugin` bundles a pydantic
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
- **Executors declare capabilities; nothing recognizes an engine by name.**
  `Executor.dialect` is the sqlglot dialect (statement splitting, the LSP's syntax checks
  and keywords; None = generic SQL's tokens, no syntax checks). Optional catalog methods
  return lazy `models.CatalogNode`s whose table leaves load `executors.base.field_path_frame`s:
  `catalog(config, root)` (a context's tree, for the TUI), `catalog_target(target, root)`
  (a `@catalog` entry; its row name is the context key a cell there would have, so the two
  dedupe), `catalog_table(config, root, parts)` (LSP column completion) and
  `catalog_relations` (dotted FROM/JOIN completion). `catalog.py` and `lsp/` hold no engine
  branches, and no module imports another module's private names (`tests/test_layering.py`).
- **Config resolution:** global → cell → run overrides (`--set k.v=x`, `QUICKSQL_K__V=x`),
  per-field merge strategy (OVERRIDE / DEEP / EXTEND). Config values are literal; Jinja
  applies to SQL bodies only.
- **Templates render sandboxed.** `render.py` (the only Jinja environment in `src/`) uses
  `jinja2.sandbox.SandboxedEnvironment`: templates call globals but can't reach
  underscore/internal attributes, so compiling (which the LSP does on every open and edit)
  can't run arbitrary Python; a blocked access is a `SecurityError` → `template error`,
  exit 3. Not covered: `run` executes the notebook's SQL, CLI commands import
  `quicksqlrc.py` (directory trust is Phase 7), `env()` reads environment variables by design.
- **Watch/TUI are reflect-only.** The file is edited elsewhere. Change detection is by
  fingerprint (`state.py`): rendered SQL, engine, context, the config fields with
  `qfield(affects_results=True)` (the default; `autorun`/`tags`/`catalog`/`render_dir`
  are False), and the *landed* fingerprints of the cell's upstreams. `plan_rerun` reruns
  cells whose fingerprint differs from `RunSession.seen` (the fingerprint the session last
  ran it with, or first saw it at via `see`), plus downstream through autorun cells.
  Each run writes the notebook's state file under `~/.quicksql/state/` (status + what each
  landed output was built from; `RunSession.landed` is seeded from it) for
  `quicksql status`; it never skips a run. `$QSQL_HOME` moves `~/.quicksql` (state and user
  templates); an autouse fixture gives every test its own, so tests never touch the real one.
  Third-party plugins load via `--plugins mod.or.path.py` or a `quicksqlrc.py` next to the file.
  The TUI runs one run at a time per session: a new run, `o` or quitting cancels the one
  under way, which stops between cells (`run_project(cancel=)`), and the next waits for it.

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
- `uv run lint-imports` — the layering gate, which `tests/test_layering.py` runs in every
  `uv run pytest`. Contracts live in `pyproject.toml` `[tool.importlinter]`; both
  `ignore_imports` lists are empty since Phase 6: fix a new violation, don't list it.
- `uv run quicksql` (bare = init) | `init` | `run` | `watch` | `tui` | `list` | `show <cell>` |
  `compile` — default file `base.qsql`; `--set key.path=value` repeatable. `quicksql tui`
  with a missing file opens a notebook picker: existing .qsql/.qsql.sql files plus
  starter templates (builtin `base` + user templates from `~/.quicksql/templates/*.qsql`);
  `o` in the TUI switches notebooks.

## Architecture map (`src/quicksql/`)

- `parser.py` — text → RawBlocks (directive YAML + SQL body + body hash)
- `registry.py` — `Registry`/`PluginRegistry`/`Registries`, the marking decorators,
  `marked(module)`; field→plugin map with collision guard
- `bootstrap.py` — the composition root: builtins list, `registries_for`, the shared
  plugin-module loader, app-level `compile_file`/`compile_text`
- `plugins/` — `base.py` (Plugin, qfield), `builtin.py` (the directive vocabulary),
  `emit_sql.py` (behavior-plugin demonstrator)
- `config.py` — model composition, scope checks, layer merging, engine/sink resolution
  (`make_sink`)
- `overrides.py` — `--set` / `QUICKSQL_*` parsing
- `render.py` — Jinja on SQL bodies; ref/source/var/env; records edges + extensions
- `sources.py` — file reader registry (csv/parquet/json/excel)
- `graph.py` — topo sort, cycle detection, downstream sets
- `state.py` — fingerprints, the per-notebook state file under `~/.quicksql/state/`,
  `statuses` for `status`
- `compiler.py` — parse → config → render → graph ⇒ `Project`
- `runner.py` — topo run; the cell protocol (`execute_cell`); plugin run-chain;
  RunSession holds the conduit connection, engine sessions, and extension cache across
  watch/TUI reruns
- `conduit.py` — the DuckDB conduit's target, the duckdb engine's session on it, and
  reading through it (extensions, previews, field paths)
- `catalog.py` — the TUI's schema catalog: each context's tree from its executor,
  `@catalog` targets, landed outputs read back through DuckDB
- `executors/`, `sinks/` — strategy leaves (duckdb/sqlite/postgres/bigquery, each
  opening an `EngineSession` and declaring its dialect and catalog; parquet/duckdb/postgres)
- `scaffold.py`, `cli.py`, `watcher.py` — CLI, watch loop
- `tui/` — the TUI. `app.py` (`QsqlApp`: layout, key routing, the notebook's lifecycle
  and watch loop), `runs.py` (`RunController`: the session, one run at a time,
  generation-tagged; it repaints through the app's `run_*` hooks), `panes.py`
  (`SheetTable` renders a Sheet; `Surface` holds the sheet verbs; `DataPanes` is the Data
  tab's stacks and split), `catalog_view.py` (the Catalog tab's loads, cache and
  per-notebook generation), `previews.py`, `screens.py`; `sheet.py` holds the pure Sheet
  ops. The subtitle has one writer, `QsqlApp.refresh_subtitle`.
- `lsp/` — `analysis.py` (diagnostics/completions on sqlglot in the cell's executor's
  dialect + length-preserving Jinja mask; tables and relations from its catalog), `server.py` (pygls stdio); bigquery cells prefer the googlesql
  `execute_query` binary when found (PATH or `QSQL_EXECUTE_QUERY` — deliberately
  not `QUICKSQL_*`, which overrides.py sweeps into config), fallback sqlglot with
  unsupported-pipe-operator errors downgraded to warnings

## Development methodology — red-green-refactor

Write the failing pytest first and watch it fail **for the expected reason**; write the
minimal code to green; refactor while green. Tests mirror `src/` layout
(`tests/test_<module>.py`); shared fixtures in `tests/conftest.py`: `registries` (this
test's own builtin copy) and `add_plugin` (decorate a scratch plugin to add it there);
pass `registries=registries` to `compile_text`/`compile_file`/`QsqlApp`/`Analyzer`.

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
