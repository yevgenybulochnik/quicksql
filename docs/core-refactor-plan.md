# Plan: in-place refactor toward a minimal core

**Status:** proposed 2026-10-15, not started.

## Context

A code review and an architecture review of `feat/qsql-fable` at `46ec2f5` (2026-10-15)
found the design sound but its boundaries leaky: the core reads builtin plugins' fields
directly, engine knowledge sits in tables keyed by engine name, every executor
re-implements the cell protocol, registries are process-global, and run outcomes hide
failures.

A from-scratch rebuild was considered and rejected. It would throw away hard-won fixes —
the HUGEINT→DECIMAL cast for parquet, decimal256 normalization, the emulator retry
settings, watching the parent directory for atomic saves, the log-snapshot OOM fix, the
LSP's Jinja masking — and leave two codebases to maintain during the switch. Every change
below lands in place, one phase at a time, with the existing 346 tests as the safety net.

| Finding (2026-10-15) | Evidence | Phase |
|---|---|---|
| Append-mode sinks report the table's total rows | `count(*)` after `INSERT` | 1 |
| TUI preview fallback assumes `data/<cell>.parquet` | ignores `output.dir` and other sinks | 1 |
| A changed `autorun: false` cell still reruns its unchanged downstream | reproduced via `plan_rerun` | 1 |
| Templates render unsandboxed; the LSP renders on file open | `''.__class__.__mro__` reachable | 1 |
| Composed config models rebuilt per cell | ~80% of compile time; 25 → 5 ms memoized | 1, 5 |
| Downstream of a failed cell runs on stale data and reports ok | reproduced, cross-context and in-session | 2 |
| DuckDB errors print as `FAIL a: ^` | reproduced | 2 |
| Cell protocol duplicated in all 4 executors; `sink_type == "none"` checks | read | 3 |
| Per-cell `input.duckdb` is silently ignored | reproduced: ran in `:memory:` | 3 |
| Dropped Postgres connections and expired BigQuery sessions go undetected | read | 3 |
| `config.input` is hand-parsed in 12 places | grep | 3, 5 |
| Process-global registries: rc plugins leak across notebooks; TUI `o` skips the new rc | read | 4 |
| Unregistering the Input plugin breaks every compile (`AttributeError`) | reproduced | 5 |
| Validator names must be unique across all plugins (configs composed by inheritance) | read | 5 |
| Engine names hard-coded in `catalog.py` (12), `lsp/schema.py` (7), the LSP dialect map | grep | 6 |
| Plugins, catalog, LSP and TUI import runner privates; 32 function-level imports | grep | 6 |
| CLI commands auto-import `quicksqlrc.py`; the LSP never loads it | read | 7 |
| Config-only edits never rerun (documented limitation) | README, CLAUDE.md | 8 |
| `tui.py` is 1,304 lines, one class with 82 methods | `wc` | 9 |
| `textual` and `polars` are core dependencies | `pyproject.toml`, `runner.py` | 10 |

## Target

- **The core** — parser, config, registry, render, compiler, graph, runner, conduit,
  models — knows no specific engine, sink, directive or UI, and reads only its own typed
  schema: `engine`, `input`, `output`, `depends_on`, `extensions`.
- **Extensions** — executors, sinks, readers, directive plugins — reach the core only
  through its contracts and declare capabilities (`dialect`, `lands_output`, catalog
  support) instead of being recognized by name.
- **One registry per notebook**, passed explicitly; built-ins come from one explicit list.
- **The runner owns** the cell protocol, cell outcomes (ok / failed / skipped) and
  fingerprints for change detection.
- **Front-ends** (CLI, watch, TUI, LSP) share one public API. import-linter enforces the
  layering on the current module layout; nothing moves into new packages.

## Phases (each ends green; test-first, conventional commits)

Sizes are relative: S, M, L.

**Phase 0 — safety net (S).**
- `tests/test_demos.py`: compile every `demo/**/*.qsql`, and run `demo/sources/sources.qsql`
  offline in a temp copy asserting every cell is ok. Ported from `tests/test_examples.py`
  on `feat/qsql-notebook-engine`; nothing guards the demos today.
- Add import-linter (dev dependency) with the target contracts — core modules import no
  extension or front-end; extensions don't import the runner, compiler, watcher or CLI —
  and list today's violations under `ignore_imports`. That list is the debt register:
  each phase deletes its entries, and Phase 6 ends with it empty.
- Record the baseline: `demo/kitchen-sink/kitchen-sink.qsql` compiles in ~25 ms.

**Phase 1 — quick fixes (S; independent red/green pairs).**
- Append-mode DuckDB and Postgres sinks report the `INSERT`'s own row count.
- The TUI's `_preview_frame` falls back to the cell's own sink (`make_sink(...).ref_expr`),
  not `data/<cell>.parquet`.
- `plan_rerun` propagates only from changed cells that will rerun.
- Templates render in `jinja2.sandbox.SandboxedEnvironment`. A prototype compiled both
  demo notebooks unchanged and blocked `''.__class__`.
- Memoize `build_models` per registry generation (bumped by `register`/`restore`):
  25 → ~5 ms. Deleted again in Phase 5.
- Remove dead code: `models.Cell`, `RunResult.pl`, `RenderedCell.uses_sources`,
  `RunContext.overrides`, unused imports (`tempfile` in `runner.py`; `Ref`/`Source` in
  `lsp/analysis.py`), unused `project` parameters (`_resolve_scope`, `relation_names`).

**Phase 2 — honest run outcomes (M).**
- `RunResult.status`: `ok` | `failed` | `skipped` (`ok` stays as a property). A structured
  `error` carries the exception type, the full engine message and the cell; the traceback
  is kept for debugging only.
- The runner skips cells whose upstream failed or was skipped, across `ref()` and
  `@depends_on` edges: `skipped: upstream 'a' failed`.
- A failed cell is removed from `session.materialized`, so its stale temp is
  re-materialized before anything reads it (Phase 3 also drops it).
- The CLI summary and `RunEvent.line()` print the error message, not the traceback's last
  line.
- `to_dict()` on `RunResult` and `RunEvent`; `quicksql run --json` prints one document and
  no progress; exit codes 1 = a cell failed, 3 = compile error (2 stays click's usage error).
- Tests first: an upstream failure skips downstream both across contexts (sqlite → duckdb)
  and inside a reused `RunSession`; the CLI shows DuckDB's `Catalog Error: …`; the JSON
  shape; the exit codes.
- Behavior change: downstream of a failure no longer runs. Note it in the README.

**Phase 3 — the runner owns the cell protocol (L).**
- Executors open one session per context with `execute(sql)`, `materialize(name, sql)`,
  `extract(sql_or_name) -> view`, `drop(name)`, `alive()` and `close()`. The runner
  implements effect-only / referenced-in-context / plain once — the
  `materialize`/`extract` split `docs/engine-context-plan.md` planned — and records which
  statement failed, completing Phase 2's structured error.
- `Sink.lands_output` replaces every `sink_type == "none"` check (four executors, the
  compiler).
- SQLite and Postgres sessions share one DB-API base. BigQuery keeps its own (scripts are
  never split) and reuses one client per context instead of creating one per cell.
- `input_spec(config, engine)` replaces the 12 hand-parsed `(config.input or {}).get(...)`
  sites until Phase 5 types them.
- A DuckDB cell whose `input.duckdb` differs from the conduit's target is a compile error;
  today it silently runs in the conduit. ATTACH-based support is deferred.
- Session liveness: the runner checks `alive()` before reuse; a dead session is reopened
  and its temps are removed from `session.materialized`, so upstreams re-materialize —
  the risk `docs/engine-context-plan.md` named.
- Tests first: a runner matrix (effect-only / referenced / plain × a fake session); the
  existing executor tests unchanged; a third-party sink with `lands_output = False`
  behaves like `none`; per-cell DuckDB input fails to compile; a dead fake session is
  reopened and its upstream re-materialized.
- Breaking: the executor contract. Update `docs/plugin-authoring.md`.

**Phase 4 — a registry per notebook (L).**
- A `Registries` object (plugins, executors, sinks, readers) per notebook, passed
  explicitly through compile, render, config, runner, sinks, watcher, TUI and LSP. This
  finishes the seam `config.py` already has (`plugins=` parameters).
- Decorators mark classes instead of registering them; built-ins come from one explicit
  list. `--plugins` and `quicksqlrc.py` load through one shared loader (moved out of
  `cli.py`) into the notebook's registry; the TUI uses it at startup and on `o`. The LSP
  stays rc-free until Phase 7.
- Hook participant lists are computed once per registry; `overriding()` re-sorts on every
  call today.
- The autouse snapshot/restore fixture in `tests/conftest.py` becomes a per-test registry.
- Tests first: two notebooks with different rc plugins in one process don't see each
  other's directives; two rc files defining the same plugin name both load; `o` into a
  notebook with its own rc picks it up; loading an rc leaves the builtin base untouched.
- Breaking: plugin registration. rc files and `--plugins` keep working.

**Phase 5 — directives validated by their owner (L).** Needs Phase 4.
- The core owns and types `engine`, `input`, `output`, `depends_on` and `extensions`.
  Each executor validates its own `input.<name>` with an `Input` model; each sink
  validates its `output.*`.
- Each plugin's `Config` validates only its own keys. The resolved config is frozen and
  read by model class: `cell.config[Vars]`. Expand, migrate, contract: attribute access
  stays as a deprecated shim while call sites move, then goes.
- Directives declare scope, merge strategy and `affects_results` (used by Phase 8).
- Deleted: `create_model` multi-inheritance, `_check_validator_names`, the Phase 1 memo,
  `input_spec`.
- Tests first: the core compiles with only core directives registered; two plugins may
  both name a validator `positive`; per-directive errors still aggregate per cell with
  line numbers; compile time stays at or below the Phase 1 number.
- Breaking: plugin config access, the biggest change for plugin authors. Update
  `docs/plugin-authoring.md`, the `emit_sql` demonstrator and the README's
  directive table.

**Phase 6 — capabilities and layering (M).** Can start after Phase 3.
- Executors declare `dialect`, replacing the engine-name-as-sqlglot-dialect convention and
  the LSP's `_DIALECT` map, and optionally a catalog capability, replacing
  `catalog._CONTEXT_BUILDERS`/`_CATALOG_TARGETS` and the engine branches in
  `lsp/schema.py`.
- `conduit.py` owns connecting, extension loading and its cache, `register_frame` and
  previews, replacing the runner privates that builtin plugins, the catalog, the LSP and
  the TUI import.
- `statement_spans`/`split_statements` and `make_sink` move into core modules, so the
  compiler imports no executor or sink module.
- import-linter's ignore list is empty and the contracts gate CI.
- Tests first: a scratch executor with `dialect = "tsql"` splits statements, gets LSP
  syntax checks and appears in the catalog with no core edits.
- Check: no engine or sink name is hard-coded outside its own module.

**Phase 7 — trust for project code (S).** Needs Phase 4.
- `quicksql trust [FILE]` records the hash of the notebook's `quicksqlrc.py` in the user
  config directory; editing the rc revokes trust.
- The CLI and TUI refuse an untrusted rc with an error naming `quicksql trust`; silently
  skipping it would surface as confusing unknown-directive errors.
- The LSP loads a trusted rc, and runs live schema lookups only for trusted notebooks;
  otherwise it reports one diagnostic on line 1.
- Tests first: an untrusted rc is never imported from the CLI, TUI or LSP (checked by a
  side effect); trust survives restarts and an edit revokes it.
- Behavior change: existing rc users run `quicksql trust` once.

**Phase 8 — fingerprints and cell state (M).** Needs Phase 5.
- Each cell's fingerprint hashes its rendered SQL, engine and context, the config fields
  marked `affects_results`, and its upstream fingerprints.
- `.quicksql/state.json` (gitignored), written after every run: each cell's last status
  and the fingerprint its landed output was built from.
- Watch and the TUI rerun cells whose fingerprint changed, plus their downstream — config
  edits included; toggling `autorun` or `tags` reruns nothing. This replaces body hashes
  and `config_only_changes`.
- `quicksql status [--json]`: ok / failed / skipped / stale per cell, where stale means
  the landed output's fingerprint differs from the current one.
- Every watch cycle ends with exactly one final status, including "no changes" and
  compile errors. The TUI compiles once per save (twice today) and `quicksql watch` once
  at startup (twice today).
- No result caching.
- Tests first: editing `output` or `vars` reruns under watch; toggling `autorun` doesn't;
  an upstream edit marks downstream stale until it reruns; each run writes the state file.
- Docs: drop the "config-only edits don't rerun" limitation from the README and CLAUDE.md.

**Phase 9 — split the TUI (L).** Best after Phases 2 and 8.
- Extract from `QsqlApp` a pane/sheet controller (stacks, split, search, filter, yank), a
  run controller (one worker owns the `RunSession`; generation tags; cancellation between
  cells) and a catalog view. `QsqlApp` keeps layout and key routing.
- Fix the open issues from the 2026-07-25 TUI review on the way: the data sheet goes
  stale after a watch rerun; sort, filter, freq and describe work on the 100-row preview
  without saying so; runs and recompiles overwrite the subtitle; a failed cell's
  traceback is re-logged on every cursor move; a replaced run still shares the session.
- Tests first: the existing pilot tests pass unchanged after each extraction; one new
  test per fixed issue; `/verify` in tmux after each step.

**Phase 10 — dependencies (S).** Last.
- Previews leave the core as columns plus rows; the TUI builds its own Polars frames.
- `textual` moves to a `tui` extra; `quicksql tui` prints an install hint without it.
- Polars remains only where frames are built (the TUI, and SQLite/Postgres extraction)
  until those move to Arrow, which is decided separately.
- Tests first: with `textual` and `polars` blocked from import, the sources demo compiles
  and runs.

## Sequencing

Phase 1 can interleave with anything. 2 before 3 (statement attribution needs the runner
to own statement execution). 4 before 5 and 7; 5 before 8 (fingerprints need
`affects_results`). 6 any time after 3. 9 after 2 and 8. 10 last.

## Risks

- **Plugin API breaks** in Phases 3, 4 and 5. Pre-1.0 with no known third-party plugins;
  each phase updates `docs/plugin-authoring.md`, the demonstrators, the README and
  CLAUDE.md in the same change.
- **User-visible behavior changes:** exit code 3 and skipped downstream (2), per-cell
  DuckDB input rejected (3), rc trust (7), config edits rerun (8). Each is noted in the
  README when it lands.
- **Phase 5 touches every config read.** The shim lets it land plugin by plugin; removing
  the shim is its own commit.
- **Fingerprints** can miss reruns or report false staleness. Caching stays out of scope,
  `status` shows why a cell is stale, and a property test checks the fingerprint changes
  exactly when an `affects_results` input does.
- **TUI regressions** in Phase 9: pilot tests plus `/verify` after each extraction.
- **Memory:** run the full suite under `systemd-run --user --scope -p MemoryMax=3G`
  (CLAUDE.md gotchas).

## Explicitly out of scope

- A from-scratch rewrite, or moving modules into `core/` and `ext/` packages;
  import-linter enforces the layering on the current layout.
- The agent-facing CLI beyond `run --json` and `status`: `check`, `catalog`,
  `status --wait`, JSON watch events and a Claude Code skill for notebooks. Separate plan;
  Phases 2 and 8 lay its foundations.
- An MCP server.
- pluggy. It fits about half of today's hooks; revisit only if pip-installable
  third-party plugins become a goal.
- ATTACH-based per-cell DuckDB contexts, parallel scheduling, result caching and
  ephemeral BigQuery intermediates (Phase 4 of `docs/engine-context-plan.md`).

## Verification

- After every phase: `uv run pytest`, and with `docker compose up -d --wait`,
  `uv run --extra postgres --extra bigquery pytest -m "not network"`, both under the
  memory cap.
- The Phase 0 demo test stays green throughout.
- `demo/kitchen-sink/kitchen-sink.qsql` compile time: 25 ms baseline, at most 6 ms after
  Phases 1 and 5.
- Mechanical checks: no `sink_type == "none"` after Phase 3; no `(config.input or {})`
  after Phase 5; import-linter passes with an empty ignore list after Phase 6.
- `/verify` (the TUI in tmux) after Phases 2, 8 and 9; the kitchen-sink notebook against
  the demo stack (ports 5433/9051) after Phases 3 and 9.
