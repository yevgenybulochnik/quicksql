# Plan: in-place refactor toward a minimal core

**Status:** in progress. Proposed 2026-10-15; revised 2026-10-30 after a review of the
plan (phase dependencies, Phase 3/4/7/8 design corrections, Phase 5 made optional).

**Progress (2026-10-30):**
- Done — within a run, downstream of a failed cell is skipped, not run on stale data;
  `RunResult.status` is `ok` | `failed` | `skipped`, shown as `⊘` in the TUI and `skip` in
  the CLI (`b80d936`, `08eb63b`, `7185bba`). The core of Phase 2.
- Done — `@dev_limit` removed rather than fixed; write `LIMIT` in the cell's SQL
  (`75bffb9`).
- Done — readable failures: `RunResult.error` is the engine's message, `traceback` rides
  apart, `ok` is a plain property of `status`; the CLI prints the message with DuckDB's
  context indented beneath (`439517a..ed58981`).
- Done — the cross-run skip: exact reruns rerun an upstream that wasn't ok this session
  (`224f100..155246d`).
- Done — `run --json [--rows N]` and exit codes 1 = a cell failed, 3 = compile error or
  invalid selection, for every command (`53fcfc5..d1575d9`). Phase 2 is complete.
- Done — the TUI gives each opened notebook its own `RunSession`, so a rerun after `o`
  can't read the previous notebook's temps, and logs a failed cell's full message once
  instead of on every cursor move (`d65a35b..247fcd5`). Two Phase 9 issues, fixed early.
- Next — the Phase 1 fixes, including the dead code (now unblocked), and the Phase 0
  safety net before Phase 3. The agent-facing CLI beyond `run --json` is planned in
  `docs/agent-cli-plan.md`.

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
below lands in place, one phase at a time, with the existing test suite as the safety net.

| Finding (2026-10-15 unless dated) | Evidence | Phase |
|---|---|---|
| Append-mode sinks report the table's total rows | `count(*)` after `INSERT` | 1 |
| TUI preview fallback assumes `data/<cell>.parquet` | ignores `output.dir` and other sinks | 1 |
| A changed `autorun: false` cell still reruns its unchanged downstream | reproduced via `plan_rerun` | 1 |
| Templates render unsandboxed; the LSP renders on file open | `''.__class__.__mro__` reachable | 1 |
| Composed config models rebuilt per cell | ~80% of compile time; 25 → 5 ms memoized | 1 |
| Downstream of a failed cell runs on stale data and reports ok | reproduced, cross-context and in-session | 2 (fixed within a run) |
| Across watch reruns, a cross-context upstream that failed earlier is still read stale (2026-10-30) | reproduced with a reused `RunSession` | 2 (fixed) |
| DuckDB errors print as `FAIL a: ^` | reproduced | 2 (fixed) |
| DuckDB's `LINE 1:` error context shows the wrapper (`CREATE OR REPLACE TEMP TABLE "a" AS …`), not the cell's SQL (2026-10-30) | reproduced | 3 |
| Cell protocol duplicated in all 4 executors; `sink_type == "none"` checks | read | 3 |
| Per-cell `input.duckdb` is silently ignored | reproduced: ran in `:memory:` | 3 |
| Dropped Postgres connections and expired BigQuery sessions go undetected | read | 3 |
| `config.input` is hand-parsed in 12 places | grep | 3, 5 |
| Process-global registries: rc plugins leak across notebooks; TUI `o` skips the new rc | read | 4 |
| An edited rc never reloads in a long-lived TUI/LSP: the loader skips names already in `sys.modules` (2026-10-30) | read: `cli._load_plugin_module` | 4 |
| `executors/`, `sinks/` and `plugins/__init__.py` import every implementation, so importing a contract (`plugins.base`) loads all builtins (2026-10-30) | read | 4, 6 |
| Unregistering the Input plugin breaks every compile (`AttributeError`) | reproduced | 5 |
| Validator names must be unique across all plugins (configs composed by inheritance) | read | 5 |
| Engine names hard-coded in `catalog.py` (12), `lsp/schema.py` (7), the LSP dialect map | grep | 6 |
| Plugins, catalog, LSP and TUI import runner privates; 32 function-level imports | grep | 6 |
| CLI commands auto-import `quicksqlrc.py`; the LSP never loads it | read | 7 |
| Config-only edits never rerun (documented limitation) | README, CLAUDE.md | 8 |
| A TUI notebook switch (`o`) kept the previous notebook's `RunSession`: a rerun of `b` read the old notebook's temp `a` (2026-10-30) | reproduced | 9 (fixed) |
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
  each phase deletes its entries, and Phase 6 ends with it empty. The eager package
  `__init__`s (`executors/`, `sinks/`, `plugins/`) belong on it: through them, `config.py`
  importing `plugins.base` loads every builtin plugin, and `compiler.py` imports
  `bootstrap`.
- There is no CI, so a pytest test runs `lint-imports`: the contracts gate every
  `uv run pytest`.
- Record the baseline: `demo/kitchen-sink/kitchen-sink.qsql` compiles in ~23 ms
  (2026-10-30), 79% of it in `build_models`, called 8 times per compile.

**Phase 1 — quick fixes (S; independent red/green pairs).**
- Append-mode DuckDB and Postgres sinks report the `INSERT`'s own row count.
- The TUI's `_preview_frame` falls back to the cell's own sink (`make_sink(...).ref_expr`),
  not `data/<cell>.parquet`.
- `plan_rerun` propagates only from changed cells that will rerun.
- Templates render in `jinja2.sandbox.SandboxedEnvironment`. A prototype compiled both
  demo notebooks unchanged and blocked `''.__class__`.
- Cache the composed models on the tuple of plugin `Config` classes being composed:
  ~23 → ~5 ms. Keyed on the classes themselves, it stays correct when registries change
  and survives Phase 4 without a generation counter. Test call counts, not wall time.
- Remove dead code: `models.Cell`, `RunResult.pl`, `RenderedCell.uses_sources`,
  `RunContext.overrides`, unused imports (`tempfile` in `runner.py`; `Ref`/`Source` in
  `lsp/analysis.py`), unused `project` parameters (`_resolve_scope`, `relation_names`).

**Phase 2 — honest run outcomes (M; done).**
- Done: `RunResult.status`: `ok` | `failed` | `skipped`.
- Done: the runner skips cells whose upstream failed or was skipped, across `ref()` and
  `@depends_on` edges, transitively: `skipped: upstream 'a' failed`. Skipped cells bypass
  the run chain; `after_run` still receives them.
- Done: a cell that didn't come out ok leaves `session.materialized`, so an exact rerun
  re-includes it instead of reading its stale temp (Phase 3 also drops it).
- Done: README notes the behavior change; the CLI prints `skip`, the TUI `⊘`.
- Done: `error` is the message — `str(exc)` for quicksql and DuckDB errors, else
  prefixed with the exception type — and a separate `traceback` field keeps the
  traceback. The CLI and `RunEvent.line()` print the message, not the traceback's last
  line; executor errors no longer repeat the cell name. `ok` is a plain read-only
  property of `status`.
- Done: `RunSession` remembers each cell's last status, and exact reruns (watch/TUI)
  pull in any upstream whose last result wasn't ok, the way missing temps already are,
  so a cross-context upstream that failed in an earlier rerun is no longer read stale.
- Done: `RunResult.to_dict()`; `quicksql run --json [--rows N]` prints one document and
  no progress, for compile errors and other pre-run failures too; exit codes 1 = a cell
  failed, 3 = compile error or invalid selection, for every command (2 stays click's
  usage error). `RunEvent.to_dict()` waits for JSON watch events
  (`docs/agent-cli-plan.md`).

**Phase 3 — the runner owns the cell protocol (L).**
- Executors open one session per context with `execute(sql)`, `materialize(name, sql)`,
  `extract(sql_or_name) -> view`, `drop(name)`, `alive()` and `close()`. The runner
  implements effect-only / referenced-in-context / plain once — the
  `materialize`/`extract` split `docs/engine-context-plan.md` planned — and records which
  statement failed, completing Phase 2's structured error.
- `Sink.lands_output` replaces every `sink_type == "none"` check (four executors, the
  compiler).
- SQLite and Postgres sessions share one DB-API base. BigQuery keeps its own (scripts are
  never split) and reuses one client per context instead of creating one per cell. A
  BigQuery cell with no in-context refs still runs as one plain job; a session opens only
  for cells that use one.
- `conduit.py` is created here: the DuckDB conduit is one of the sessions. Phase 6 moves
  the remaining runner privates into it.
- `input_spec(config, engine)` replaces the 12 hand-parsed `(config.input or {}).get(...)`
  sites until Phase 5 types them.
- A DuckDB cell whose `input.duckdb` differs from the conduit's target is a compile error;
  today it silently runs in the conduit. ATTACH-based support is deferred.
- Session liveness, the risk `docs/engine-context-plan.md` named. `alive()` is a cheap
  local check (psycopg's `closed`/`broken`), never a round trip. The runner calls it at run
  start, before `_expand_missing_temps` fixes the cell list: a dead session is reopened
  and its temps leave `session.materialized`, so upstreams re-materialize. BigQuery
  session expiry can't be checked locally, so it is handled when a query fails: reopen,
  re-materialize, retry once. A session that dies mid-run fails its cell, and Phase 2
  skips the downstream.
- Tests first: a runner matrix (effect-only / referenced / plain × a fake session); the
  existing executor tests unchanged; a third-party sink with `lands_output = False`
  behaves like `none`; per-cell DuckDB input fails to compile; a dead fake session is
  reopened and its upstream re-materialized.
- Breaking: the executor contract. Update `docs/plugin-authoring.md`.

**Phase 4 — a registry per notebook (L).**
- A `Registries` object (plugins, executors, sinks, readers) per notebook, passed
  explicitly through compile, render and config. `Project` carries its `Registries`, so
  the runner, sinks, watcher, TUI and LSP get them from the project. This finishes the
  seam `config.py` already has (`plugins=` parameters).
- Decorators mark classes instead of registering them; built-ins come from one explicit
  list in an app-layer composition root, which import-linter classifies with the
  front-ends. The package `__init__`s stop importing their implementations.
- `--plugins` and `quicksqlrc.py` load through one shared loader (moved out of `cli.py`)
  into the notebook's registry; the TUI uses it at startup and on `o`. The loader keys
  modules on the rc's content hash, so an edited rc reloads in a long-lived TUI. The LSP
  stays rc-free until Phase 7.
- Hook participant lists are computed once per registry; `overriding()` re-sorts on every
  call today.
- The autouse snapshot/restore fixture in `tests/conftest.py` becomes a per-test registry.
- Tests first: two notebooks with different rc plugins in one process don't see each
  other's directives; two rc files defining the same plugin name both load; `o` into a
  notebook with its own rc picks it up; editing the rc and recompiling picks up the
  change; loading an rc leaves the builtin base untouched.
- Breaking: plugin registration. rc files and `--plugins` keep working.

**Phase 5 — directives validated by their owner (L; optional).** Needs Phase 4. Its wins
(validator names, typed `input`, a core that compiles without the Input plugin) are
internal and nothing depends on it, so it goes last, if at all.
- The core owns and types `engine`, `input`, `output`, `depends_on` and `extensions`.
  Each executor validates its own `input.<name>` with an `Input` model; each sink
  validates its `output.*`.
- Each plugin's `Config` validates only its own keys. The resolved config is frozen.
  Typed access by model class is added (`cell.config[Vars]`), and attribute access
  (`cell.config.vars`) stays for good: the registry's field guard keeps directive keys
  unique across plugins, so it is unambiguous.
- Open decision: whether the decision hooks `resolve_engine`, `resolve_sink` and
  `collect_edges` survive once the core owns those fields. They exist so builtin
  directives could own them. Either way, update CLAUDE.md's "everything is a plugin".
- Deleted: `create_model` multi-inheritance, `_check_validator_names`, the Phase 1 cache,
  `input_spec`.
- Tests first: the core compiles with only core directives registered; two plugins may
  both name a validator `positive`; per-directive errors still aggregate per cell with
  line numbers; compile time stays at or below the Phase 1 number.
- Breaking: plugin config access, the biggest change for plugin authors. Update
  `docs/plugin-authoring.md`, the `emit_sql` demonstrator and the README's
  directive table.

**Phase 6 — capabilities and layering (M).** Needs Phases 3 and 4: the empty ignore list
needs Phase 4's composition root and lazy package `__init__`s.
- Executors declare `dialect`, replacing the engine-name-as-sqlglot-dialect convention and
  the LSP's `_DIALECT` map, and optionally a catalog capability, replacing
  `catalog._CONTEXT_BUILDERS`/`_CATALOG_TARGETS` and the engine branches in
  `lsp/schema.py`.
- `conduit.py` (created in Phase 3) takes over extension loading and its cache,
  `register_frame` and previews, replacing the runner privates that builtin plugins, the
  catalog, the LSP and the TUI import.
- `statement_spans`/`split_statements` and `make_sink` move into core modules, so the
  compiler imports no executor or sink module.
- import-linter's ignore list is empty; the Phase 0 pytest gate enforces it.
- Tests first: a scratch executor with `dialect = "tsql"` splits statements, gets LSP
  syntax checks and appears in the catalog with no core edits.
- Check: no engine or sink name is hard-coded outside its own module.

**Phase 7 — trust for project code (S).** Needs Phase 4.
- Trust has two units, recorded in the user config directory by `quicksql trust [FILE]`:
  - the content hash of the notebook's `quicksqlrc.py` gates importing project code;
    editing the rc revokes it;
  - the notebook's directory gates the LSP's live schema lookups, which connect to the DSNs
    and projects a notebook names. Notebooks without an rc are covered too.
- Modules the rc imports aren't hashed; document it, as direnv does. `--plugins` is an
  explicit choice and needs no trust.
- The CLI and TUI refuse an untrusted rc with an error naming `quicksql trust`; silently
  skipping it would surface as confusing unknown-directive errors.
- The LSP loads a trusted rc, and runs live schema lookups only in trusted directories;
  otherwise it reports one diagnostic on line 1.
- Tests first: an untrusted rc is never imported from the CLI, TUI or LSP (checked by a
  side effect); trust survives restarts and an edit revokes it.
- Behavior change: existing rc users run `quicksql trust` once.

**Phase 8 — fingerprints and cell state (M).** Needs Phase 2.
- Fields declare `affects_results` on `qfield`, the way they declare `qsql_merge` today:
  default true; `autorun`, `tags`, `catalog` and `render_dir` false. No Phase 5 needed.
- Each cell's fingerprint hashes its rendered SQL, engine and context, the config fields
  marked `affects_results`, and the *landed* fingerprints of its upstreams: those of the
  outputs it actually read. Hashing upstreams' current fingerprints would be wrong. Edit
  an `autorun: false` cell `a` and `b`'s fingerprint changes, so `b` reruns on `a`'s old
  output and is recorded fresh. When `a` reruns later, `b`'s fingerprint doesn't change
  again, so `b` keeps old-`a` data while reported fresh. The landed rule also covers
  Phase 1's `plan_rerun` fix.
- `.quicksql/state.json` (gitignored), written atomically after every run, one entry per
  notebook: each cell's last status and the fingerprint its landed output was built from.
- Watch and the TUI rerun a cell when its current fingerprint differs from the one its
  landed output was built from in this session, plus downstream — config edits included;
  toggling `autorun` or `tags` reruns nothing. This replaces body hashes and
  `config_only_changes`. The state file feeds `status` only: startup still runs every
  autorun cell (no caching).
- `quicksql status [--json]`: ok / failed / skipped / stale per cell, where stale means
  the landed output's fingerprint differs from the current one. Stale is relative to the
  notebook: source files and remote tables aren't fingerprinted; say so in `status`
  help.
- Every watch cycle ends with exactly one final status, including "no changes" and
  compile errors. The TUI compiles once per save (twice today) and `quicksql watch` once
  at startup (twice today).
- No result caching.
- Tests first: editing `output` or `vars` reruns under watch; toggling `autorun` doesn't;
  an upstream edit marks downstream stale until it reruns; after an edited
  `autorun: false` upstream is rerun by hand, its downstream is stale; each run writes the
  state file.
- Docs: drop the "config-only edits don't rerun" limitation from the README and CLAUDE.md.

**Phase 9 — split the TUI (L).** Best after Phases 2 and 8.
- Extract from `QsqlApp` a pane/sheet controller (stacks, split, search, filter, yank), a
  run controller (one worker owns the `RunSession`; generation tags; cancellation between
  cells) and a catalog view. `QsqlApp` keeps layout and key routing.
- Fix the open issues from the 2026-07-25 TUI review on the way: the data sheet goes
  stale after a watch rerun; sort, filter, freq and describe work on the 100-row preview
  without saying so; runs and recompiles overwrite the subtitle; a replaced run still
  shares the session (Textual's `exclusive` only marks the old thread worker cancelled).
  Already fixed: per-notebook sessions and the per-cursor-move re-log (2026-10-30).
- Smaller issues found on the way: a catalog load still running across a notebook switch
  fails with DuckDB's "Connection already closed" (no crash: the switch closes the old
  session); a single-line failure is logged twice, as the `FAILED` line and the full
  message block (log the block only for multi-line messages).
- Tests first: the existing pilot tests pass unchanged after each extraction; one new
  test per fixed issue; `/verify` in tmux after each step.

**Phase 10 — dependencies (S).** Last.
- Previews leave the core as columns plus rows; the TUI builds its own Polars frames.
- `textual` moves to a `tui` extra; `quicksql tui` prints an install hint without it.
- Polars remains only where frames are built (the TUI, and SQLite/Postgres extraction)
  until those move to Arrow, which is decided separately. The SQLite and Postgres
  executors import it lazily; today they import it at module top.
- Tests first: with `textual` and `polars` blocked from import, the sources demo compiles
  and runs.

## Sequencing

Phase 1 can interleave with anything, and each item is its own branch.
- 0 before 3, the first L.
- 2 before 3 (statement attribution needs the runner to own statement execution) and
  before 8 (fingerprints record outcomes).
- 3 and 4 before 6; 4 before 7 and 5.
- 9 after 2 and 8; 10 last; 5 is optional.

Suggested order: 0, 1, 2, 3, 8, 4, 6, 7, 9, 10, then 5 if still wanted. Work lands as
one focused change at a time, each on its own worktree branch.

## Risks

- **Plugin API breaks** in Phases 3, 4 and 5. Pre-1.0 with no known third-party plugins;
  each phase updates `docs/plugin-authoring.md`, the demonstrators, the README and
  CLAUDE.md in the same change.
- **User-visible behavior changes:** skipped downstream (2, landed and in the README),
  exit code 3 (2), per-cell DuckDB input rejected (3), rc trust (7), config edits rerun
  (8). Each is noted in the README when it lands.
- **Phase 5 touches every config read.** Attribute access stays, so it can land plugin by
  plugin.
- **Fingerprints** can miss reruns or report false staleness. Caching stays out of scope,
  `status` shows why a cell is stale, and a property test checks the fingerprint changes
  exactly when an `affects_results` input or an upstream's landed fingerprint does.
- **TUI regressions** in Phase 9: pilot tests plus `/verify` after each extraction.
- **Memory:** run the full suite under `systemd-run --user --scope -p MemoryMax=3G`
  (CLAUDE.md gotchas).

## Explicitly out of scope

- A from-scratch rewrite, or moving modules into `core/` and `ext/` packages;
  import-linter enforces the layering on the current layout.
- The agent-facing CLI beyond `run --json` and `status`: `check`, `catalog`,
  `status --wait`, JSON watch events and a Claude Code skill for notebooks. Planned in
  `docs/agent-cli-plan.md`; Phases 2 and 8 lay its foundations.
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
- `demo/kitchen-sink/kitchen-sink.qsql` compile time: ~23 ms baseline, at most 6 ms after
  Phase 1. Measured by a benchmark script, not asserted in pytest.
- Mechanical checks: no `sink_type == "none"` after Phase 3; no `(config.input or {})`
  outside `input_spec` after Phase 3; import-linter passes with an empty ignore list after
  Phase 6.
- `/verify` (the TUI in tmux) after Phases 2, 8 and 9; the kitchen-sink notebook against
  the demo stack (ports 5433/9051) after Phases 3 and 9.
