# quicksql — a notebook for SQL, in one file

A single `.qsql` / `.sql` file *is* the notebook: valid SQL split into named **cells** by
`-- @cell` comment directives. Configuration is YAML tucked inside `@`-comments; SQL bodies
are Jinja-templated. Cells wire themselves into a dependency **DAG**, run on pluggable
**engines** (DuckDB, SQLite, Postgres, BigQuery), and land through pluggable **sinks**
(parquet files, DuckDB tables, Postgres tables) that also define how downstream cells read
them back.

No project scaffold, no YAML config file, no separate model files — the SQL file carries
everything. `git diff` shows real SQL. Your editor highlights real SQL.

```sql
-- @engine: duckdb
-- @output: { type: parquet, dir: data/ }

-- @cell users
SELECT * FROM {{ source('seeds/users.csv') }};

-- @cell legacy_orders
/*@ input: { sqlite: legacy.db } */
SELECT * FROM orders;

-- @cell report
-- @output: { type: duckdb, path: warehouse.db }
SELECT u.name, count(*) AS orders
FROM {{ ref('users') }} u
JOIN {{ ref('legacy_orders') }} o USING (user_id)
GROUP BY 1;
```

Three cells, three engines' worth of wiring, one file. `users` reads a CSV, `legacy_orders`
runs against a SQLite database, and `report` joins them — DuckDB is the conduit that
extracts the SQLite result and lands everything in a DuckDB warehouse.

## Quick start

```console
$ uv sync
$ uv run quicksql              # bare invocation scaffolds base.qsql
$ uv run quicksql run          # run every cell in dependency order, land data/<cell>.parquet
$ uv run quicksql run --json   # the same run as one JSON document, for scripts and agents
$ uv run quicksql list         # cells in topo order: engine -> sink, autorun, deps
$ uv run quicksql show report  # rendered SQL for one cell
$ uv run quicksql compile      # rendered SQL for every cell
$ uv run quicksql explain      # the effective plugin chain + per-cell engine/sink/context
$ uv run quicksql watch        # re-run changed cells + their dependents on every save
$ uv run quicksql status       # each cell: ok, failed, skipped, stale (and why), or new
$ uv run quicksql tui          # interactive VisiData-style terminal UI (the `tui` extra)
```

`quicksql` and the shorter `qsql` are the same entry point. The default file is `base.qsql`;
every command takes an explicit path as its first argument.

A `run` prints a live progress trace and a green/red per-cell summary:

```console
$ uv run quicksql run
  users: started (duckdb → parquet)
  users: executing on duckdb
  users: landing via parquet
  events: started (duckdb → parquet)
  ...
  ok users                 3 rows     3.9 ms  -> data/users.parquet
  ok events               10 rows     3.2 ms  -> data/events.parquet
  ok active_user_events    7 rows     3.2 ms  -> data/active_user_events.parquet
```

When a cell fails, the summary leads with the engine's message and indents its context
(DuckDB's `LINE`/caret) beneath. Every cell downstream of it — through `ref()` or
`@depends_on`, transitively — is **skipped**, not run (`⊘` in the TUI), and independent
cells still run:

```console
  FAIL users: Catalog Error: Table with name user_seed does not exist!
      Did you mean "sqlite_master"?

      LINE 1: CREATE OR REPLACE TEMP TABLE "users" AS SELECT * FROM user_seed, (VALUES
                                                                    ^
  ok events                         10 rows       4.0 ms  -> data/events.parquet
  skip active_user_events: upstream 'users' failed
```

*Behavior change:* earlier versions ran the downstream cells anyway, on the previous run's
output, and reported them ok.

Across reruns too: when watch or the TUI reruns a cell whose upstream failed (or was
skipped) earlier in the session, it reruns that upstream first, so the cell never reads its
stale output.

### Scripts and agents: `--json` and exit codes

`quicksql run --json` prints one JSON document on stdout and nothing else: no progress
lines and no colours. Compile warnings still go to stderr. The document is indented on a
terminal and one line when piped. `--rows N` adds the first `N` rows of each ok cell's
output, read from the cached preview, so at most 100 (`--rows` without `--json` is a
usage error). The failing run above, as JSON (messages cut short):

```json
{
  "file": "base.qsql",
  "status": "failed",
  "summary": {"ok": 1, "failed": 1, "skipped": 1},
  "elapsed": 0.0536,
  "errors": [],
  "cells": [
    {"cell": "users", "status": "failed", "rows": null, "target": null, "elapsed": 0.0219,
     "error": "Catalog Error: Table with name user_seed does not exist!\nDid you mean ...",
     "statement": 1, "traceback": "Traceback (most recent call last):\n ...",
     "preview": null},
    {"cell": "events", "status": "ok", "rows": 10, "target": "data/events.parquet",
     "elapsed": 0.0045, "error": null, "statement": null, "traceback": null,
     "preview": {"columns": ["user_id", "event"], "rows": [[1, "event_0"]]}},
    {"cell": "active_user_events", "status": "skipped", "rows": null, "target": null,
     "elapsed": 0.0, "error": "upstream 'users' failed", "statement": null,
     "traceback": null, "preview": null}
  ]
}
```

`status` is `ok`, `failed`, `compile_error`, or `error` when something else stopped the run
before any cell ran, such as a `--plugins` module that won't import (exit 1). A cell's
`error` is the message the summary prints and `traceback` the full trace; `statement`
numbers, from 1, the cell's statement that failed (`null` when the failure was outside
them, such as connecting), and the message starts `statement 2 of 3:` when the cell has
more than one. `elapsed` is in seconds. `preview` appears only with
`--rows`. Its values are JSON-safe: dates and times as ISO text, decimals as exact text,
NaN as `"nan"`. On `compile_error` or `error`, `cells` is empty and `errors` lists each
problem as `{"cell", "line", "message"}`; `cell` and `line` are `null` when the problem
isn't tied to a cell, such as an unknown `--select` cell.

Every command exits with one of these codes:

| Exit | Meaning |
|---|---|
| 0 | Success: every cell ran ok. |
| 1 | A cell failed. A skipped cell never causes it alone, since every skip has a failed upstream in the same run. Also other runtime failures, such as `init` refusing to overwrite. |
| 2 | Usage error: an unknown option or a bad value. |
| 3 | The notebook didn't compile (a parse, config or cycle error), or the selection names an unknown cell (`run --select`, `compile --select`, `show`). |
| 4 | `quicksql live`: no TUI has the notebook open (or it timed out, or speaks another protocol version: restart the TUI). |
| 5 | Refused: `quicksql live` asked for something the session won't do (a run while one is under way, autorun on for a cell the file switches off), or `run`/`watch` while a TUI has the notebook open. |

*Behavior change:* compile errors used to exit 1, like a failed cell, and `compile --select`
with an unknown cell printed nothing and exited 0.

## Anatomy of a cell

A cell is **input → engine → output**. The file is parsed into cells at each `-- @cell
<name>`; everything before the first `@cell` is a **global header** whose directives every
cell inherits.

- **Directives** are `@`-comments. Line form `-- @key: value`, block form `/*@ ... */` for
  multi-line YAML. Values are literal YAML (Jinja never touches config).
- **The SQL body** is Jinja-templated. `{{ ref('cell') }}`, `{{ source('path') }}`,
  `{{ var('key') }}`, and `{{ env('VAR') }}` are the built-in globals; `{% if %}` control
  flow works too.
- **`{{ ref('other') }}`** records a dependency edge *and* renders to however the producer's
  sink is read back — a bare temp-table name when the two cells share an engine, or a
  `read_parquet(...)` / attached-table expression when they don't.

```sql
-- @cell active_user_events
-- @depends_on: [users]
SELECT u.name, e.event
FROM {{ ref('users') }} u
JOIN {{ ref('events') }} e USING (user_id)
{% if var('active_only') %}WHERE u.active{% endif %};
```

`quicksql show active_user_events --set vars.active_only=false` renders the `ref()`s to the
concrete read-back and drops the `WHERE`:

```sql
SELECT u.name, e.event
FROM users u
JOIN events e USING (user_id)
;
```

### Multiple statements per cell

A cell body may be several `;`-separated statements. The **last** statement is the one that
lands (and that downstream cells `ref()`); the earlier statements run for their side effects
first — build up a temp, transform it, then select from it:

```sql
-- @cell enriched
CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM {{ ref('raw') }} WHERE ok;
UPDATE staging SET amount = amount * 100;   -- run for effect
SELECT * FROM staging;                      -- <- this is what lands
```

Set `-- @output: { type: none }` to make a cell **effect-only** — every statement runs and
nothing lands. Effect-only cells can't be `ref()`'d (there's no result to read back); depend
on one for ordering with `@depends_on`.

```sql
-- @cell load_warehouse
-- @output: { type: none }
INSERT INTO warehouse.fact SELECT * FROM {{ ref('enriched') }};
```

Notes: a failure names its statement when the cell has several (`statement 2 of 3: ...`).
On Postgres/SQLite the statements share the cell's (autocommit) session, so a failure
partway leaves earlier statements' effects in place. A Postgres connection that drops is
reopened on the next run, and its temps re-created. On DuckDB, `CREATE TEMP TABLE` in a setup
statement lives on the shared conduit — prefer `CREATE OR REPLACE TEMP TABLE` to avoid clashes
across cells. BigQuery runs the whole body as one native script (its terminal statement's rows
land); a BigQuery cell that is `ref()`'d in-context must be a single statement.

## How it works

- **Cells + DAG.** Edges come from `{{ ref('cell') }}` and explicit `@depends_on`. Cells are
  topo-sorted with cycle detection; upstreams run first.
- **Sinks are the interchange.** Every cell lands via its sink (default
  `data/<cell>.parquet`). A sink also knows how to be *read back* — that read-back is exactly
  what a downstream `ref()` renders to.
- **Engine contexts.** Cells partition by `(engine, connection target)`. When two cells share
  a context, `ref()` resolves to a bare temp-table name in that engine's own dialect and the
  dependency executes right next to the data (a session-scoped DuckDB conduit, a shared
  SQLite connection, or a BigQuery session). The cell still lands via its sink — one
  execution, two consumers.
- **DuckDB is the cross-context conduit.** A cell that reaches across contexts — cross-context
  `ref()`/`source()`, or `@extensions` — must run on DuckDB (a compile-time guardrail).
  Other engines extract their result to Arrow/Polars and DuckDB writes it into whatever sink
  you asked for.
- **Everything is a plugin.** Directives, engines, file readers, and sinks all register via
  decorators. A plugin bundles config fields (pydantic-validated) with behavior hooks that
  can wrap cell execution — retries, caching, row caps, SQL emission. `quicksql explain`
  prints the effective chain. See [docs/plugin-authoring.md](docs/plugin-authoring.md).

## Directive vocabulary

Every directive below is a plugin's config field; adding a plugin adds directives. Scope is
where it may appear (global header, per-cell, or both). `quicksql directives [NOTEBOOK]`
lists exactly what a notebook can write, its `quicksqlrc.py` plugins included: each
directive's scope, type, default, merge strategy, whether editing it reruns the cell, and
what it does (`--json`, or `--markdown`).

| Directive | Scope | What it does |
|---|---|---|
| `engine` | both | Pick the executor explicitly (`duckdb`, `sqlite`, `postgres`, `bigquery`). |
| `input` | both | Connection config; the engine is inferred from the sole top-level key (e.g. `{ sqlite: legacy.db }`). |
| `output` | both | Sink config. Default `{ type: parquet, dir: data/ }`. `{ type: none }` makes the cell effect-only (runs, lands nothing, not `ref()`-able). |
| `sources` | both | Named file sources that `source('name')` resolves (path + reader options). |
| `extensions` | both | DuckDB extensions to load on the conduit before the cell runs. |
| `vars` | both | Values that `var('key')` reads. |
| `depends_on` | cell | Extra dependency edges beyond what `ref()` records. |
| `schema` | both | Target schema injected into database-sink configs. |
| `autorun` | both | Default `true`; set `false` to skip a cell in watch/TUI rerun cascades. |
| `catalog` | global | Extra schema-browser targets for the TUI's `S` view, keyed by engine. |
| `tags` | both | Free-form labels. |
| `render_dir` | global | Write each cell's rendered SQL to a directory before it runs (demonstrator plugin). |

Jinja globals available in SQL bodies: `ref('cell')`, `source('name_or_path')`,
`var('key'[, default])`, `vars` (the whole dict), `env('VAR'[, default])`.

SQL templates render in Jinja's sandbox: they can call these globals but can't reach
Python internals through them (`''.__class__`, `__globals__`), so compiling a notebook,
or opening it in the language server, can't run arbitrary Python. A blocked access is a
`template error` at compile time. The sandbox covers templates only: `quicksql run` still
executes the notebook's SQL, CLI commands still import a `quicksqlrc.py` next to it
(trusting a directory is planned), and `env()` reads environment variables by design.

## Engines, sinks, and sources

**Engines** (where a cell executes):

| Engine | Notes |
|---|---|
| `duckdb` | Default. The cross-context conduit; the only engine that can serve cross-context `ref()`/`source()`/`@extensions`. Its database is the header's `input: { duckdb: path.db }` (in memory without one); a cell naming a different one is a compile error, since every duckdb cell runs on the conduit. |
| `sqlite` | `input: { sqlite: path.db }`. Same-file cells share one connection so in-engine `ref()` works. |
| `postgres` | `input: { postgres: { dsn: "$PG_DSN" } }`. Needs the `postgres` extra; same-DSN cells share a session. |
| `bigquery` | `input: { bigquery: { project: ..., endpoint: ... } }` (or just `{ bigquery: project }`). Needs the `bigquery` extra; `endpoint` targets an emulator. Same-project cells share one client; an expired session is reopened and the cell retried once. |

**Sinks** (where a cell lands, and how it's read back):

| Sink | Config | Read-back |
|---|---|---|
| `parquet` | `{ type: parquet, dir: data/ }` | `read_parquet(...)` |
| `duckdb` | `{ type: duckdb, path: warehouse.db }` | attached DuckDB table |
| `postgres` | `{ type: postgres, dsn: "$PG_DSN", table: schema.name }` | attached Postgres table |

The Postgres *sink* needs **no** Python driver — DuckDB's `postgres` extension handles the
write. The Postgres *engine* (running cells next to the data) is what needs the `postgres`
extra.

**Source readers** (for `source()` / the `sources` directive): `csv`, `parquet`, `json`,
`excel` (the Excel reader pulls in DuckDB's `excel` extension automatically).

## Configuration and overrides

Config resolves in layers: **global header → per-cell → run-time overrides**. Each field
merges by its own strategy — `OVERRIDE` (replace), `DEEP` (recursive dict merge, e.g.
`input`/`output`/`vars`), or `EXTEND` (append lists, e.g. `extensions`/`depends_on`).

Two override channels, both applied last:

```console
$ uv run quicksql run --set output.type=duckdb --set vars.active_only=false
$ QUICKSQL_VARS__MONTHS=12 uv run quicksql run
```

`--set key.path=value` is repeatable; `QUICKSQL_KEY__PATH=value` uses `__` as the path
separator. Overrides are literal values, layered on top of the merged global+cell config.

## Plugins

`@plugin` / `@executor` / `@source_reader` / `@sink` decorators only mark a class: each
notebook's registries hold the builtins plus the classes its `quicksqlrc.py` and `--plugins`
modules define. An executor declares its sqlglot `dialect` and, optionally, a catalog, so a new engine
gets statement splitting, LSP syntax checks and column completion, and the TUI catalog
with no core edits.
A `Plugin` pairs a pydantic `Config` (whose fields *become* directives) with behavior hooks —
`run`/`before_execute`/`after_execute` wrap execution, `render_context` contributes Jinja
globals, `after_render` transforms SQL, and decision hooks like `resolve_engine` /
`resolve_sink` / `should_rerun` let directives own their behavior. Lower `priority` = outer.

One demonstrator ships in-tree and needs zero core edits: `emit_sql` (`plugins/emit_sql.py`),
a `run` wrapper that writes every rendered cell to `render_dir` before it executes.

Third-party plugins load via `--plugins mod.name` / `--plugins path/to/file.py`, or a
`quicksqlrc.py` sitting next to the notebook (auto-loaded, by the CLI, `watch` and the
TUI). They load into that notebook's own registries: another notebook in the same TUI
never sees them, and an edited `quicksqlrc.py` reloads on the next recompile, no restart.
The language server doesn't load them yet. `quicksql explain` prints the resulting chain
and which plugins participate in each hook:

```console
$ uv run quicksql explain
run chain:      emit_sql(-100) → extensions(0) → [execute on engine, land via sink]
render_context: sources(0) → vars(0) → refs(0) → env(0)
after_render:   (none)
resolve_engine: engine(0) → input(0)
resolve_sink:   output(0)
...
cells:
  users              duckdb → parquet  ctx: duckdb::memory:  deps: -
  active_user_events duckdb → parquet  ctx: duckdb::memory:  deps: users, events
```

## The TUI

`quicksql tui` opens a reflect-only terminal UI over the notebook (the file is edited
elsewhere; the TUI shows results). It needs the `tui` extra (`pip install 'quicksql[tui]'`;
a checkout's dev group has it already). Opening it on a missing file brings up a notebook picker —
existing `.qsql` files plus starter templates.

| Key | Action | Key | Action |
|---|---|---|---|
| `j`/`k`, `Enter` | move / dive into a cell or catalog | `F` | frequency table for a column |
| `r` / `R` | run cell / run all | `I` | describe a column |
| `t` | toggle raw ↔ rendered SQL | `S` | schema-browser catalog (resumes where you left it) |
| `a` / `A` | toggle autorun (cell / global) | `V` | open the row set in real VisiData |
| `/` | search | `o` | open another notebook |
| `?` | help | `Ctrl+R` | refetch |
| `1`–`5` | jump to SQL / Data / Config / Log / Catalog | `Esc` | leave a sheet for the cell list without popping (the dive is kept) |
| `Ctrl+H` / `Ctrl+L` | previous / next tab, from any mode | `Ctrl+K` / `Ctrl+J` | up to the cell list (like `Esc`) / down into the sheet (like `Enter` on the cell list) |

`Ctrl+H` needs a terminal with the kitty keyboard protocol (herdr, kitty, WezTerm, foot,
ghostty…); under tmux's defaults it arrives as Backspace and does nothing, while `1`–`5`
work everywhere.

The Data tab shows the selected cell's preview: what its last run cached, at most its
first 100 rows. When the preview holds only those, its subtitle ends `first 100 rows
only`, and so do the subtitles of sheets sorted, filtered or counted from it. A rerun of
the cell refreshes the preview; sheets derived from it stay as they were until you pop
back. Leaving the Data or Catalog tab keeps its dive for when you come back (on the Data
tab, while the same cell is selected). One run goes at a time: `R`, `r` or a save's rerun
during a run stops the run under way after its current cell, and the new one starts once
it has.

### What reruns, and what's stale

Every cell has a **fingerprint**: a hash of its rendered SQL, engine and context, the
directives that affect its result, and the fingerprints of the upstream outputs it read.
`watch` and the TUI rerun a cell when its fingerprint differs from the one it last ran with
in that session, plus its downstream. So editing a cell's `output` or `vars` reruns it,
while toggling `autorun` or `tags` reruns nothing. An `autorun: false` cell never reruns on
its own, and its downstream keeps reading its old output until it does. Each save ends with
one closing line: the rerun's counts, `nothing to rerun`, or the compile error.

Each run records, in a per-notebook file under `~/.quicksql/state/`, every cell's last
status and the fingerprint its landed output was built from; nothing is written next to the
notebook. Set `QSQL_HOME` to move `~/.quicksql` (state files and user templates) elsewhere.
The file is keyed on the notebook's path, so a moved or renamed notebook starts over as
`new`.
`quicksql status [--json]` reads it: a cell is `ok`, `failed` or `skipped` from its last
run, `stale` when its landed output was built from something else than the cell would be
built from now (it says why: its SQL or config changed, or an upstream landed a new output),
or `new` if it never ran. Stale is relative to the notebook: source files and remote tables
aren't fingerprinted. The state file never skips a run: startup still runs every autorun
cell.

### Working alongside an agent: `quicksql live`

While the TUI has a notebook open, it serves a live session on a Unix socket (yours alone:
0600, under `~/.quicksql/run/`). A coding agent in another pane edits the same file and
talks to your TUI through `quicksql live`: it sees what you see, and drives it the way your
keys do. Everything it does shows in the Log tab as `agent: …`, and the title shows
`· agent` while one is connected.

```
quicksql status base.qsql --json        # "live": true, queued/running cells, controls
quicksql live arm base.qsql             # p: saves rerun what changed
# ... the agent edits a cell and saves ...
quicksql live wait base.qsql --json     # the outcome of exactly that save
quicksql head orders base.qsql --json   # read an output through your session
quicksql live query "SELECT status, count(*) FROM orders GROUP BY 1" base.qsql
quicksql live events base.qsql --follow # NDJSON: compiles, cells, runs, control changes
```

- `live wait` returns once the TUI has compiled the text as it was saved and rerun what
  changed: `ran` (per-cell results; a failure carries the engine's message, the failing
  statement and its SQL, and the cell's lines), `compile_failed`, `paused` or
  `nothing_to_rerun`. If you saved over it first, it returns yours, marked `superseded`.
- Control: `live arm` / `disarm` (`p`), `live autorun on|off -s CELL` / `--all` (`a` /
  `A`), `live run -s CELL` / `--all` (`r` / `R`; `--wait --json` prints `run --json`'s
  document), `live cancel`, `live focus CELL --tab data`, `live notify "…"`. Commands set a
  state, never toggle, so they can't race your keys. A run while one is under way is
  refused unless `--cancel`, so an agent never cuts yours short. Session autorun only
  switches cells off; the file switches them on.
- Reads go through your session, so they work for every sink, including the DuckDB files
  the TUI holds (DuckDB locks them against other processes). `head` falls back to a fresh
  connection when no TUI is open; `live query` takes one read-only SELECT with each landed
  cell as a view.
- One runner per notebook: `quicksql run` and `watch` are refused while a TUI has it open.

### An agent skill: `quicksql skill`

quicksql ships an [agent skill](src/quicksql/skill/SKILL.md) (the
[Agent Skills](https://agentskills.io) format) that teaches an agent notebooks: the cell
format, the offline loop, and this live loop. It's a folder: a lean `SKILL.md`, references
the agent reads when it needs them (notebook format, directives, engines and sinks, the
live session, JSON shapes) and example notebooks that run. `quicksql skill` prints
`SKILL.md`, `quicksql skill --path` says where the folder is, and `quicksql skill --install`
copies it to `.agents/skills/quicksql/` in the current project (`--dir ~` for every
project), the folder Codex, Gemini CLI and opencode read. Claude Code reads only
`.claude/skills/`: link the folder there. Rerun `--install` after upgrading quicksql: it
updates the files you haven't edited and refuses to overwrite ones you have (`--force`
replaces them). The skill always matches the CLI.

## Editor integration

A language server ships behind the `lsp` extra: `quicksql lsp` (stdio) gives per-cell,
engine-aware **linting** and **column completion** by reusing the compiler for diagnostics
and the catalog for schema introspection.

- **Diagnostics:** unknown `ref()`/`depends_on`, cross-context refs that must run on DuckDB,
  bad directive YAML, cycles, and per-dialect SQL syntax errors.
- **Completion:** columns of the tables/`ref()`/`source()` in scope (alias-scoped, and
  narrowed to the cursor's statement in a multi-statement cell), `ref('…')` cell names,
  dialect-aware keywords and functions; BigQuery cells complete dataset/table relation
  paths against a live connection.
- **Go-to-definition** on `ref('cell')` and **document symbols** for the cell outline.

Clients live in [`editors/`](editors/README.md): a VS Code extension (TextMate grammar +
LSP client) and a self-contained Neovim setup (`editors/nvim/qsql.lua`).

## Optional extras

```console
$ uv sync --extra bigquery      # BigQuery engine (google-cloud-bigquery + pyarrow)
$ uv sync --extra postgres      # Postgres engine (psycopg)
$ uv sync --extra tui           # the TUI (textual); the dev group has it already
$ uv sync --extra visidata      # the TUI's V deep-dive into real VisiData
$ uv sync --extra lsp           # the language server (pygls + sqlglot)
```

## Demos

[`demo/`](demo/README.md) holds self-contained demos with their own seed data. The
zero-setup one is [`demo/sources/`](demo/sources/README.md) — a bookshop's analytics built
entirely from local files (`source()` over CSV, NDJSON, pipe-delimited text, and Parquet),
no backend to stand up:

```console
$ uv run quicksql run demo/sources/sources.qsql
```

For the full tour, [`demo/kitchen-sink/`](demo/kitchen-sink/README.md) brings docker-compose
backends and spans all three remote engines:

- **Postgres** holds a synthetic claims warehouse (including a 250-column flattened 837
  extract to stress the TUI catalog).
- The **BigQuery emulator** holds drug-compendia reference data.
- **DuckDB** joins them cross-engine, lands one cell in a DuckDB warehouse instead of
  parquet, and reads it back for a downstream per-member-per-month calc.

```console
$ docker compose -f demo/kitchen-sink/docker-compose.yml up -d --wait
$ uv run --extra postgres --extra bigquery quicksql tui demo/kitchen-sink/kitchen-sink.qsql
```

The stack runs on offset host ports (Postgres `5433`, emulator `9051`) so it coexists with
the repo-root pytest stack.

## Development

Built test-first (red-green-refactor) with pytest; commits follow Conventional Commits.
Tests mirror `src/` layout under `tests/`.

```console
$ uv run pytest                                   # network/bigquery/postgres/terminal tests deselected by default
$ docker compose up -d --wait                     # local Postgres + bigquery-emulator (pytest stack)
$ uv run --extra postgres pytest -m postgres      # postgres integration tests
$ uv run --extra bigquery pytest -m bigquery      # bigquery integration tests
$ uv run --extra postgres --extra bigquery pytest -m "not network"   # everything but the network-gated tests
```

An explicit `-m` overrides the default deselection in `pyproject.toml`. The backend-gated
tests **skip** (never fail) when their backend is down, so the plain `uv run pytest` is safe
without Docker.

### Architecture map (`src/quicksql/`)

| Module | Role |
|---|---|
| `parser.py` | text → raw blocks (directive YAML + SQL body + body hash) |
| `registry.py` | plugin/executor/sink/reader registries + decorators |
| `config.py` | compose plugin Configs into models; scope checks; layer merging; engine/sink resolution |
| `render.py` | Jinja on SQL bodies; `ref`/`source`/`var`/`env`; records edges + extensions |
| `graph.py` | topo sort, cycle detection, downstream sets |
| `state.py` | fingerprints, the per-notebook state file under `~/.quicksql/state/`, `status` |
| `compiler.py` | parse → config → render → graph ⇒ a `Project` |
| `runner.py` | topo run; the cell protocol (`execute_cell`) over each context's engine session; the plugin run-chain; the run session |
| `conduit.py` | the DuckDB conduit: its target, the duckdb engine's session on it, and reading through it (extensions, previews) |
| `catalog.py` | the TUI's schema catalog: each context's tree from its executor, `@catalog` targets, landed outputs |
| `executors/`, `sinks/`, `sources.py` | strategy leaves for the engines (each opens an `EngineSession` per context and declares its dialect and catalog), sinks, and file readers |
| `plugins/` | `base.py` (Plugin, qfield), `builtin.py` (the directive vocabulary), demonstrators |
| `cli.py`, `watcher.py`, `scaffold.py` | CLI, watch loop, notebook templates |
| `tui/` (`app.py`, `runs.py`, `panes.py`, `catalog_view.py`, `previews.py`, `screens.py`, `agent.py`), `sheet.py` | the TUI: layout and keys, runs, sheet surfaces, the catalog browser, the live host's driver; `sheet.py` holds the pure sheet operations |
| `live/` (`host.py`, `server.py`, `client.py`, `protocol.py`) | the live session an agent reaches over the TUI's socket: the controls, run mirror and saves' outcomes; the server on the TUI's loop; the CLI's client |
| `lsp/` | diagnostics/completions over sqlglot; a pygls stdio server |
