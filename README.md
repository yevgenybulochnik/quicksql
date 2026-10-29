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
$ uv run quicksql list         # cells in topo order: engine -> sink, autorun, deps
$ uv run quicksql show report  # rendered SQL for one cell
$ uv run quicksql compile      # rendered SQL for every cell
$ uv run quicksql explain      # the effective plugin chain + per-cell engine/sink/context
$ uv run quicksql watch        # re-run changed cells + their dependents on every save
$ uv run quicksql tui          # interactive VisiData-style terminal UI
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

Notes: on Postgres/SQLite the statements share the cell's (autocommit) session, so a failure
partway leaves earlier statements' effects in place. On DuckDB, `CREATE TEMP TABLE` in a setup
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
where it may appear (global header, per-cell, or both).

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

## Engines, sinks, and sources

**Engines** (where a cell executes):

| Engine | Notes |
|---|---|
| `duckdb` | Default. The cross-context conduit; the only engine that can serve cross-context `ref()`/`source()`/`@extensions`. |
| `sqlite` | `input: { sqlite: path.db }`. Same-file cells share one connection so in-engine `ref()` works. |
| `postgres` | `input: { postgres: { dsn: "$PG_DSN" } }`. Needs the `postgres` extra; same-DSN cells share a session. |
| `bigquery` | `input: { bigquery: { project: ..., endpoint: ... } }`. Needs the `bigquery` extra; `endpoint` targets an emulator. |

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

`@plugin` / `@executor` / `@source_reader` / `@sink` decorators register into the runtime.
A `Plugin` pairs a pydantic `Config` (whose fields *become* directives) with behavior hooks —
`run`/`before_execute`/`after_execute` wrap execution, `render_context` contributes Jinja
globals, `after_render` transforms SQL, and decision hooks like `resolve_engine` /
`resolve_sink` / `should_rerun` let directives own their behavior. Lower `priority` = outer.

One demonstrator ships in-tree and needs zero core edits: `emit_sql` (`plugins/emit_sql.py`),
a `run` wrapper that writes every rendered cell to `render_dir` before it executes.

Third-party plugins load via `--plugins mod.name` / `--plugins path/to/file.py`, or a
`quicksqlrc.py` sitting next to the notebook (auto-loaded). `quicksql explain` prints the
resulting chain and which plugins participate in each hook:

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
elsewhere; the TUI shows results). Opening it on a missing file brings up a notebook picker —
existing `.qsql` files plus starter templates.

| Key | Action | Key | Action |
|---|---|---|---|
| `j`/`k`, `Enter` | move / dive into a cell or catalog | `F` | frequency table for a column |
| `r` / `R` | run cell / run all | `I` | describe a column |
| `t` | toggle raw ↔ rendered SQL | `S` | schema-browser catalog |
| `a` / `A` | toggle autorun (cell / global) | `V` | open the row set in real VisiData |
| `/` | search | `o` | open another notebook |
| `?` | help | `Ctrl+R` | refetch |

`watch` and the TUI hash **SQL bodies** to decide what changed, so config-only edits don't
trigger reruns (they're logged as `config changed (no rerun)`).

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
| `compiler.py` | parse → config → render → graph ⇒ a `Project` |
| `runner.py` | topo run; per-cell executor + sink; the plugin run-chain; the shared conduit session |
| `executors/`, `sinks/`, `sources.py` | strategy leaves for the engines, sinks, and file readers |
| `plugins/` | `base.py` (Plugin, qfield), `builtin.py` (the directive vocabulary), demonstrators |
| `cli.py`, `watcher.py`, `tui.py`, `sheet.py`, `scaffold.py` | CLI, watch loop, TUI |
| `lsp/` | diagnostics/completions over sqlglot; a pygls stdio server |
