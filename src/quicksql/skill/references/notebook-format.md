# The notebook format

A notebook is a `.qsql` (or `.qsql.sql`) file of valid SQL: any SQL tool can open it, and
quicksql splits it into named cells.

## Cells and the header

```sql
-- the header: everything before the first @cell; its directives apply to every cell
-- @vars: { region: eu }

-- @cell users
SELECT * FROM range(10) t(id);

-- @cell eu_users
-- @depends_on: [users]
SELECT * FROM {{ ref('users') }} WHERE '{{ var("region") }}' = 'eu';
```

- `-- @cell <name>` opens a cell, which runs to the next `-- @cell` or the end of the file.
  A name is an identifier (`[A-Za-z_][A-Za-z0-9_]*`), unique in the file: it names the
  cell's output too.
- Text before the first `@cell` is the **header**. Its directives are every cell's
  defaults; a cell overrides them per key (see Merging).
- Plain `--` and `/* */` comments are just SQL and pass through.

## Directives

Directives are `@`-comments holding YAML:

Line form, one directive per line:

```sql
-- @output: { type: duckdb, path: warehouse.db }
```

Consecutive `-- @` lines join into one YAML document:

```sql
-- @sources:
-- @   orders: seeds/orders.csv
```

Block form, for longer YAML:

```sql
/*@
vars:
  since: 2026-01-01
  regions: [eu, us]
*/
```

Nothing else may follow a directive on its line: a trailing `-- comment` is read as part
of the YAML.

- Values are **literal YAML**: Jinja never touches them, and `{{ }}` in a directive is
  text. DSNs expand `$ENV_VARS`.
- A key repeated within one block: the later one wins, and the compiler warns.
- Every directive, with its scope, type and default: [directives.md](directives.md). For a
  notebook with its own plugins, `quicksql directives NOTEBOOK`.

### Merging

A cell's config is its header, then the cell, then run overrides, merged per directive:
- `override` (most): the later value replaces the earlier.
- `deep` (`input`, `output`, `sources`, `vars`, `catalog`): mappings merge key by key, so a
  cell's `-- @output: { table: x }` keeps the header's `type` and `path`.
- `extend` (`depends_on`, `extensions`, `tags`): lists append.

Run overrides beat both: `--set vars.region=us` on any command, or
`QUICKSQL_VARS__REGION=us` in the environment (`__` nests).

## Templates

The SQL body is Jinja, rendered in a sandbox (no Python reachable from a template; a
template error is a compile error, exit 3). The globals:

| Global | Renders to |
|---|---|
| `ref('cell')` | how that cell's output is read here: a bare temp-table name when both cells share an engine session, else its sink's read-back (`read_parquet('data/cell.parquet')`, an attached table). Records the dependency. |
| `source('name')` | a file read: a `sources:` entry by name, or a path (`source('seeds/x.csv')`). Options pass through: `source('x', delim=';')`. Readers: csv, parquet, json, excel (by extension, or `type:`). |
| `var('key')`, `var('key', default)` | a `vars:` value; undefined with no default is a compile error. `vars` is the whole mapping. |
| `env('VAR')`, `env('VAR', default)` | an environment variable (empty when unset). |

`{% if %}`, `{% for %}` and filters work. `quicksql show CELL NOTEBOOK` prints a cell's
rendered SQL; `quicksql compile NOTEBOOK -s CELL` does too, for several.

## Multiple statements

A cell may hold several `;`-separated statements. The earlier ones run for effect; the
**last** one is the result that lands and that `ref()` reads:

```sql
-- @cell enriched
CREATE OR REPLACE TEMP TABLE staging AS SELECT * FROM {{ ref('raw') }} WHERE ok;
UPDATE staging SET amount = amount * 100;
SELECT * FROM staging;
```

- A failure names its statement: `statement 2 of 3: …`, and JSON carries `statement` (and,
  through a live session, `statement_sql`).
- On DuckDB, setup temps live on the shared connection: prefer `CREATE OR REPLACE TEMP
  TABLE`, so cells don't collide.
- Postgres and SQLite autocommit each statement: a failure partway keeps the earlier ones'
  effects.
- BigQuery runs the whole body as one script; a BigQuery cell that another cell on the
  same project `ref()`s must be a single statement.

`-- @output: { type: none }` makes a cell **effect-only**: every statement runs, nothing
lands, and it can't be `ref()`'d. Order a cell after it with `depends_on`.

## Rules that bite

- **Cross-engine work happens on duckdb.** A cell that `ref()`s a cell in another engine or
  connection, calls `source()`, or loads `extensions` must run on duckdb, where every other
  engine's output can be read. The compiler says so.
- **One DuckDB database per notebook.** Every duckdb cell runs on the header's
  `input: { duckdb: path.db }` (in memory without one); a cell naming another is a compile
  error.
- **Cycles** (`ref()` or `depends_on` looping back) are a compile error naming them.
- **`skipped`** means an upstream failed in the same run. Fix the failed cell; the skipped
  ones run once it passes.
- **`autorun: false`** cells never rerun on a save, in `watch` or an armed TUI; run them
  explicitly. Editing `autorun`, `tags`, `catalog` or `render_dir` reruns nothing.
- **Stale** (in `status`) means a cell's output was built from something else than the
  cell would be now: its SQL or result-affecting config changed, or an upstream landed a
  new output. Source files and remote tables aren't tracked: a changed CSV leaves a cell
  `ok`.
