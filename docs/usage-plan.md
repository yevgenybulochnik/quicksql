# Plan: usage tracking — the source columns notebooks actually read

**Status:** proposed (2026-11-09), revised twice the same day after review (see the last
two sections); nothing built. Needs no core-refactor phase; source readers gain one class
attribute (step 1) and executors one method (step 5).

## Context

Goal: a ranked, curated list of the source tables people query and the columns they read
from them, across every notebook and session, that an agent reads before writing SQL.

**Only source columns count.** A column counts when a statement reads it straight from a
table in an engine (or a file). A computed column (`amount * qty AS revenue`), a CTE's or
subquery's output, a `ref()`'d cell's output and a table the notebook itself creates never
count as columns of their own: a calculation's source columns count where the calculation
reads them, and a column passed through unchanged (by name or by `*`) counts where it's
finally read (decision 9).

What exists:
- `RenderedCell.sql`: the cell's SQL as it ran (Jinja rendered, `after_render` applied).
  `context_refs` / `external_refs` name the cells it reads through `ref()`, which render
  to the cell's bare name (same context) or to the producer sink's `ref_expr`
  (`config.make_sink(config, root, registries)`): `read_parquet('…')`, an attached
  `alias."schema"."table"`.
- `source()` renders to its reader's `expr` — `read_csv_auto`, `read_parquet`,
  `read_json_auto`, `read_xlsx` — with the path resolved against the notebook.
- `executors.base.statement_spans`: top-level statement offsets from the tokenizer.
- `catalog._tables_read` finds the tables a cell reads in FROM/JOIN over the LSP's Jinja
  mask (`lsp/mask.py`), for warming. The mask exists because the LSP has no render for
  half-typed code; a run has one, so the recorder doesn't use the mask, and both stay put.
- `Plugin.after_run(project, results)`: one call per run from `run_project`, so `run`,
  `watch`, the TUI and `live run` all reach it; failures are contained (`runner._notify`).
- `$QSQL_HOME` with the autouse `qsql_home` fixture; catalog leaves carry
  `(column, field_path, type, mode, description)` rows.

What's missing: column-level attribution, and any record that outlives a run.

### Probed (2026-11-07 – 09, sqlglot 30.12.0)

`qualify(tree, validate_qualify_columns=False)` then `traverse_scope` attributes every
column to its scope's source; keeping only columns whose source is an `exp.Table` naming
a table gives the direct source reads:

| SQL | Recorded | Not recorded |
|---|---|---|
| `select o.amount * o.qty as revenue, upper(c.name) … join customers c on c.id = o.customer_id` | `orders.amount`, `orders.qty`, `customers.name`, `customers.id` (join), `orders.customer_id` (join) | `revenue`, `customer` (output names only) |
| `with lines as (select order_id, amount * qty as revenue from order_lines) select order_id, sum(revenue) … group by order_id` | `order_lines.order_id`, `.amount`, `.qty` | `lines.revenue`, `lines.order_id` |
| `with adj as (select amount * 1.1 as amount from orders) select amount from adj` | `orders.amount`, once | the outer `amount` (same name, derived) |
| `select amount * qty as revenue from orders where revenue > 100` (DuckDB) | `orders.amount`, `orders.qty`, in select **and** where | `orders.revenue` |
| `select amount * qty as revenue, revenue * 0.1 as tax from orders` (DuckDB) | `orders.amount`, `orders.qty` | `orders.revenue` |
| `select id, amount from orders join payments using (order_id)` | `orders.order_id`, `payments.order_id` (join) | `id`, `amount`: ambiguous without a schema |

`qualify` expands an alias reference (WHERE, GROUP BY, a DuckDB lateral alias) into its
expression **in every dialect, Postgres included**. In Postgres WHERE can't see a select
alias, so a cell that ran there read a real column of that name, and sqlglot credits the
alias's inputs instead (rare: an alias shadowing a real column). A HAVING or ORDER BY
alias reference yields no column: no false column, but its inputs get no
`having`/`order` role. Unqualified columns with two or more tables in scope, and `USING`
joins, resolve only with a schema.

Where a bare `qualify` + `traverse_scope` falls short, and what the design does about it:

| SQL | Bare result | The design |
|---|---|---|
| `with base as (select * from orders where status = 'x') select amount, qty from base` | `orders.status` only | pass-through through `*` (decision 9) |
| a `select *` staging cell, then `select member_id … from {{ ref('stage') }}` | the table, no columns | pass-through across `ref()` (decision 9) |
| `with lines as (…) select order_id … group by order_id` (row 2 above) | the outer `group` role lost | pass-through role credit |
| `update orders set amount = 0 where status = 'void'`; `UPDATE … FROM`, `DELETE`, `MERGE` | no scope at all, and no error | rewritten to the SELECT they read |
| `select a from read_csv('x.csv')`, `generate_series(1, 10)` | an `exp.Table` whose `this` is a function, name `''` | reader functions are files; others no source |
| `from {{ env('S') }}.orders` over the mask | table `q3qqqqqqqqq.orders`; the placeholder's index shifts with edits | scan the rendered SQL (decision 10) |
| `… where exists (select 1 from payments p where p.oid = o.id)` | `o.id` has no source in its own scope | resolved against the enclosing scopes |
| `select amount from {{ ref('x') }} r join orders o on …` | `amount` unresolved: the ref or `orders` | the ref ruled in or out by its producer's outputs |
| a window's `ORDER BY` / `PARTITION BY` | read as `order` / `select` | role `window` |
| BigQuery `FROM t \|> WHERE … \|> SELECT …` | parses (as CTEs) and attributes | scanned like any query |

Further probes (second review):

| SQL | Bare result | The design |
|---|---|---|
| `create table t as select …`, `insert into t select …`, `copy (select …) to …` | scopes and columns as for the SELECT | scanned as is, no rewrite |
| `select a from 'data/x.parquet'` (a DuckDB replacement scan) | a table named `data/x.parquet` | a file (decision 5) |
| `select a from read_parquet(['a.parquet', 'b.parquet'])` | a function table, name `''` | each literal is a file |
| `select address.city from customers c join orders o on …` | column `city` on a source `address` that doesn't exist | the struct root `address`, unresolved |
| `… where exists (select 1 from payments where oid = id)` | `id` credited to `payments`, though it may be the outer table's | unresolved: the inner relation, then the enclosing ones |
| `select * exclude (x) from orders`, `* replace (… as amount)` | a star | `x` and `amount` don't pass through |
| `call foo(1)`, `vacuum t`, BigQuery `if x then select …` / `end if` | `exp.Command`, with a warning on stderr | `unsupported`, the logger silenced |
| a statement that fails to parse | a `ParseError` whose message quotes the statement, literals included | only a kind and a statement index are logged |

`sqlglot.parse` at the default error level fails a whole body on one bad statement;
`ErrorLevel.IGNORE` (what `_tables_read` uses) returns a mangled tree (`SELECT FROM
WHERE garbage`). So the scanner splits first and parses each statement strictly.

Context keys aren't global: `duckdb:<path as the notebook wrote it>`, `sqlite:<path>`
and `duckdb::memory:` are relative to the notebook, so two notebooks in different
directories share a key for different databases, and every in-memory notebook shares
one. Nothing sets the process's cwd or DuckDB's `file_search_path`, so a hand-written
relative `read_csv('x.csv')` reads against the cwd, not the notebook (`source()` renders
an absolute path).

Cost (dev box): 0.5–1.2 ms median per real cell (3 ms max), 3–9 ms per demo notebook
(kitchen-sink, sources, stress), 22 ms for a 4 KB cell of 8 chained CTEs with 3 joins
each. `sqlglot.optimizer` adds ~4 ms once (sqlglot itself is already loaded by
`split_statements`); appending 300 NDJSON rows takes < 1 ms. `after_run` is synchronous,
before `_save_state` and `run_finished`, so its time delays the TUI's finish and
`live wait` by that much.

Two traps on the run path: `catalog_store` imports polars at module level (~100 ms, and
`test_demos` runs the core path with polars blocked), and `Executor.catalog_table` can
load remotely (a round trip per table). So the run path never touches the catalog.

Layering: an extension (`plugins.*`) may import neither `lsp` nor `catalog`; the scanner
lives outside both and needs neither.

## Decisions (defaults, to confirm)

1. **Off by default, opt in by environment.** Nothing is scanned or written unless
   `QSQL_USAGE=1` is set in the process that runs the notebook (`run`, `watch`, the
   TUI); unset, empty or any other value is off (not `QUICKSQL_*`: `overrides.py` reads
   those as config). With it on, `-- @usage: false` in the header opts a notebook out,
   in a cell that cell: it logs no line, and a column another cell reads through it
   drops. The directive can only turn recording off, never on: a notebook can't opt its
   reader in. The log never leaves `$QSQL_HOME/usage/`. `quicksql usage` reads whatever
   was logged, whether or not the variable is set.
2. **A use is a distinct (notebook, local day)** of a table, a column or a join. Reruns,
   watch saves, two processes logging the same cell, and several cells of one notebook
   reading the same column (a column passed through three staging cells is logged by
   each) count once. What a cell read on a day is the union of its lines for that day:
   an edit logs a new line, and going back to an earlier version logs none.
   Score = Σ 0.5^(age in days / 30).
3. **Only `ok` cells record.** A failed cell may name columns that don't exist.
4. **`select *` counts the table, not its columns**, unless a later scope (a CTE, a
   subquery, a downstream cell) reads named columns through it: those count (9).
   Otherwise every column of a wide table looks popular, and a `select *` staging cell
   would hide every column read downstream of it.
5. **Files are source tables**, kind `file`, on the conduit's engine only:
   - a call to one of the notebook's reader functions (`source()` renders one; a
     hand-written `read_csv('…')` is one too) whose first argument is a path literal or
     a list of them (each a file);
   - a replacement scan: a one-part table name that is a quoted path ending in a
     reader's extension (`from 'data/x.parquet'`).

   A file is keyed by its path alone, with no context: absolute as `source()` rendered
   it, a relative one resolved against the process's cwd (what DuckDB reads), a glob as
   written, a URL's userinfo and query stripped. Any other table function
   (`generate_series`, `postgres_scan`, …) is no source, and its arguments are never
   logged (one can carry a DSN).
6. **The log keeps the notebook's resolved path** (local, for the report's "used in"),
   never SQL text, literals, or a raw context identity (one can carry a DSN: hash it, as
   `catalog_store` does). A scan error is a statement index and a kind, never sqlglot's
   message, which quotes the statement.
7. **Retention 365 days.** Once per process (per usage directory) the recorder deletes
   older day files and compacts finished days into one file each.
8. **Schemas, canonical names and types come in at report time only.**
9. **Pass-through.** A column that a CTE, a subquery or a `ref()`'d cell passes through
   unchanged is a read of its source column, with the reading scope's role. Unchanged
   means a bare column, renamed or not (`amount as amt`: reading `amt` reads `amount`),
   or a `*` over one relation, less what it leaves out (`* exclude (x)` /
   `* except (x)` doesn't pass `x`; a `* replace (… as amount)` column is computed). A
   `*` over several relations leaves it unresolved, with those relations as candidates.
   Never through a calculation, a table the notebook creates, or a cell with
   `@usage: false`.
10. **Scan what ran.** The recorder scans `RenderedCell.sql`, not `sql_raw` over the
    mask: `var()`, `env()`, `{% if %}` and loops are as they ran, and the `ref()`
    relations are known exactly (`context_refs`, `external_refs`).
11. **A report is for a notebook.** `quicksql usage NOTEBOOK` compiles it (as `run`
    would, `--set` included) and shows what every logged notebook read on *its*
    contexts, enriched from their catalogs. Only notebooks named on the command line
    compile, so no other directory's `quicksqlrc.py` runs. Without one, the report lists
    every context by hash, unenriched.
12. **A context is identified globally.** The log hashes
    `Executor.context_identity(config, root)`, not `context_key`: the key by default
    (Postgres's redacted DSN, BigQuery's project), the resolved database path for DuckDB
    and SQLite, and None for an in-memory database, which the recorder replaces with the
    notebook's path (nothing outside that notebook can read it).

## Design

### Modules

| Module | Layer | What |
|---|---|---|
| `sqlscan.py` | neutral (sqlglot, `executors.base`) | `scan`: one cell's rendered SQL, statement by statement |
| `usage_log.py` | neutral (stdlib) | the on-disk log: append, read, today's keys, prune, compact |
| `plugins/usage.py` | extension | `UsagePlugin`: the `usage` directive; `after_run` scans, resolves lineage across the notebook, records; in `BUILTIN_MODULES` |
| `sources.py` | extension | `SourceReader.functions`: the DuckDB table functions that read its format |
| `executors/` | core contract, extensions | `Executor.context_identity(config, root)`: the key by default; DuckDB and SQLite resolve their path |
| `usage.py` | front-end | the report: aggregate, score, a notebook's contexts, enrich from the catalog, curate, render |
| `cli.py` | front-end | `quicksql usage` |

Import-linter: `sqlscan` and `usage_log` join the core contract's sources (they import
only stdlib, sqlglot and core modules: `executors.base`, `state` for `$QSQL_HOME`), so
neither can grow an extension or
front-end import; `plugins.usage` joins the extensions contract's sources and the core
contract's forbidden list (as `plugins.builtin` does); `usage` joins both forbidden lists.
`plugins/usage.py` imports `sqlscan` (and with it `sqlglot.optimizer`) inside
`after_run`, since every compile and the LSP load the builtins.

### The scanner

```python
@dataclass(frozen=True)
class Rel:
    kind: Literal["table", "file", "ref"]
    parts: tuple[str, ...]          # table: qualifiers first, as qualify normalized them;
                                    # file: (path as written,); ref: (cell,)

@dataclass(frozen=True)
class ColumnUse:
    rel: Rel
    column: str
    roles: frozenset[str]           # select|where|join|group|having|order|window|from

@dataclass(frozen=True)
class Unresolved:
    column: str
    roles: frozenset[str]
    candidates: tuple[Rel | None, ...]  # innermost scope first; None: a CTE or subquery
                                        # whose columns aren't known

@dataclass(frozen=True)
class Scan:
    tables: tuple[tuple[Rel, bool], ...]  # each relation read; whether a `*` over it reaches a result
    columns: tuple[ColumnUse, ...]        # ref columns included; the recorder resolves them
    joins: tuple[tuple[tuple[Rel, str], tuple[Rel, str]], ...]
    unresolved: tuple[Unresolved, ...]
    outputs: tuple[tuple[str, tuple[Rel, str] | None], ...]
                                          # the terminal statement's named columns: each one's
                                          # pass-through origin, None when computed
    star: tuple[tuple[Rel, frozenset[str]], ...]
                                          # relations a terminal `*` passes through, each
                                          # with the columns it leaves out or replaces
    created: frozenset[tuple[str, ...]]   # what the cell CREATEs
    errors: tuple[tuple[int, str], ...]   # (statement index, kind): "parse", "qualify",
                                          # "unsupported:<Kind>"; (0, "too large")

def scan(sql: str, dialect: str | None, refs: Mapping[str, str],
         file_functions: frozenset[str], file_suffixes: frozenset[str]) -> Scan: ...
```

`refs` maps the text each `ref()` rendered to onto the cell it names. `file_functions`
and `file_suffixes` are empty for a cell that isn't on the conduit's engine. The scanner
reads no file system and no cwd: a file's path stays as written, for the recorder to
resolve.

- **Statements.** Split with `executors.base.statement_spans(sql, dialect or "")` (the
  module function: the BigQuery executor's `split_statements` never splits), then parse
  each in the executor's dialect at the default error level, with the `sqlglot` logger
  silenced for the scan (a `Command` fallback warns on stderr). A statement that fails
  to parse or qualify adds an error (its index and kind, never the message) and nothing
  else; the others still scan.
- **Relations.** An `exp.Table` whose `this` is an identifier is a table, unless it
  matches a `refs` entry (by normalized SQL), is a CTE, is in `created`, or is a
  replacement scan (one quoted part ending in one of `file_suffixes`: a `file`). A table
  function is a ref when it matches `refs` (a parquet sink's `read_parquet`), a `file`
  per literal when its name is in `file_functions` and its first argument is a string
  literal or a list of them, and no source otherwise.
- **Columns.** A column counts on its scope's source. When its qualifier names no source
  there, it counts on the enclosing scope's source that the qualifier names (a
  correlated subquery). When it names none anywhere, the qualifier is a struct root
  (`address.city` reads `address`), placed like an unqualified column. An unqualified
  column in a subquery that can be correlated (in WHERE, a select list, a lateral) is
  unresolved unless the subquery is uncorrelated by construction (no enclosing
  relation): its candidates are the inner relation, then the enclosing ones.
- **DML.** UPDATE, DELETE and MERGE have no scope, so each is rewritten to the SELECT it
  reads and that is scanned: `UPDATE t SET a = e FROM s WHERE c` → `SELECT e FROM t, s
  WHERE c`; `DELETE FROM t USING s WHERE c` → `SELECT 1 FROM t, s WHERE c`; a MERGE's
  `ON` becomes the join and the values it assigns or inserts become the select list.
  Assigned columns are writes, not reads. CREATE … AS, INSERT … SELECT and
  COPY (SELECT …) scan as they are. A statement that reads no relation (CREATE without
  AS, DROP, INSERT … VALUES, SET, ATTACH, LOAD, …) records nothing; any other kind with
  no scope (a `Command`, a DuckDB `PIVOT` statement, a BigQuery script fragment) records
  `unsupported:<Kind>`.
- **Roles** come from the nearest clause ancestor. A `Window`'s partition and order are
  `window`; a column read inside FROM (an UNNEST or table-function argument) is `from`.
- **Joins**: equality predicates in `ON`, `USING` columns, and top-level `WHERE`
  conjuncts that equate columns of two different relations in one scope (a comma join).
  Each side is a `(Rel, column)`, directly or through a pass-through; ref sides resolve
  in the recorder. An unqualified read of a `USING` column adds its role to both sides
  (both are known to have it); a `NATURAL` join's columns stay unresolved.
- **Pass-through** (decision 9) inside the cell: a column read from a CTE or subquery
  scope follows that scope's select of the same output name when it is a bare column
  (aliased or not). It follows the scope's `*` when that `*` covers one relation and
  doesn't exclude or replace the column; over several relations it goes to
  `unresolved`. On reaching a relation, it adds the reading role to that relation's
  `ColumnUse`, creating the `ColumnUse` when the column came through a `*`.
- **Unresolved**: an unqualified column (or struct root) with several relations in
  scope that qualify can't place, or one in a correlated subquery. Its candidates are
  those relations (a ref as `Rel("ref", (cell,))`), or None for a CTE or subquery whose
  columns aren't known.
- Bodies over 100 KB scan to `errors=((0, "too large"),)`.

### The log

`$QSQL_HOME/usage/` (0700). A process appends to `YYYY-MM-DD.<pid>.ndjson` (0600), so a
TUI and a CLI run never share a file and nothing needs a lock; a run's lines go in one
`os.write` on an `O_APPEND` fd (repeated on a short write). Compaction folds each day
older than yesterday into `YYYY-MM-DD.ndjson`: it lists the day's files, opens every one
of them (the compacted file included) before reading any, drops duplicate keys, writes a
temp file and renames it, then unlinks the pid files. If a listed file is gone by the
time it's opened, another process is compacting that day, and this one leaves the day
alone: otherwise it could rename a result missing that file's lines over the other's.
With that rule two processes compacting at once lose nothing, and a past day costs one
file however many processes logged in it. One line per logged cell:

```json
{"v": 1, "at": "2026-11-09T14:03:12+00:00", "day": "2026-11-09",
 "notebook": "/home/me/claims/claims.qsql", "cell": "paid_by_month",
 "key": "9f2c41d07a6e1b3c", "engine": "postgres", "context": "4be0a7c9d1f2",
 "tables": [{"engine": "postgres", "context": "4be0a7c9d1f2", "kind": "table",
             "parts": ["public", "claims"], "star": false},
            {"engine": "postgres", "context": "4be0a7c9d1f2", "kind": "table",
             "parts": ["public", "members"], "star": false}],
 "columns": [{"table": 0, "column": "paid_amount", "roles": ["select"]},
             {"table": 0, "column": "service_date", "roles": ["group", "where"]},
             {"table": 0, "column": "member_id", "roles": ["join"]},
             {"table": 1, "column": "id", "roles": ["join"]}],
 "joins": [[[0, "member_id"], [1, "id"]]],
 "unresolved": [], "errors": []}
```

Columns, joins and unresolved candidates (an index, or null for an unknown derived
relation) point into the line's `tables`. A table carries its own engine and context: a
column a cell reads through `ref()` resolves to its producer's table, on the producer's
context. A file has neither (`{"kind": "file", "parts": ["/data/x.parquet"]}`): its path
identifies it. `key` hashes the resolved content (tables, columns, joins, unresolved,
errors); `context` is `sha256(context identity)[:12]` (decision 12). An error is
`[statement index, kind]`. The reader skips a torn or unknown-version line.

### The recorder (`UsagePlugin`)

- `Config`: `usage: bool = qfield(True, affects_results=False, description=…)`, scope both.
- `after_run(project, results)`: nothing unless `QSQL_USAGE=1` (read on each call, so
  no import of `sqlscan` or `sqlglot.optimizer`, and no usage directory, when it's off),
  and nothing when `project.path` is None. Otherwise:
  1. **Scan every cell** in `project.order`, memoized by dialect + SQL + refs. `refs` comes
     from `context_refs` (the bare name) and `external_refs` (the producer's
     `make_sink(…).ref_expr(name)`); `file_functions` and `file_suffixes` come from the
     notebook's readers, for cells on `conduit.ENGINE` only. A relative file path
     resolves against the process's cwd.
  2. **Created, notebook-wide**: every cell's `created`, plus the relation each cell's
     sink lands (its `ref_expr`: a table matched on its trailing `(schema, table)`, a file
     on its path). Reads of them drop: an effect-only cell's `CREATE TABLE` is derived for
     its readers, and so is a sink table a sibling reads by name.
  3. **Lineage, in topo order.** A ref `ColumnUse` resolves through its producer's
     `outputs` and `star`:
     - a pass-through to a source column becomes a `ColumnUse` on it, with the reader's
       roles and the producer's (engine, context);
     - a computed column drops;
     - a ref origin recurses (already resolved, since the order is topological);
     - a `*` over several relations becomes unresolved;
     - a column the producer's `*` excludes or replaces drops;
     - anything through a producer with `usage` off drops.

     A ref candidate in `unresolved` whose producer has no `*` either resolves the column
     (its outputs have it; the run was ok, so no other candidate does) or is ruled out.
     A single table candidate left after that resolves the column. Neither rule applies
     to a column of a correlated subquery (an inner and an outer relation may both have
     it), which stays unresolved for the report. Ref join sides resolve the same way.
  4. **Log** each `ok` cell with `usage` on as one line, unless its (notebook, cell, day,
     `key`) is already logged. On the first write in a process, the logged set is seeded
     from today's files, so a new `quicksql run` of unchanged cells adds nothing. `key`
     covers the resolved content, so an upstream edit that changes what a downstream
     cell reads logs that cell anew. Each table's context is its cell's executor's
     `context_identity`, hashed (decision 12).
- A log that can't be written (`OSError`) is reported once per process, not on every
  run.
- The scan memo, the logged set and the once-per-process work are module-level, bounded
  (~4096) and keyed by the usage directory: registries, and with them plugin instances,
  rebuild when an rc file changes, and each test has its own `$QSQL_HOME`.
- On its first write per usage directory in a process, it prunes past retention and
  compacts finished days.
- No `catalog`, `catalog_store` or polars import (`test_demos` guards it).

### The report: `quicksql usage`

```
quicksql usage [NOTEBOOK ...] [--set k.v=x ...] [--plugins MOD ...] [--days 90]
               [--top 20] [--columns 15] [--engine E] [--half-life 30] [--load]
               [--json | --markdown]
```

- Reads the day files in `--days`. What a (notebook, cell) read on a day is the union of
  its lines for that day; a table, column or join is then used once per (notebook, day)
  (decision 2), so "uses" are notebook-days.
- **With NOTEBOOKs**, each compiles as `run` would (its `quicksqlrc.py`, `--plugins`,
  `--set`, `QUICKSQL_*`); a compile error exits 3. The report keeps the tables on their
  contexts (the hashes of their cells' context identities), showing what *every* logged
  notebook read there, and names each context by the named notebook's cells on it. It
  keeps the files a named notebook read or that sit under its directory. Logged
  notebooks that weren't named are only read, never compiled.
- **Without**, every context is listed, named by its hash, unenriched.
- Groups by table (engine, context, kind, parts; a file by its path). Per table: score,
  uses, the notebooks it's used in (most uses first; the first three, then "+N"),
  columns ranked by score with their roles, its common joins, and its `select *` count.
- Enrichment (with NOTEBOOKs): the named notebook's executor and config for a context
  give the leaf `catalog_table(config, root, parts)`; building it loads nothing.
  - **Merging needs no load.** Two spellings whose leaves share a `cache_key` (`orders`
    and `public.orders`) are one table, on every engine.
  - **Columns need the leaf's frame**, read from the disk store only (a new no-load
    read on `CatalogCache`; a CLI process starts with nothing in memory). Only engines
    with `catalog_persists` (BigQuery, Postgres) are on disk, so DuckDB, SQLite and
    file tables get columns only with `--load`, which loads every leaf that's missing.
    The frame:
    - supplies types and descriptions;
    - resolves an `unresolved` column only when every candidate is a table whose frame
      is at hand: to the first candidate in order (innermost scope first) that has the
      column, when the candidates come from nested scopes; to the only one that has it
      otherwise (a null or ref candidate leaves it unresolved);
    - flags recorded columns that no longer exist.

  Without enrichment, rows key on qualify's normalized names.
- Curation, `$QSQL_HOME/usage/curation.yaml`: `pin` and `hide` (globs on
  `engine:table.column`) and `notes` (shown next to the column in every format).
- `--markdown` is the agent's format:

```markdown
## postgres · public.claims — context of paid_by_month, members · 9 notebooks, score 41.2
Used in: claims/claims.qsql (31), claims/denials.qsql (12), +7
| column | type | uses | read as | note |
|---|---|---|---|---|
| member_id | bigint | 31 | select, join, group | |
| paid_amount | numeric | 24 | select | net of adjustments; use for $ totals |
| service_date | date | 19 | where, group | |
Joins: claims.member_id = members.id (12) · claims.provider_id = providers.id (7)
```

The report reads only files and compiles only the notebooks it's given, so it works
while a TUI hosts the notebook (unlike `run`).

### The agent surface

`SKILL.md` gains a short "before writing SQL against a warehouse" section: run
`quicksql usage --markdown <the notebook>`, and prefer pinned, noted and high-score
columns and the joins it lists; an empty report means recording is off or new, and the
agent carries on without it (it doesn't set the variable itself). `references/usage.md`
covers turning it on (`QSQL_USAGE=1`), the output, the score, the notebook argument and
the curation file.

## Steps (each one focused change, test-first: a `test(...)` red commit, then the green)

**Phase 1: the scanner, nothing recorded**
1. `feat(sources)`: `SourceReader.functions`. Each builtin reader names the DuckDB table
   functions that read its format (`read_csv`/`read_csv_auto`, `read_parquet`/
   `parquet_scan`, `read_json`/`read_json_auto`/`read_ndjson`/`read_ndjson_auto`,
   `read_xlsx`); a test checks that each reader's `expr` calls one of its own. A reader
   without the attribute (a third party's) names none.
2. `sqlscan` statements and relations:
   - raise the floor to `sqlglot>=30.12`, the version the probes ran on;
   - split, then parse strictly (one unreadable statement leaves the others scanned);
   - an error is an index and a kind: a statement with a string literal that fails to
     parse leaves the literal out of the `Scan`;
   - nothing on stderr for a `Command` fallback (`call`, `vacuum`, a BigQuery `if`);
   - tables;
   - files: reader functions (a literal, a list of literals), a replacement scan, a
     URL's userinfo and query stripped, a non-reader table function no source, none of
     it off the conduit's engine;
   - refs: a bare cell name, a parquet sink's `read_parquet`, an attached sink table;
   - CTEs and `created` (a multi-statement cell's temp table);
   - the DML rewrites, CREATE … AS / INSERT … SELECT / COPY as they are, and
     `unsupported`;
   - the size cap.
3. Column attribution: every row of the three probe tables, in DuckDB, Postgres and
   BigQuery (the alias expansion pinned per dialect); a BigQuery `` `proj.ds.tbl` ``, a
   Postgres `schema.table`, pipe syntax, a correlated subquery (qualified: the enclosing
   source; unqualified: unresolved, inner first), a struct root with one relation in
   scope and with two.
4. Roles (`window`, `from`), joins (ON, USING and an unqualified read of its column, a
   comma join, NATURAL), in-cell pass-through (bare, renamed, `*`, `* exclude`,
   `* replace`), unresolved candidates, `select *`, and a terminal statement's `outputs`
   and `star`.

**Phase 2: recording**
5. `feat(executor)`: `Executor.context_identity(config, root)`. The context key by
   default; DuckDB and SQLite return the resolved database path, None in memory. Two
   notebooks in different directories with the same relative path differ; two that
   reach one file agree.
6. `usage_log`: append, read, today's keys, prune, compact (a second compactor that
   finds a listed file gone leaves the day alone; two run concurrently lose no line);
   0700/0600; a file per pid per day; a torn last line skipped on read.
7. Lineage, a pure function over a project's scans in `plugins/usage.py`:
   - a ref column through a pass-through;
   - a `select *` staging cell;
   - a chain of refs;
   - a computed column stopping the trace;
   - a `*` over a join becoming unresolved;
   - a ref candidate ruled in, and one ruled out;
   - a correlated subquery's column left unresolved;
   - a renamed column, and one the producer's `*` excludes;
   - a producer with `usage` off stopping the trace;
   - a resolved column carrying its producer's context;
   - CREATEs and sink-landed relations as `created`.
8. `UsagePlugin` (each test sets `QSQL_USAGE=1` unless it says otherwise):
   - with the variable unset, empty or `0`, a run creates no usage directory and
     imports no `sqlscan`;
   - a run of `ok`, failed and skipped cells logs only the `ok` ones;
   - `@usage: false` (header, cell) logs nothing; `@usage: true` with the variable
     unset logs nothing;
   - a relative hand-written file path is logged against the cwd, run from another
     directory;
   - two notebooks' in-memory contexts hash apart;
   - an unwritable usage directory is reported once, over two runs;
   - an unchanged rerun adds no line, in the same process or a new one;
   - an edited cell adds one line;
   - an edited upstream that changes a downstream cell's reads adds one line for that
     cell;
   - a raising recorder leaves the run ok, with the failure in its log.

   Register it in `BUILTIN_MODULES`, update the import-linter contracts, and regenerate
   `references/directives.md`.
9. `test_demos`: the polars-blocked subprocess `run` with `QSQL_USAGE=1` writes a usage
   file, and nothing new on stderr; the existing run, without it, writes none.

**Phase 3: the report**
10. Aggregation: a cell's day as the union of its lines (A, then B, then A again);
    distinct (notebook, day) uses, so a column three cells of one notebook read counts
    once; decay, ranking, roles, joins, the `select *` rule, "used in", the `--days` /
    `--engine` filters.
11. `quicksql usage` without a NOTEBOOK: text, `--json`, `--markdown`; contexts by hash;
    an empty log says so and names `QSQL_USAGE=1` (exit 0); bad flags exit 2.

**Phase 4: notebooks, enrichment and curation**
12. NOTEBOOK arguments, compiled with `--set` / `--plugins`:
    - the context filter, and the files kept;
    - contexts named by the notebook's cells;
    - a compile error exits 3;
    - a logged notebook nobody named never compiles (its rc would raise; the report
      doesn't).
13. `CatalogCache`: a read that never loads (memory, then the disk store).
14. Enrichment:
    - `orders`/`public.orders` merged by their leaf's `cache_key`, with nothing loaded;
    - types and descriptions;
    - `unresolved` resolved only when every candidate is known (nested scopes: the
      innermost that has it);
    - vanished columns flagged;
    - a DuckDB table: no columns without `--load`, columns with it.
15. `curation.yaml`: pin, hide, notes.

**Phase 5: docs**
16. `SKILL.md` section and `references/usage.md`; `test_skill` green.
17. `CLAUDE.md`: the concepts line and the architecture map.

## Out of scope

- `live query` / `head` reads: they read landed outputs, which are derived.
- Lineage through a calculation, or through a table a notebook creates (a temp table, a
  sink table read by name): pass-through only, within a notebook.
- Across notebooks: a table one notebook builds and another reads is a source to the
  reader.
- Nested fields: a BigQuery STRUCT field (`o.address.city`) or an UNNEST'd element
  records its top-level column (`address`, `items`).
- BigQuery script blocks: the splitter cuts `IF … THEN … END IF` and `BEGIN … END` at
  their semicolons, and the fragments record `unsupported`, so reads inside them are
  lost.
- Folding BigQuery date-sharded tables (`events_20261109`) into one: each is its own
  table, and a wildcard (`events_*`) another.
- Enriching a logged notebook nobody named: its `quicksqlrc.py` would run.
- Ranking LSP completions by usage (a follow-up that reads the same aggregation).
- Sharing usage between people or machines: the log is per user.
- Engine-side logs (BigQuery `INFORMATION_SCHEMA.JOBS`, `pg_stat_statements`): tables
  only, no columns, and blind to which notebook asked.

## Risks

- **sqlglot gaps by dialect** (pipe operators sqlglot doesn't know, engine-only
  functions): an unreadable statement records an error and nothing, and a statement kind
  with no scope records `unsupported`. Usage undercounts, visibly, but never invents a
  column. Alias-reference expansion runs in every dialect: in Postgres, a WHERE alias
  reference that ran was really a column of that name, and it's credited to the alias's
  inputs instead (rare: an alias shadowing a column); step 3 pins it. Attribution shifts
  between sqlglot versions: the floor is the tested version, the lockfile pins what
  the suite runs, and the probe tables are tests, so a newer sqlglot that shifts one
  fails a test rather than the log. An installed wheel resolving a newer sqlglot is
  unguarded.
- **Attribution without a schema**: only single-source scopes resolve at run time, and
  ref candidates by their producer's outputs; the rest wait in `unresolved`. A candidate
  the report can't check keeps a column unresolved. Unqualified columns in correlated
  subqueries always wait, and on DuckDB, SQLite and files they resolve only with
  `--load`.
- **Name identity**: `orders`, `public.orders` and `"Orders"` stay apart until
  enrichment merges them by leaf. Postgres's `catalog_table` takes an unqualified name
  as `public`'s, whatever the `search_path`, so a notebook that sets one merges
  `orders` into the wrong schema. Two readings of the same Postgres table also stay
  apart: through a DuckDB ATTACH (`pg.claims.eligibility`, engine duckdb) and from a
  Postgres cell (`claims.eligibility`). A notebook that moved or was deleted can't
  enrich.
- **Context identity**: a context hash is of the run's context identity, overrides
  included. A named notebook matches what ran with other `--set`s only when `usage`
  gets them too. Two spellings of one database stay apart (a URI and a keyword DSN, two
  users, a symlinked path), and a moved database file starts over.
- **Run latency**: synchronous in `after_run`. The memo and the 100 KB cap bound it, and
  lineage is one pass over cached scans. Every `quicksql run` is a new process, so each
  pays the seed from today's files and the prune/compact check.
- **Disk growth**: bounded by distinct resolved reads per day, since the logged set is
  seeded from today's files and repeated runs add nothing; one file per day after
  compaction; the 365-day prune caps it.
- **Privacy**: table names, column names, file paths (URL userinfo and query stripped)
  and notebook paths sit in `$QSQL_HOME` (0700/0600). No SQL text, literals, DSNs,
  table-function arguments or parser messages are logged.

## Verification

- `uv run pytest` and `uv run lint-imports` (the suite under
  `systemd-run --user --scope -p MemoryMax=3G`).
- Latency: time `UsagePlugin.after_run` in process on the sources and kitchen-sink
  projects: under 15 ms cold (the optimizer import, the seed from a day of 50 pid files
  and the prune/compact check included), under 2 ms for an unchanged rerun. (Comparing
  five CLI runs each way is noisier than the effect.)
- Off by default: run the sources demo with `QSQL_USAGE` unset; `$QSQL_HOME/usage/`
  doesn't exist afterwards.
- End to end on the compose stack, with `QSQL_USAGE=1` exported: run
  `demo/kitchen-sink/kitchen-sink.qsql`, then
  `quicksql usage --markdown demo/kitchen-sink/kitchen-sink.qsql`. It should list the
  `claims.*` and `compendia.*` columns under their two contexts, credit
  `class_spend`'s reads through `ref()` (`pharmacy_claims.ndc` and
  `therapeutic_classes.class_name`, among others) to those tables, and show none of the
  notebook's computed aliases or `class_spend`'s sink table. Running it again adds no
  log lines. In the TUI (`/verify`): arm, save unchanged ten times → no new log lines;
  edit a cell → one; `quicksql usage` while the TUI is open.

## Changes from the first draft (review, 2026-11-09)

- **Scan the rendered SQL** (decision 10), not `sql_raw` over the mask. Jinja in a
  qualifier recorded placeholder tables whose names shifted with edits, and `{% if %}`
  kept both branches. `lsp/mask.py` no longer moves and `catalog._tables_read` no
  longer lifts (old steps 1–2).
- **Pass-through through `*` and across `ref()`** (decisions 4, 9): a `select *` CTE or
  staging cell recorded the table and none of the columns read through it. Lineage
  across `ref()` moved from out of scope into the recorder (pass-through only); a
  resolved column carries its producer's context.
- **Unresolved columns resolve only when every candidate is known.** Before, a ref
  candidate the catalog can't check let "exactly one candidate has it" credit the wrong
  table.
- **DML statements**: UPDATE, DELETE and MERGE yield no scope and no error. They're now
  rewritten to the SELECT they read; other scope-less kinds record `unsupported`.
- **Table functions**: `read_csv(…)`, `generate_series(…)` read as `exp.Table`. Reader
  functions are now files (`SourceReader.functions`); others are no source, and their
  arguments aren't logged.
- **Disk growth**: the per-process logged set made every CLI run relog every cell into
  a new pid file. It's now seeded from today's files, and finished days compact.
- **The report is for a notebook** (decision 11). It filters to the named notebook's
  contexts (narrowing to a notebook only showed what that notebook already reads) and
  names those contexts by their cells. Only named notebooks compile, so no other
  directory's rc runs, and `--set` / `--plugins` match the run's overrides.
- Smaller:
  - statements split with `statement_spans`, then parsed strictly;
  - correlated subqueries resolve against enclosing scopes;
  - comma joins count as joins;
  - the `from` role;
  - sink-landed tables join `created`;
  - the memo is keyed by usage directory, for test isolation;
  - `sqlscan` and `usage_log` are guarded by the core contract;
  - lazy imports in the builtin plugin;
  - `sqlglot>=30.12`;
  - in-process latency check;
  - corrected probe notes (pipe syntax parses; alias expansion applies in Postgres
    too);
  - the ATTACH and override identity risks.

## Changes from the second review (2026-11-09)

- **Contexts are identified globally** (decision 12, step 5). `context_key` is relative
  to the notebook for DuckDB and SQLite, and one key for every in-memory notebook, so
  the report merged unrelated databases. The log now hashes
  `Executor.context_identity`; files carry no context and are keyed by path.
- **A use is a (notebook, day)** (decision 2), and a cell's day is the union of its
  lines. Before, several lines for one (notebook, cell, day) had no rule (and last-wins
  breaks on A→B→A, which logs no third line), and pass-through counted one column once
  per cell that read it, so deep pipelines scored higher.
- **Compaction could lose lines**: a second compactor that missed an unlinked pid file
  renamed a short result over the first's. It now leaves a day alone when a listed
  file is gone.
- **Errors are an index and a kind.** sqlglot's `ParseError` message quotes the
  statement, literals included.
- **Files**:
  - a relative hand-written path resolves against the cwd, as DuckDB reads it, not the
    notebook;
  - a replacement scan (`from 'x.parquet'`) and a list of paths are files;
  - reader functions count on the conduit's engine only.
- **Unqualified columns in correlated subqueries are unresolved** (sqlglot credits the
  inner table, which may be wrong), and a qualifier naming no source is a struct root.
- **Pass-through spelled out** (decision 9): a rename passes through; `* exclude` and
  `* replace` columns don't; a `USING` column credits both sides; a producer with
  `@usage: false` stops the trace.
- **Enrichment's reach**: merging spellings uses the leaf's `cache_key` and loads
  nothing; columns come only from the disk store, which holds BigQuery and Postgres, so
  other engines need `--load`.
- **Off by default** (decision 1, at the user's request): recording is opt-in through
  `QSQL_USAGE=1`; `@usage: false` still opts a notebook or cell out, and can't opt in.
- Smaller:
  - the `sqlglot` logger is silenced during a scan (`Command` fallbacks warned on
    stderr);
  - CREATE … AS, INSERT … SELECT and COPY scan without a rewrite;
  - `statement_spans(sql, dialect or "")`;
  - an unwritable log is reported once per process;
  - the latency budget covers the seed and the prune/compact check;
  - BigQuery script blocks and date-sharded tables are out of scope;
  - the `public` assumption, DSN spellings and the unguarded sqlglot upgrade are risks.

## Changes made while building (2026-11-10)

Where a step didn't work as written, the smallest change that keeps its intent:

- **`home.py` holds `$QSQL_HOME`** (step 8). The plan had `usage_log` import `state` for
  `quicksql_home`. The extensions contract is transitive and counts `TYPE_CHECKING`
  imports, so `plugins.usage -> usage_log -> state -> compiler` broke it. `HOME_ENV` and
  `quicksql_home` moved to a new core module, `quicksql.home`, which imports nothing of
  quicksql; `state` still exports `quicksql_home`, so no caller changed.
- **`Unresolved` carries `nested`** (steps 3, 7, 14). The report resolves candidates from
  nested scopes to the innermost that has the column, and flat ones to the only one that
  has it; the plan's dataclass and log line had no way to tell the two apart. Logged as
  `"nested": true|false` on each unresolved entry.
- **Statement indexes count from 1** (step 2), as `RunResult.statement` does; `(0, "too
  large")` is then the one error that names no statement.
- **BigQuery doesn't expand alias references** (step 3). The plan says `qualify` expands a
  WHERE or lateral alias reference "in every dialect"; on 30.12.0 it does in DuckDB and
  Postgres, and in BigQuery it qualifies the reference as a column of the table
  (`orders.revenue`). That is right for BigQuery, where neither clause can see a select
  alias. Both behaviours are pinned.
- **A bare column in a LATERAL subquery** (step 4). `qualify` rewrites `b` in
  `lateral (select b from u where …)` to a field of a column `u` on the enclosing table.
  The scanner reads that back as the column `b`, unresolved (inner relation first), and
  drops any other struct guess `qualify` makes inside a correlated subquery.
- **An UNNEST or other non-query derived relation is a null candidate** (step 4): its
  output columns aren't known (a BigQuery UNNEST of structs exposes their fields), so an
  unqualified column beside one stays unresolved rather than being credited to the table.
- **A CTE or subquery that names its columns rules itself in or out inside the scan**
  (step 4), the rule the plan gives the recorder for `ref()` candidates: `qualify` places
  a column on a derived relation that outputs it, and one that doesn't is no candidate,
  so a single table left over is the read.
- **`LOAD` is a `Command` to sqlglot** (step 2). It is let through by keyword as a
  statement that reads nothing; every other `Command` is `unsupported:Command`.
- **A replacement scan keeps its path as written** (step 2): DuckDB's dialect folds a
  quoted identifier's case, so the scanner notes each table's name before `qualify`.
- **An unwritable log is reported by raising once** (step 8). `after_run` gets no run
  context, so the one channel to the run's log is the contained failure `_notify`
  records; the recorder raises on the first failed write per process and is silent after.
- **`sqlscan.relation` reads a sink's `ref_expr`** (step 8, after timing): the recorder
  first ran a full scan of `SELECT * FROM <ref_expr>` per cell to learn what its sink
  lands. It now parses that without qualifying, and ref keys are remembered per process.
- **The report lists unresolved columns** under each candidate table (step 10): the plan
  showed none, which hid the undercount it promises is visible.
- **`hide` also takes `engine:table`** (step 15), for a whole table; the plan's globs were
  on `engine:table.column` only.

## Verification carried out (2026-11-10)

- `uv run pytest` under `systemd-run --user --scope -p MemoryMax=3G`: 1111 passed, 1
  skipped; `uv run lint-imports`: 2 kept. With the compose stack up,
  `pytest -m "postgres or bigquery"`: 20 passed.
- **Latency, in process, a day of 50 pid files seeded** (this box):
  - unchanged rerun: 0.9–1.1 ms median, under the 2 ms budget;
  - cold: **50–65 ms, over the 15 ms budget**, on both demo notebooks (7 cells each).
    Of it: importing `sqlscan` and the optimizer 8 ms, the seed 4 ms, the prune/compact
    check 0.3 ms, and scanning 40–48 ms on a first pass (16 ms on a second pass in the
    same process: sqlglot's first use of each dialect and its lazy imports are the
    rest). A cell costs 0.8–4.4 ms to scan here, against the 0.5–1.2 ms the plan measured,
    so the budget isn't reachable on this box by trimming the recorder. Every
    `quicksql run` with recording on pays the cold cost once; a TUI or `watch` session
    pays it on its first run only.
- Off by default: the sources demo run with `QSQL_USAGE` unset leaves `$QSQL_HOME` with
  `state/` only.
- End to end on the kitchen-sink stack with `QSQL_USAGE=1`: the run logs 7 lines, a
  second run adds none; `quicksql usage --markdown` lists `claims.*` under the Postgres
  context and `compendia.*` under the BigQuery one, credits `class_spend`'s reads through
  `ref()` (`pharmacy_claims.ndc`, `therapeutic_classes.class_name`, the join
  `pharmacy_claims.ndc = ndc_directory.ndc`), and shows no computed alias and no
  `class_spend` sink table. `--load` adds types and descriptions from both engines.
- TUI (tmux, a DuckDB notebook): armed, one run logs 1 line; ten unchanged saves leave
  1; an edit makes 2; `quicksql usage NOTEBOOK --markdown` answers while the TUI is open.
