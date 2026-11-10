# Usage: the tables and columns notebooks actually read

`quicksql usage` ranks the source tables notebooks query and the columns they read from
them, from a log of past runs. Read it before writing SQL against a warehouse you don't
know: it says which of a wide table's columns people use, how tables are joined, and
what someone noted about a column.

```
quicksql usage NOTEBOOK --markdown
```

## Recording is opt-in

Nothing is recorded unless `QSQL_USAGE=1` is set in the process that runs notebooks
(`quicksql run`, `watch`, `tui`). That is the person's choice: an empty report means
recording is off or new, so carry on without it. With it on, each run logs what its `ok`
cells read, under `$QSQL_HOME/usage/` (`~/.quicksql/usage/`): table names, column names,
file paths and the notebook's path. Never SQL text, literals or connection strings.
`-- @usage: false` in a notebook's header opts the notebook out, in a cell that cell; it
can't turn recording on. The report reads whatever was logged, whether or not the
variable is set, and works while a TUI has the notebook open.

## What counts

- **Only source columns.** A column counts when a statement reads it from a table in an
  engine or from a file. A computed column, a CTE's output and another cell's output don't
  count themselves: a column passed through unchanged (by name, renamed, through `*`,
  through `ref()`) counts on the table it came from, where it's finally read.
- **`select *` counts the table**, not its columns, unless something downstream reads
  named columns through it.
- **A use is a (notebook, day).** Reruns, and several cells of one notebook reading the
  same column, count once. `score` is the sum over uses of `0.5 ** (age in days / 30)`,
  so recent uses weigh more.
- **Unresolved** columns are ones the SQL alone couldn't place (unqualified, with several
  tables in the query): each is listed under every table that may have it.

## The command

```
quicksql usage [NOTEBOOK ...] [--set k.v=x] [--plugins MOD] [--days 90] [--top 20]
               [--columns 15] [--engine E] [--half-life 30] [--load]
               [--json | --markdown]
```

- **With NOTEBOOKs** the report shows what *every* logged notebook read on those
  notebooks' contexts (their databases), each context named by the notebook's cells on
  it, plus the files they read or that sit under their directories. Each named notebook
  compiles as `run` would, so pass the `--set` and `--plugins` its runs use (a context is
  identified by the connection the run had). No other notebook is compiled.
- **Without one**, every context is listed, named by a hash.
- With NOTEBOOKs, types and descriptions come from the catalog already on disk (BigQuery
  and Postgres, once a TUI or the LSP has browsed them). `--load` reaches the engines for
  the tables that are missing (DuckDB, SQLite, files): it makes round trips.
- `--days`: how far back to read. `--top`: tables shown. `--columns`: columns per table.
  `--engine`: one engine's tables (`file`: the files). `--half-life`: days for a use to
  count half.
- Exit 0 with an empty report, 2 for a bad flag, 3 when a NOTEBOOK doesn't compile, 1 for
  a curation file that doesn't read.

## `--markdown`

```markdown
## postgres · public.claims — context of paid_by_month, members · 9 notebooks, score 41.2
Used in: claims/claims.qsql (31), claims/denials.qsql (12), +7
| column | type | uses | read as | note |
|---|---|---|---|---|
| member_id | bigint | 31 | select, join, group | |
| paid_amount | numeric | 24 | select | pinned; net of adjustments; use for $ totals |
| old_code | | 2 | where | no longer in the table |
Joins: claims.member_id = members.id (12) · claims.provider_id = providers.id (7)
Read with `select *`: 4
Unresolved (this table or another in the query has it): status (3)
```

Prefer pinned and noted columns, then high-score ones, and join the way `Joins` shows.
`read as` lists the clauses that read the column: `select`, `from`, `join`, `where`,
`group`, `having`, `window`, `order`.

## `--json`

```json
{"today": "2026-11-09", "days": 90, "half_life": 30.0, "notebooks": ["/home/me/claims/claims.qsql"],
 "tables": [{"engine": "postgres", "context": "4be0a7c9d1f2",
             "context_label": "context of paid_by_month, members", "kind": "table",
             "name": "public.claims", "parts": ["public", "claims"],
             "score": 41.2, "uses": 43, "stars": 4,
             "notebooks": [{"path": "/home/me/claims/claims.qsql", "uses": 31}],
             "columns": [{"name": "member_id", "score": 30.1, "uses": 31,
                          "roles": ["select", "join", "group"], "type": "bigint",
                          "description": "", "note": "", "pinned": false, "missing": false}],
             "joins": [{"left": {"table": "public.claims", "column": "member_id"},
                        "right": {"table": "public.members", "column": "id"},
                        "uses": 12, "score": 11.4}],
             "unresolved": [{"name": "status", "uses": 3, "roles": ["where"]}]}]}
```

A file has `"kind": "file"`, its path as `"name"`, and null `"engine"` and `"context"`.
`"missing"`: recorded, but the table no longer has the column. An empty report has
`"tables": []` and a `"hint"` saying recording is opt-in.

## Curation

`$QSQL_HOME/usage/curation.yaml` is where a person shapes the ranking; it applies to every
report. Globs match `engine:table.column` (`file:` for a file's path):

```yaml
pin:    ["postgres:public.claims.paid_amount"]     # listed first, marked pinned
hide:   ["postgres:*.ssn", "duckdb:scratch_*"]     # a column, or a whole table
notes:
  "postgres:public.claims.paid_amount": net of adjustments; use for $ totals
```

`pin` and `hide` are lists, `notes` a mapping of glob to text. Suggest an entry to the
person when you learn something worth keeping; it is their file.
