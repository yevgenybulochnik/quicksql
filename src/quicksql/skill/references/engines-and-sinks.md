# Engines, sinks and sources

A cell is **input → engine → output**: an engine runs its SQL, a sink lands the result,
and a downstream cell's `ref()` reads it back.

## Engines

The engine is `engine:`, or inferred from `input:`'s one key. Default: `duckdb`.

| Engine | `input` | Notes |
|---|---|---|
| `duckdb` | `{duckdb: path.db}` (header only; in memory without one) | The conduit: every duckdb cell runs on one connection, the only engine that can read other engines' outputs, `source()` files and `extensions`. |
| `sqlite` | `{sqlite: path.db}` | Cells on the same file share a connection. |
| `postgres` | `{postgres: {dsn: $PG_DSN}}` or `{postgres: $PG_DSN}` | Needs the `postgres` extra. `$VARS` in the DSN expand. Cells with the same DSN share a session; a dropped connection reopens on the next run. |
| `bigquery` | `{bigquery: {project: p, endpoint: http://...}}` or `{bigquery: p}` | Needs the `bigquery` extra; `endpoint` targets an emulator. Same-project cells share a session; an expired one reopens and the cell retries once. |

`quicksql directives NOTEBOOK --json` lists the engines a notebook can name, its plugins'
included, with each one's SQL dialect.

### Which cells share a session

Cells partition into **contexts** by engine and target (the DuckDB conduit, one SQLite
file, one Postgres DSN, one BigQuery project):

- A `ref()` **within** a context renders to a bare temp-table name in that engine's own
  SQL: the upstream ran right there, so the downstream reads it without a round trip.
- A `ref()` **across** contexts renders to the upstream's sink read-back, and the
  downstream must run on duckdb: every other engine's result is extracted and landed
  through DuckDB, which reads them all.

So a SQLite cell can `ref()` another cell on the same SQLite file, but to join it with a
Postgres cell, write the join in a duckdb cell. See
[../examples/cross-engine.qsql](../examples/cross-engine.qsql).

## Sinks

Where a cell's result lands: `output:` (`type` picks the sink).

| Sink | `output` | Read back by `ref()` as |
|---|---|---|
| `parquet` | `{type: parquet, dir: data/}` (the default) | `read_parquet('data/<cell>.parquet')` |
| `duckdb` | `{type: duckdb, path: warehouse.db, table: name, schema: main, mode: replace}` | the table, with the file ATTACHed |
| `postgres` | `{type: postgres, dsn: $PG_DSN, table: schema.name, mode: replace}` | the table, through DuckDB's postgres extension |
| `none` | `{type: none}` | nothing: the cell is effect-only and can't be `ref()`'d |

- `table` defaults to the cell's name; `schema` comes from `output.schema`, else the
  `schema:` directive, else `main` (DuckDB) or `public` (Postgres).
- `mode: append` (duckdb, postgres) inserts each run's rows instead of replacing the table.
- `output` merges deep: set the sink once in the header and override one key per cell.
- While a TUI has the notebook open, it holds the DuckDB files it attached: read them with
  `quicksql head` and `quicksql live query`, not by opening them.

## Sources

Files a cell reads, through `source()`; the cell runs on duckdb.

```sql
-- @sources: { orders: seeds/orders.csv, events: { path: logs/*.json, type: json } }
-- @cell daily
SELECT * FROM {{ source('orders') }} JOIN {{ source('events') }} USING (id);
```

- Readers: csv (`read_csv_auto`), parquet (`read_parquet`), json (`read_json_auto`), excel
  (`read_xlsx`, loads the `excel` extension). Picked by extension, or by `type:`.
- Paths are relative to the notebook's directory. Other keys in an entry, and keyword
  arguments to `source()`, pass to the reader: `source('orders', delim=';')`.
- `source('seeds/x.csv')` with a path works without a `sources:` entry.
