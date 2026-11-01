---
name: quicksql
description: Work on quicksql notebooks (.qsql / .qsql.sql files of SQL cells split by `-- @cell name`) — write and fix cells, run them, read their outputs, and work alongside a person who has `quicksql tui` open on the notebook (watch its runs, wait for a save's outcome, read data through it, arm reruns, switch autorun). Use whenever a task touches a .qsql file or mentions quicksql.
---

# quicksql notebooks

A notebook is one file of valid SQL split into named cells. Each cell is
**input → engine → output**: it runs on an engine (DuckDB by default) and lands its result
through a sink (default `data/<cell>.parquet`). Cells form a DAG through `{{ ref('cell') }}`.

## The file

```sql
-- global header: directives here apply to every cell
-- @vars: { min_total: 10 }

-- @cell orders
SELECT range AS id, range % 3 AS store, (range * 7) % 100 AS total FROM range(1000);

-- @cell by_store
-- @output: { type: duckdb, path: warehouse.db }
SELECT store, sum(total) AS total
FROM {{ ref('orders') }}
WHERE total >= {{ var('min_total') }}
GROUP BY 1;
```

- `-- @cell <name>` starts a cell. Everything before the first one is the global header.
- Directives are `@`-comments: line form `-- @key: value`, block form `/*@ ... */` for
  multi-line YAML. Values are literal YAML; only the SQL body is Jinja.
- Jinja globals: `ref('cell')` (a dependency edge, rendered as however that cell is read
  back), `source('name')`, `var('key')`, `env('VAR')`; `{% if %}` works.
- Directives: `engine` (duckdb, sqlite, postgres, bigquery), `input` (connection; the engine
  is inferred from its one key), `output` (sink: `{type: parquet}` default, `{type: duckdb,
  path: …}`, `{type: postgres, dsn: …, table: …}`, `{type: none}` for effect-only),
  `sources`, `extensions`, `vars`, `depends_on`, `schema`, `autorun` (default true),
  `tags`, `catalog`.
- A cell may hold several `;`-separated statements: earlier ones run for effect, the
  **last** one lands. A failure names its statement (`statement 2 of 3: …`).

Rules that bite:
- A cell that `ref()`s a cell on another engine or connection, uses `source()`, or loads
  `extensions` must run on duckdb (a compile error says so).
- An effect-only cell (`output: {type: none}`) lands nothing and can't be `ref()`'d; order
  after it with `depends_on`.
- `skipped` means an upstream failed in the same run: fix the failed one, not the skipped.
- `autorun: false` cells never rerun on a save; run them explicitly.
- Every duckdb cell runs on the header's DuckDB database; a cell naming another
  `input.duckdb` is a compile error.

## First: is a TUI open on the notebook?

```
quicksql status NOTEBOOK --json
```

`"live": true` means the person has `quicksql tui` open on it. Then **work through the
TUI** (next section): `quicksql run` and `quicksql watch` are refused (exit 5) while it's
open, and the DuckDB files it holds are locked to every other process — read them with
`quicksql head` and `quicksql live query`, never by opening them yourself.

`"live": false`: work offline.

```
quicksql list NOTEBOOK                     # cells, engines, sinks, dependencies
quicksql show CELL NOTEBOOK                # one cell's rendered SQL
quicksql run NOTEBOOK -s CELL --json       # run CELL and its upstreams; --rows N adds output rows
quicksql head CELL NOTEBOOK --json         # a cell's landed output, without running
quicksql status NOTEBOOK --json            # ok / failed / skipped / stale / new per cell
```

## Alongside the TUI

The person edits and watches in the TUI; you edit the same file and drive the TUI through
`quicksql live`. Everything you do shows in its log as `agent: …`.

The loop:

1. **Arm reruns** once: `quicksql live arm NOTEBOOK`. A TUI opens *paused* — saves only
   recompile until it's armed (the person can also press `p`). Ask before arming if they
   may not want reruns.
2. **Edit** the cell in the file and save it.
3. **Wait** for that save's outcome: `quicksql live wait NOTEBOOK --json`. It returns once
   the TUI has compiled *exactly the text you saved* and finished rerunning what changed:
   - `"state": "ran"`: `results` per cell; a failed one has `error` (the engine's
     message), `statement` and `statement_sql` (which statement failed), and `line` /
     `line_end` (the cell's place in the file). Exit 1 if a cell failed.
   - `"compile_failed"`: `errors`, each with `cell`, `line`, `message`. Exit 3.
   - `"paused"`: saves don't rerun yet; `changed` lists what would. Arm, or `live run`.
   - `"nothing_to_rerun"`: the save changed no cell that autoruns.
   - `"superseded": true`: the person saved over your text first; this outcome is theirs.
4. **Read** the result: `quicksql head CELL NOTEBOOK --json` (first rows), or
   `quicksql live query "SELECT … FROM cell …" NOTEBOOK --json` — each landed cell is a
   view named after it; one read-only SELECT (also `FROM x`, `SUMMARIZE x`, `DESCRIBE x`).
5. Fix and repeat from 2.

More of the TUI:

```
quicksql live run NOTEBOOK -s CELL --wait --json   # run as `r` does; same JSON as run --json
quicksql live run NOTEBOOK --all --wait --json     # run all as `R` does (also arms)
quicksql live cancel NOTEBOOK                      # stop the run under way after its cell
quicksql live disarm NOTEBOOK                      # pause reruns again
quicksql live autorun off NOTEBOOK -s CELL         # this session only, like `a`
quicksql live autorun on NOTEBOOK --all            # like `A`
quicksql live events NOTEBOOK --follow             # NDJSON: every compile, cell, run, control change
quicksql live events NOTEBOOK --since SEQ          # what happened after event SEQ, then exit
quicksql live focus CELL NOTEBOOK --tab data       # show the person a cell (sql/data/config/log/catalog)
quicksql live notify "message" NOTEBOOK            # a toast in their TUI
```

- `live run` is refused (exit 5, `busy`) while a run is under way, so you never cut the
  person's run short. Wait for it (`live wait`), or pass `--cancel` if you mean to.
- Session autorun only switches cells **off**. `live autorun on` for a cell whose file
  says `autorun: false` is refused: to switch it on for good, edit the file.
- A lasting change is a file edit. The session's switches end when the TUI quits.
- `live events --follow` suits a background watcher (Claude Code's Monitor tool): each
  line is one event with a `seq`; kinds are `compiled`, `compile_failed`, `run_started`,
  `cell_started`, `cell_step`, `cell_finished`, `run_finished`, `run_ended`, `settled`,
  `controls_changed` (`by`: `keys` or `agent`), `notebook_closed`.
- Point the person at your work: `live focus` the cell, then `live notify` what changed.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | ok |
| 1 | a cell failed, a query failed, or an output can't be read |
| 2 | usage error |
| 3 | the notebook didn't compile, or an unknown cell |
| 4 | no TUI has the notebook open (or it timed out, or speaks another protocol: restart it) |
| 5 | refused: a run is under way, the file says otherwise, your run was replaced, or `run` while a TUI is open |

With `--json`, stdout holds exactly one JSON document (errors too: `{"error": {"code",
"message"}}`); `live events` prints one per line.
