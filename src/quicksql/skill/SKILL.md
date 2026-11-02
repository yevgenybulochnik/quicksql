---
name: quicksql
description: Work on quicksql notebooks (.qsql / .qsql.sql files of SQL cells split by `-- @cell name`) — write and fix cells, run them, read their outputs, and work alongside a person who has `quicksql tui` open on the notebook (watch its runs, wait for a save's outcome, read data through it, arm reruns, switch autorun). Use whenever a task touches a .qsql file or mentions quicksql.
---

# quicksql notebooks

A notebook is one file of valid SQL split into named cells. Each cell runs on an engine
(DuckDB by default) and lands its result through a sink (default `data/<cell>.parquet`).
`{{ ref('cell') }}` reads another cell's result, which makes the cells a DAG.

```sql
-- @cell orders
SELECT range AS id, range % 3 AS store FROM range(1000);

-- @cell by_store
SELECT store, count(*) AS n FROM {{ ref('orders') }} GROUP BY 1;
```

Read what the task needs:
- [references/notebook-format.md](references/notebook-format.md): cells, the header,
  directive syntax and how values merge, Jinja (`ref`, `source`, `var`, `env`),
  multi-statement cells, the rules that bite.
- [references/directives.md](references/directives.md): every builtin directive. A notebook
  with plugins (a `quicksqlrc.py` next to it) can write more: `quicksql directives NOTEBOOK`
  lists exactly its own.
- [references/engines-and-sinks.md](references/engines-and-sinks.md): engines and how they
  connect, which cells share a session, sinks, file sources.
- [references/live.md](references/live.md): working alongside the open TUI, in depth.
- [references/json.md](references/json.md): the JSON every `--json` command prints.
- Notebooks that run: [examples/basics.qsql](examples/basics.qsql),
  [examples/cross-engine.qsql](examples/cross-engine.qsql),
  [examples/effect-only.qsql](examples/effect-only.qsql).

They sit next to this file. If it reached you through `quicksql skill`, `quicksql skill
--path` prints where.

## First: is a TUI open on the notebook?

```
quicksql status NOTEBOOK --json
```

`"live": true`: the person has `quicksql tui` open on it, so **work through the TUI**
(next section). `quicksql run` and `quicksql watch` are refused while it's open (exit 5),
and the DuckDB files it holds are locked to other processes: read them with
`quicksql head` or `quicksql live query`, never by opening them yourself.

`"live": false`: work offline.

```
quicksql list NOTEBOOK                  # cells: engine, sink, autorun, dependencies
quicksql show CELL NOTEBOOK             # a cell's rendered SQL (what each ref() became)
quicksql run NOTEBOOK -s CELL --json    # run CELL and its upstreams; --rows N adds rows
quicksql head CELL NOTEBOOK --json      # a landed output's first rows, without running
quicksql status NOTEBOOK --json         # ok / failed / skipped / stale / new per cell
```

## Alongside the TUI

1. **Arm** reruns once: `quicksql live arm NOTEBOOK`. A TUI opens paused, and saves only
   recompile until it's armed. Ask first if the person may not want reruns.
2. **Edit** the cell in the file, and save. Writing against an unfamiliar table? Read its
   exact columns and types first:
   `quicksql live catalog NOTEBOOK CONTEXT SCHEMA TABLE --json` (no path: the contexts).
3. **Wait** for that save: `quicksql live wait NOTEBOOK --json` returns the outcome of
   exactly the text you saved: `ran` (each cell's result; a failure carries `error`,
   `statement_sql` and its `line`/`line_end` in the file), `compile_failed` (exit 3),
   `paused` (arm, or run it) or `nothing_to_rerun`. Exit 1 if a cell failed.
4. **Read** the result: `quicksql head CELL NOTEBOOK --json`, or
   `quicksql live query "SELECT … FROM cell" NOTEBOOK --json` (each landed cell is a view).
5. **Fix** and go back to 2. When it's done, point the person at it:
   `quicksql live focus CELL NOTEBOOK --tab data`, then
   `quicksql live notify "what changed" NOTEBOOK`.

Also: `quicksql live run NOTEBOOK -s CELL --wait --json` runs as `r` does (`--all` as `R`),
`quicksql live events NOTEBOOK --follow` streams every compile, cell and run as NDJSON (for
a background watcher), and the TUI's switches are `live disarm`, `live autorun`,
`live cancel`. Their rules are in [references/live.md](references/live.md).

## Rules that bite

- `skipped` means an upstream failed in the same run: fix the failed cell, not the skipped.
- A cell that `ref()`s a cell on another engine or connection, or uses `source()` or
  `extensions`, must run on duckdb.
- An effect-only cell (`output: {type: none}`) lands nothing and can't be `ref()`'d; order
  a cell after it with `depends_on`.
- An `autorun: false` cell never reruns on a save; run it explicitly.
- A lasting change is a file edit. The TUI session's switches end when it quits.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | ok |
| 1 | a cell failed, a query failed, or an output can't be read |
| 2 | usage error |
| 3 | the notebook didn't compile, or an unknown cell |
| 4 | no TUI has the notebook open (or it timed out, or speaks another protocol: restart it) |
| 5 | refused: a run is under way, the file says otherwise, your run was replaced, or `run` while a TUI is open |

With `--json`, stdout holds exactly one JSON document (errors too:
`{"error": {"code", "message"}}`); `live events` prints one per line.
