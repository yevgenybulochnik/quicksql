# JSON output

Every `--json` command prints exactly one JSON document on stdout, and nothing else there
(warnings go to stderr). It's indented on a terminal and one line in a pipe. `live events`
prints one document per line. Values are JSON-safe: dates and times as ISO text, decimals
as exact text, NaN as `"nan"`.

## A cell's result

`quicksql run --json`'s `cells`, `quicksql live wait`'s `results`, `live run --wait`'s
`cells` and `cell_finished` events all carry this:

```json
{"cell": "orders_clean", "status": "failed", "rows": null, "target": null,
 "elapsed": 0.0219, "error": "Binder Error: Referenced column \"amount\" not found ...",
 "statement": 2, "traceback": "Traceback (most recent call last): ...",
 "preview": null,
 "line": 41, "line_end": 58, "statement_sql": "SELECT amount * 100 FROM staging"}
```

- `"status"`: `ok`, `failed` or `skipped` (an upstream failed; `"error"` names it).
- `"rows"`, `"target"`: what landed and where (a path, or `db::schema.table`).
- `"elapsed"`: seconds.
- `"error"`: the engine's own message, with its context lines. `"traceback"`: the full
  Python trace, rarely needed.
- `"statement"`: which statement failed, from 1 (`null` when the failure was outside the
  statements, such as connecting).
- `"preview"`: `{"columns": [...], "rows": [[...]]}`, only with `run --json --rows N` (at
  most 100 rows).
- Through a live session only: `"line"` and `"line_end"` (the cell in the file) and
  `"statement_sql"` (the failing statement, rendered).

## `quicksql run --json`

```json
{"file": "base.qsql", "status": "failed",
 "summary": {"ok": 1, "failed": 1, "skipped": 1}, "elapsed": 0.0536,
 "errors": [], "cells": [ ...results... ]}
```

`"status"`: `ok`, `failed`, `compile_error` (then `"cells"` is empty and `"errors"` lists
`{"cell", "line", "message"}`), `error` (something stopped the run before any cell, such as
a plugin that won't import), or `refused` (a TUI has the notebook open). `quicksql live run
--wait --json` prints the same document, plus `"run_id"`, with `"status"` also `replaced`
or `dropped` when another run took its place.

## `quicksql status --json`

Offline:

```json
{"file": "base.qsql", "live": false, "errors": [],
 "cells": [{"cell": "orders", "status": "stale", "reason": "its SQL or config changed",
            "fingerprint": "9f2c...", "landed": "41aa..."}]}
```

Live (a TUI has it open):

```json
{"file": "base.qsql", "live": true, "errors": [], "hash": "9f2c...",
 "controls": {"armed": true, "autorun_global": true, "autorun_off": ["slow_cell"]},
 "run": {"run_id": 7, "state": "running", "hash": "9f2c...", "cells": ["a", "b"],
         "current": "b", "done": ["a"]},
 "cells": [{"cell": "b", "status": "running", "reason": "", "line": 12, "line_end": 20,
            "engine": "duckdb", "sink": "parquet", "context": "duckdb::memory:",
            "depends_on": ["a"],
            "autorun": {"effective": true, "file": true, "session": true, "global": true},
            "rows": null, "elapsed": null, "target": null,
            "error": null, "statement": null, "statement_sql": null}]}
```

A cell's `"status"`: `queued`, `running`, `ok`, `failed`, `skipped`, `stale` or `new`.
`"run"` is `null` when nothing runs. `"errors"` holds the last save's compile errors.

## `quicksql live wait --json`

```json
{"hash": "9f2c...", "state": "ran", "superseded": false, "seq": 118,
 "changed": ["orders_clean", "daily"], "errors": [],
 "summary": {"ok": 1, "failed": 1, "skipped": 0}, "results": [ ...results... ],
 "controls": {"armed": true, "autorun_global": true, "autorun_off": []}}
```

`"state"`: `ran`, `compile_failed`, `paused` or `nothing_to_rerun` (see
[live.md](live.md)).

## Reads

`quicksql head CELL --json`:
`{"cell": "orders", "columns": ["id", "total"], "rows": [[1, "9.50"]], "source": "live"}`
(`"source"` is `offline` without a TUI).

`quicksql live query SQL --json`: `{"columns": [...], "rows": [[...]]}`.

`quicksql live catalog NOTEBOOK [PATH...] --json`, one page of one level:

```json
{"path": ["bigquery:proj", "sales"], "title": "bigquery:proj/sales", "leaf": false,
 "columns": ["table", "type"], "rows": [["orders", "TABLE"], ["orders_2024", "VIEW"]],
 "names": ["orders", "orders_2024"], "total": 412, "offset": 0, "next_offset": 200,
 "cached": true, "age": 7200, "stale": true}
```

- `names` holds each row's next path segment; it's `null` at a leaf.
- A leaf's columns are `column`, `field_path`, `type`, `mode` and `description`.
- `next_offset` is `null` on the last page.
- `cached` says the level came from the cache (the TUI's, or the disk an earlier session
  left) rather than a load of its own.
- `age` is how many seconds ago the level was loaded from its engine. `stale` means a
  BigQuery or Postgres level is past revalidation (15 minutes): it's refreshing behind
  this answer, and a `catalog_invalidated` with reason `refreshed` follows if it changed.
- `total` counts the rows after `--match` and `--top-level`.

## Events

`quicksql live events` prints `{"seq": 57, "kind": "controls_changed", ...}` per line; the
kinds and their fields are in [live.md](live.md).

## Errors

A `live` command that fails prints `{"error": {"code": "busy", "message": "..."}}` and exits
with the code's exit status:

| `code` | Exit |
|---|---|
| `no_session`, `timeout`, `protocol`, `events_lost` | 4 |
| `busy`, `refused` | 5 |
| `unknown_cell`, `unknown_path`, `no_project` | 3 |
| `invalid`, `unknown_method`, `unsupported` | 2 |
| `not_readable`, `query_failed`, `load_failed`, `internal` | 1 |

## `quicksql directives NOTEBOOK --json`

```json
{"file": "base.qsql",
 "directives": [{"key": "autorun", "scope": "both", "type": "boolean", "default": true,
                 "merge": "override", "reruns": false, "plugin": "autorun",
                 "source": "builtin", "description": "..."}],
 "engines": [{"name": "duckdb", "dialect": "duckdb", "input_shorthand": "path",
              "source": "builtin", "description": "..."}],
 "sinks": [{"name": "parquet", "lands_output": true, "source": "builtin",
            "description": "..."}]}
```

`"source"` is `builtin`, or the file that defines the plugin (such as `quicksqlrc.py`).
