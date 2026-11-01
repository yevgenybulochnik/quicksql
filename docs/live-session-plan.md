# Plan: an agent alongside the open TUI

**Status:** steps 1–8 done (2026-11-01, branch `feat/live-session`); step 9 open.
Builds on `docs/agent-cli-plan.md` and replaces its `status --wait` and `watch --json`
(step 7) and its "no long-running daemon" exclusion. Needs no core-refactor phase; Phase
7's trust applies unchanged. All four decisions below went with their defaults.

As built, where it differs from the plan below:
- The save's hash rides on the project: `Project.source_hash` (`models.source_hash` of the
  text compiled) instead of `bootstrap.compile_file(text=)`. Whatever compiled a project,
  the host reads the hash of exactly that text.
- `Preview.to_dict` and `errors.error_dicts` are public, so the host and the CLI share the
  JSON shapes without importing private names.
- Extra protocol: `run.wait` (what `live run --wait` calls), a `run_ended` event (a run
  replaced before it started never emits the runner's events), and `events.since` without
  a `seq` returns the whole ring.
- `client.events()` subscribes on return, not on the first `next()`: a lazy generator
  missed events that happened in between (found by the `events_lost` test hanging).
- The agent marker is in the title (`▶ live · agent`), next to the armed state, not the
  subtitle, which keeps its one writer.
- A failing `live query` is `query_failed` (exit 1); a replaced or dropped `live run
  --wait` exits 5.
- Offline `head` never ATTACHes (the TUI's `read_landed` rule), so with no TUI open it
  reads Parquet outputs; database outputs read through a live session.
- The skill ships in the package: `quicksql skill` prints it and `quicksql skill
  --install` writes `.claude/skills/quicksql/SKILL.md` (as `herdr --skill` does); a test
  keeps it naming every `quicksql live` command and runs its example notebook.
- Verified end to end in tmux: a duckdb-sink output the TUI holds locked reads through
  `head` while an offline open fails on the lock; a broken cell's `live wait` names its
  lines, statement and DuckDB's message; the fix's wait comes back ok.

## Context

The intended setup is one person and one coding agent on one notebook: you have
`quicksql tui` open, and the agent edits the `.qsql` file in another pane. The agent
should see which cells are running, know when they finish, read what they produced, fix
the SQL of the ones that fail, and control the TUI the way your keys do (arm watch reruns,
switch autorun per cell, run, cancel).

None of that can reach the agent today:
- The TUI's run state (`RunController.running`, `results`) and its controls (`armed`,
  `autorun_global`, `autorun_off`) live in its process. The state file is written once,
  when a run ends, and records only each cell's status and what it was built from.
- While the TUI is open, its conduit holds every DuckDB file it has attached (a duckdb
  sink's, or the conduit's own `input.duckdb`). Another process can't open them, even
  read-only: `Could not set lock on file … Conflicting lock is held` (reproduced
  2026-11-01). An agent's `quicksql run` or a planned `head` on a fresh conduit fails on
  them, and a concurrent `quicksql run` would race the TUI on every sink and on the state
  file.
- The TUI opens paused (`armed = False`): until `R` or `p`, a save only logs which cells
  changed.

A state file written after every cell was considered (no server; ~0.2 ms a write when
previews stay out of it). It covers watching and waiting but not control, and not reads
through the locked files, so the socket is the choice. Measured on the dev box, the
socket's speed isn't the reason: every client is a `quicksql` process that takes about
300 ms to start, against 8–50 ms for a file watch to notice a write.

Prior art, and what's taken from it:
- **herdr**: a Unix socket speaking newline-delimited JSON (`{id, method, params}` →
  `{id, result}` / `{id, error}`); clients bootstrap from a snapshot, then subscribe to
  typed events; a subscriber that falls behind gets `events_lost` and re-reads the
  snapshot; waits are server-owned and *pinned* to what they wait on; a protocol number
  and "clients ignore unknown fields".
- **opencode**: the TUI is a client of the same API everyone else uses; the domain API
  (sessions) is kept apart from the TUI's own verbs (`/tui/show-toast`,
  `/tui/execute-command`); SDK v2's per-session event log replays from a sequence number.
- Not taken: a standalone daemon the TUI attaches to (herdr's model). Turning the TUI into
  a remote client would redo Phase 9 across a process boundary (previews, sheets and the
  catalog all read through `RunController.connect()`), add server lifecycle and staleness
  to manage, and buy detach/reattach, which running the TUI in a herdr pane already
  gives. HTTP/OpenAPI is not taken either: every client is local Python, and TCP would
  need authentication.

## What an agent can do

Everything goes through plain `quicksql` commands; the agent never speaks the protocol.

**Watch the TUI.**
- `quicksql status --json` shows each cell's state: `queued`, `running`, `ok`, `failed`,
  `skipped`, `stale` or `new`. It also shows its rows, elapsed time, target, its effective
  autorun and where that came from, whether watch reruns are armed, and the run under way.
  With no TUI open, it reads the state file, as today.
- `quicksql live events --follow` streams every event as NDJSON: compiles, compile
  errors, each cell starting and finishing, run ends, control changes. It fits Claude
  Code's Monitor tool. `--since SEQ` returns what happened after an event it already saw
  and exits, for an agent that checks between turns.

**Get the outcome of its own edit.** After saving, `quicksql live wait --json` blocks
until the TUI has finished reacting to *that file content*, and prints one document:
- `ran`: per-cell results, failures first.
- `compile_failed`: errors with cell and line.
- `paused`: which cells changed, and that watch reruns aren't armed.
- `nothing_to_rerun`.

The wait is pinned to the hash of the file as the agent saved it, so it never returns the
run before its save. If you save over it first, it returns your outcome, marked
`superseded`.

**Fix a failure precisely.** A failed cell carries:
- the engine's own message (`Binder Error: …`)
- the failing statement's number and its rendered SQL
- the cell's line range in the file

A skipped cell names the failed upstream that caused the skip.

**Read the data, through the TUI's own connection.** Because the reads use the TUI's
session, they work for every sink, including the DuckDB files the TUI locks:
- `quicksql head CELL --rows N --json` reads the head of a cell's landed output.
- `quicksql live query "SELECT status, count(*) FROM orders GROUP BY 1"` runs a read-only
  query in which each cell's name is a view of its landed output.

**Control the TUI the way your keys do.**

| Command | Your key |
|---|---|
| `quicksql live arm` / `disarm` | `p` |
| `quicksql live autorun CELL... on\|off` | `a` |
| `quicksql live autorun --all on\|off` | `A` |
| `quicksql live run -s CELL...` | `r` |
| `quicksql live run --all` | `R` (also arms) |
| `quicksql live cancel` | — |

- Commands *set* a state; they never toggle, so they can't race your keys.
- A run requested while one is under way is refused unless `--cancel` is given, so the
  agent never silently cuts off yours.
- `run --wait --json` prints the same document as `quicksql run --json`.
- Every agent command shows in the TUI's log as `agent: …`.

**Point you at something.**
- `quicksql live focus CELL --tab data` selects the cell and shows the tab.
- `quicksql live notify "orders: 3% of amount is NULL"` shows a toast and logs it.

**What it can't do.** These keep the file the source of truth and the TUI yours:
- Edit cells or config over the socket. Edits are file edits; a lasting autorun change is
  `-- @autorun: false` in the file.
- Change the session's `--set` overrides or plugins.
- Open another notebook (`o`).
- Run with its own `quicksql run` while a TUI hosts the notebook. That run is refused and
  names `quicksql live run`.

An agent's loop, end to end:

```
$ quicksql live arm
$ # edit orders_clean in the .qsql file and save
$ quicksql live wait --json
{"hash": "9f2c…", "state": "ran", "results": [
  {"cell": "orders_clean", "status": "failed", "statement": 2,
   "line": 41, "line_end": 58, "statement_sql": "SELECT … amount …",
   "error": "Binder Error: Referenced column \"amount\" not found …"},
  {"cell": "daily_totals", "status": "skipped", "error": "upstream failed: orders_clean"}]}
$ # fix lines 41–58 and save
$ quicksql live wait --json            # state: ran, both ok
$ quicksql head orders_clean --rows 5 --json
$ quicksql live notify "orders_clean fixed: amount → total_amount"
```

## Design

### The pieces (all front-end layer)

- `live/protocol.py`: message shapes, `PROTOCOL = 1`, error codes, and
  `source_hash(bytes)`, the sha256 prefix both sides compute from the notebook's bytes.
  It imports only the stdlib.
- `live/host.py`: `LiveHost`, one per open notebook, with no UI. It owns:
  - the controls, moved out of `QsqlApp`: `armed`, `autorun_global`, `autorun_off`,
    `cell_autorun`, and the "which planned cells rerun" filter from `_on_recompiled`
  - a mirror of the run state (queued, running, results, the run's id and hash)
  - the outcome per source hash
  - the event fan-out with sequence numbers and a bounded ring of recent events
  - the snapshot builder

  It starts runs, cancels them and reads data through a small driver interface the TUI
  implements over `RunController`, so tests can drive a host with a fake driver and no
  Textual.
- `live/server.py`: an `asyncio.start_unix_server` on the TUI's own event loop. Requests
  dispatch there, the same thread as key handlers, so the controls need no locks. DuckDB
  reads (`head`, `query`) go to a worker thread with their own cursor.
- `live/client.py`: a blocking client the CLI uses. It imports neither textual, polars
  nor duckdb.
- `QsqlApp` creates a host and a server in `_load_notebook` and closes both on `o` and on
  unmount. `armed`, `autorun_off` and `autorun_global` become read-through properties, as
  Phase 9 did for the run state. Keys call host methods with `by="keys"`, socket requests
  with `by="agent"`, and every change emits `controls_changed`.
- `import-linter`: `quicksql.live` and `quicksql.live.**` join the core contract's
  forbidden list. A subprocess test, like `tests/test_demos.py`'s, runs `quicksql live`
  commands with polars and textual blocked.

### The socket

- **Path:** `$QSQL_HOME/run/<name>-<key>.sock`, with the same 12-hex key on the resolved
  notebook path as `state.state_file`. The client derives it the same way, so nothing
  needs to discover it.
- **Long paths:** Unix socket paths are limited to 104–108 bytes (pytest's `tmp_path`
  homes can exceed that). Past the limit the socket moves to
  `$XDG_RUNTIME_DIR/quicksql/`, else `/tmp/quicksql-$UID/`, keyed on the home *and* the
  notebook.
- **Permissions:** the directory is 0700 and the socket 0600. Same user only; that
  grants nothing beyond what running `quicksql` as you already can.
- **One host per notebook:** at bind, a path that still answers `hello` means another TUI
  hosts the notebook. This one then runs without a socket and logs why. A path that
  refuses connections is stale and is replaced. A client that finds no answering socket
  exits 4.

### Protocol

One JSON document per line. The first request on a connection must be `hello`.

```
→ {"id": 1, "method": "hello", "params": {"protocol": 1, "client": "quicksql 0.x"}}
← {"id": 1, "result": {"protocol": 1, "notebook": "/abs/x.qsql", "pid": 4242, "methods": […]}}
→ {"id": 2, "method": "autorun", "params": {"cells": ["stg"], "on": false}}
← {"id": 2, "result": {"controls": {"armed": true, "autorun_global": true, "autorun_off": ["stg"]}}}
← {"event": {"seq": 118, "kind": "cell_finished", "cell": "stg", "result": {…}}}   (subscribers only)
← {"id": 3, "error": {"code": "busy", "message": "a run is under way: stg, orders (pass cancel)"}}
```

Clients ignore unknown fields, and a protocol mismatch at `hello` fails with a message to
restart the TUI. That case is a long-open TUI meeting a newer CLI after `git pull`.

| Method | Params | Result |
|---|---|---|
| `hello` | `protocol`, `client` | the session's identity and methods |
| `snapshot` | — | notebook, `hash`, `compile_error`, controls, cells, the run under way, `seq` |
| `events.subscribe` | `since?`, `kinds?`, `cells?` | an ack with `seq`, then pushed events |
| `events.since` | `seq` | the ring's events after `seq`, or `events_lost` |
| `wait` | `hash`, `timeout` | the outcome for `hash` (below) |
| `head` | `cell`, `rows` (≤ 1000) | `columns`, `rows` |
| `query` | `sql`, `rows` (≤ 1000) | `columns`, `rows` |
| `arm` / `disarm` | — | controls |
| `autorun` | `cells` or `all`, `on` | controls |
| `run` | `cells` or `all`, `cancel` | `run_id` |
| `cancel` | — | the cells that won't run |
| `focus` | `cell`, `tab?` | — |
| `notify` | `message`, `severity?` | — |

`focus` and `notify` are the TUI's own verbs, kept apart from the session's, as opencode
keeps `/tui/*`. A host without a screen (a later headless `watch`) won't list them in
`methods`.

Events:

| Kind | Payload | Source |
|---|---|---|
| `compiled` | `hash`, `cells`, `changed` | the host, after each compile |
| `compile_failed` | `hash`, `errors` (cell, line, message) | the host |
| `run_started` | `run_id`, `hash`, `cells` (post-expansion) | the runner's `RunEvent`, plus `run_id`/`hash` |
| `cell_started` / `cell_step` | `cell`, `detail` | the runner |
| `cell_finished` | `cell`, `result` (`RunResult.to_dict()` + `line`, `line_end`, `statement_sql`) | the runner, plus the host's line info |
| `run_finished` | `run_id`, counts, `cancelled` | the runner |
| `note` | `detail` | the runner |
| `settled` | `hash`, `state` | the host: it has finished reacting to `hash` |
| `controls_changed` | controls, `by` (`keys` / `agent`) | the host |
| `notebook_closed` | — | the host, before the server closes on `o` or quit |

The runner and `RunResult` don't change. Line ranges come from `project.cells`. The
failing statement's SQL comes from splitting the rendered SQL with the cell's executor's
`split_statements`, the same split the runner made. Compiles and controls are the host's
events, not the runner's.

**`wait`, pinned to a hash.**
- The client reads the notebook's bytes and sends `source_hash(bytes)`.
- The host compiles from the bytes it hashed: `bootstrap.compile_file` gains an optional
  `text`, so the hash and the compile can't see different saves.
- It settles a hash when one of these happens:
  - the compile fails
  - it's paused and has logged what changed
  - nothing needs to rerun
  - the run started for that hash finishes; a run you replace with `r` carries the same
    hash, so the wait follows it to the end
- A wait for a hash not seen yet waits for the watcher. If a newer hash settles first, the
  newer outcome comes back with `superseded: true`.
- On timeout the client exits 4.

**Reads through the session.**
- `head` reuses `read_landed`'s rule: read through the sink's `ref_expr`, never its
  `prepare()`. A database sink is readable once this session has attached it. Until then
  the answer is `not_attached`, and the client falls back to reading offline, since the
  TUI holds no lock on a file it hasn't attached. The rule moves out of `tui/previews.py`
  into a UI-free helper returning `models.Preview`, which the TUI wraps in a frame as
  before.
- `query` runs on its own cursor, which shares the session's attached databases, with
  each landed cell as a `TEMP VIEW`. Temp objects are per connection, so the run's
  connection never sees them. The SQL is wrapped as `SELECT * FROM (<sql>) LIMIT n`, so
  anything but a single query is a syntax error.
- A cell that is running answers `busy`; read it after its `cell_finished`.

**Bounded everywhere.** Each subscriber has a fixed-size queue, and one that falls behind
gets `events_lost` and is dropped. The ring of recent events is fixed-size. Nothing grows
with a slow or dead client (see CLAUDE.md's OOM gotcha).

### CLI

Every command keeps `FILE`'s default and its `--json` conventions: one document on
stdout, nothing else.

```
quicksql status [--json]                 live-aware: queued/running and controls when hosted
quicksql head CELL [--rows N] [--json]   through the session when hosted, else offline
quicksql live events [--follow] [--since SEQ] [--kinds K,…]
quicksql live wait [--timeout SECONDS] [--json]
quicksql live query SQL [--rows N] [--json]
quicksql live run (-s CELL… | --all) [--cancel] [--wait] [--json]
quicksql live cancel
quicksql live arm | disarm
quicksql live autorun (CELL… | --all) on|off
quicksql live focus CELL [--tab sql|data|config|log]
quicksql live notify MESSAGE
```

Exit codes extend the shared ones:

| Code | Meaning |
|---|---|
| 0 | ok |
| 1 | a cell failed (`wait`, `run --wait`) |
| 2 | usage error |
| 3 | compile error, or an unknown cell |
| 4 (new) | no live session, a protocol mismatch, or a timeout |
| 5 (new) | refused: `busy`, or `autorun on` for a cell the file switches off |

`quicksql run` refuses with exit 5 while a TUI hosts the notebook, naming
`quicksql live run`.

## Decisions (defaults, to confirm)

1. **Busy:** an agent's `run` is refused while a run is under way unless `--cancel` is
   given. Your keys keep cancelling, as now.
2. **Session autorun can only switch off.** `autorun CELL on` for a cell whose file says
   `autorun: false` is refused, with the line to edit. The other option is to let the
   session force cells on, which would also change what `a` does.
3. **`quicksql run` while live:** refused with a hint. The alternative is delegating it to
   the session, which would silently ignore `--set` and `--plugins` that differ.
4. **`query` ships in step 6.** It could wait until `head` alone proves too little.

## Steps (each one focused change, test-first)

1. **Prep (S).**
   - `RunEvent.to_dict()`, next to `RunResult.to_dict()`.
   - `bootstrap.compile_file(text=)`.
   - The landed-read helper returning `Preview`, with `tui/previews.py` built on it.

   No behavior changes, and the existing pilot tests stay green.
2. **The host (M).** `live/host.py` with the controls moved out of `QsqlApp`, the run
   mirror, outcomes by hash, and the event fan-out. The TUI reads through it and its keys
   call it. Still no socket.
   - Tests first: host unit tests with a fake driver (set, not toggle; `by` attribution;
     settling per hash, including paused and superseded).
   - The existing `a`, `A`, `p`, `r`, `R` pilot tests pass unchanged.
3. **Read-only socket (M).** `live/protocol.py`, `live/server.py`, `live/client.py`; the
   TUI hosts per notebook; `live events`; a live-aware `status`.
   - Tests first, against a host with a fake driver and no Textual:
     - `hello` and protocol mismatch
     - snapshot, then subscribe with no gap
     - `events_lost` on a slow subscriber
     - `since` past the ring
     - a stale socket replaced; a second host refused
     - the long-path fallback
     - 0600 permissions
   - One pilot test drives a real TUI and reads its events from a client thread.
   - The blocked-import subprocess test.
4. **`live wait` (M).** Hash pinning, the four states, `superseded`, timeout.
   - Tests first: a wait sent before the watcher saw the save; a save during the run; a
     replaced run; the paused TUI.
5. **Control (M).** `arm`, `disarm`, `autorun`, `run`, `cancel`, `run --wait`; `agent: …`
   log lines and a subtitle marker while an agent is connected; `quicksql run` refused
   while live.
   - Tests first: busy and `--cancel`; `autorun on` refused by the file; a key and a
     request land in order.
6. **Data (S–M).** `head` and `query` through the session; `not_attached` and its offline
   fallback; `busy` for a running cell.
   - Tests first: a duckdb-sink notebook open in the TUI is readable through `head` while
     an offline read fails on the lock; `query` rejects DDL and DML.
7. **TUI verbs (S).** `focus` and `notify`; `focus` is refused while a modal screen (the
   picker, help) is up.
8. **The skill (S).** `agent-cli-plan.md` step 4's skill, plus this loop: arm, edit,
   wait, read, fix; the exit codes; when to use `live` and when not.
9. **Later, if wanted.** Headless `quicksql watch` hosts the same socket, without
   `focus` or `notify`; a TUI key to make the socket read-only.

Sequencing: `agent-cli-plan.md` steps 2–3 (`check`, `--json` on `list` and `compile`)
are independent and can land in either order. Its offline `head` (step 5) lands with
step 6 here, as the fallback.

## Out of scope

- A standalone daemon, `tui --attach`, or more than one TUI per notebook.
- Remote access: TCP, HTTP, authentication, or an MCP server.
- Writing cells or config through the socket.
- Agent-side overrides of the session's `--set` and `--plugins`.

## Risks

- **Blocking the UI.** Handlers on Textual's loop must be O(1); every DuckDB read goes to
  a thread. A pilot test keeps the TUI responsive during a `query`.
- **Fighting over controls.** Commands set rather than toggle, every change is
  attributed, and the busy policy applies. If it still bites, add a read-only key (step 9).
- **Version skew** between a long-open TUI and a newer CLI: the protocol number and
  `hello`.
- **Reading a table mid-write.** `head` and `query` refuse a running cell. Other cells
  read under DuckDB's MVCC.
- **Platform:** Unix sockets only. On Windows the `live` commands exit 4.

## Verification

- Each step: `uv run pytest` under `systemd-run --user --scope -p MemoryMax=3G`, and
  `uv run lint-imports`.
- `/verify` after steps 3, 5 and 7: the TUI in one tmux pane, `quicksql live …` in
  another, checking the log's `agent:` lines, the subtitle marker, and focus/notify on
  screen.
- After step 6: kitchen-sink against the demo stack (duckdb and postgres sinks), reading
  each output through `head` with the TUI open.
- After step 8: a fresh Claude Code session, given only the skill, fixes a deliberately
  broken cell while the TUI is open, then reads its result.
