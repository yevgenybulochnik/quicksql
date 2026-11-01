# Working alongside the open TUI

When the person has `quicksql tui` open on a notebook, the TUI serves a live session on a
socket only they can reach. `quicksql live …` talks to it: you see what they see, and drive
the TUI the way their keys do. Everything you do shows in their Log tab as `agent: …`, and
their title shows `· agent` while you're connected.

`quicksql status NOTEBOOK --json` says `"live": true` when a TUI has the notebook open. With
none, every `live` command exits 4 and says so.

## The rules

- **The TUI is the only runner.** `quicksql run` and `quicksql watch` are refused while it
  has the notebook (exit 5). Run cells with `quicksql live run`, or let a save rerun them.
- **The file is yours to edit; the TUI never writes it.** Nothing over the socket changes a
  cell or a directive: a lasting change is a file edit, which the TUI picks up on save.
- **You never cut the person's run short.** `live run` is refused (exit 5, `busy`) while a
  run is under way. Wait for it (`live wait`), or pass `--cancel` only when you mean to
  replace it.
- **Commands set a state, they never toggle,** so a command and a key press can't undo each
  other: `live arm` arms whether or not it already was.
- **Ask before changing their setup** (arming, switching autorun off) unless they asked you
  to drive.

## Waiting for a save

```
quicksql live wait NOTEBOOK --json [--timeout 600]
```

Run it right after saving. It hashes the file as it is now, and returns once the TUI has
compiled exactly that text and finished rerunning what changed, so it never returns a run
from before your save. The outcome's `state`:

| State | What happened | Exit |
|---|---|---|
| `ran` | It reran what changed (or a run of this text finished): `results` per cell, `summary` counts | 1 if a cell failed, else 0 |
| `compile_failed` | The text didn't compile: `errors`, each with `cell`, `line`, `message` | 3 |
| `paused` | The TUI isn't armed: `changed` lists the cells that would rerun. `live arm`, or `live run` them | 0 |
| `nothing_to_rerun` | The save changed no cell that autoruns (only comments, or `autorun: false` cells) | 0 |

- `"superseded": true`: the person saved over your text before the TUI got to it; the
  outcome is of their text. Read the file again before editing.
- Saving the same text again keeps the last run's results: a `ran` stays `ran`.
- A failed cell's result: `error` (the engine's own message, with DuckDB's `LINE 1:` and
  caret context), `statement` and `statement_sql` (which statement failed, rendered),
  `line` and `line_end` (where the cell is in the file). Fix the SQL there.
- A `skipped` cell's `error` names the failed upstream. Fix that one.
- Exit 4 with `timeout`: nothing settled in time. Check `quicksql status NOTEBOOK`: a long
  run may still be going.

## Running cells

```
quicksql live run NOTEBOOK -s CELL [-s CELL…] [--wait] [--json] [--cancel]
quicksql live run NOTEBOOK --all [--wait] [--json]
quicksql live cancel NOTEBOOK
```

- `-s CELL` runs the cell and its upstreams, as `r` does. `--all` runs every cell and arms
  reruns, as `R` does.
- `--wait --json` blocks until it ends and prints the same document as `quicksql run
  --json` (plus `run_id`). Exit 1 if a cell failed; 5 if the person started another run
  that replaced yours (`"status": "replaced"`).
- Without `--wait` it prints `{"run_id": N}`: the run goes on in the TUI.
- `live cancel` stops the run under way after its current cell and lists what won't run.

## The TUI's switches

```
quicksql live arm NOTEBOOK                     # p: a save reruns the cells it changed
quicksql live disarm NOTEBOOK                  # saves only recompile
quicksql live autorun off NOTEBOOK -s CELL     # a: this cell stops rerunning on saves
quicksql live autorun on NOTEBOOK -s CELL
quicksql live autorun off NOTEBOOK --all       # A: no cell reruns on saves
```

- A cell reruns on a save when the TUI is armed, autorun is on for all, the file doesn't
  say `autorun: false`, and the session hasn't switched it off.
- The session can only switch a cell **off**. `autorun on` for a cell whose file says
  `autorun: false` is refused (exit 5) with the line to edit.
- The switches last until the TUI quits or opens another notebook; `status --json` shows
  each cell's `autorun` (`effective`, and the `file`, `session` and `global` parts).

## Reading data

```
quicksql head CELL NOTEBOOK [--rows 10] [--json]
quicksql live query "SELECT … FROM cell …" NOTEBOOK [--rows 20] [--json]
```

- Both read through the TUI's own session, so they work for every sink, including the
  DuckDB files the TUI holds locked.
- `head` reads a cell's landed output. Without a TUI it reads Parquet outputs on its own.
- `live query` runs **one** read-only SELECT (also `FROM x`, `SUMMARIZE x`, `DESCRIBE x`)
  where each landed cell is a view named after it: `SELECT status, count(*) FROM orders
  GROUP BY 1`. Anything else is refused (exit 5); SQL that fails exits 1 with DuckDB's
  message.
- A cell that is running answers `busy`: read it after it finishes.
- A database output this session hasn't attached yet (nothing ran it since the TUI
  opened) can't be read through it: run the cell once.

## Watching

```
quicksql live events NOTEBOOK --follow [--kinds cell_finished,settled]
quicksql live events NOTEBOOK --since SEQ
```

One JSON object per line, each with a growing `seq`. `--follow` streams until the TUI
closes the notebook (a background watcher, e.g. Claude Code's Monitor tool, sees each
line). Without it, the recent events (after `--since`) print and the command exits: a way
to catch up between turns. The kinds:

| Kind | Fields | When |
|---|---|---|
| `compiled` | `hash`, `cells`, `changed` | the TUI compiled a save |
| `compile_failed` | `hash`, `errors` | a save didn't compile |
| `run_started` | `run_id`, `hash`, `cells` (everything that will run) | a run began |
| `cell_started` | `run_id`, `cell`, `detail` (engine → sink) | a cell began |
| `cell_step` | `run_id`, `cell`, `detail` | a step within a cell (preparing, executing, landing) |
| `cell_finished` | `run_id`, `cell`, `result` | a cell ended: ok, failed or skipped |
| `run_finished` | `run_id`, `detail` (counts) | the runner finished |
| `run_ended` | `run_id`, `state` (finished, replaced, dropped), `summary` | the run is over, or never ran |
| `note` | `detail` | something the runner wants said (a reopened session, a cancel) |
| `settled` | `hash`, `state` | a save's outcome is ready (what `live wait` waits for) |
| `controls_changed` | `armed`, `autorun_global`, `autorun_off`, `by` (`keys` or `agent`) | a switch changed |
| `notebook_closed` | `notebook` | the TUI opened another notebook or quit |

A watcher that falls too far behind gets `events_lost` (exit 4): read
`quicksql status NOTEBOOK --json`, then follow again.

## Pointing the person at something

```
quicksql live focus CELL NOTEBOOK [--tab sql|data|config|log|catalog]
quicksql live notify "orders: 3% of totals are NULL" NOTEBOOK [--severity warning]
```

`focus` selects the cell in their TUI and shows the tab (refused while a dialog is open).
`notify` shows a toast and logs it. Use them when you finish something they should look
at, not for every step.
