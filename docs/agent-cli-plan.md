# Plan: a CLI agents can drive

**Status:** proposed 2026-10-30; revised 2026-11-11 after a hands-on pass over the whole
CLI (what it found is below), and twice 2026-11-12 after reviews of that revision against
the code (the last two sections). Of the first draft, steps 1, 4, 5 and 7 are done (step 1
is `53fcfc5..d1575d9`). Nothing of this revision is built: steps 8–22.

## Context

Coding agents (Claude Code and others) work on notebooks through the shell. They edit
the `.qsql` file, then need to check it, run it and read what it produced, and branch
on the outcome.

The first draft gave them the JSON half of that: `run --json` with exit codes, `status`,
`head`, the packaged skill, and (through `docs/live-session-plan.md`) `quicksql live`.
What's left is of two kinds. Two commands the first draft named were never built: `check`
and `--json` on `list` and `compile`. And driving the CLI the way an agent does, without
the skill in hand, turned up places where it misleads: a guess that writes a file, an
error that names the wrong thing, a read that looks whole and isn't.

`docs/core-refactor-plan.md` keeps only `run --json` with exit codes (Phase 2) and
`quicksql status` (Phase 8) and points here for the rest. This plan adds no new
architecture: each command is a thin front-end over what the compiler, runner, LSP
analyzer and catalog already compute.

## The loop

An agent's loop is **check → run → read**: catch mistakes without touching data, run
the cells it changed plus their upstreams, then look at enough of the result to judge it.
Every step below serves that loop.

## What the hands-on pass found (2026-11-11)

Every offline command was run against a three-cell scratch notebook (one cell failing);
the `live` commands were read from source and run only without a TUI.

| Found | Evidence | Step |
|---|---|---|
| The bare command writes a file | `quicksql` in an empty directory scaffolded `base.qsql` (`cli.py:49`); `-h` and `--version` are "No such option" | 8 |
| `quicksql live` alone exits 2 | it prints help, through click ≥ 8.2's `no_args_is_help` | 8 |
| Help costs three times what it needs to | top-level help is 4,139 bytes boxed, 1,248 with `TYPER_USE_RICH=0`; `status`'s row is its whole first paragraph | 9 |
| Text errors land on stdout | compile errors, unknown cells, the TUI refusal (`_project`, `cli.py:80`); `live` errors go to stderr | 10 |
| A plugin that won't import is a boxed traceback | `directives --plugins nope.mod`, exit 1; `run --json` already makes it one document | 10 |
| Swapped positionals name the wrong thing | `show nb.qsql orders` → `no such notebook: orders`; `show nb.qsql` → `no such notebook: base.qsql` | 11 |
| An unknown cell gets no help | `unknown cell: ordrs`, while `live catalog` names the closest rows | 11 |
| `--json` failures have three shapes, and two holes | `run`/`status`: `errors`; `head`/`live`: `error`; `usage --json` and `directives --json` print text | 12 |
| Reads don't say they were cut | `head`, `live query`: `columns` and `rows` only; `run --json --rows 500` returned 100 | 13 |
| A failed run is mostly traceback | 1,525 of a 2,320-byte document, against 150 for the error | 14 |
| JSON values are unbounded | text tables cut at 40 characters, JSON doesn't | 14 |
| `run`'s text is mostly progress | 14 lines for three cells before the 3 result lines | 14 |
| `status` forgets why a cell failed | `failed  broken` with no reason: the state file keeps no error | 15 |
| Offline and live `status --json` differ | offline cells lack `line`, `engine`, `sink`, `depends_on` | 15 |
| `list`, `show`, `compile` have no `--json` | first draft's step 3 | 16 |
| Nothing checks without running | `compile` prints every cell's SQL; first draft's step 2 | 17 |
| No query without a TUI | offline there's only `head`, so an agent opens the Parquet files itself | 18 |
| Waits outlast the agent's shell | `live wait` defaults to 600 s; Claude Code's shell call to 120 s | 19 |
| `live events --follow` can hang silently | a typo in `--kinds` filters everything; no `--until`; help omits `cell_step`, `note` | 19 |
| The skill's references take two steps | SKILL.md links are relative: `skill --path`, then read files | 20 |

## Conventions (every command)

Kept from the first draft:

- `--json` prints exactly one JSON document on stdout and nothing else: no progress, no
  colour. Streams are NDJSON, one event per line. Warnings and logs go to stderr.
- Exit codes are shared: 0 ok, 1 a cell or SQL check failed, 2 usage error (click),
  3 the notebook can't be compiled or the selection is invalid, 4 no live session (or a
  timeout), 5 refused.
- Messages are the engine's own (`Catalog Error: …`), as `RunResult.error` carries them.
- rc plugins and `--plugins` load exactly as for the text commands.
- Bounded output: an agent can't flood its own context by accident.

Added by this revision:

- **stdout holds only what was asked for.** Without `--json`, every error goes to stderr.
- **A failing `--json` document carries `"error": {"code", "message"}`,** and the exit
  code is that code's (`_EXIT_FOR`). The key is absent in two cases only: exit 0, and
  exit 1 because a cell failed, which the cells say (decision 15). The codes are
  `live.protocol`'s, offline too. A document that has more to say (`run`'s `errors`,
  `status`'s) keeps saying it.
- **A read says when it was cut:** `"truncated"` for rows, `"values_cut"` (the columns)
  for values.
- **Arguments forgive what can't be misread.** A cell name can't hold `.` or `/`
  (`parser.CELL_NAME`), so a notebook path where a cell belongs is recognised. A list
  option takes repeats or commas.
- **An error that knows the fix says it:** the closest cell names, the command to run
  instead.
- **Help is plain text**, and each command's summary is one line.

## Decisions (defaults, to confirm)

1. **The bare command prints help.** `quicksql init` scaffolds. This changes what people
   get (`README.md:40`, CLAUDE.md's "bare = init"). The alternative is to scaffold only on
   a terminal, which keeps the shortcut for people and makes the command behave two ways.
2. **Argument order stays `THING [FILE]`,** and a swap is accepted, not just explained.
   The alternative, notebook first everywhere, breaks every documented command and the
   skill for a problem the swap already removes.
3. **A compile error's `error.code` is `no_project`,** as it is live and in offline
   `head` today. `run`'s `"status": "compile_error"` stays. A cell that isn't in the
   notebook is `unknown_cell` everywhere, `run --select` included: `head` and the live
   session already say so. Both exit 3.
4. **`traceback` leaves result JSON.** `run --traceback` brings it back. Nothing in
   `src/` but `to_dict` reads `RunResult.traceback`, and live results stop carrying it.
5. **A JSON value over 500 characters is cut,** marked in the value (`…+1234`) and by
   `"values_cut": ["blob"]`, the columns it touched. A string keeps its first 500. A list
   or struct is measured as its JSON text and, when over, becomes that text's first 500
   as a string: cutting only the strings inside it would leave a 10,000-element array
   whole. `--max-chars 0` keeps them whole. The CLI cuts, never the model or the socket.
   It applies to `query` too, where the agent asked for that value: a cut costs it one
   more call, a flood costs its context.
6. **`live wait` and `live run --wait` wait 110 s by default,** not 600. `--timeout` still
   reaches 3600. A wait that times out can be asked again: `live wait` is keyed on the
   file's hash. A run is not: asking `live run --wait` again answers `busy`, so a run is
   waited for by its id (decision 16).
7. **Off a terminal, `run` and `watch` drop `cell_step` lines.** Each cell keeps its
   `started` line and its result, so a CI log still shows which cell is slow.
8. **The state file keeps each cell's last result** (rows, target, elapsed, error,
   statement) **and is written 0600**: an engine's message can quote data. A message can
   also echo a DSN, so a failure's error and traceback pass the executors' `redact` where
   the run records the result. That covers what `run` prints and a live session sends as
   well, and a failed result that a plugin's `run` hook built.
9. **Offline `query` runs in the caller's process** on a fresh in-memory DuckDB: one
   SELECT, a view per output that reads without an ATTACH (Parquet), as offline `head`.
10. **`skill --install` keeps `.agents/skills/` as its default;** `--target claude`
    writes `.claude/skills/quicksql/`.
11. **`usage --columns N` stays a count,** though `--columns` is a name list on `head`
    and `live catalog`. The alternative is to rename it `--per-table`: cheap now, two
    days after it shipped, and never cheaper. If taken, it joins step 11.
12. **`status` exits 0 when cells failed.** It reports; `run` and `live wait` judge.
13. **`check` stays off polars.** Polars is still a core dependency (`pyproject.toml`),
    so `check` would run on the analyzer as it is. The split in step 17 keeps it among
    the commands `tests/test_demos.py` runs with polars blocked (`compile`, `list`,
    `explain`, `run --json`, `status`), and saves its import: about 0.1 s of a 0.4 s
    start (measured). Without the split, step 17 is the command alone.
14. **A plugin that won't load is a compile error:** exit 3, and `compile_error` in
    `run --json`, where today it's `error` and exit 1. The loader wraps it
    (`bootstrap.load_plugin_module` already does for an unreadable path), so commands
    keep catching `QsqlError` alone and a bug in quicksql keeps its traceback. The
    alternative, catching every exception in `_project`, keeps exit 1 and hides bugs.
    The message carries the exception's type and the line of the plugin's file it was
    raised on: once wrapped, that one line is all the plugin's author sees.
15. **`"error"` marks a command that didn't do what it was asked,** whatever else its
    document holds. So the CLI adds it to three live results that exit non-zero without
    one today: `live wait` on a save that didn't compile (`no_project`, exit 3),
    `live run --wait` on a run that was replaced or dropped (`refused`, exit 5), and live
    `status` with a compile error (`no_project`). A failed cell is not one: the command
    ran, the cells carry the errors, and it exits 1. The alternative, `error` only when
    there's no document at all, leaves an agent the three shapes the pass found.
16. **A run is waited for by its id:** `live wait --run ID` prints what
    `live run --wait` would have. The session has had `run.wait` all along, so it works
    against a TUI that predates this. The alternative is to send the agent to plain
    `live wait`, which also ends when the run does. But it answers with a save's outcome,
    a different document, and with another save's if the file changed meanwhile.
17. **A failure is marked once its cell has changed.** `status` says `failed` for a cell
    whose last run failed, whatever happened to its text afterwards (`state.py:222`). It
    holds live too: a save into a paused session keeps the old error. With the error now
    printed (step 15), an agent that fixed the cell and hasn't rerun it would read its
    own fix as the failure. So the state file keeps the fingerprint the failed run was
    built from, and a row whose cell no longer matches it says `changed since it failed`.
    An upstream that landed anew marks it too: its fingerprint is part of the cell's, and
    the failure may have been its data. The row stays `failed`: `stale` is about a landed
    output, and a failed run landed none.
18. **What stops a run before any cell is `failed`,** a code the CLI adds
    (`protocol.FAILED`, exit 1). Not `internal`: the protocol defines that as a bug in
    the session, and a locked database or an unreadable curation file is neither a bug
    nor the session's.

## Steps (each one focused change, test-first: a `test(...)` red commit, then the green)

Each step updates the skill reference it changes in its green commit: `tests/test_skill.py`
pins them.

### Stop misleading

8. **The bare command prints help; `-h`; `--version`.** `cli.main` prints `ctx.get_help()`.
   `context_settings={"help_option_names": ["-h", "--help"]}` on `app` and `live_app`. An
   eager `--version` from `importlib.metadata`. `quicksql live` alone does the same, in
   place of `no_args_is_help`, which exits 2. Tests: the bare command creates nothing
   (replaces `test_bare_invocation_scaffolds_qsql_file`) and both exit 0. Docs:
   `README.md:40`, CLAUDE.md.

9. **Plain help.** `rich_markup_mode=None` and `pretty_exceptions_enable=False` on both
   apps. Every command gets a `short_help` that click doesn't cut (it cut ten of
   fifteen in the plain rendering). The top level gains an epilog: the exit codes, FILE's
   default, and "agents: `quicksql skill`". Tests: no box characters and no `...` in any
   `--help`; the top-level help names `quicksql skill`.

10. **Errors on stderr, one line each.** One `_fail(message, exit_code)` replaces the
    bare `typer.secho(..., fg="red")` calls in `_project`, `show`, `compile_cmd`, `run`,
    `_refuse_while_live`, `_init`, `skill`, `directives_cmd`, `usage_cmd`, `tui` and
    `lsp` (whose stdout is the protocol's channel once it starts). What a command was
    asked for stays on stdout, the failures in it included: a cell's `FAIL` line, a
    save's `compile error` in `watch` and `live wait`.
    `bootstrap.load_plugin_module` wraps whatever `Exception` importing a plugin raises
    in a `QsqlError` naming the module, the exception's type and the line of the plugin's
    file it was raised on (decision 14), so `_project` still catches `QsqlError` alone.
    Tests assert `result.stdout == ""` and the message in `result.stderr`;
    `test_run_json_other_errors_before_cells_still_print_one_document` moves to
    `compile_error` and exit 3.

11. **Arguments that forgive.**
    - `_cell_and_file(thing, file)` for `show`, `head`, `live focus`: when `thing` holds
      `.` or `/` and `file` is a valid cell name, swap them. When `file` isn't one (it
      defaulted), exit 2 with the form and the notebook's cells.
    - `live query` and `live notify` take free text first (step 18's `query` too): swap
      when the first positional is an existing file and the second, given, isn't. When
      FILE was left out and the first positional is an existing file, exit 2 with the
      form: the text is what's missing. A swap there would send the default's name as the
      text: `live notify nb.qsql` would toast `base.qsql` in the notebook's TUI and exit
      0. Left out is `ctx.get_parameter_source("file").name == "DEFAULT"`, compared by
      name: Typer 0.26 carries its own copy of click, so click's enum never compares
      equal to the one it returns (probed).
    - `Project.unknown_cells(names)` in `compiler.py`: the message, with
      `difflib.get_close_matches` and, up to 20 cells, all of them. `show`, `compile`,
      `head`, the runner's `--select` check (`runner.py:130`) and `LiveHost._known` use it.
      The runner raises it as `errors.UnknownCellError`, a `ConfigError`: what catches
      `QsqlError` still does, and step 12 can tell it from a compile error.
      It adds to today's text, so the tests that match `unknown cell(s) in --select: ghost`
      hold; `tests/test_cli.py:364` pins the whole entry and changes.
    - `_names(values)` splits `-s/--select`, `--kinds` and `--columns` on commas. Not
      `usage --table`: a glob can hold one (`[a,b]`).
    - `_refuse_while_live` names the same run: `quicksql live run nb.qsql -s orders
      --wait --json`.

12. **One failure shape under `--json`.** `_live_fail` becomes the one way a `--json`
    command fails before its result. `usage --json` and `directives --json` use it.
    `run --json` and `status --json` (offline and live) add `"error"` beside what they
    print: `no_project` for a compile error, `unknown_cell` for a `--select` that names
    none (decision 3), `refused` while a TUI has the notebook, `failed` for `run`'s
    `"status": "error"` (what stopped it before any cell, such as a locked database;
    decision 18). The live results that exit non-zero gain it too (decision 15), added by
    the CLI to what the socket returned: `live wait` on `compile_failed`,
    `live run --wait` on `replaced` and `dropped`. Nothing is removed.
    - `live wait` on a notebook it can't read is a click usage error today, with `--json`
      too: exit 2, a boxed message, nothing on stdout (run). It connects first instead,
      so a notebook no TUI has answers `no_session` and exit 4, as every other `live`
      command does. A file a TUI has and the CLI can't read is `no_project`, exit 3.
    - `live events` takes a hidden `--json` that does nothing.
    - Tests: every `--json` command × (missing notebook, one that doesn't compile,
      `--plugins nope.mod`, an unknown cell where it takes one) prints one document with
      `error.code`, and exits by `_EXIT_FOR`. The commands are one parametrised list,
      which steps 16, 17, 18 and 22 add theirs to. And the rule itself, in the helper
      every `--json` test parses through: `"error"` is present exactly when the exit code
      is neither 0 nor a failed cell's 1.

### Outputs that say what they are

13. **Reads say when they're cut.** `Preview` gains `truncated`. `conduit.preview`
    fetches `limit + 1` rows and drops the extra, which covers offline `head`, live `head`
    and the runner's cached preview. `LiveHost.query` does the same. `Preview.to_dict`
    emits `"truncated"`: the preview's, or true when `rows` cut it further, so
    `run --rows 500` says so. Text output ends `more rows: -n N (at most 1000)`. The
    protocol only gains a key; against a TUI that predates it the CLI prints
    `"truncated": null`.

14. **Lean results.**
    - `RunResult.to_dict(rows=0, traceback=False)`; `run --traceback` (text: under the
      FAIL block). `tests/test_cli.py:304` and `tests/test_models.py:25,46,51` pin the
      field and change. The live host's results go through `to_dict` too
      (`result_dict`), so its events, outcomes and run records stop carrying it. A TUI
      that predates this still sends it, so the CLI drops the key from every live result
      it prints (`live wait`, `live run --wait`, `live events`).
    - One CLI function cuts a read's values (`columns` and `rows` in, the same out, plus
      `"values_cut"`) for `head`, `live query` and `run --rows`, with `--max-chars` on
      each: a string past the limit, and a list or struct whose JSON text is past it
      (decision 5). `Preview.to_dict` stays as it is: the live host calls it too, and a
      cut made there couldn't be undone by `--max-chars 0`.
    - `head --columns a,b`, selected by the same function's caller; an unknown column is
      an error naming the columns.
    - So live reads are cut and selected from what the socket returned, and the protocol
      doesn't change.
    - `_live_event` prints `cell_step` lines only when stdout is a terminal (decision 7);
      `run` and `watch` share it. `test_run_streams_progress_lines` follows.

15. **`status` remembers the last result.**
    - `state.CellState` gains `rows`, `target`, `elapsed`, `error` (capped at 2,000
      characters), `statement` and `built_from`: the fingerprint the run computed for the
      cell (`runner.py:448`; a skipped cell never ran and has none). Readers use `.get`,
      so `_VERSION` stays 2 and an older quicksql ignores the keys. The file is written
      0600, created so and not chmodded after. `runner._save_state` fills them from a new
      `RunSession.last_result` beside `last_status`, which is all the session keeps
      today. A cell the session hasn't run keeps what the file had, as its status does.
    - Where the run records a result, after the plugin chain returns (`runner.py:450`),
      its error and traceback pass every executor's `redact` (decision 8). Not in
      `_failed`: a failed result that a plugin's `run` hook built never passes there.
      `catalog.redact` is that loop; it moves to `executors/base.py`, which core may
      import, and `catalog` calls it there.
    - `statuses()` marks a failure its cell has changed since (decision 17): a `failed`
      row whose `built_from` isn't the cell's fingerprint now gets the reason
      `changed since it failed`. The live snapshot's rows are `statuses()`'s, so it's
      marked there too. Such a row's `statement_sql` is null: the host cuts it from the
      cell's SQL as it is now, where the statement's number may point elsewhere.
    - Text `status` prints the reason, else the error's first line, as live status does
      (`cli.py:407`).
    - `RenderedCell.to_dict()` (core) holds the cell's own fields: `cell`, `line`,
      `line_end`, `engine`, `sink`, `context`, `depends_on`. `tags` and the file's
      `autorun` are directives. `autorun` is a plugin's answer (`should_rerun`, which the
      live host asks, `host.py:204`), not `config.autorun`, and core can't ask it. So
      `watcher.cell_dict(project, name)`, beside `should_rerun`, adds both. Offline
      `status --json` cells are that plus the status and the last result: the live cell's
      keys, with `autorun.session` and `autorun.global` null. `LiveHost.snapshot` builds
      from the same dict.

16. **`--json` on `list`, `compile` and `show`** (the first draft's step 3).
    `{"file", "cells": [...]}` of step 15's `cell_dict`, plus `context_refs` and
    `external_refs`, plus `"sql"` in `compile` and `show` (one cell). Text `list` ends
    each row with the cell's lines.

### New commands

17. **`check [FILE] [--json]`** (the first draft's step 2), with `--set` and `--plugins`
    as `run` takes them.
    - `lsp/analysis.py` imports `lsp/schema.py`, which imports polars and `catalog`
      (probed: importing the analyzer with polars blocked fails). To keep `check` off
      polars (decision 13), move the offline half to a new `lsp/diagnostics.py`:
      `Diagnostic` (gains `cell`), `_line_diag`, `_syntax_diagnostics`, `_cell_vars`,
      `_dialect` and a `diagnostics(text, root, registries, gsql_bin, overrides=None)`
      function. `Analyzer.diagnostics` delegates. None of them touches `lsp/schema.py`.
    - `_dialect` and `_cell_vars` move under public names (`dialect_of`, `cell_vars`).
      The analyzer's completions still call both, and `tests/test_layering.py` fails a
      module that imports another's underscored names. `analysis.py` keeps exporting
      `Diagnostic`: `lsp/server.py` and `tests/test_lsp.py:863` import it from there.
    - `check` passes `gather_overrides` (`--set` and `QUICKSQL_*`), as every command that
      compiles does. Without them it would check another config than `run` runs: other
      `vars` in the masked SQL, or a compile error `run` wouldn't meet.
    - A compile error and a SQL error are both `error` diagnostics. Their `source` tells
      them apart (`quicksql`, against `sqlglot` or `googlesql`), and that's how `check`
      picks exit 3 over 1.
    - A cell with `{%` control flow, or on an engine that declares no dialect, is never
      parsed (`_syntax_diagnostics` returns nothing for it). `check` names those cells:
      `"unchecked": [{"cell", "why"}]`, and `ok: 2 cells, 1 not checked (by_month: Jinja
      control flow)`. Without it, `ok` would cover SQL nobody read.
    - A `Diagnostic`'s rows count from 0, as LSP does; `check` prints lines from 1.
    - Text: `nb.qsql:8: error: [broken] …` per problem, `ok: 3 cells` when clean. JSON:
      `{"file", "status", "summary": {"errors", "warnings"}, "diagnostics": [...]}`.
      Exit 0 clean (warnings too), 1 SQL errors, 3 compile errors.
    - Its help says what it can't see: it parses, it doesn't bind, so a wrong column
      name still needs a run.
    - `tests/test_demos.py` runs it with polars and textual blocked.

18. **`query SQL [FILE]`**, routed as `head` is.
    - `conduit.query_landed(conn, sql, views, rows)`: the one-SELECT check, the TEMP
      views and the cut, lifted from `LiveHost.query`. It raises its own `QueryRefused`
      and `QueryFailed` (core can't import `live`); the host maps them to its codes.
    - With a TUI open it goes through the session. Without one, a fresh connection with a
      view per output `read_landed` can read. When a query names a cell that got no view,
      the error says which cells have none offline, and why.
    - `"source"` is `live` or `offline`, as `head`'s. `live query` stays, on the same code.

19. **Waits that end.**
    - `live wait` and `live run --wait` default to 110 s. On `timeout` the CLI reads a
      snapshot and adds `"run"` to the error document; the text names the running cell
      and the command that waits again. For `live wait` that's itself. For
      `live run --wait` it's `quicksql live wait nb.qsql --run 7 --json` (decision 16),
      never itself: a second `live run` answers `busy`, whose message offers `--cancel`,
      and an agent that takes the offer restarts a run longer than 110 s every time.
    - `live wait --run ID`: `run.wait` for that run, with `live run --wait`'s document,
      text and exit codes. Only the CLI changes: the protocol has had the method.
    - On `busy`, `live run` does as on `timeout`: `"run"` from a snapshot, and the wait
      command ahead of `--cancel` in the text. The CLI adds both, so they hold against a
      TUI that predates this.
    - `protocol.EVENT_KINDS`: one tuple for the host's and the runner's kinds. The help
      is built from it, and a test holds `references/live.md`'s table to it. A kind
      outside it in `--kinds` or `--until` gets a warning on stderr naming the known
      ones, and the command carries on: `RunContext.emit` takes any string
      (`models.py:251`), so a plugin can emit kinds of its own.
    - `live events --until KIND[,KIND]` follows, and exits 0 after the first event of
      that kind, one that `--since` replays included. Its kinds join the subscription's:
      the session filters by `--kinds` before the CLI sees anything, so
      `--kinds cell_finished --until run_ended` would otherwise never end. It waits
      `--timeout` seconds (110 by default, as the waits do) and exits 4 past it: without
      one, an event that never comes is the hang again.

### The skill

20. **`quicksql skill [TOPIC]`** prints a reference (`live`, `json`, `directives`,
    `notebook-format`, `engines-and-sinks`, `usage`) or an example by name; `--list` names
    them. `--install --target agents|claude`. SKILL.md's "they sit next to this file"
    becomes `quicksql skill live`.

21. **The skill and docs, read through.** SKILL.md's offline loop becomes check → run →
    read with `check` and `query`; its "is a TUI open?" section loses what `query` and the
    refusal's hint now cover. README's agent section and CLAUDE.md's Commands follow.

22. **`catalog [--json]` without a TUI** (the first draft's step 6, unchanged): routed as
    `head` and `query` are, `live catalog`'s paging and `--match`. It reaches remote
    engines, so it runs only when asked, and Phase 7's directory trust applies.

## Out of scope

- An MCP server, and a standalone daemon. The CLI plus a skill covers the loop; live use
  alongside an open TUI goes through the TUI's own socket (`docs/live-session-plan.md`).
- Writing cells through the CLI. Agents edit the file directly; it's plain SQL.
- Result caching (as in the core plan).
- Notebook-first argument order, or a global `--file` (decision 2).
- `check --bind`: binding a DuckDB cell against its landed upstreams' views to catch a
  wrong column without a run. It would sit on step 18's views; worth a look once that
  lands.
- `explain --json`. It serves plugin authors, not the loop.
- Tracebacks through a live session (decision 4).

## Risks

- **Text output changes for people and scripts:** the bare command, errors on stderr,
  fewer progress lines off a terminal. Each is a decision above, and each lands alone, so
  one can be dropped.
- **A cut value read as the whole value.** The cut is marked twice (decision 5). The text
  tables have cut at 40 characters all along.
- **An old TUI with a new CLI.** Two steps change what the socket carries: 13 adds a key,
  and 14 drops `traceback` from every result, which the CLI also drops from an older
  TUI's. The rest of 14, and 12's and 19's additions, are made on the client side for
  that reason: `live wait --run` rides on `run.wait`, which the protocol has had.
- **The state file holds engine messages.** It becomes 0600, and the stored error is
  capped at 2,000 characters.
- **The swap is a guess.** For a cell it can't be wrong (`CELL_NAME`). For `live query`
  it needs SQL that is also the path of an existing file, with a second argument that
  isn't one. With FILE left out there's no swap, only a refusal, and it costs one case: a
  toast whose whole text is an existing file's path needs FILE named.
- **A failure marked `changed` that would fail again.** The mark says the error is of
  another text or other data, not that the cell is fixed. The row stays `failed`
  (decision 17).
- **The `lsp/diagnostics.py` split moves code the LSP tests cover.** It moves whole
  functions, two of them under public names; `tests/test_lsp.py` stays green without
  edits, or the split is wrong.
- **A plugin failure changes its exit code,** 1 to 3 (decision 14). A script that
  branches on 1 for it would notice; it's the one place this plan changes what the
  first draft's step 1 shipped.
- **Typer's floor is 0.12; the probes ran on 0.26.8** (click 8.5.0): plain help and
  errors, `-h`, the epilog, a `short_help` kept whole, separate stdout and stderr in
  the test runner. An older Typer may still box a usage error. Raise the floor in
  step 9 if a test on the oldest allowed version shows it. Typer 0.26 carries its own
  copy of click (`typer._click`) and imports none, so the installed click's version
  describes only an older Typer, which uses it. Nothing here may compare against
  `click`'s own classes (step 11's parameter source).

## Verification

- Each step: `uv run pytest` under the memory cap, and `uv run lint-imports`. Every
  `--json` test parses `result.stdout` with `json.loads`; every text-error test asserts
  stdout is empty.
- Steps 13, 14, 15 and 19 touch the live session: the `/verify` skill, against a TUI in
  tmux. For 19, a run longer than its `--timeout`: the timeout names
  `live wait --run`, and that command returns the run's document.
- After step 20: rerun the pass that produced the table above, row by row, and record
  the outcome here.
- After step 21: a fresh Claude Code session given only `quicksql --help` finds the
  skill, writes a two-cell notebook, checks it, runs it and queries a result, without
  opening the README.

## Changes from the first draft (hands-on pass, 2026-11-11)

- Steps 2, 3 and 6 are renumbered 17, 16 and 22 and placed in build order. Steps 1, 4, 5
  and 7 are done and dropped from the list.
- `check` (17) now names the split that keeps it off polars (decision 13): the analyzer
  imports it today.
- Steps 8–15 and 18–21 are new: the pass's findings.
- The conventions gain the stderr rule, the one error shape, the truncation flags and
  the forgiving arguments. The exit-code line gains 4 and 5, which the live session
  added.
- `head --csv` (the first draft's step 5) was never built and isn't carried over:
  `--json` covers an agent, and nobody asked for it.

## Changes from the review (2026-11-12)

The revision was read against the code, and its Typer settings probed on a scratch app.

Wrong, and fixed:

- **Decision 7 had no step.** Dropping `cell_step` lines off a terminal is now in
  step 14, with its row in the table.
- **Step 14 contradicted itself.** It cut values in `Preview.to_dict`, which the live
  host also calls, so the TUI would have cut them before the socket and `--max-chars 0`
  couldn't bring them back. The cut is now one CLI function.
- **Step 19 would have refused a plugin's event kinds.** Kinds are open
  (`RunContext.emit(kind: str)`), so an unknown one warns. `--until` gained a default
  timeout: without one it could hang as `--follow` does.
- **Step 10 hid bugs.** Catching every exception in `_project` made a quicksql bug one
  line on every command. The plugin loader wraps its own failures instead (decision 14),
  at the cost of an exit code that changes.
- **Step 15 put engine messages on disk unredacted.** `Executor.redact` exists for a DSN
  echoed in an error; a failure's message now passes it where the result is made.
- **Step 17's `ok` covered cells nobody parsed:** those with Jinja control flow, or on
  an engine without a dialect. `check` names them.

Smaller:

- `quicksql live` alone exits 2 today; step 8 makes it 0.
- `usage --table` no longer splits on commas (step 11).
- `"values_cut"` names the columns, not just true.
- The rerun of the pass moves after step 20, whose row it includes.
- Steps 11, 14 and 15 name the tests they change and the session field they add.

Checked, and unchanged: `conduit.preview`'s extra row reaches every caller (the runner,
`read_landed`, the TUI's previews); offline `head` reads from another directory, so
`query`'s views will; `cell.context` is already redacted, so it's safe in `list --json`;
the live tests assert an unknown cell's code, not its text; the client's socket timeout
follows the request's.

## Changes from the second review (2026-11-12)

The revision was read against the code once more, asking of each step what it does to the
command next to it. Two things were run: `live wait --json` on a missing notebook, and
Typer's report of an argument left at its default.

Wrong, and fixed:

- **A timed-out `live run --wait` had no way back.** Its only retry was itself, which
  answers `busy` and offers `--cancel`. `live wait --run ID` waits for the run
  (decision 16), and the timeout's and `busy`'s texts name it (step 19).
- **`status` would have shown an old error against new SQL.** `failed` survives an edit,
  and nothing recorded what a failed run was built from. The state file keeps that
  fingerprint, and the row says `changed since it failed` (decision 17, step 15).
- **The one failure shape had gaps.** An unknown `--select` came out `no_project`.
  `live wait` on a missing notebook is a usage error with nothing on stdout. `live wait`
  and `live run --wait` exit 3 and 5 without an `error`. A locked database was
  `internal`, the protocol's word for a bug. Decisions 3, 15 and 18; step 12.
- **The value cut left arrays whole.** It cut string leaves, and a preview keeps lists
  and structs nested (`models._jsonable`). A value is measured as its JSON text
  (decision 5, step 14).
- **The free-text swap sent the default's name.** With FILE left out,
  `live notify nb.qsql` became a toast reading `base.qsql`. It's a usage error (step 11).
- **Step 17's split would have failed the layering test,** by importing `_dialect` and
  `_cell_vars` across modules. And `check` compiled without `--set` or `QUICKSQL_*`.
- **Redaction sat in `_failed`,** which a result built by a plugin never passes, and it
  left the traceback's copy of the message alone. It moves to where the run records the
  result (decision 8, step 15).
- **`RenderedCell.to_dict()` promised the file's `autorun`,** a plugin's answer that core
  can't ask. `watcher.cell_dict` adds it, and `tags` (step 15).

Smaller:

- The Risks said only step 13 touches the protocol. Step 14 drops `traceback` from live
  results, and the CLI now drops an older TUI's too.
- Step 10's list gains `lsp`, and says what stays on stdout.
- Decision 11 read both ways. It names its default and its alternative.
- `--until`'s kinds join the subscription that `--kinds` filters (step 19).
- A wrapped plugin failure keeps its type and its line (decision 14).
- The 2,000-character cap is in step 15, where it was only in the Risks.
- Step 12's test list is one that the later commands join.
- The Typer risk says that 0.26 uses its own copy of click, not the installed one.

Checked, and unchanged: `live wait` does end when a run of the same text does
(`wait_outcome`, `run_ended`), which is why decision 16 has an alternative; `lsp/mask.py`
and `lsp/googlesql.py` import only the standard library and `lsp/__init__.py` imports
nothing, so the split keeps `check` off polars; `tests/test_lsp.py` imports no private
name of the analyzer; `watcher.py` imports neither polars nor Textual, so offline
`status` can call it; the tests and lines the steps cite are where they say.
