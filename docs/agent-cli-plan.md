# Plan: a CLI agents can drive

**Status:** proposed 2026-10-30. Step 1 is done (`53fcfc5..d1575d9`).

## Context

Coding agents (Claude Code and others) work on notebooks through the shell. They edit
the `.qsql` file, then need to check it, run it and read what it produced, and branch
on the outcome. Today every command prints aligned, coloured text. `run` mixes progress
lines into stdout, and a compile error and a cell failure both exit 1. Nothing reads a
cell's landed data back, and nothing checks SQL without running it. The LSP's checks
never reach the CLI. The TUI's schema browser has no CLI equivalent.

`docs/core-refactor-plan.md` keeps only `run --json` with exit codes (Phase 2) and
`quicksql status` (Phase 8) and points here for the rest. This plan adds no new
architecture: each command is a thin front-end over what the compiler, runner, LSP
analyzer and catalog already compute.

## The loop

An agent's loop is **check → run → read**: catch mistakes without touching data, run
the cells it changed plus their upstreams, then look at enough of the result to judge it.
Every step below serves that loop. Schema discovery and state come after it works.

## Conventions (every command)

- `--json` prints exactly one JSON document on stdout and nothing else: no progress, no
  colour. Streams (later) are NDJSON, one event per line. Warnings and logs go to stderr.
- Exit codes are shared: 0 ok, 1 a cell or SQL check failed, 2 usage error (click),
  3 the notebook can't be compiled or the selection is invalid.
- Messages are the engine's own (`Catalog Error: …`), as `RunResult.error` already
  carries them. Tracebacks appear only as a separate field.
- rc plugins and `--plugins` load exactly as for the text commands. Phase 7's trust will
  apply to them unchanged.
- Bounded output: data previews take `--rows N`, capped at the cached 100-row preview,
  so an agent can't flood its own context by accident.

## Steps (each one focused change, test-first)

1. **`run --json [--rows N]` and exit codes.** Done.
   `RunResult.to_dict()`; one document per run with per-cell status, rows, target,
   elapsed, error, and preview rows on request; compile errors as `errors` with cell and
   line; exit codes for every command.
2. **`check [--json] [FILE]`.** Compile plus the LSP's offline diagnostics:
   `lsp.analysis.Analyzer.diagnostics` already returns compile errors, repeated-directive
   warnings and per-cell SQL syntax errors with lines. It executes nothing and reaches no
   network: the LSP's live schema lookups stay out. Exit 0 clean, 1 SQL errors,
   3 compile errors. It's the cheapest step that saves an agent a failed run.
3. **`--json` on `list` and `compile`.** Per cell: name, engine, sink, context, line
   range, autorun, tags, `depends_on`, `context_refs`/`external_refs`, and rendered SQL
   (in `compile`). This lets an agent see the graph without parsing aligned text.
4. **A notebook skill for Claude Code** (`.claude/skills/quicksql-notebook/SKILL.md`).
   It covers the cell format, the directive vocabulary, the check → run → read loop using
   steps 1–3, and the rules that bite: cross-context `ref()` runs on duckdb, effect-only
   cells can't be `ref()`'d, and skipped cells mean an upstream failed. It's written
   after 1–3 so it documents stable JSON, not text that's about to change.
5. **`head <cell> [--rows N] [--json|--csv]`.** It reads a cell's *landed* output through
   its sink's `ref_expr` on a fresh conduit, without running anything. This covers outputs
   from earlier runs and every sink (parquet, duckdb, postgres), where an agent today
   would guess at `data/<cell>.parquet`. It reuses the runner's preview path, and the
   TUI's preview fallback should move onto it too (a Phase 1 item). While a TUI is open,
   a fresh conduit can't open the DuckDB files the TUI has attached (DuckDB's file lock),
   so `head` reads through the TUI's session then: `docs/live-session-plan.md` step 6,
   where this offline `head` lands as the fallback.
6. **`catalog [--json] [--engine E]`.** Contexts → schemas → tables → columns, from
   `catalog.project_root_node`, for writing SQL against real names. It reaches remote
   engines, so it only runs when asked, and Phase 7's directory trust applies.
7. **After core Phase 8:** `status [--json]` (ok / failed / skipped / stale per cell)
   landed with Phase 8. Superseded: `status --wait` and `watch --json` became
   `quicksql live wait` and `quicksql live events`, served by the open TUI over a
   socket (`docs/live-session-plan.md`).

## Out of scope

- An MCP server, and a standalone daemon. The CLI plus a skill covers the loop; live
  use alongside an open TUI goes through the TUI's own socket
  (`docs/live-session-plan.md`).
- Writing cells through the CLI. Agents edit the file directly; it's plain SQL.
- Result caching (as in the core plan).

## Verification

- Each step: `uv run pytest` under the memory cap. Every `--json` output test parses
  stdout with `json.loads` and asserts nothing else was printed there.
- After step 4: a fresh Claude Code session given only the skill writes a two-cell
  notebook, checks it, runs it and reads a result without opening the README.
