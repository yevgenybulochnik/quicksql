# Plan: the schema catalog for agents, fast on big warehouses

**Status:** Phases 0–3 done (2026-11-02, branch `feat/live-catalog`), so issue #11
(`quicksql live catalog`) is implemented and the remote catalog persists across sessions.
The stress harness and baseline are in `stress/catalog/`, the catalog core is in
`catalog.py`, the disk store in `catalog_store.py`, and the live side is in
`live/host.py`, `live/server.py` and the CLI. Every stress scenario meets its budget; no
xfail is left. Phases 4–5 (warming, an offline `quicksql catalog`) are a follow-up issue.

Phase 3 as built, where it differs from the plan below:
- **One file per level, not two.** `$QSQL_HOME/catalog/<sha256(key)>.level` holds a JSON
  header line (format version, when the load began, scope, title) and then the frame as
  zstd Arrow IPC, so one rename publishes both. The directories are 0700, the files 0600,
  writes go through a temp file and `os.replace`, and the least recently read go past a
  256 MiB cap (one level bigger than the cap is kept alone). An unreadable or
  other-version file is a miss and is deleted. Any disk failure is a miss, never an error.
- **A scoped clear makes its levels misses, not stale.** A run may have changed them, so
  serving the old listing right after the run, even marked stale, would show the agent
  what the run replaced. A scope gets a marker, `scopes/<sha256(scope)>`, when a level is
  first stored under it (made whole and linked into place, so it never overwrites a
  concurrent clear). A clear writes its time there, and a level saved before it is a miss
  in this session and the next. A scope nothing was stored under, such as a cell's
  output, writes nothing, so saves leave no files. The reader's scope decides, so the LSP
  reading a level the TUI wrote checks the cell's context.
- **A clear of everything** (`invalidate()`) passes over what the disk had, for that cache
  only. A refetch (`invalidate(node)`, ctrl+r, `--refresh`) deletes the level's file.
- **Ages apply to persisted levels only,** in memory too: a memory hit past
  `revalidate_after` refreshes behind as a disk hit does. DuckDB, SQLite and output levels
  behave as in Phase 2, since a background reload could reach a session a notebook switch
  is closing.
- **Defaults:** `revalidate_after` 15 minutes, `max_age` 7 days, 4 refresh threads per
  cache. The threads are daemons that end when idle, so quitting never waits on one.
  `QSQL_CATALOG_PERSIST=0` (also `false`, `no`, `off`) is in; the other settings stay in
  Phase 4.2.
- **Refreshes behind** load like an owner of the level, so a second asker shares the load
  and a clear that overtakes it means it isn't kept. A failed refresh keeps the level it
  had. `on_refresh(node)` fires when the frame changed, after its waiters are released;
  the host turns it into `catalog_invalidated` with reason `refreshed` through the new
  `Driver.post` (`AppDriver`: `call_from_thread`), dropped when the cache belongs to an
  earlier notebook.
- **The API:** `CatalogCache.read(node)` returns a `Hit` (`frame`, `cached`, `age`,
  `stale`); `load` still returns `(frame, cached)`. `Resolved` gains `age` and `stale`, the
  socket response `age` (whole seconds) and `stale`, and the CLI's header says
  `cached 2 h ago, refreshing`.
- **What persists:** `Executor.catalog_persists` (BigQuery, Postgres) and
  `CatalogNode.persist`, stamped down each subtree by `catalog.stamp` (which replaced
  `_scoped`). Outputs never persist.
- **BigQuery keys name their endpoint** (`bigquery:p@http://localhost:9052/ds.t`), so two
  emulators holding a project of the same name never share a level on disk. Titles are
  unchanged.
- **The LSP:** `SchemaCache` loads table leaves through a `CatalogCache` over the default
  store, scoped to the cell's context, so a BigQuery run's clear reaches them. Its own
  per-process memo still sits on top, so within one LSP process a table's columns don't
  refresh. FROM/JOIN name completion (`catalog_relations`) doesn't go through the store.
- **Stress:** S8, S9, S10 and S11 meet their budgets, and so does S12, since persistence
  alone loads each named table once. Phase 4's warming moves those loads to the open; it
  doesn't change how many there are. S8 now reopens through real `LiveHost`s and also
  reopens with every level due: the answer costs 0 round trips, and the refreshes behind
  it cost BigQuery 3, Postgres 12. The meter counts a refresh thread's trips apart
  (`behind.`).
  - S8, reopen: BigQuery 3 → 0 round trips, Postgres 12 → 0.
  - S9, the LSP after a restart: BigQuery 1 → 0, Postgres 4 → 0 (212 → 1.2 ms).
  - S12, second open: BigQuery 30 → 0, Postgres 120 → 0.
  - S10a also scans every file under the store for the password.
- **Verified end to end:**
  - A TUI on the stress emulator loaded a BigQuery level and quit. A new TUI's first
    `live catalog` answered it `cached` and 32 s old, and its Catalog tab drilled the same
    level.
  - An LSP process completed the 2050-column wide table (413 ms). A second process with
    BigQuery unreachable completed all 2050 columns from disk (17 ms). With an empty
    `QSQL_HOME` it got none.
- **Gaps:**
  - The Catalog tab doesn't repaint on a `refreshed` event; ctrl+r or drilling again
    shows the refreshed level.
  - A clear in one process can come before another process's first store under that
    scope, from a load that began before the clear. That level then reads as fresh until
    it revalidates. Within one process the load's own clear flag covers this.

Phase 2 as built, where it differs from the plan below:
- **The host owns the catalog.** `LiveHost.catalog_cache` (a fresh one per notebook) and
  `catalog_root(project)` (one per compiled project, built with `Driver.connect`, which
  is new) are shared by the Catalog tab and `quicksql live catalog`.
- **A save clears the top level and the outputs,** not just the top level, because a
  save can move where an output lands.
- **A run clears the contexts on every engine its cells ran on or landed in,** a sink type
  that names an engine counting too (a sqlite cell with a duckdb sink clears duckdb). It
  also clears the outputs of the cells that ran; a skipped cell clears nothing.
- **The event:** `catalog_invalidated` carries `reason`, `run_id`, `top_level` and
  `scopes`.
- **The socket method.** `catalog` takes `path`, `offset`, `rows` (200), `match`,
  `columns`, `top_level`, `refresh` and `timeout` (60 s). It answers
  `{path, title, leaf, columns, rows, names, total, offset, next_offset, cached}`:
  `names` is new (the next segment for each row), and there's no `key`.
- **Errors:** `unknown_path` (exit 3) names the segment and the closest rows;
  `load_failed` (exit 1) is redacted.
- **The CLI.** `quicksql live catalog FILE [PATH...]` checks that the TUI's `hello` lists
  `catalog` and says to restart it if not. Text output shows each row's name whole and
  once.
- **Stress:** S4–S7 meet their budgets.
  - S4, twenty saves: BigQuery 60 → 0 round trips, Postgres 240 → 0.
  - S5: after a DuckDB run 15 → 0; after a BigQuery run 15 → 3.
  - S7: the largest response for the 9950-column leaf 10 MB → 1.07 MB.
- **Verified end to end against the stress stack:** the TUI hosted the stress notebook
  while the CLI read its catalog. It checked exact paths, paging, filters, a mistyped
  segment, Postgres through its redacted name (the password never shown), a level the
  TUI loaded read back cached by the CLI, and a save's event.

Phase 1 as built, where it differs from the plan below:
- **Walking by name.** `catalog.resolve(root, path, cache)` returns a `Resolved` (node,
  frame, cached, path). A bad path raises `CatalogPathError`, which carries the depth,
  the segment, a reason (`unknown` or `leaf`), up to 10 close names and the level's total.
- **Guesses are checked when the end fails or comes back empty.** A level that declares
  `name_columns` and isn't cached is never listed on the way: the walk guesses the next
  level from the segment. Postgres and SQLite answer an unknown table with no columns
  rather than an error, so an empty end also triggers the check.
- **Pages name their rows.** `catalog.page` returns each row's `names` (the segment that
  goes beneath it), and `catalog.narrow` filters before paging.
- **The cache:** a 64 MiB cap by default, plus `peek`, `invalidate(node | scopes=…)` and
  `size_bytes`. The TUI's Catalog tab gets all of it unchanged; verified in tmux.
- **Redaction.** `Executor.redact(text)` is the hook, and `catalog.redact` applies every
  engine's. Postgres hides `password=…` and URIs' `user:password@` in free text.
  `_redacted` now also hides a password written without `=`: the context-key bug S10b
  found.
- **BigQuery:** `page_size = 1000` on its listings, so on the fake browsing went from 243
  calls to 15, whatever the API's default page.
- **Postgres connection reuse: skipped.** With walking by name, an exact path is one load
  (a connect and a query, 4 round trips). Browsing still opens a connection per level;
  Phase 2's long-lived root node is where reuse would fit, if S1 calls for it.
- **Stress:** S2, S6, S10b and S11 meet their budgets.
  - S2, round trips: BigQuery 3 → 1, the fake at 50 per page 43 → 1, Postgres 12 → 4.
  - S6: 2 loads → 1.
  - S11: 138 → 63.8 MB held.
- **Arrow keys don't move the Catalog tab's cursor (pre-existing).** Its DataTable can't
  take focus, and the app routes `j`/`k`.

Phase 0 as built, where it differs from the plan below:
- **It lives in `stress/catalog/`,** next to the existing compile and TUI benchmarks in
  `stress/`.
- **The budgets are already assertions.** `tests/test_stress_catalog.py` asserts each
  scenario's budget, and a budget today's code misses is `xfail(strict=True)` naming the
  phase that meets it. A phase is done when its marks come off: a strict xfail that starts
  passing fails the run.
- **Results.** `baseline.json` (committed) holds Phase 0's results. Runs merge into
  `report.json` (ignored), and `compare.py` diffs the two, exiting 1 when round trips go
  up.
- **The emulator's ceiling** (`stress/catalog/README.md`), under the 3 GB cap:
  - about 150 datasets: each costs it ~14 MB, and at 200 the cap killed it;
  - 5000 tables in one dataset, though startup took 554 s;
  - 2000 columns per table, a limit of the SQLite it stores tables in.

  The default seed therefore uses 100 datasets. Larger sizes run on the fake: 2000
  datasets, 10,000 shards, and a 9950-column table with 1024-character descriptions.
- **Its YAML loader rejects column descriptions,** so the wide table is created through
  the REST API after startup.
- **Postgres.** It connects as a `stress_reader` role with a distinctive password, so the
  secrets check means something. The seed commits in batches, because dropping 3000
  tables in one transaction runs out of lock slots.
- **A BigQuery `@catalog` target can't name an endpoint,** so the stress notebook reaches
  the emulator through cells only.
- **Found, and in the baseline:**
  - A Postgres DSN without `=` after `password` (`password <secret>`) isn't redacted, so
    its password shows in the context name: the Catalog tab's top level, `explain` and
    the TUI log. S10b covers it; Phase 1's redaction fixes it.
  - A 9950-column leaf with 1024-character descriptions is 10 MB as one response, 2.4
    times `MAX_LINE`. A 1000-row page is 1.07 MB, so paging is what keeps responses within
    the limit, and the size guard stays a safety net.

Baseline (today's code, round trips):

| Scenario | BigQuery (emulator) | Postgres |
|---|---|---|
| S2: cold exact path to the wide table | 3 calls; the LSP's direct lookup takes 1 | 3 connects + 3 queries |
| S2 on the fake, 2002 datasets at 50 per page | 43 calls | — |
| S4: 20 saves, the wide table read after each | 60 calls; 9.6 s here, ~34 s at 400 ms per call | 60 connects + 60 queries |
| S6: two askers, one cold table | 2 loads | 2 loads |
| S11: 400 wide tables (fake) | 138 MB held, no cap | — |
| S12: 30 named tables, opened twice | 30 calls each time | 30 connects each time |

## Context

Issue #11 asks for the TUI's Catalog tab over the live socket, so an agent can read the
same schema tree the person sees. Two requirements came out of the discussion:

- **Large BigQuery projects:** hundreds of datasets per project, sharded datasets with
  thousands of tables, and tables with 1000+ columns.
- **Real timings from GCP aren't available.** Performance is designed and checked against
  what runs locally: the goccy BigQuery emulator and Postgres. Budgets are counted in
  round trips, because a round-trip count holds on real BigQuery whatever its latency is.

### Where the issue's sketch doesn't fit the code

1. **No common `name` column.** Each level names its rows differently: root `name`;
   Postgres `schema`, then `table`; SQLite `table`; BigQuery `dataset`, then `table`; DuckDB
   `schema` and `table` together on one level (`main.orders`).
2. **The proposed `key` field would leak the Postgres password.** Postgres cache keys
   contain the raw DSN (`executors/postgres_exec.py:158,165,168`). Responses return the
   resolved `path` instead.
3. **An oversized response breaks instead of failing.** Nothing checks outgoing size, and
   the client reads `readline(MAX_LINE)` (`live/client.py:146`), so a response over 4 MiB is
   cut off and fails to parse.
4. **Redaction only works on a DSN.** `postgres_exec._redacted` is private and expects a
   DSN. Error text can echo DSN fragments that contain no `password=`.

### Where large BigQuery projects would hurt

1. **Every save and every run clears the whole cache** (`tui/app.py:879`, `:913`). An
   agent editing, waiting and browsing in a loop would always start cold.
2. **Walking a path lists every level on the way.** Cold, `bigquery:proj sales orders`
   costs `list_datasets` + `list_tables(sales)` + `get_table`. Each level's `child()` needs
   only the row's name; `catalog_table` (LSP) already relies on that.
3. **A wide table floods the agent's context.** 2000+ field-path rows with descriptions
   cost tens of thousands of tokens, when the agent needs a handful.
4. **A new BigQuery client per tree.** `context_node` keeps one client per tree it
   builds, and each socket request would build a new tree, so every cold request pays for
   a fresh client and token fetch.
5. **Separate in-memory caches.** The TUI's `CatalogCache`, the LSP's `SchemaCache`
   (`lsp/schema.py:42`, in the editor's process) and soon the host's. Every new session
   starts cold, and the LSP has it worst: the first completion on a wide table waits on
   `get_table`.

### What the local stack can do (probed 2026-11-01)

goccy `bigquery-emulator:latest` on :9050, `google-cloud-bigquery` 3.42.2, median of 5:

| Operation | Size | Time | Pages |
|---|---|---|---|
| `list_datasets` | 150 datasets | 29 ms | 1, with `page_size=50` too |
| `list_tables` | 400 tables in one dataset | 101 ms | 1, with `page_size=100` too |
| `get_table`, narrow | 1 column | 103 ms | — |
| `get_table`, wide | 1550 columns + 50 REPEATED RECORDs × 10 = 2050 field paths, 200-character descriptions | 157 ms | — |
| seeding through the API | per table | ~72 ms (400 tables: 29 s) | — |

- **The emulator ignores `page_size`,** so paging can't be tested against it.
- **Its latency is local.** It says nothing about GCP and must not be extrapolated.
- **It breaks at scale.** Deleting the 400-table dataset panicked its SQL engine
  (`googlesqlite: slice bounds out of range`), and the container exited about two minutes
  later. A restart wipes its in-memory state, which is the safe way to clean up.
- **Postgres:** the `quicksql` stress database is empty. Postgres allows 1600 columns per
  table, so 1000+ column tables can be stress-tested there with real catalog queries. Its
  catalog opens a new connection for every load (`postgres_exec.context_node.query`).

So the stress setup is:
- a **fake BigQuery client**, for paging and exact call counts;
- a **latency-injecting wrapper**, to project wall time;
- the **emulator and Postgres**, for real end-to-end behavior;
- a **separate emulator instance**, so stress runs never crash the one pytest uses.

## Decisions (defaults, to confirm)

1. **A load already in flight is shared.** A second request for the same level waits for
   the first load instead of answering `busy`. The agent never has to retry, and the TUI
   benefits too.
2. **Responses carry `path`, not `key`.** Keys can contain a DSN.
3. **New error codes:** `unknown_path` (exit 3) for an unknown segment or a step past a
   leaf, and `load_failed` (exit 1) for a load that raised, with its message redacted.
4. **The catalog persists to disk by default, for remote engines only** (Phase 3), under
   `$QSQL_HOME/catalog/`: keys hashed, files 0600, `QSQL_CATALOG_PERSIST=0` to turn it
   off. Column descriptions end up on disk; say so in the docs.
5. **No user-level config file.** Cache settings are defaults plus `QSQL_CATALOG_*`
   environment variables. They are deliberately not `QUICKSQL_*`, which `overrides.py`
   sweeps into config, the same reason `QSQL_EXECUTE_QUERY` exists. Which tables to warm
   comes from the notebook (Phase 4), not a list in `~/.quicksql`.
6. **Stress runs on a separate emulator**, a compose `stress` profile on :9052, so a
   crash there never takes down the :9050 instance that `pytest -m bigquery` uses.

## Phase 0: the stress harness and a baseline (before any feature code)

0. **Stress stack (S).**
   - In `docker-compose.yml`: a `stress` profile with `bigquery-emulator-stress` on :9052,
     seeded at startup from a generated YAML (`--data-from-yaml`, as kitchen-sink does).
   - Postgres stress data goes into the existing `quicksql` database.
   - Cleanup is always a restart, never a delete.
1. **Generators (S)** in `stress/catalog/`, deterministic like `gen_wide_claims.py`:
   - `gen_bq_seed.py`, a YAML seed. Defaults are the sizes probed above: 150 datasets, a
     sharded dataset with 400 `events_YYYYMMDD` tables, and the 2050-field-path wide table.
     `--scale N` pushes past them.
   - `gen_pg_seed.py`, SQL:
     - 300 schemas of 5 tables each;
     - one schema with 3000 tables;
     - a 1500-column table, plus a 1590-column one near Postgres's limit;
     - column comments on the wide tables.
   - `catalog.qsql`, the stress notebook:
     - cells on duckdb, postgres and bigquery (:9052);
     - the Postgres DSN from an environment variable that carries a password;
     - `@catalog` targets.
2. **The meter (S)**, test-side only, with no production change. A fixture wraps
   `BigQueryExecutor.make_client` and `PostgresExecutor.make_connection` to:
   - count API calls and connections;
   - optionally sleep `QSQL_STRESS_LATENCY_MS` per call;
   - record response bytes, rows and peak RSS.

   A fake BigQuery client with real page tokens covers paging, which the emulator lacks.
3. **Find the emulator's ceiling (S).**
   - Raise `--scale` until startup seeding, listing or `get_table` fails.
   - Record the largest sizes that work in `stress/README.md`. Later phases test inside
     that ceiling and use the fake client beyond it.
   - Measure the YAML seed's startup time against API seeding (~72 ms per table).
4. **Baseline (S).** `tests/test_stress_catalog.py` (marker `stress`, deselected by
   default like `network`/`bigquery`/`postgres`) runs scenarios S1–S12 below against
   today's code paths, `project_root_node` + `CatalogCache` (the TUI's path) and the LSP's
   `SchemaCache`. It writes `stress/catalog/report.json`. Every later phase compares against it.

## Phase 1: the catalog core (no socket)

1. **Walking by name (M)** in `models.py` and `catalog.py`:
   - `CatalogNode.name_columns` and `row_name(row)`: by default `name`, else the first
     column. DuckDB's context node sets `("schema", "table")`.
   - `catalog.walk(root, path, cache)` builds each child directly from its path segment.
     It checks the segment against the parent's list only when that list is already
     cached. If the final load fails, it loads the parent then and raises with the
     segment and its candidates. `catalog.py` stays independent of `live`.
   - Tests first: DuckDB, SQLite and output leaves; an unknown segment; a step past a
     leaf; a cold exact path loads only its leaf (fake client: 1 `get_table`).
2. **Pages and filters (S):**
   - `catalog.page(frame, offset, rows)`;
   - `match` (case-insensitive substring on the row name, or on `field_path` at a leaf);
   - `columns` (keep a subset, e.g. no `description`);
   - `top_level` (field paths without a dot).

   All applied before paging; `total` counts the matches.
3. **`CatalogCache` (M):**
   - a load already in flight is shared (lock + event per key; an error reaches every
     waiter);
   - a generation counter, so a load that straddles a clear answers its caller but isn't
     stored;
   - scope tags, so entries can be cleared per top-level row: `project_root_node`'s
     `child` stamps each subtree with its row name;
   - an LRU cap on `estimated_size()`.
4. **Redaction (S):**
   - `Executor.redact(text)`, doing nothing by default. Postgres implements it with
     free-text versions of its patterns (`password=…`, `://user:pw@`), and `_redacted`
     reuses them.
   - `catalog.redact(text, registries)` applies every registered executor's hook.
5. **Engine round trips (S):**
   - BigQuery: an explicit `page_size` on `list_datasets` and `list_tables`.
   - Postgres: one connection per tree instead of one per load, but only if Phase 0's
     connection counts show it matters.

**Stress gate:** S2 makes exactly 1 call; S4 and S5 hold in the cache's own tests.

## Phase 2: `quicksql live catalog` (issue #11)

1. **The host owns the cache (M):**
   - `LiveHost.catalog_cache` is reset per notebook, and the host keeps one root node per
     compiled project, so a BigQuery client lives across requests.
   - Scoped clearing:
     - `recompiled` clears the top level only;
     - `run_ended` clears the contexts the run's cells ran in, plus `@catalog` targets on
       their engines, plus the outputs of the cells that ran.

     Each clear emits `catalog_invalidated` (`reason`, `run_id`, `scopes`).
   - The TUI's `CatalogView` uses the host's cache, still picked up when each load starts
     so the per-notebook generation guard keeps working. Remove the clears in
     `run_landed` and `_on_recompiled`.
   - Tests first: a duckdb-only run keeps BigQuery entries; a save keeps everything below
     the top level; the TUI and the host share frames.
   - `/verify`.
2. **The socket method (M).** `catalog` takes `path`, `offset`, `rows`, `match`,
   `columns`, `top_level`, `refresh` and `timeout`.
   - On the loop: `host.catalog_scope()`, or `no_project`.
   - On a worker, inside `driver.reading()`: walk, load, filter, page, redact.
   - `asyncio.wait_for` returns `timeout` while the load carries on and fills the cache.
   - The response shrinks its row count until it fits `MAX_LINE` minus a margin.
   - Response: `{path, title, leaf, columns, rows, total, offset, next_offset, cached}`.
3. **CLI (S):** `quicksql live catalog [FILE] [PATH...]` with `--rows/-n` (default 200),
   `--offset`, `--match`, `--columns`, `--top-level`, `--refresh`, `--timeout` and
   `--json`. Add the new codes to `_EXIT_FOR`, and `catalog_invalidated` to the event
   kinds in `live events`' help. If the TUI's `hello` lacks `catalog`, say "restart the
   TUI".
4. **Docs (S):**
   - `live.md`: a "Reading the catalog" section.
   - `json.md`: the response document, the two error codes and the event.
   - `SKILL.md`: add the command (`test_skill.py` requires every `live` command to be
     named).
   - `CLAUDE.md`: the live-session paragraph.

**Stress gate:** S1–S7 and S10 within budget; `/verify` with the Catalog tab and
`live catalog` side by side.

## Phase 3: a persistent catalog

1. **Disk store (M):** `$QSQL_HOME/catalog/`, one Arrow IPC file per entry, named by the
   sha256 of its key, plus a small JSON metadata file (saved at, scope, engine).
   - The directory is 0700 and files are 0600.
   - Writes go to a temp file and are renamed into place, since the TUI and the LSP both
     write.
   - A size cap evicts the least recently used entries.
   - The autouse `QSQL_HOME` fixture keeps tests off the real directory.
2. **What persists (S):** executors declare it with `catalog_persists: ClassVar[bool]`:
   BigQuery and Postgres yes; DuckDB and SQLite no (local, fast); cell outputs never.
3. **Read-through, refresh behind (M):** memory, then disk, then a load.
   - A disk hit answers at once with `age` and `stale`. Past `revalidate_after`, it
     refreshes in the background and emits `catalog_invalidated` if anything changed.
   - Past `max_age` it counts as a miss.
   - A scoped clear (Phase 2) also marks that scope's disk entries stale.
4. **The LSP reads through it (S).** `SchemaCache` loads through the shared store, so
   column completion on a wide table is warm after a restart.

**Stress gate:** S8, S9, S10 (now also on disk files), S11.

## Phase 4: warming (follow-up issue)

1. **From the notebook (M).** After open and after each save, collect the tables each
   cell names literally in FROM/JOIN, parsed by sqlglot in its executor's dialect as the
   LSP does; tables named through Jinja are skipped. Resolve them with
   `Executor.catalog_table`, and load in the background those not fresh on disk.
   Concurrency is bounded (default 4), so opening the notebook isn't slowed.
2. **Settings (S):** `QSQL_CATALOG_MAX_AGE`, `QSQL_CATALOG_REVALIDATE_AFTER`,
   `QSQL_CATALOG_SIZE_MB`, `QSQL_CATALOG_PERSIST`, `QSQL_CATALOG_WARM`.
3. **A manual warm list (later, if wanted):** a `warm:` option on `@catalog` entries,
   only if 4.1 measurably misses tables people use.

**Stress gate:** S12; the TUI's open-to-first-paint time is unchanged with warming on.

## Phase 5: offline `quicksql catalog` (optional)

`quicksql catalog NOTEBOOK [PATH...] --json`. If a TUI has the notebook open (`probe`),
it goes through the socket. Otherwise it compiles and walks in-process, on top of the disk
store from Phase 3.

## Stress scenarios

Budgets are counted in round trips: BigQuery API calls and Postgres connections. Wall
time is reported with no added latency and with 50, 150 and 400 ms per call injected, as
a projection only.

| # | Scenario | Budget |
|---|---|---|
| S1 | Cold browse: root → BigQuery context → sharded dataset → wide table | 1 call per level (+ pages), nothing more |
| S2 | Cold exact path `bigquery:… ds wide` | 1 call (`get_table`) |
| S3 | S1 and S2 repeated, warm | 0 calls; well under 50 ms on the server |
| S4 | 20 saves of a duckdb cell, re-reading the wide table each time | 0 BigQuery calls after the first |
| S5 | A duckdb-only run, then a BigQuery run | 0 BigQuery calls, then that context's levels only |
| S6 | TUI drill and agent request on the same node at once | 1 load |
| S7 | Wide leaf: every page, `--match`, `--columns`, `--top-level` | every response under `MAX_LINE`; filters cut rows and bytes |
| S8 | Close and reopen the TUI, then request the wide table | 0 calls before the answer; revalidation in the background, ≤4 at once |
| S9 | LSP completion on the wide table after a restart | served from disk, 0 calls on the hot path |
| S10 | Postgres DSN with `password=hunter2`, including a connect that fails | `hunter2` never appears in raw socket bytes, logs or disk files |
| S11 | 50 wide tables loaded | cache memory under its cap; LRU evicts |
| S12 | Open a notebook naming 30 BigQuery tables | 30 `get_table` calls once; 0 on the next open while fresh |

Postgres runs S1–S7 and S10–S11 with its own shapes (300 schemas, a 3000-table schema,
1500- and 1590-column tables) against real `information_schema` queries.

## Out of scope

- Benchmarking real GCP: no access. The round-trip budgets are what transfers.
- A catalog built on `INFORMATION_SCHEMA` queries: those are billed per query, while the
  metadata API calls used today are not.
- A cache shared between machines or users.

## Risks

- **Emulator fidelity:** no paging, local latency, crashes at scale. Covered by the fake
  client, injected latency, the separate stress instance and the ceiling recorded in
  Phase 0. Real-GCP wall times stay unknown.
- **Stale schemas** from changes made outside quicksql: refresh in the background,
  `max_age`, `--refresh`, and `age`/`stale` in every response.
- **Secrets:** hashed keys on disk, redaction on every outgoing string, and S10 checking
  raw bytes and files.
- **Memory** on a 7.5 GiB box: LRU caps in memory and on disk. Stress runs under
  `systemd-run --user --scope -p MemoryMax=3G`.
- **Thread starvation:** requests waiting on a shared load block `asyncio.to_thread`
  workers. Warming has its own bounded pool.
- **BigQuery API quota:** metadata calls are free but rate-limited. Warming is bounded and
  skips entries that are still fresh.

## Verification

- Each step: `uv run pytest` under `systemd-run --user --scope -p MemoryMax=3G`, and
  `uv run lint-imports`.
- Each phase:
  - `docker compose --profile stress up -d --wait`;
  - `uv run --extra postgres --extra bigquery pytest -m stress`;
  - `uv run python stress/catalog/compare.py`: the run against the Phase 0 baseline.
- `/verify` after 2.1, 2.3 and 3.3.
