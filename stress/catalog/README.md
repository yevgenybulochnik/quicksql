# Catalog stress

What browsing a big warehouse costs: Phase 0 of `docs/catalog-plan.md`. The scenarios
(S1–S12 in the plan) live in `tests/test_stress_catalog.py`. Each one measures the TUI's
and the LSP's catalog paths against the backends here, records into `report.json`, and
asserts the plan's budget. A budget today's code misses is `xfail(strict=True)` naming
the phase that meets it.

**Budgets are round trips, not wall time.** We have no timings from real BigQuery, and a
round-trip count holds there whatever its latency. Wall time is recorded as measured
locally, then projected as `local + round_trips × latency` at 50, 150 and 400 ms. A
Postgres connect counts as 3 round trips (TCP, startup, auth) and a query as 1. The
projection leaves out real BigQuery's own server time, which is unknown.

## Run it

```sh
uv run python stress/catalog/stack.py up      # stress emulator on :9052 + Postgres stress schemas
systemd-run --user --scope -p MemoryMax=3G \
    uv run --extra postgres --extra bigquery pytest -m stress
uv run python stress/catalog/compare.py       # report.json against baseline.json
```

Each scenario skips when its backend is missing or holds another seed. The fake-BigQuery
scenarios need neither backend.

## The backends

| | Where | Shape |
|---|---|---|
| BigQuery | the `bigquery-emulator-stress` compose service (profile `stress`, :9052, project `quicksql-stress`, 3 GB memory cap) | 100 datasets × 3 tables (as many as its memory comfortably holds; see the ceiling); `sharded`: 400 `events_YYYYMMDD` tables; `wide.wide_table`: 1500 described STRING columns + 50 REPEATED RECORDs × 10 fields = 2050 field paths |
| Postgres | the compose server's `quicksql` database, role `stress_reader` | 300 schemas × 5 tables; `stress_many`: 3000 tables; `stress_wide.wide_1500` (commented columns) and `wide_1590` |
| Fake BigQuery | in the test (`FakeBigQuery`) | sizes past the emulator's ceiling (2000 datasets, 10,000 shards, a 9950-column table with 1024-character descriptions); pages at 50 or 1000 per request |

- `gen_bq_seed.py` and `gen_pg_seed.py` generate the seeds deterministically, and
  `stack.py` loads them.
- `catalog.qsql` is the notebook every scenario compiles. Open it in the TUI to browse
  the same trees by hand (its header says how).
- `stress_reader`'s password (`hunter2-stress-secret`) is distinctive on purpose. S10
  checks it never shows, and the database's own password (`quicksql`) also appears in user
  and database names, so it can't serve that check.

### Emulator quirks

Found by probing goccy `bigquery-emulator:latest` on 2026-11-01:

- **It ignores `page_size`** on `list_datasets` and `list_tables`; every listing comes
  back as one page. Paging is measured on the fake instead.
- **Its YAML loader rejects column `description`.** `mode` and nested `fields` load. So
  `stack.py` creates the wide table through the REST API after startup.
- **Deleting a dataset that holds hundreds of tables panics it**
  (`googlesqlite: slice bounds out of range`), and the container exits minutes later.
  Never clean up by deleting: `stack.py up` recreates the container, which wipes its
  in-memory state.
- **Seeding speed:** loading from YAML at startup takes ~20 ms per table for the first
  thousand or so, then slows sharply (see the ceiling); creating tables through the API
  takes ~72 ms each.
- **Datasets cost memory** (~14 MB each), and **tables cap at 2000 columns** (SQLite). See
  the ceiling below.
- **Its latency is local:** 50–200 ms per call here. That says nothing about real
  BigQuery, so budgets count round trips.
- **A BigQuery `@catalog` target can't name an endpoint** (`catalog_target` takes only a
  project), so the stress notebook reaches the emulator through cells' `input`, never
  `@catalog`.

### The emulator's ceiling

`ceiling.py` raises one dimension at a time, with the others small, and restarts the
emulator on each seed. Run it again after upgrading the image.

Results from 2026-11-01, with the 3 GB memory cap:

| Dimension (others small) | Holds | Fails | Notes |
|---|---|---|---|
| Datasets, 3 tables each | 150: startup 10 s, 2.26 GiB | 200: the cap killed it while listing (exit 137, out of memory); 500: killed while loading | About 14 MB per dataset. The default seed uses 100, which runs at 1.3–1.7 GiB. |
| Tables in one dataset | 5000: startup 554 s, listing 422 ms, 732 MiB | not reached | Startup grows much faster than the table count: 400 → 4 s, 1000 → 16 s, 2500 → 114 s, 5000 → 554 s. |
| Columns in one table | 1500: `get_table` 85 ms | 3000: HTTP 500, `sqlite3: too many columns` | The emulator stores tables in SQLite, which caps them at 2000 columns. Wider tables are the fake's. |

Past these limits (hundreds of datasets, 10,000 shards, BigQuery's 10,000 columns), the
scenarios use `FakeBigQuery`.

## Files

- `stack.py`: `up` / `seed-bq` / `seed-pg` / `down`.
- `gen_bq_seed.py`, `gen_pg_seed.py`: the seed generators.
- `ceiling.py`: the ceiling search; writes `.seed/ceiling.json`.
- `catalog.qsql`: the stress notebook.
- `compare.py`: a run against the baseline; exits 1 when round trips went up.
- `baseline.json`: Phase 0's results (committed). `report.json`: the latest run (ignored).
