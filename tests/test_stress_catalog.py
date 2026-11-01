"""Catalog stress (docs/catalog-plan.md, Phase 0): what browsing big
warehouses costs, counted in round trips.

Each scenario (S1–S12 in the plan) measures today's code paths — the TUI's
(``project_root_node`` + ``CatalogCache``, drilling a level at a time) and the
LSP's (``SchemaCache`` + ``catalog_table``) — against the stress backends,
records what it saw in ``stress/catalog/report.json``, then asserts the plan's
budget. A budget today's code misses is ``xfail(strict=True)`` naming the
phase that meets it: when that phase lands, the XPASS says to drop the mark.
Only budgets assert; a broken setup fails with ``pytest.fail`` or a skip, so
an xfail can't hide it.

Budgets are round trips (BigQuery API requests; Postgres connects and
queries), not wall time: a count holds on real BigQuery whatever its latency.
Wall time is recorded as measured here, and projected at injected
per-round-trip latencies.

    uv run python stress/catalog/stack.py up
    systemd-run --user --scope -p MemoryMax=3G \\
        uv run --extra postgres --extra bigquery pytest -m stress
    uv run python stress/catalog/compare.py     # report.json vs baseline.json
"""

from __future__ import annotations

import json
import math
import os
import shutil
import threading
import time
import urllib.parse
import urllib.request
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterator

import pytest

from quicksql.bootstrap import compile_file
from quicksql.catalog import CatalogCache, CatalogNode, project_root_node, redact, resolve
from quicksql.compiler import Project
from quicksql.executors.bigquery_exec import BigQueryExecutor
from quicksql.executors.postgres_exec import PostgresExecutor
from quicksql.live.protocol import MAX_LINE, MAX_ROWS, encode
from quicksql.lsp.schema import SchemaCache, columns_for
from quicksql.overrides import gather_overrides
from quicksql.runner import run_project

pytestmark = pytest.mark.stress

STRESS = Path(__file__).resolve().parent.parent / "stress" / "catalog"
NOTEBOOK = STRESS / "catalog.qsql"
REPORT = STRESS / "report.json"
BQ_ENDPOINT = os.environ.get("QSQL_STRESS_BQ_ENDPOINT", "http://localhost:9052")
BQ_PROJECT = "quicksql-stress"
PG_DSN = os.environ.get(
    "QSQL_STRESS_PG_DSN",
    "postgresql://stress_reader:hunter2-stress-secret@localhost:5432/quicksql",
)
SECRET = urllib.parse.urlsplit(PG_DSN).password or "hunter2-stress-secret"
LATENCIES_MS = (50, 150, 400)  # projections only: real BigQuery's is unknown
PG_CONNECT_RTT = 3  # TCP + startup + auth, before the first query
SAVES = 20
CACHE_CAP_MB = 64  # the in-memory cap Phase 1 introduces


# ---------- the meter ----------


def rtts(kind: str) -> int:
    return PG_CONNECT_RTT if kind == "pg.connect" else 1


@dataclass
class Meter:
    """Round trips by kind; sleeps ``latency`` seconds per round trip."""

    latency: float = 0.0
    calls: Counter = field(default_factory=Counter)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def hit(self, kind: str) -> None:
        with self.lock:
            self.calls[kind] += 1
        if self.latency:
            time.sleep(self.latency * rtts(kind))

    def reset(self) -> None:
        with self.lock:
            self.calls.clear()


@contextmanager
def measuring(meter: Meter) -> Iterator[dict[str, Any]]:
    """What the block cost: calls by kind, round trips, local wall time, and
    that time projected at each of LATENCIES_MS per round trip."""
    meter.reset()
    out: dict[str, Any] = {}
    start = time.perf_counter()
    yield out
    local = (time.perf_counter() - start) * 1000
    trips = sum(n * rtts(k) for k, n in meter.calls.items())
    out.update(
        calls=dict(sorted(meter.calls.items())),
        round_trips=trips,
        local_ms=round(local, 1),
        projected_ms={str(ms): round(local + trips * ms) for ms in LATENCIES_MS},
    )


def engine_trips(m: dict[str, Any], prefix: str) -> int:
    return sum(n * rtts(k) for k, n in m["calls"].items() if k.startswith(prefix))


def bq_kind(method: str, url: str) -> str:
    """The API method a BigQuery REST request is."""
    parts = urllib.parse.urlsplit(url).path.strip("/").split("/")
    tail = parts[parts.index("projects") + 2 :] if "projects" in parts else []
    if method == "GET" and tail == ["datasets"]:
        return "bq.datasets.list"
    if method == "GET" and len(tail) == 3 and tail[2] == "tables":
        return "bq.tables.list"
    if method == "GET" and len(tail) == 4 and tail[2] == "tables":
        return "bq.tables.get"
    return f"bq.other.{method.lower()}"


class MeteredConnection:
    """A psycopg connection whose queries count on the meter."""

    def __init__(self, con: Any, meter: Meter) -> None:
        self._con, self._meter = con, meter

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        self._meter.hit("pg.query")
        return self._con.execute(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._con, name)


# ---------- a BigQuery past the emulator ----------


@dataclass(frozen=True)
class FakeWorld:
    """Sizes past the stress emulator's ceiling (stress/catalog/README.md)."""

    datasets: int = 2000
    tables_per_dataset: int = 3
    shards: int = 10_000
    wide_tables: int = 1
    wide_columns: int = 9400  # + 50 records x 10 fields: BigQuery's 10,000 columns
    wide_records: int = 50
    record_fields: int = 10
    description_len: int = 1024  # BigQuery's longest column description


class FakeBigQuery:
    """A BigQuery client over a synthetic project, for sizes past the
    emulator's ceiling and for paging, which the emulator ignores. Each page
    is a round trip on the meter: ``page_size`` items when the caller sets
    it, else ``default_page`` (real BigQuery's default isn't documented: the
    scenarios run at 50 and 1000 to bound it)."""

    def __init__(self, meter: Meter, world: FakeWorld, default_page: int) -> None:
        self.meter, self.world, self.default_page = meter, world, default_page
        desc = ("lorem ipsum dolor sit amet " * (world.description_len // 27 + 1))
        self._wide = [
            _field(f"col_{i:04d}", "STRING", "NULLABLE", desc[: world.description_len])
            for i in range(world.wide_columns)
        ] + [
            _field(f"rec_{r:03d}", "RECORD", "REPEATED", "",
                   [_field(f"f{j:02d}", "INT64") for j in range(world.record_fields)])
            for r in range(world.wide_records)
        ]
        self._small = [_field("id", "INT64", "REQUIRED"), _field("name", "STRING")]

    def list_datasets(self, page_size: int | None = None, **_: Any) -> Iterator[Any]:
        names = [f"ds_{d:04d}" for d in range(self.world.datasets)] + ["sharded", "wide"]
        return self._pages("bq.datasets.list", [SimpleNamespace(dataset_id=n) for n in names], page_size)

    def list_tables(self, dataset: Any, page_size: int | None = None, **_: Any) -> Iterator[Any]:
        items = [SimpleNamespace(table_id=t, table_type="TABLE") for t in self._tables(str(dataset))]
        return self._pages("bq.tables.list", items, page_size)

    def get_table(self, ref: Any, **_: Any) -> Any:
        self.meter.hit("bq.tables.get")
        dataset, _, table = str(ref).rpartition(".")
        dataset = dataset.rpartition(".")[2]
        if table not in self._tables(dataset):
            raise LookupError(f"Not found: Table {ref}")
        return SimpleNamespace(schema=self._wide if dataset == "wide" else self._small)

    def _tables(self, dataset: str) -> list[str]:
        w = self.world
        if dataset == "sharded":
            start = date(2024, 1, 1)
            return [f"events_{start + timedelta(days=i):%Y%m%d}" for i in range(w.shards)]
        if dataset == "wide":
            return ["wide_table"] if w.wide_tables == 1 else [f"wide_{i:03d}" for i in range(w.wide_tables)]
        if dataset.startswith("ds_"):
            return [f"t_{t:02d}" for t in range(w.tables_per_dataset)]
        raise LookupError(f"Not found: Dataset {dataset}")

    def _pages(self, kind: str, items: list[Any], page_size: int | None) -> Iterator[Any]:
        size = page_size or self.default_page
        for start in range(0, len(items) or 1, size):
            self.meter.hit(kind)
            yield from items[start : start + size]


def _field(name: str, type_: str, mode: str = "NULLABLE", description: str = "",
           fields: list[Any] | None = None) -> Any:
    return SimpleNamespace(name=name, field_type=type_, mode=mode,
                           description=description, fields=fields or [])


# ---------- the backends ----------


@dataclass(frozen=True)
class Shape:
    """Where each scenario goes in one engine's tree."""

    engine: str  # the root row's engine
    big: dict[str, str]  # from the context: the level with many tables
    wide: tuple[dict[str, str], ...]  # from the context down to the wide leaf
    wide_rows: int  # the wide leaf's field paths
    cell: str  # a cell in that context, for the LSP
    wide_relation: str  # the wide table as that cell names it
    named: tuple[str, ...]  # 30 tables a notebook names (S12)

    @property
    def segments(self) -> tuple[str, ...]:
        """The wide leaf's path beneath the context, a row name per level."""
        return tuple(next(iter(p.values())) for p in self.wide)


BQ = Shape(
    "bigquery", {"dataset": "sharded"}, ({"dataset": "wide"}, {"table": "wide_table"}), 2050,
    "bq_wide_sample", "wide.wide_table", tuple(f"ds_{i:03d}.t_00" for i in range(30)),
)
PG = Shape(
    "postgres", {"schema": "stress_many"}, ({"schema": "stress_wide"}, {"table": "wide_1500"}), 1500,
    "pg_wide_sample", "stress_wide.wide_1500", tuple(f"stress_s{i:03d}.t_00" for i in range(30)),
)
FAKE = Shape(
    "bigquery", {"dataset": "sharded"}, ({"dataset": "wide"}, {"table": "wide_table"}), 9950,
    "bq_wide_sample", "wide.wide_table", (),
)


@pytest.fixture
def meter(monkeypatch) -> Meter:
    """The meter, with Postgres connects and queries counting on it."""
    m = Meter()
    real = PostgresExecutor.make_connection

    def make_connection(self: PostgresExecutor, dsn: str) -> Any:
        m.hit("pg.connect")
        return MeteredConnection(real(self, dsn), m)

    monkeypatch.setattr(PostgresExecutor, "make_connection", make_connection)
    return m


@lru_cache(maxsize=1)
def _emulator_problem() -> str | None:
    """Why the stress emulator can't serve the scenarios, or None."""
    api = f"{BQ_ENDPOINT}/bigquery/v2/projects/{BQ_PROJECT}"
    try:
        with urllib.request.urlopen(f"{api}/datasets/sharded/tables?maxResults=100000", timeout=5) as r:
            shards = len(json.load(r).get("tables", []))
        with urllib.request.urlopen(f"{api}/datasets/wide/tables/wide_table", timeout=5) as r:
            fields = json.load(r)["schema"]["fields"]
    except Exception as exc:
        return f"no stress emulator at {BQ_ENDPOINT} ({exc}): uv run python stress/catalog/stack.py up"
    paths = sum(1 + len(f.get("fields", [])) for f in fields)
    if (shards, paths) != (400, BQ.wide_rows):
        return f"the stress emulator holds another seed ({shards} shards, {paths} paths): stack.py up"
    return None


@pytest.fixture
def bq_live(meter, monkeypatch) -> Meter:
    """The stress emulator, every REST request counting on the meter."""
    pytest.importorskip("google.cloud.bigquery", reason="needs the 'bigquery' extra")
    problem = _emulator_problem()
    if problem:
        pytest.skip(problem)
    import requests
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    class MeteredSession(requests.Session):
        def request(self, method, url, *args, **kwargs):  # type: ignore[override]
            meter.hit(bq_kind(method, url))
            return super().request(method, url, *args, **kwargs)

    def make_client(self: BigQueryExecutor, spec: dict[str, Any]) -> Any:
        return bigquery.Client(
            project=spec.get("project"),
            credentials=AnonymousCredentials(),
            client_options=ClientOptions(api_endpoint=spec.get("endpoint") or BQ_ENDPOINT),
            _http=MeteredSession(),
        )

    monkeypatch.setattr(BigQueryExecutor, "make_client", make_client)
    return meter


@lru_cache(maxsize=1)
def _postgres_problem() -> str | None:
    try:
        import psycopg
    except ImportError:
        return "needs the 'postgres' extra"
    try:
        with psycopg.connect(PG_DSN, connect_timeout=3) as con:
            n = con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'stress_many'"
            ).fetchone()[0]
    except psycopg.OperationalError as exc:
        return f"no stress postgres at {PG_DSN.replace(SECRET, '***')}: {exc}"
    if n != 3000:
        return "the stress schemas aren't seeded: uv run python stress/catalog/stack.py seed-pg"
    return None


@pytest.fixture
def pg_live(meter) -> Meter:
    problem = _postgres_problem()
    if problem:
        pytest.skip(problem)
    return meter


@pytest.fixture
def bq_fake(meter, monkeypatch) -> Callable[..., Meter]:
    """``bq_fake(page, world)``: BigQuery cells reach a FakeBigQuery."""

    def install(page: int, world: FakeWorld = FakeWorld()) -> Meter:
        fake = FakeBigQuery(meter, world, page)
        monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: fake)
        return meter

    return install


def backend(request: pytest.FixtureRequest, engine: str) -> tuple[Shape, Meter]:
    if engine == "bq":
        return BQ, request.getfixturevalue("bq_live")
    if engine == "pg":
        return PG, request.getfixturevalue("pg_live")
    if engine.startswith("fake"):  # fake50, fake1000: the default page size
        return FAKE, request.getfixturevalue("bq_fake")(int(engine[4:]))
    raise ValueError(engine)


@pytest.fixture
def stress_project(tmp_path, monkeypatch, registries) -> Callable[[], Project]:
    """Compile the stress notebook (again, as a save does) in a scratch root."""
    monkeypatch.setenv("QSQL_STRESS_PG_DSN", PG_DSN)
    nb = tmp_path / "catalog.qsql"
    shutil.copy(NOTEBOOK, nb)
    sets = [f"input.bigquery.endpoint={BQ_ENDPOINT}"] if BQ_ENDPOINT != "http://localhost:9052" else []
    overrides = gather_overrides(sets, environ={})
    return lambda: compile_file(nb, overrides, registries=registries)


# ---------- the report ----------

_RECORDS: list[dict[str, Any]] = []


@pytest.fixture(scope="module", autouse=True)
def report() -> Iterator[None]:
    """Merge this run's records into report.json: a run of some scenarios
    (``-k s5``) replaces theirs and keeps the rest, each stamped with its run."""
    _RECORDS.clear()
    yield
    if not _RECORDS:
        return
    kept: dict[tuple[str, str], dict[str, Any]] = {}
    if REPORT.exists():
        kept = {(r["scenario"], r["engine"]): r for r in json.loads(REPORT.read_text())["records"]}
    kept.update({(r["scenario"], r["engine"]): r for r in _RECORDS})
    doc = {
        "projection": {"latencies_ms": list(LATENCIES_MS), "pg_connect_rtt": PG_CONNECT_RTT},
        "records": sorted(kept.values(), key=lambda r: (int(r["scenario"][1:3].rstrip("ab")), r["engine"])),
    }
    REPORT.write_text(json.dumps(doc, indent=2) + "\n")


def record(scenario: str, engine: str, data: dict[str, Any]) -> None:
    at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _RECORDS.append({"scenario": scenario, "engine": engine, "at": at, **data})


# ---------- walking the tree, as the Catalog tab does ----------


def pick(frame: Any, where: dict[str, str]) -> dict[str, Any]:
    for row in frame.iter_rows(named=True):
        if all(row.get(k) == v for k, v in where.items()):
            return row
    pytest.fail(f"no row {where} among {frame.height} in {frame.columns}: has the seed changed?")


def response_bytes(frame: Any) -> int:
    """The level as one unpaged socket response."""
    return len(encode({"id": 1, "result": {"columns": frame.columns, "rows": frame.rows()}}))


def context(cache: CatalogCache, project: Project, engine: str) -> CatalogNode:
    root = project_root_node(project)
    frame, _ = cache.load(root)
    return root.child(pick(frame, {"kind": "context", "engine": engine}))


def drill(cache: CatalogCache, node: CatalogNode, picks: tuple[dict[str, str], ...] | list[dict[str, str]]):
    """Load ``node`` and the levels ``picks`` choose beneath it, as Enter
    does: returns the last node, every frame, and each level's size."""
    frames, levels = [], []
    while True:
        frame, cached = cache.load(node)
        frames.append(frame)
        levels.append({"title": node.title, "rows": frame.height,
                       "bytes": response_bytes(frame), "cached": cached})
        if len(frames) > len(picks):
            return node, frames, levels
        node = node.child(pick(frame, picks[len(frames) - 1]))


def walk_to_wide(cache: CatalogCache, project: Project, shape: Shape):
    return drill(cache, context(cache, project, shape.engine), shape.wide)


# ---------- S1–S3: browsing ----------


@pytest.mark.parametrize("engine", ["bq", "pg", "fake50", "fake1000"])
def test_s1_cold_browse(engine, request, stress_project) -> None:
    """Root → context → the many-tables level, then → the wide leaf: one
    round trip per level loaded (plus its pages), nothing more."""
    shape, meter = backend(request, engine)
    project, cache = stress_project(), CatalogCache()
    with measuring(meter) as m:
        ctx = context(cache, project, shape.engine)
        _, _, big = drill(cache, ctx, [shape.big])
        _, frames, wide = drill(cache, ctx, shape.wide)
    m["levels"] = big + wide[1:]
    record("S1", engine, m)
    if frames[-1].height != shape.wide_rows:
        pytest.fail(f"wide leaf has {frames[-1].height} rows, not {shape.wide_rows}")
    loaded = len(m["levels"])  # context, many tables, wide container, leaf
    if engine == "pg":
        assert m["calls"] == {"pg.connect": loaded, "pg.query": loaded}
    else:
        # the executor asks for full pages (Phase 1), whatever the API's default
        size = getattr(BigQueryExecutor, "page_size", None) or int(engine[4:]) if engine.startswith("fake") else 0
        pages = sum(math.ceil(max(lvl["rows"], 1) / size) for lvl in m["levels"][:-1]) \
            if engine.startswith("fake") else loaded - 1
        assert engine_trips(m, "bq.") == pages + 1  # every list page, and one tables.get


@pytest.mark.parametrize("engine", ["bq", "pg", "fake50", "fake1000"])
def test_s2_cold_exact_path(engine, request, stress_project) -> None:
    """Straight to the wide leaf by its names (``catalog.resolve``): only the
    leaf's load."""
    shape, meter = backend(request, engine)
    project = stress_project()
    root = project_root_node(project)
    context_row = pick(root.load(), {"kind": "context", "engine": shape.engine})
    path = [context_row["name"], *shape.segments]
    with measuring(meter) as m:
        got = resolve(root, path, CatalogCache())
    m["path_depth"] = len(path)
    record("S2", engine, m)
    # for reference, the LSP's direct lookup, which name-first walking now matches
    cell = project.cells[shape.cell]
    executor = project.registries.executors.get(cell.engine)
    with measuring(meter) as direct:
        executor.catalog_table(cell.config, project.root, shape.wide_relation.split(".")).load()
    record("S2", f"{engine}/lsp-direct", direct)
    if got.frame.height != shape.wide_rows:
        pytest.fail(f"wide leaf has {got.frame.height} rows, not {shape.wide_rows}")
    assert m["calls"] == ({"pg.connect": 1, "pg.query": 1} if engine == "pg" else {"bq.tables.get": 1})


@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s3_warm_browse(engine, request, stress_project) -> None:
    """S1 again on the same cache: no round trips, well under 50 ms."""
    shape, meter = backend(request, engine)
    project, cache = stress_project(), CatalogCache()
    ctx = context(cache, project, shape.engine)
    drill(cache, ctx, [shape.big])
    drill(cache, ctx, shape.wide)
    with measuring(meter) as m:
        ctx = context(cache, project, shape.engine)
        drill(cache, ctx, [shape.big])
        drill(cache, ctx, shape.wide)
    record("S3", engine, m)
    assert m["round_trips"] == 0
    assert m["local_ms"] < 50


# ---------- S4–S6: what clears the cache, and who loads ----------


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="phase 2: a save clears the top level only")
@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s4_saves_keep_the_warehouse(engine, request, stress_project) -> None:
    """20 saves, the wide leaf read after each: no warehouse round trips."""
    shape, meter = backend(request, engine)
    cache = CatalogCache()
    walk_to_wide(cache, stress_project(), shape)
    with measuring(meter) as m:
        for _ in range(SAVES):
            project = stress_project()  # the watcher recompiles
            cache.invalidate()  # as QsqlApp._on_recompiled does today
            walk_to_wide(cache, project, shape)
    m["saves"] = SAVES
    record("S4", engine, m)
    assert m["round_trips"] == 0


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="phase 2: a run clears only the contexts it ran in")
def test_s5_runs_clear_only_their_contexts(bq_live, pg_live, stress_project) -> None:
    """A duckdb-only run keeps both warehouses; a BigQuery run reloads only
    BigQuery's levels."""
    meter = bq_live  # one meter counts both
    project, cache = stress_project(), CatalogCache()
    walk_to_wide(cache, project, BQ)
    walk_to_wide(cache, project, PG)

    def run(cell: str) -> None:
        results = run_project(project, select=[cell])
        if not all(r.ok for r in results):
            pytest.fail(f"{cell} didn't run: {[r.error for r in results if not r.ok]}")

    run("local_numbers")
    with measuring(meter) as duck:
        cache.invalidate()  # as QsqlApp.run_landed does today, whatever ran
        walk_to_wide(cache, project, BQ)
        walk_to_wide(cache, project, PG)
    record("S5", "after a duckdb run", duck)
    run("bq_shard")
    with measuring(meter) as bq:
        cache.invalidate()
        walk_to_wide(cache, project, BQ)
        walk_to_wide(cache, project, PG)
    record("S5", "after a bigquery run", bq)
    assert duck["round_trips"] == 0
    assert engine_trips(bq, "pg.") == 0
    assert engine_trips(bq, "bq.") == 3  # its context, the wide container, the leaf


@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s6_one_load_for_two_askers(engine, request, stress_project) -> None:
    """The TUI and an agent ask for the same cold leaf at once: one load."""
    shape, meter = backend(request, engine)
    leaf, _, _ = walk_to_wide(CatalogCache(), stress_project(), shape)
    cache, gate, errors = CatalogCache(), threading.Barrier(2), []

    def ask() -> None:
        try:
            gate.wait()
            cache.load(leaf)
        except Exception as exc:  # surfaced below, not lost in the thread
            errors.append(exc)

    meter.latency = 0.2  # widen the window the second asker arrives in
    try:
        with measuring(meter) as m:
            threads = [threading.Thread(target=ask) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(60)
    finally:
        meter.latency = 0.0
    record("S6", engine, m)
    if errors:
        pytest.fail(f"a load failed: {errors[0]!r}")
    loads = m["calls"].get("pg.connect", 0) + m["calls"].get("bq.tables.get", 0)
    assert loads == 1


# ---------- S7: the size of a wide leaf ----------


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="phase 2: the leaf pages, each response within MAX_ROWS and MAX_LINE")
@pytest.mark.parametrize("engine", ["bq", "pg", "fake1000"])
def test_s7_wide_leaf_fits_the_socket(engine, request, stress_project) -> None:
    """The wide leaf's sizes: whole, one page, and filtered as Phase 1's
    match / columns / top-level would cut it."""
    shape, _ = backend(request, engine)
    _, frames, _ = walk_to_wide(CatalogCache(), stress_project(), shape)
    leaf = frames[-1]
    paths = leaf["field_path"]
    top = leaf.filter(~paths.str.contains(".", literal=True))
    matched = leaf.filter(paths.str.contains("col_01", literal=True))
    data = {
        "rows": leaf.height,
        "bytes": response_bytes(leaf),
        "pages_at_max_rows": math.ceil(leaf.height / MAX_ROWS),
        "first_page_bytes": response_bytes(leaf.head(MAX_ROWS)),
        "no_description_bytes": response_bytes(leaf.drop("description")),
        "top_level_rows": top.height,
        "match_col_01_rows": matched.height,
        "match_col_01_bytes": response_bytes(matched),
        "max_line": MAX_LINE,
    }
    record("S7", engine, data)
    assert data["rows"] <= MAX_ROWS and data["bytes"] <= MAX_LINE  # one response today


# ---------- S8–S9: a new session ----------


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="phase 3: the catalog persists across sessions")
@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s8_reopen_answers_without_round_trips(engine, request, stress_project) -> None:
    shape, meter = backend(request, engine)
    walk_to_wide(CatalogCache(), stress_project(), shape)
    with measuring(meter) as m:
        walk_to_wide(CatalogCache(), stress_project(), shape)  # a new TUI: a new cache
    record("S8", engine, m)
    assert m["round_trips"] == 0


@pytest.mark.xfail(strict=True, raises=AssertionError, reason="phase 3: the LSP reads the persisted catalog")
@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s9_lsp_completes_after_a_restart_without_round_trips(engine, request, stress_project) -> None:
    shape, meter = backend(request, engine)
    project = stress_project()
    cell = project.cells[shape.cell]
    with measuring(meter) as first:
        columns = columns_for(project, cell, shape.wide_relation, SchemaCache())
    with measuring(meter) as again:
        columns_for(project, cell, shape.wide_relation, SchemaCache())  # a new LSP process
    record("S9", f"{engine}/first", first)
    record("S9", f"{engine}/after restart", again)
    if len(columns) != shape.wide_rows:
        pytest.fail(f"the LSP saw {len(columns)} columns, not {shape.wide_rows}")
    assert again["round_trips"] == 0


# ---------- S10: secrets ----------


def test_s10a_no_secret_in_names_or_titles(pg_live, stress_project) -> None:
    """Every name and title the catalog would show is free of the password."""
    project, cache = stress_project(), CatalogCache()
    root = project_root_node(project)
    frame, _ = cache.load(root)
    ctx = root.child(pick(frame, {"kind": "context", "engine": "postgres"}))
    leaf, frames, levels = drill(cache, ctx, PG.wide)
    shown = [root.title, *(lvl["title"] for lvl in levels)]
    shown += [str(v) for f in (frame, *frames) for row in f.rows() for v in row]
    leaked = [s for s in shown if SECRET in s]
    record("S10a", "postgres", {
        "strings_checked": len(shown),
        "leaked": len(leaked),
        # keys are never shown, but Phase 3 writes them to disk: it must hash them
        "secret_in_cache_keys": any(SECRET in n.cache_key for n in (ctx, leaf)),
    })
    assert not leaked


BAD_TARGETS = {
    "malformed keyword DSN": f"host=localhost password {SECRET} dbname=quicksql",
    "unreachable host": f"postgresql://stress_reader:{SECRET}@nohost.invalid:5432/quicksql",
    "wrong password": f"postgresql://stress_reader:{SECRET}-wrong@localhost:5432/quicksql",
}


def test_s10b_no_secret_in_a_failing_targets_name_or_error(pg_live, stress_project) -> None:
    """``@catalog`` targets that fail to load: neither the row name nor the
    error text, as the catalog shows it (``catalog.redact``), carries the
    password."""
    project = stress_project()
    executor = project.registries.executors.get("postgres")
    leaks: dict[str, list[str]] = {}
    for what, target in BAD_TARGETS.items():
        name, node = executor.catalog_target(target, project.root)
        try:
            node.load()
            pytest.fail(f"{what}: the load should fail")
        except Exception as exc:
            message = str(exc)
        shown = (("name", name), ("error", redact(message, project.registries)))
        leaks[what] = [part for part, text in shown if SECRET in text]
    record("S10b", "postgres", {"leaks": leaks})
    assert not any(leaks.values())


# ---------- S11: memory ----------


def test_s11_cache_memory_is_capped(bq_fake, stress_project) -> None:
    """400 wide leaves (2050 field paths, 200-character descriptions) loaded:
    the cache holds at most CACHE_CAP_MB of them."""
    world = FakeWorld(datasets=1, shards=1, wide_tables=400, wide_columns=1500,
                      wide_records=50, record_fields=10, description_len=200)
    meter = bq_fake(1000, world)
    cache = CatalogCache()
    ctx = context(cache, stress_project(), "bigquery")
    container, frames, _ = drill(cache, ctx, [{"dataset": "wide"}])
    rss_before = _rss_mb()
    loaded = 0
    with measuring(meter) as m:
        for row in frames[-1].iter_rows(named=True):
            frame, _ = cache.load(container.child(row))
            loaded += frame.estimated_size()
    # today's cache never evicts, so it holds everything it loaded; Phase 1's
    # cap reports what it holds
    held = getattr(cache, "size_bytes", loaded)
    m.update(entries=len(cache), loaded_mb=round(loaded / 2**20, 1),
             held_mb=round(held / 2**20, 1), rss_delta_mb=round(_rss_mb() - rss_before, 1))
    record("S11", "fake", m)
    assert held <= CACHE_CAP_MB * 2**20


def _rss_mb() -> float:
    pages = int(Path("/proc/self/statm").read_text().split()[1])
    return pages * os.sysconf("SC_PAGE_SIZE") / 2**20


# ---------- S12: the tables a notebook names ----------


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="phases 3–4: named tables are warmed once and persist")
@pytest.mark.parametrize("engine", ["bq", "pg"])
def test_s12_named_tables_load_once(engine, request, stress_project) -> None:
    """A notebook naming 30 tables, opened twice: 30 loads the first time,
    none the second while they're fresh."""
    shape, meter = backend(request, engine)
    project = stress_project()
    cell = project.cells[shape.cell]

    def session() -> list[list[Any]]:
        cache = SchemaCache()  # each session (TUI, LSP) starts its own today
        return [columns_for(project, cell, t, cache) for t in shape.named]

    with measuring(meter) as first:
        columns = session()
    with measuring(meter) as again:
        session()
    record("S12", f"{engine}/first open", first)
    record("S12", f"{engine}/second open", again)
    if not all(columns):
        pytest.fail("a named table had no columns: has the seed changed?")
    loads = lambda m: m["calls"].get("pg.connect", 0) + m["calls"].get("bq.tables.get", 0)  # noqa: E731
    assert loads(first) == len(shape.named)
    assert loads(again) == 0
