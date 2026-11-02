import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest

from quicksql.catalog import (
    CatalogCache,
    CatalogNode,
    CatalogPathError,
    narrow,
    page,
    project_root_node,
    redact,
    resolve,
    sink_output_node,
)
from quicksql import config
from quicksql.bootstrap import builtin_registries, compile_text
from quicksql.conduit import duckdb_field_paths
from quicksql.config import make_sink
from quicksql.executors.bigquery_exec import BigQueryExecutor, bq_field_paths
from quicksql.executors.duckdb_exec import DuckDBExecutor
from quicksql.executors.postgres_exec import PostgresExecutor
from quicksql.executors.sqlite_exec import SQLiteExecutor

BUILTINS = builtin_registries()  # read-only here


def resolve_cell(global_raw, cell_raw):
    return config.resolve_cell(global_raw, cell_raw, registries=BUILTINS)

FIELD_PATH_COLUMNS = ["column", "field_path", "type", "mode", "description"]


def _sf(name, field_type, mode="NULLABLE", fields=(), description=None):
    """Stands in for a bigquery SchemaField: name/field_type/mode/fields/description."""
    return SimpleNamespace(
        name=name, field_type=field_type, mode=mode, fields=list(fields), description=description
    )


def test_bq_field_paths_one_row_per_path_level() -> None:
    schema = [
        _sf("id", "INTEGER", "REQUIRED", description="surrogate key"),
        _sf(
            "event",
            "RECORD",
            "NULLABLE",
            [
                _sf("name", "STRING", description="event name"),
                _sf(
                    "params",
                    "RECORD",
                    "REPEATED",
                    [_sf("key", "STRING"), _sf("value", "INTEGER")],
                ),
            ],
        ),
    ]
    frame = bq_field_paths(schema)
    assert frame.columns == FIELD_PATH_COLUMNS
    # depth-first, document order, intermediate RECORDs included; descriptions
    # ride along like information_schema.column_field_paths
    assert frame.rows() == [
        ("id", "id", "INTEGER", "REQUIRED", "surrogate key"),
        ("event", "event", "RECORD", "NULLABLE", ""),
        ("event", "event.name", "STRING", "NULLABLE", "event name"),
        ("event", "event.params", "RECORD", "REPEATED", ""),
        ("event", "event.params.key", "STRING", "NULLABLE", ""),
        ("event", "event.params.value", "INTEGER", "NULLABLE", ""),
    ]


def test_bq_field_paths_empty_schema() -> None:
    frame = bq_field_paths([])
    assert frame.columns == FIELD_PATH_COLUMNS
    assert frame.height == 0


@pytest.fixture
def con():
    con = duckdb.connect()
    yield con
    con.close()


def test_duckdb_field_paths_flattens_structs_and_lists(con) -> None:
    rel = con.sql(
        "SELECT 1 AS n, {'name': 'x', 'params': [{'key': 'k', 'value': 1}]} AS event LIMIT 0"
    )
    frame = duckdb_field_paths(rel.columns, rel.types)
    assert frame.columns == FIELD_PATH_COLUMNS
    assert frame["description"].to_list() == [""] * frame.height  # no comments here
    paths = {path: (type_, mode) for _, path, type_, mode, _ in frame.rows()}
    # a LIST wrapper adds no path segment: params -> params.key, not params.child.key
    assert set(paths) == {
        "n",
        "event",
        "event.name",
        "event.params",
        "event.params.key",
        "event.params.value",
    }
    assert paths["n"] == ("INTEGER", "")
    assert paths["event"][0].startswith("STRUCT")
    assert paths["event.name"] == ("VARCHAR", "")
    assert paths["event.params"][0].endswith("[]")  # the full nested type string
    assert paths["event.params"][1] == "REPEATED"
    assert paths["event.params.key"] == ("VARCHAR", "")
    # column ties every path back to its top-level column, depth-first order
    assert [row[0] for row in frame.rows()] == ["n"] + ["event"] * 5


def test_duckdb_field_paths_survives_map_and_fixed_arrays(con) -> None:
    # exotic nested types must not crash the walker (fixed arrays carry a
    # non-type child; .children raises on scalars)
    rel = con.sql("SELECT MAP {'a': 1} AS m, CAST([1, 2, 3] AS INT[3]) AS fixed LIMIT 0")
    frame = duckdb_field_paths(rel.columns, rel.types)
    paths = {path for _, path, _, _, _ in frame.rows()}
    assert "m" in paths
    assert "fixed" in paths


def test_duckdb_field_paths_empty() -> None:
    frame = duckdb_field_paths([], [])
    assert frame.columns == FIELD_PATH_COLUMNS
    assert frame.height == 0


# ---------- drillable nodes ----------


def test_project_root_node_lists_contexts_and_outputs(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\nSELECT 1 AS x;\n"
        "-- @cell b\n/*@ input: { sqlite: nums.db } */\nSELECT 2 AS y;\n"
        "-- @cell c\n/*@ input: { sqlite: nums.db } */\nSELECT 3 AS z;\n",
        root=tmp_path,
    )
    node = project_root_node(project)
    frame = node.load()
    assert frame.columns == ["kind", "name", "engine", "detail"]
    contexts = frame.filter(pl.col("kind") == "context")
    # b and c share one sqlite context (deduped by context_key); a is duckdb
    assert contexts["engine"].to_list() == ["duckdb", "sqlite"]
    outputs = frame.filter(pl.col("kind") == "output")
    assert outputs["name"].to_list() == ["a", "b", "c"]
    assert outputs["detail"].to_list() == ["duckdb → parquet"] + ["sqlite → parquet"] * 2

    out_child = node.child(outputs.row(0, named=True))
    assert isinstance(out_child, CatalogNode) and out_child.child is None  # leaf
    ctx_child = node.child(contexts.row(1, named=True))
    assert isinstance(ctx_child, CatalogNode)


def test_catalog_directive_declares_extra_browse_targets(tmp_path, monkeypatch) -> None:
    created: list[dict] = []

    class FakeClient:
        def list_datasets(self, page_size=None):
            return [SimpleNamespace(dataset_id="ds1")]

        def list_tables(self, dataset_id, page_size=None):
            assert dataset_id == "analytics"
            return [SimpleNamespace(table_id="events", table_type="TABLE")]

    def make(self, spec):
        created.append(spec)
        return FakeClient()

    monkeypatch.setattr(BigQueryExecutor, "make_client", make)
    project = compile_text(
        "/*@ catalog: { bigquery: [proj-a, proj-b.analytics] } */\n"
        "-- @cell a\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    node = project_root_node(project)
    contexts = node.load().filter(pl.col("kind") == "context")
    by_name = {row["name"]: row for row in contexts.iter_rows(named=True)}
    assert "bigquery:proj-a" in by_name
    assert "bigquery:proj-b/analytics" in by_name
    assert by_name["bigquery:proj-a"]["detail"] == "@catalog"

    # a bare project drills into its datasets
    a = node.child(by_name["bigquery:proj-a"])
    assert a.load()["dataset"].to_list() == ["ds1"]
    assert created[-1] == {"project": "proj-a"}
    # project.dataset jumps straight to that dataset's tables
    b = node.child(by_name["bigquery:proj-b/analytics"])
    assert b.load()["table"].to_list() == ["events"]
    assert created[-1] == {"project": "proj-b"}


def test_catalog_directive_dedupes_against_cell_contexts(tmp_path) -> None:
    project = compile_text(
        "/*@ catalog: { bigquery: proj-a } */\n"  # bare string coerces to a list
        "-- @cell a\n/*@ input: { bigquery: { project: proj-a } } */\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    assert project.config.catalog == {"bigquery": ["proj-a"]}
    frame = project_root_node(project).load()
    names = frame.filter(pl.col("kind") == "context")["name"].to_list()
    assert names.count("bigquery:proj-a") == 1  # cell context and @catalog merge


def test_catalog_directive_supports_other_engines(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(PostgresExecutor, "make_connection", lambda self, dsn: FakeCatalogPg())
    project = compile_text(
        "/*@ catalog: { postgres: ['postgresql://elsewhere/db'] } */\n"
        "-- @cell a\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    node = project_root_node(project)
    contexts = node.load().filter(pl.col("kind") == "context")
    row = next(
        r for r in contexts.iter_rows(named=True) if r["name"] == "postgres:postgresql://elsewhere/db"
    )
    assert node.child(row).load()["schema"].to_list() == ["public", "analytics"]


def test_a_third_party_engines_context_drills_into_its_own_catalog(
    tmp_path, tsql_engine, registries
) -> None:
    project = compile_text(
        "-- @engine: mssql\n-- @cell a\nSELECT 1 AS x;\n", root=tmp_path, registries=registries
    )
    node = project_root_node(project)
    [row] = node.load().filter(pl.col("kind") == "context").iter_rows(named=True)
    assert (row["name"], row["engine"]) == ("mssql:wh", "mssql")
    wh = node.child(row)
    assert wh.title == "mssql:wh"
    leaf = wh.child(wh.load().row(0, named=True))
    assert leaf.load()["field_path"].to_list() == ["x"]


def test_a_third_party_engine_takes_catalog_directive_targets(
    tmp_path, tsql_engine, registries
) -> None:
    project = compile_text(
        "/*@ catalog: { mssql: [wh, lake] } */\n-- @engine: mssql\n-- @cell a\nSELECT 1 AS x;\n",
        root=tmp_path,
        registries=registries,
    )
    contexts = project_root_node(project).load().filter(pl.col("kind") == "context")
    # wh is the cell's own context: the @catalog target dedupes against it
    assert contexts.select("name", "detail").rows() == [("mssql:wh", ""), ("mssql:lake", "@catalog")]


def test_an_engine_without_a_catalog_has_no_context_to_drill(tmp_path, registries) -> None:
    from quicksql.executors.base import Executor
    from quicksql.registry import executor

    @executor("bare")
    class Bare(Executor):
        def open_session(self, config, ctx):
            raise NotImplementedError

    registries.add(Bare)
    project = compile_text(
        "/*@ catalog: { bare: [x] } */\n-- @engine: bare\n-- @cell a\nSELECT 1 AS x;\n",
        root=tmp_path,
        registries=registries,
    )
    node = project_root_node(project)
    contexts = node.load().filter(pl.col("kind") == "context")
    assert contexts["name"].to_list() == ["bare:"]  # no @catalog row: it takes no targets
    assert node.child(contexts.row(0, named=True)) is None


def test_a_postgres_catalog_target_hides_its_password_and_dedupes(tmp_path) -> None:
    project = compile_text(
        "/*@ catalog: { postgres: ['postgresql://u:s3cret@db/x', 'host=h password=pw'] } */\n"
        "-- @cell a\n/*@ input: { postgres: 'postgresql://u:s3cret@db/x' } */\nSELECT 1;\n",
        root=tmp_path,
    )
    contexts = project_root_node(project).load().filter(pl.col("kind") == "context")
    # the first target is the cell's own context: shown once, as its context key
    assert contexts.select("name", "detail").rows() == [
        ("postgres:postgresql://u:***@db/x", ""),
        ("postgres:host=h password=***", "@catalog"),
    ]


def test_a_postgres_catalog_target_expands_env_vars_like_an_input_dsn(
    tmp_path, monkeypatch
) -> None:
    connected: list[str] = []

    def connect(self, dsn):
        connected.append(dsn)
        return FakeCatalogPg()

    monkeypatch.setattr(PostgresExecutor, "make_connection", connect)
    monkeypatch.setenv("WAREHOUSE_DSN", "postgresql://u:pw@wh/db")
    project = compile_text(
        "/*@ catalog: { postgres: [$WAREHOUSE_DSN] } */\n-- @cell a\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    node = project_root_node(project)
    contexts = node.load().filter(pl.col("kind") == "context")
    row = next(r for r in contexts.iter_rows(named=True) if r["detail"] == "@catalog")
    assert row["name"] == "postgres:postgresql://u:***@wh/db"
    node.child(row).load()
    assert connected == ["postgresql://u:pw@wh/db"]


def test_catalog_directive_is_global_only(tmp_path) -> None:
    from quicksql.errors import QsqlError

    with pytest.raises(QsqlError, match="catalog"):
        compile_text(
            "-- @cell a\n-- @catalog: { bigquery: [p] }\nSELECT 1 AS x;\n",
            root=tmp_path,
        )


def test_catalog_directive_rejects_unknown_engines(tmp_path) -> None:
    from quicksql.errors import QsqlError

    with pytest.raises(QsqlError, match="snowflake"):
        compile_text(
            "/*@ catalog: { snowflake: [wh1] } */\n-- @cell a\nSELECT 1 AS x;\n",
            root=tmp_path,
        )


def test_sqlite_catalog_chain(tmp_path) -> None:
    db = sqlite3.connect(tmp_path / "nums.db")
    db.execute("CREATE TABLE nums (n INTEGER NOT NULL, label TEXT)")
    db.commit()
    db.close()

    node = SQLiteExecutor().context_node("nums.db", tmp_path)
    tables = node.load()
    assert tables["table"].to_list() == ["nums"]
    leaf = node.child(tables.row(0, named=True))
    cols = leaf.load()
    assert cols.columns == FIELD_PATH_COLUMNS
    assert cols.rows() == [
        ("n", "n", "INTEGER", "REQUIRED", ""),
        ("label", "label", "TEXT", "", ""),
    ]
    assert leaf.child is None


def test_sqlite_memory_context_not_browsable(tmp_path) -> None:
    node = SQLiteExecutor().context_node(":memory:", tmp_path)
    assert node.child is None
    assert "memory" in node.load()["info"][0]


def test_duckdb_catalog_chain(tmp_path) -> None:
    con = duckdb.connect(str(tmp_path / "wh.db"))
    con.execute("CREATE TABLE t AS SELECT {'a': 1} AS s")
    con.close()

    node = DuckDBExecutor().context_node("wh.db", tmp_path)
    tables = node.load()
    assert tables.columns == ["schema", "table"]
    assert tables.rows() == [("main", "t")]
    leaf = node.child(tables.row(0, named=True))
    assert leaf.load()["field_path"].to_list() == ["s", "s.a"]
    assert leaf.child is None


def test_duckdb_memory_context_not_browsable(tmp_path) -> None:
    node = DuckDBExecutor().context_node(None, tmp_path)
    assert node.child is None
    assert "memory" in node.load()["info"][0]


def test_a_duckdb_context_named_memory_is_not_browsable_and_creates_no_file(tmp_path) -> None:
    project = compile_text(
        "/*@ input: { duckdb: ':memory:' } */\n-- @cell a\nSELECT 1 AS x;\n", root=tmp_path
    )
    node = project_root_node(project)
    [row] = node.load().filter(pl.col("kind") == "context").iter_rows(named=True)
    context = node.child(row)
    assert context.child is None
    assert "memory" in context.load()["info"][0]
    assert list(tmp_path.iterdir()) == []  # not a database file named ':memory:'


def test_bigquery_catalog_chain(monkeypatch) -> None:
    created: list[dict] = []

    class FakeClient:
        def list_datasets(self, page_size=None):
            return [SimpleNamespace(dataset_id="analytics"), SimpleNamespace(dataset_id="raw")]

        def list_tables(self, dataset_id, page_size=None):
            assert dataset_id == "analytics"
            return [SimpleNamespace(table_id="events", table_type="TABLE")]

        def get_table(self, ref):
            assert ref == "analytics.events"
            return SimpleNamespace(
                schema=[
                    _sf("id", "INTEGER", "REQUIRED"),
                    _sf(
                        "event",
                        "RECORD",
                        "REPEATED",
                        [_sf("name", "STRING", description="event name")],
                    ),
                ]
            )

    def make(self, spec):
        created.append(spec)
        return FakeClient()

    monkeypatch.setattr(BigQueryExecutor, "make_client", make)
    node = BigQueryExecutor().context_node({"project": "p"})
    datasets = node.load()
    assert datasets["dataset"].to_list() == ["analytics", "raw"]
    tables_node = node.child(datasets.row(0, named=True))
    tables = tables_node.load()
    assert tables["table"].to_list() == ["events"]
    leaf = tables_node.child(tables.row(0, named=True))
    paths = leaf.load()
    assert "event.name" in paths["field_path"].to_list()
    by_path = {row["field_path"]: row["description"] for row in paths.iter_rows(named=True)}
    assert by_path["event.name"] == "event name"
    assert leaf.child is None
    assert len(created) == 1  # one client, memoized across the whole drill chain


class FakeCatalogPgCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeCatalogPg:
    def __init__(self):
        self.closed = False

    def execute(self, sql, params=None):
        if "schemata" in sql:
            return FakeCatalogPgCursor([("public",), ("analytics",)])
        if "information_schema.tables" in sql:
            return FakeCatalogPgCursor([("users",)])
        if "information_schema.columns" in sql:
            assert "col_description" in sql  # column comments ride along
            return FakeCatalogPgCursor(
                [("id", "integer", "NO", "primary key"), ("email", "text", "YES", None)]
            )
        raise AssertionError(f"unexpected catalog sql: {sql}")

    def close(self):
        self.closed = True


def test_postgres_catalog_chain(monkeypatch) -> None:
    created: list[FakeCatalogPg] = []

    def make(self, dsn):
        assert dsn == "postgresql://x"
        created.append(FakeCatalogPg())
        return created[-1]

    monkeypatch.setattr(PostgresExecutor, "make_connection", make)
    node = PostgresExecutor().context_node("postgresql://x")
    schemas = node.load()
    assert schemas["schema"].to_list() == ["public", "analytics"]
    tables_node = node.child(schemas.row(0, named=True))
    tables = tables_node.load()
    assert tables["table"].to_list() == ["users"]
    leaf = tables_node.child(tables.row(0, named=True))
    cols = leaf.load()
    assert cols.rows() == [
        ("id", "id", "integer", "REQUIRED", "primary key"),
        ("email", "email", "text", "", ""),
    ]
    assert leaf.child is None
    # throwaway connection per load, closed every time
    assert len(created) == 3 and all(c.closed for c in created)


def test_sink_output_node_reads_parquet_struct(tmp_path, con) -> None:
    (tmp_path / "data").mkdir()
    con.execute(
        f"COPY (SELECT 1 AS id, {{'name': 'x'}} AS event)"
        f" TO '{tmp_path}/data/c.parquet' (FORMAT PARQUET)"
    )
    node = sink_output_node("c", resolve_cell({}, {}), tmp_path, BUILTINS)
    frame = node.load()
    assert frame["field_path"].to_list() == ["id", "event", "event.name"]
    assert node.child is None


@pytest.mark.postgres
def test_postgres_catalog_chain_live(pg_dsn) -> None:
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS catalog_live")
        con.execute("CREATE TABLE catalog_live (id int NOT NULL, email text)")
        con.execute("COMMENT ON COLUMN catalog_live.id IS 'primary key'")
    try:
        node = PostgresExecutor().context_node(pg_dsn)
        assert "public" in node.load()["schema"].to_list()
        tables_node = node.child({"schema": "public"})
        assert "catalog_live" in tables_node.load()["table"].to_list()
        cols = tables_node.child({"table": "catalog_live"}).load()
        assert cols.rows() == [
            ("id", "id", "integer", "REQUIRED", "primary key"),
            ("email", "email", "text", "", ""),
        ]
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as con:
            con.execute("DROP TABLE IF EXISTS catalog_live")


# ---------- cache ----------


def _counting_node(key: str, n: int, calls: list[int]) -> CatalogNode:
    def load() -> pl.DataFrame:
        calls.append(n)
        return pl.DataFrame({"n": [n]})

    return CatalogNode(title=f"title{n}", load=load, key=key)


def test_catalog_cache_serves_repeat_loads_by_key() -> None:
    calls: list[int] = []
    cache = CatalogCache()
    frame, cached = cache.load(_counting_node("a", 1, calls))
    assert not cached and calls == [1]
    # drilling recreates nodes; the key, not the instance, is the identity
    frame, cached = cache.load(_counting_node("a", 2, calls))
    assert cached and calls == [1]
    assert frame["n"].to_list() == [1]
    assert len(cache) == 1


def test_catalog_cache_invalidates_one_node_or_everything() -> None:
    calls: list[int] = []
    cache = CatalogCache()
    node_a, node_b = _counting_node("a", 1, calls), _counting_node("b", 2, calls)
    cache.load(node_a)
    cache.load(node_b)
    cache.invalidate(node_a)
    assert len(cache) == 1
    _, cached = cache.load(node_a)
    assert not cached and calls == [1, 2, 1]
    cache.invalidate()
    assert len(cache) == 0
    _, cached = cache.load(node_b)
    assert not cached


def test_catalog_cache_load_failure_caches_nothing() -> None:
    cache = CatalogCache()

    def boom() -> pl.DataFrame:
        raise RuntimeError("offline")

    node = CatalogNode(title="x", load=boom)
    with pytest.raises(RuntimeError):
        cache.load(node)
    assert len(cache) == 0


def test_cache_key_defaults_to_title() -> None:
    frame = pl.DataFrame()
    assert CatalogNode(title="t", load=lambda: frame).cache_key == "t"
    assert CatalogNode(title="t", load=lambda: frame, key="k").cache_key == "k"


def test_node_cache_keys_disambiguate_connections(monkeypatch) -> None:
    # two DSNs / projects must not share cache entries even where display
    # titles collide (e.g. leaf "schema.table" on both servers)
    monkeypatch.setattr(PostgresExecutor, "make_connection", lambda self, dsn: FakeCatalogPg())
    def pg_leaf(dsn: str) -> CatalogNode:
        node = PostgresExecutor().context_node(dsn)
        tables_node = node.child(node.load().row(0, named=True))
        return tables_node.child(tables_node.load().row(0, named=True))

    assert pg_leaf("postgresql://one").cache_key != pg_leaf("postgresql://two").cache_key

    class FakeClient:
        def list_datasets(self, page_size=None):
            return [SimpleNamespace(dataset_id="ds")]

        def list_tables(self, dataset_id, page_size=None):
            return [SimpleNamespace(table_id="t", table_type="TABLE")]

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())

    def bq_leaf(project: str) -> CatalogNode:
        node = BigQueryExecutor().context_node({"project": project})
        tables_node = node.child(node.load().row(0, named=True))
        return tables_node.child(tables_node.load().row(0, named=True))

    assert bq_leaf("p1").cache_key != bq_leaf("p2").cache_key


def test_sink_output_node_duckdb_sink_needs_the_conduit_cursor(tmp_path) -> None:
    cfg = resolve_cell({"output": {"type": "duckdb", "path": "warehouse.db"}}, {})
    conduit = duckdb.connect()
    try:
        sink = make_sink(cfg, tmp_path, BUILTINS)
        sink.prepare(conduit)
        conduit.execute(f"CREATE TABLE {sink.ref_expr('c')} AS SELECT {{'a': 1}} AS s")
        # a throwaway connection cannot re-ATTACH the conduit-held file...
        with pytest.raises(duckdb.Error, match="[Uu]nique file handle"):
            sink_output_node("c", cfg, tmp_path, BUILTINS).load()
        # ...but a cursor of the conduit sees the existing ATTACH
        node = sink_output_node("c", cfg, tmp_path, BUILTINS, connect=conduit.cursor)
        assert node.load()["field_path"].to_list() == ["s", "s.a"]
    finally:
        conduit.close()


# ---------- walking by name (docs/catalog-plan.md, Phase 1) ----------


def _duckdb_project(tmp_path):
    con = duckdb.connect(str(tmp_path / "wh.db"))
    con.execute("CREATE TABLE orders AS SELECT 1 AS id, {'a': 1} AS s")
    con.execute("CREATE SCHEMA sales")
    con.execute("CREATE TABLE sales.orders_2024 AS SELECT 1 AS id")
    con.close()
    return compile_text("/*@ input: { duckdb: wh.db } */\n-- @cell a\nSELECT 1 AS x;\n", root=tmp_path)


def test_a_rows_name_is_its_name_columns_else_name_else_its_first_column() -> None:
    frame = pl.DataFrame()
    plain = CatalogNode(title="t", load=lambda: frame)
    assert plain.row_name({"kind": "context", "name": "duckdb:wh.db"}) == "duckdb:wh.db"
    assert plain.row_name({"dataset": "analytics"}) == "analytics"
    dotted = CatalogNode(title="t", load=lambda: frame, name_columns=("schema", "table"))
    assert dotted.row_name({"schema": "main", "table": "orders"}) == "main.orders"


def test_each_engines_levels_declare_the_columns_that_name_their_rows(tmp_path, monkeypatch) -> None:
    """Declared names let a walk build the next level from a path segment
    without listing the level first."""
    assert DuckDBExecutor().context_node("wh.db", tmp_path).name_columns == ("schema", "table")
    assert SQLiteExecutor().context_node("nums.db", tmp_path).name_columns == ("table",)
    pg = PostgresExecutor().context_node("postgresql://x")
    assert pg.name_columns == ("schema",)
    assert pg.child({"schema": "public"}).name_columns == ("table",)
    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: None)
    bq = BigQueryExecutor().context_node({"project": "p"})
    assert bq.name_columns == ("dataset",)
    assert bq.child({"dataset": "analytics"}).name_columns == ("table",)


def test_resolve_walks_a_duckdb_file_by_names(tmp_path) -> None:
    project = _duckdb_project(tmp_path)
    got = resolve(project_root_node(project), ["duckdb:wh.db", "main.orders"], CatalogCache())
    assert got.node.child is None
    assert got.frame["field_path"].to_list() == ["id", "s", "s.a"]
    assert got.path == ("duckdb:wh.db", "main.orders")
    level = resolve(project_root_node(project), ["duckdb:wh.db"], CatalogCache())
    assert [level.node.row_name(r) for r in level.frame.iter_rows(named=True)] == [
        "main.orders", "sales.orders_2024",
    ]


def test_resolve_walks_sqlite_by_names(tmp_path) -> None:
    db = sqlite3.connect(tmp_path / "nums.db")
    db.execute("CREATE TABLE nums (n INTEGER NOT NULL, label TEXT)")
    db.commit()
    db.close()
    project = compile_text(
        "-- @cell a\n-- @engine: sqlite\n/*@ input: { sqlite: nums.db } */\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    got = resolve(project_root_node(project), ["sqlite:nums.db", "nums"], CatalogCache())
    assert got.frame["field_path"].to_list() == ["n", "label"]


def test_resolve_reads_a_cells_landed_output(tmp_path) -> None:
    from quicksql.runner import run_project

    project = compile_text(
        "/*@ output: { type: parquet, dir: data/ } */\n-- @cell a\nSELECT 1 AS x, {'k': 2} AS s;\n",
        root=tmp_path,
    )
    assert all(r.ok for r in run_project(project))
    got = resolve(project_root_node(project), ["a"], CatalogCache())
    assert got.frame["field_path"].to_list() == ["x", "s", "s.k"]


def test_resolve_names_an_unknown_segment_and_the_names_close_to_it(tmp_path) -> None:
    project = _duckdb_project(tmp_path)
    root = project_root_node(project)
    with pytest.raises(CatalogPathError) as err:
        resolve(root, ["duckdb:wh.db", "main.ordrs"], CatalogCache())
    assert (err.value.depth, err.value.segment, err.value.reason) == (1, "main.ordrs", "unknown")
    assert "main.orders" in err.value.candidates
    assert "main.ordrs" in str(err.value)
    with pytest.raises(CatalogPathError) as err:
        resolve(root, ["duckdb:nope.db"], CatalogCache())
    assert err.value.depth == 0 and "duckdb:wh.db" in err.value.candidates


def test_resolve_refuses_a_step_past_a_leaf(tmp_path) -> None:
    project = _duckdb_project(tmp_path)
    with pytest.raises(CatalogPathError) as err:
        resolve(project_root_node(project), ["duckdb:wh.db", "main.orders", "id"], CatalogCache())
    assert (err.value.depth, err.value.reason) == (2, "leaf")
    memory = compile_text("-- @cell a\nSELECT 1 AS x;\n", root=tmp_path)
    with pytest.raises(CatalogPathError) as err:  # an in-memory context can't be browsed
        resolve(project_root_node(memory), ["duckdb::memory:", "main.t"], CatalogCache())
    assert (err.value.depth, err.value.reason) == (1, "leaf")


class CountingBigQuery:
    """analytics.events only; every call recorded."""

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def list_datasets(self, page_size=None):
        self.calls.append(("list_datasets",))
        return [SimpleNamespace(dataset_id="analytics")]

    def list_tables(self, dataset_id, page_size=None):
        self.calls.append(("list_tables", dataset_id))
        if dataset_id != "analytics":
            raise LookupError(f"Not found: Dataset {dataset_id}")
        return [SimpleNamespace(table_id="events", table_type="TABLE")]

    def get_table(self, ref):
        self.calls.append(("get_table", ref))
        if ref != "analytics.events":
            raise LookupError(f"Not found: Table {ref}")
        return SimpleNamespace(schema=[_sf("id", "INTEGER")])


@pytest.fixture
def bq_project(tmp_path, monkeypatch):
    client = CountingBigQuery()
    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: client)
    project = compile_text(
        "-- @cell a\n-- @engine: bigquery\n/*@ input: { bigquery: p } */\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    return project, client


def test_a_cold_exact_path_loads_only_its_leaf(bq_project) -> None:
    project, client = bq_project
    got = resolve(project_root_node(project), ["bigquery:p", "analytics", "events"], CatalogCache())
    assert got.frame["field_path"].to_list() == ["id"]
    assert client.calls == [("get_table", "analytics.events")]


def test_a_wrong_guess_is_checked_against_the_level_above(bq_project) -> None:
    project, client = bq_project
    with pytest.raises(CatalogPathError) as err:
        resolve(project_root_node(project), ["bigquery:p", "analytcs", "events"], CatalogCache())
    assert (err.value.depth, err.value.segment, err.value.candidates) == (1, "analytcs", ["analytics"])
    assert client.calls == [("get_table", "analytcs.events"), ("list_datasets",)]


def test_a_cached_level_checks_a_segment_without_a_round_trip(bq_project) -> None:
    project, client = bq_project
    cache = CatalogCache()
    resolve(project_root_node(project), ["bigquery:p", "analytics"], cache)  # lists its tables
    calls = list(client.calls)
    with pytest.raises(CatalogPathError) as err:
        resolve(project_root_node(project), ["bigquery:p", "analytics", "evnts"], cache)
    assert err.value.candidates == ["events"]
    assert client.calls == calls


class PgByName:
    """public.users only: unknown schemas and tables list nothing, as
    information_schema does."""

    def __init__(self, log: list[str]) -> None:
        self.log = log

    def execute(self, sql, params=None):
        if "schemata" in sql:
            self.log.append("schemata")
            return FakeCatalogPgCursor([("public",)])
        if "information_schema.tables" in sql:
            self.log.append("tables")
            return FakeCatalogPgCursor([("users",)] if params == ("public",) else [])
        self.log.append("columns")
        rows = [("id", "integer", "NO", None)] if params == ("public", "users") else []
        return FakeCatalogPgCursor(rows)

    def close(self):
        pass


def test_an_empty_leaf_after_a_guess_checks_the_path(tmp_path, monkeypatch) -> None:
    """Postgres answers an unknown table with no columns, not an error."""
    log: list[str] = []
    monkeypatch.setattr(PostgresExecutor, "make_connection", lambda self, dsn: PgByName(log))
    project = compile_text(
        "-- @cell a\n-- @engine: postgres\n/*@ input: { postgres: 'postgresql://x' } */\nSELECT 1;\n",
        root=tmp_path,
    )
    root = project_root_node(project)
    got = resolve(root, ["postgres:postgresql://x", "public", "users"], CatalogCache())
    assert got.frame["field_path"].to_list() == ["id"] and log == ["columns"]
    with pytest.raises(CatalogPathError) as err:
        resolve(root, ["postgres:postgresql://x", "nope", "users"], CatalogCache())
    assert (err.value.depth, err.value.segment) == (1, "nope")


# ---------- pages and filters ----------


def _tables_level(n: int) -> tuple[CatalogNode, pl.DataFrame]:
    frame = pl.DataFrame({"schema": ["main"] * n, "table": [f"t{i:04d}" for i in range(n)]})
    node = CatalogNode(title="duckdb:wh.db", load=lambda: frame, child=lambda row: None,
                       name_columns=("schema", "table"))
    return node, frame


def test_page_slices_rows_names_them_and_says_where_the_next_starts() -> None:
    node, frame = _tables_level(2500)
    first = page(node, frame, offset=0, rows=1000)
    assert (len(first["rows"]), first["total"], first["next_offset"]) == (1000, 2500, 1000)
    assert first["columns"] == ["schema", "table"]
    assert first["rows"][0] == ["main", "t0000"]
    assert first["names"][:2] == ["main.t0000", "main.t0001"]
    assert first["leaf"] is False and first["title"] == "duckdb:wh.db"
    last = page(node, frame, offset=2000, rows=1000)
    assert (len(last["rows"]), last["next_offset"]) == (500, None)


def test_a_leafs_page_has_no_names() -> None:
    from quicksql.executors.base import field_path_frame

    fields = field_path_frame([("id", "id", "INT64", "", "")])
    leaf = CatalogNode(title="t", load=lambda: fields)
    assert page(leaf, fields, offset=0, rows=10)["names"] is None


def test_narrow_matches_row_names_case_insensitively() -> None:
    node, frame = _tables_level(2500)
    got = narrow(node, frame, match="T000")
    assert got["table"].to_list() == [f"t000{i}" for i in range(10)]


def test_narrow_at_a_leaf_matches_field_paths_keeps_top_level_and_picks_columns() -> None:
    from quicksql.executors.base import field_path_frame

    fields = field_path_frame([
        ("id", "id", "INT64", "", ""),
        ("rec", "rec", "RECORD", "REPEATED", ""),
        ("rec", "rec.Customer_id", "INT64", "", "the customer"),
    ])
    leaf = CatalogNode(title="t", load=lambda: fields)
    assert narrow(leaf, fields, match="customer")["field_path"].to_list() == ["rec.Customer_id"]
    assert narrow(leaf, fields, top_level=True)["field_path"].to_list() == ["id", "rec"]
    assert narrow(leaf, fields, columns=["field_path", "type"]).columns == ["field_path", "type"]
    with pytest.raises(ValueError, match="description"):  # names what there is
        narrow(leaf, fields, columns=["nope"])


# ---------- the cache: shared loads, scopes, a cap ----------


def _gated_node(key: str, gate: threading.Event, calls: list[str], fail: bool = False) -> CatalogNode:
    def load() -> pl.DataFrame:
        calls.append(key)
        gate.wait(5)
        if fail:
            raise RuntimeError("offline")
        return pl.DataFrame({"n": [1]})

    return CatalogNode(title=key, load=load)


def _ask_twice(cache: CatalogCache, node: CatalogNode, gate: threading.Event, calls: list[str]):
    """Two askers, the second arriving while the first loads."""
    out: list = []

    def ask() -> None:
        try:
            out.append(cache.load(node))
        except Exception as exc:
            out.append(exc)

    first, second = threading.Thread(target=ask), threading.Thread(target=ask)
    first.start()
    for _ in range(500):
        if calls:
            break
        time.sleep(0.01)
    second.start()
    time.sleep(0.1)  # the second is waiting by now
    gate.set()
    first.join(5)
    second.join(5)
    return out


def test_two_askers_share_one_load() -> None:
    gate, calls, cache = threading.Event(), [], CatalogCache()
    out = _ask_twice(cache, _gated_node("a", gate, calls), gate, calls)
    assert calls == ["a"]
    assert [frame["n"].to_list() for frame, _ in out] == [[1], [1]]


def test_a_shared_load_failure_reaches_every_asker() -> None:
    gate, calls, cache = threading.Event(), [], CatalogCache()
    out = _ask_twice(cache, _gated_node("a", gate, calls, fail=True), gate, calls)
    assert calls == ["a"]
    assert [type(o) for o in out] == [RuntimeError, RuntimeError]
    assert len(cache) == 0


def test_a_load_that_straddles_a_clear_answers_but_isnt_kept() -> None:
    gate, calls, cache, out = threading.Event(), [], CatalogCache(), []
    node = _gated_node("a", gate, calls)
    thread = threading.Thread(target=lambda: out.append(cache.load(node)))
    thread.start()
    for _ in range(500):
        if calls:
            break
        time.sleep(0.01)
    cache.invalidate()  # a run ended while it loaded: what it read may be stale
    gate.set()
    thread.join(5)
    assert out[0][0]["n"].to_list() == [1]
    assert len(cache) == 0


def test_invalidating_scopes_drops_only_their_levels() -> None:
    frame = pl.DataFrame({"n": [1]})
    a = CatalogNode(title="a", load=lambda: frame, scope="duckdb:wh.db")
    b = CatalogNode(title="b", load=lambda: frame, scope="bigquery:p")
    cache = CatalogCache()
    cache.load(a)
    cache.load(b)
    cache.invalidate(scopes={"duckdb:wh.db"})
    assert cache.peek(a) is None and cache.peek(b) is not None


def test_project_root_node_stamps_each_subtree_with_its_rows_name(tmp_path) -> None:
    project = _duckdb_project(tmp_path)
    root = project_root_node(project)
    rows = {row["name"]: row for row in root.load().iter_rows(named=True)}
    context = root.child(rows["duckdb:wh.db"])
    assert context.scope == "duckdb:wh.db"
    assert context.child({"schema": "main", "table": "orders"}).scope == "duckdb:wh.db"
    assert root.child(rows["a"]).scope == "a"
    assert root.scope is None


# ---------- what persists (Phase 3) ----------


def test_remote_engines_declare_that_their_catalogs_persist() -> None:
    assert BigQueryExecutor.catalog_persists and PostgresExecutor.catalog_persists
    assert not DuckDBExecutor.catalog_persists and not SQLiteExecutor.catalog_persists


def test_project_root_node_stamps_persistence_from_each_rows_engine(tmp_path) -> None:
    project = compile_text(
        "/*@ catalog: { bigquery: [other.ds] } */\n"
        "-- @cell bq\n-- @engine: bigquery\n/*@ input: { bigquery: p } */\nSELECT 1 AS x;\n\n"
        "-- @cell pg\n-- @engine: postgres\n/*@ input: { postgres: 'postgresql://u@h/db' } */\n"
        "SELECT 1 AS x;\n\n"
        "-- @cell local\n/*@ input: { sqlite: s.db } */\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    root = project_root_node(project)
    rows = {row["name"]: row for row in root.load().iter_rows(named=True)}
    bq = root.child(rows["bigquery:p"])
    leaf = bq.child({"dataset": "ds"}).child({"table": "t"})
    assert bq.persist and leaf.persist and leaf.scope == "bigquery:p"
    target = root.child(rows["bigquery:other/ds"])
    assert target.persist and target.child({"table": "t"}).persist
    pg = root.child(rows["postgres:postgresql://u@h/db"])
    assert pg.persist and pg.child({"schema": "public"}).child({"table": "t"}).persist
    local = root.child(rows[next(n for n in rows if n.startswith("sqlite:"))])
    assert not local.persist and not local.child({"table": "t"}).persist
    assert not root.child(rows["duckdb::memory:"]).persist
    assert not any(root.child(rows[cell]).persist for cell in ("bq", "pg", "local"))  # outputs
    assert not root.persist


def test_stamp_marks_a_tree_built_outside_the_root(monkeypatch) -> None:
    """The LSP builds a leaf with ``catalog_table``: stamped, it reads and
    writes the same disk level as the Catalog tab's."""
    from quicksql.catalog import stamp

    node = stamp(BigQueryExecutor().context_node({"project": "p"}), scope="bigquery:p", persist=True)
    leaf = node.child({"dataset": "ds"}).child({"table": "t"})
    assert (leaf.scope, leaf.persist) == ("bigquery:p", True)
    assert stamp(None, scope="s", persist=True) is None


def test_bigquery_keys_name_the_endpoint_they_were_read_from() -> None:
    """Two emulators can hold a project of the same name: on disk, across
    sessions, their levels must not meet."""
    def keys(spec: dict) -> list[str]:
        node = BigQueryExecutor().context_node(spec)
        tables = node.child({"dataset": "ds"})
        return [node.cache_key, tables.cache_key, tables.child({"table": "t"}).cache_key]

    real = keys({"project": "p"})
    assert real == ["bigquery:p", "bigquery:p/ds", "bigquery:p/ds.t"]
    one = keys({"project": "p", "endpoint": "http://localhost:9050"})
    two = keys({"project": "p", "endpoint": "http://localhost:9052"})
    assert len({*real, *one, *two}) == 9
    # the title, what the person sees, stays the project's
    node = BigQueryExecutor().context_node({"project": "p", "endpoint": "http://localhost:9050"})
    assert node.title == "bigquery:p" and node.child({"dataset": "ds"}).title == "bigquery:p/ds"


def test_the_cache_keeps_the_most_recently_used_within_its_cap() -> None:
    frame = pl.DataFrame({"s": ["x" * 1000] * 100})
    nodes = {k: CatalogNode(title=k, load=lambda: frame) for k in "abc"}
    cache = CatalogCache(max_bytes=int(frame.estimated_size() * 2.5))
    cache.load(nodes["a"])
    cache.load(nodes["b"])
    cache.load(nodes["a"])  # a is now the most recently used
    cache.load(nodes["c"])
    assert cache.peek(nodes["b"]) is None
    assert cache.peek(nodes["a"]) is not None and cache.peek(nodes["c"]) is not None
    assert cache.size_bytes <= cache.max_bytes


def test_a_level_bigger_than_the_cap_is_still_kept_alone() -> None:
    frame = pl.DataFrame({"n": list(range(100))})
    cache = CatalogCache(max_bytes=10)
    cache.load(CatalogNode(title="a", load=lambda: frame))
    cache.load(CatalogNode(title="b", load=lambda: frame))
    assert len(cache) == 1 and cache.peek(CatalogNode(title="b", load=lambda: frame)) is not None


# ---------- redaction and BigQuery pages ----------


def test_a_keyword_dsns_password_is_hidden_even_without_an_equals_sign() -> None:
    name, _ = PostgresExecutor().catalog_target("host=h password s3cret dbname=d", Path("."))
    assert "s3cret" not in name and name.startswith("postgres:host=h password ")


def test_postgres_redacts_passwords_in_free_text() -> None:
    pg = PostgresExecutor()
    text = "could not connect to postgresql://u:s3cret@h/db (password=other sslpassword='x y')"
    out = pg.redact(text)
    assert "s3cret" not in out and "other" not in out and "x y" not in out
    assert "postgresql://u:***@h/db" in out
    assert pg.redact('password authentication failed for user "u"') == (
        'password authentication failed for user "u"'
    )


def test_catalog_redact_asks_every_engine(registries) -> None:
    from quicksql.executors.base import Executor
    from quicksql.registry import executor

    @executor("vault")
    class Vault(Executor):
        def open_session(self, config, ctx):
            raise NotImplementedError

        def redact(self, text):
            return text.replace("TOPSECRET", "***")

    registries.add(Vault)
    assert redact("TOPSECRET and postgresql://u:pw@h/db", registries) == (
        "*** and postgresql://u:***@h/db"
    )


def test_bigquery_listings_ask_for_full_pages(monkeypatch) -> None:
    asked: list[tuple] = []

    class FakeClient:
        def list_datasets(self, page_size=None):
            asked.append(("datasets", page_size))
            return []

        def list_tables(self, dataset_id, page_size=None):
            asked.append(("tables", page_size))
            return []

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    node = BigQueryExecutor().context_node({"project": "p"})
    node.load()
    node.child({"dataset": "d"}).load()
    assert BigQueryExecutor.page_size == 1000
    assert asked == [("datasets", 1000), ("tables", 1000)]
