import duckdb
import pytest

from quicksql.errors import ConfigError, SinkError
from quicksql.models import RenderedCell
from quicksql.registry import SINKS


def _cell(name: str) -> RenderedCell:
    return RenderedCell(
        name=name, config=None, sql_raw="", sql="", hash="",
        engine="duckdb", sink_type="parquet",
    )


@pytest.fixture
def conn():
    c = duckdb.connect()
    c.execute("CREATE TEMP VIEW v AS SELECT 1 AS x UNION ALL SELECT 2")
    yield c
    c.close()


def test_parquet_sink_lands_file_named_after_cell(conn, tmp_path) -> None:
    sink = SINKS.get("parquet")({}, tmp_path)
    rows, target = sink.write(_cell("users"), "v", conn)
    assert rows == 2
    assert target == str(tmp_path / "data" / "users.parquet")
    assert (tmp_path / "data" / "users.parquet").exists()
    assert sink.ref_expr("users") == f"read_parquet('{tmp_path}/data/users.parquet')"


def test_parquet_sink_lands_hugeint_as_decimal_not_double(conn, tmp_path) -> None:
    # duckdb's parquet writer stores HUGEINT as DOUBLE — silent precision loss;
    # the sink casts to DECIMAL(38,0) so values land exactly
    conn.execute(
        "CREATE TEMP VIEW hv AS SELECT 123456789012345678901234567890::HUGEINT AS h, 1 AS i"
    )
    sink = SINKS.get("parquet")({}, tmp_path)
    rows, target = sink.write(_cell("sums"), "hv", conn)
    assert rows == 1
    kind, value = conn.sql(f"SELECT typeof(h), h FROM read_parquet('{target}')").fetchone()
    assert kind == "DECIMAL(38,0)"
    assert int(value) == 123456789012345678901234567890

    import polars as pl

    frame = pl.read_parquet(target)
    assert str(frame["h"][0]) == "123456789012345678901234567890"
    assert frame["i"].dtype == pl.Int32  # untouched columns stay as they were


def test_parquet_sink_hugeint_overflow_fails_loudly(tmp_path) -> None:
    from quicksql.compiler import compile_text

    project = compile_text(
        "-- @cell too_big\nSELECT 170141183460469231731687303715884105727::HUGEINT AS h;",
        root=tmp_path,
    )
    result = project.run()[0]
    assert result.ok is False  # a conversion error beats silent corruption
    assert "conversion" in result.error.lower() or "range" in result.error.lower()


def test_parquet_sink_honors_dir(conn, tmp_path) -> None:
    sink = SINKS.get("parquet")({"dir": "out/"}, tmp_path)
    _, target = sink.write(_cell("u"), "v", conn)
    assert target == str(tmp_path / "out" / "u.parquet")


def test_duckdb_sink_writes_table_readable_via_ref_expr(conn, tmp_path) -> None:
    sink = SINKS.get("duckdb")({"path": "wh.db"}, tmp_path)
    sink.prepare(conn)
    rows, target = sink.write(_cell("landed"), "v", conn)
    assert rows == 2
    assert target.endswith("::main.landed")
    assert conn.sql(f"SELECT count(*) FROM {sink.ref_expr('landed')}").fetchone() == (2,)
    assert (tmp_path / "wh.db").exists()


def test_duckdb_sink_append_mode_accumulates_and_reports_the_rows_it_inserted(
    conn, tmp_path
) -> None:
    sink = SINKS.get("duckdb")({"path": "wh.db", "mode": "append"}, tmp_path)
    sink.prepare(conn)
    sink.write(_cell("acc"), "v", conn)
    rows, _ = sink.write(_cell("acc"), "v", conn)
    assert rows == 2  # this run's rows, not the table's total
    assert conn.sql(f"SELECT count(*) FROM {sink.ref_expr('acc')}").fetchone() == (4,)


def test_duckdb_sink_custom_schema(conn, tmp_path) -> None:
    sink = SINKS.get("duckdb")({"path": "wh.db", "schema": "analytics"}, tmp_path)
    sink.prepare(conn)
    _, target = sink.write(_cell("t"), "v", conn)
    assert target.endswith("::analytics.t")
    assert '."analytics"."t"' in sink.ref_expr("t")


def test_postgres_sink_dsn_expands_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PG_DSN", "host=db user=me")
    sink = SINKS.get("postgres")({"dsn": "$PG_DSN", "table": "analytics.active"}, tmp_path)
    assert sink.dsn == "host=db user=me"
    assert sink.ref_expr("active") .endswith('."analytics"."active"')
    assert sink.requires == ["postgres"]


def test_postgres_sink_missing_dsn_raises(tmp_path) -> None:
    sink = SINKS.get("postgres")({}, tmp_path)
    with pytest.raises(SinkError, match="dsn"):
        sink.dsn


def test_none_sink_lands_nothing_and_is_unreadable(conn, tmp_path) -> None:
    sink = SINKS.get("none")({}, tmp_path)
    assert sink.lands_output is False
    rows, target = sink.write(_cell("effect"), "v", conn)
    assert rows == 0  # nothing lands; the view is ignored
    with pytest.raises(ConfigError, match="lands nothing"):
        sink.ref_expr("effect")


def test_landing_sinks_report_lands_output_true(tmp_path) -> None:
    assert SINKS.get("parquet")({}, tmp_path).lands_output is True
    assert SINKS.get("duckdb")({"path": "w.db"}, tmp_path).lands_output is True
