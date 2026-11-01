"""BigQuery-shaped Arrow types through the conduit.

duckdb cannot scan decimal256 (BigQuery BIGNUMERIC) at any precision, so
register_frame normalizes it: decimal128 when precision fits, string otherwise.
The other BQ mappings (decimal128, month_day_nano intervals, extension types,
null columns) were probed and scan fine — no code paths exist for them.
"""

import decimal

import duckdb
import polars as pl
import pytest

pa = pytest.importorskip("pyarrow")

from quicksql.bootstrap import builtin_registries, compile_text
from quicksql.executors.base import register_frame
from quicksql.executors.bigquery_exec import BigQueryExecutor
from quicksql.models import RunContext


@pytest.fixture
def ctx(tmp_path):
    conn = duckdb.connect()
    yield RunContext(conn=conn, root=tmp_path, registries=builtin_registries())
    conn.close()


def test_small_decimal256_downcasts_to_decimal128(ctx) -> None:
    table = pa.table(
        {"amount": pa.array([decimal.Decimal("12.3456")], pa.decimal256(20, 4))}
    )
    register_frame(ctx, "v", table)
    ((value,),) = ctx.conn.sql('SELECT amount FROM "v"').fetchall()
    assert value == decimal.Decimal("12.3456")
    assert ctx.conn.sql('SELECT typeof(amount) FROM "v"').fetchone()[0] == "DECIMAL(20,4)"
    assert ctx.log == []  # lossless downcast is silent


def test_wide_decimal256_casts_to_string_and_logs(ctx) -> None:
    table = pa.table(
        {"big": pa.array([decimal.Decimal("123.45")], pa.decimal256(76, 38))}
    )
    register_frame(ctx, "v", table)
    ((value,),) = ctx.conn.sql('SELECT big FROM "v"').fetchall()
    assert decimal.Decimal(value) == decimal.Decimal("123.45")  # lossless text
    assert ctx.conn.sql('SELECT typeof(big) FROM "v"').fetchone()[0] == "VARCHAR"
    assert any("big" in line and "decimal256" in line for line in ctx.log)


def test_mixed_table_only_touches_decimal256_columns(ctx) -> None:
    table = pa.table(
        {
            "id": pa.array([1], pa.int64()),
            "numeric": pa.array([decimal.Decimal("1.5")], pa.decimal128(38, 9)),
            "bignumeric": pa.array([decimal.Decimal("2.5")], pa.decimal256(76, 38)),
        }
    )
    register_frame(ctx, "v", table)
    types = dict(
        ctx.conn.sql(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM \"v\")"
        ).fetchall()
    )
    assert types["id"] == "BIGINT"
    assert types["numeric"] == "DECIMAL(38,9)"
    assert types["bignumeric"] == "VARCHAR"


def test_bignumeric_cell_lands_end_to_end(tmp_path, monkeypatch) -> None:
    result_table = pa.table(
        {
            "user_id": pa.array([1, 2], pa.int64()),
            "balance": pa.array(
                [decimal.Decimal("10.5"), decimal.Decimal("20.25")],
                pa.decimal256(76, 38),
            ),
        }
    )

    class FakeJob:
        def to_arrow(self):
            return result_table

    class FakeClient:
        def query(self, sql):
            return FakeJob()

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    project = compile_text(
        "-- @cell balances\n/*@ input: { bigquery: { project: p } } */\n"
        "SELECT user_id, balance FROM accounts;",
        root=tmp_path,
    )
    result = project.run()[0]
    assert result.ok, result.error
    assert result.rows == 2
    landed = pl.read_parquet(tmp_path / "data" / "balances.parquet")
    assert [decimal.Decimal(v) for v in landed["balance"]] == [
        decimal.Decimal("10.5"),
        decimal.Decimal("20.25"),
    ]
