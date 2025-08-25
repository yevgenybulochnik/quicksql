import pytest
import duckdb
import polars as pl


@pytest.fixture
def db_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(":memory:")  # pyright: ignore[reportUnknownMemberType]


@pytest.fixture
def user_table(db_conn: duckdb.DuckDBPyConnection) -> None:
    records = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
        {"id": 4, "name": "Diana", "age": 28},
        {"id": 5, "name": "Ethan", "age": 40},
    ]
    df = pl.from_records(records)
    db_conn.sql(
        f"""
        CREATE TABLE user AS SELECT * FROM df
        """
    )
