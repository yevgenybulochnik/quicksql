from pathlib import Path
import pytest
import duckdb
import ibis


@pytest.fixture
def db_conn(tmp_path):
    db_path = tmp_path / "test.ddb"
    conn = duckdb.connect(str(db_path))
    yield conn
    conn.close()


@pytest.fixture
def user_table(db_conn: duckdb.DuckDBPyConnection) -> None:
    df = ibis.memtable(  # noqa: F841
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )
    db_conn.sql(
        """
        CREATE TABLE user AS SELECT * FROM df
        """
    )


@pytest.fixture
def qsql_file_basic() -> Path:
    file_path = Path("tests/qsql_basic.sql").absolute()
    return file_path
