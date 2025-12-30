import duckdb


def test_db_conn_fixture(db_conn: duckdb.DuckDBPyConnection):
    """Test that db_conn fixture creates a valid DuckDB connection."""
    assert db_conn is not None
    assert isinstance(db_conn, duckdb.DuckDBPyConnection)

    # Test basic functionality
    result = db_conn.execute("SELECT 1 as test_col").fetchall()
    assert result == [(1,)]


def test_user_table_fixture(db_conn: duckdb.DuckDBPyConnection, user_table: None):
    """Test that user_table fixture creates a user table with expected data."""
    # Query the user table
    result = db_conn.execute("SELECT * FROM user ORDER BY id").fetchall()

    # Expected data
    expected = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]

    assert result == expected

    # Test table structure
    table_info = db_conn.execute("DESCRIBE user").fetchall()
    column_names = [col[0] for col in table_info]
    expected_columns = ["id", "name", "age"]
    assert column_names == expected_columns
