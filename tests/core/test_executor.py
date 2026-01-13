"""Tests for the executor module."""

from quicksql.core.executor import ExecutorBuilder
from quicksql.core.file import QsqlFile
from textwrap import dedent
import duckdb
import ibis
import pytest


def test_executor_builder_basic(user_table, tmp_path):
    """Test basic executor creation and execution."""
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    result = executor.execute_cell("query_1")

    assert result is not None
    assert len(result) == 3  # 3 users from fixture


def test_executor_with_multiple_backends(user_table, tmp_path):
    """Test executor with multiple backend connections."""
    # Create second database with fruits
    second_db_path = tmp_path / "another.ddb"
    conn = duckdb.connect(str(second_db_path))

    df = ibis.memtable(
        {
            "id": [1, 2, 3],
            "name": ["apple", "orange", "banana"],
        }
    )

    conn.sql("CREATE TABLE fruits AS SELECT * FROM df")
    conn.close()

    # SQL file with header config and cell-level override
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;

        -- name: query_2
        /*
        input: {tmp_path}/another.ddb
        */
        SELECT *
        FROM fruits
        WHERE name = 'apple';
        """).strip()

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    results = executor.execute_many(["query_1", "query_2"])

    assert "query_1" in results
    assert "query_2" in results
    assert len(results["query_1"]) == 3  # 3 users
    assert len(results["query_2"]) == 1  # 1 apple


def test_executor_with_explicit_backend_config(user_table, tmp_path):
    """Test executor with explicit backend specification."""
    sql_text = dedent(f"""
        /*
        input:
            duckdb: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    result = executor.execute_cell("query_1")

    assert result is not None
    assert len(result) == 3


def test_executor_with_logging_decorator(user_table, tmp_path, caplog):
    """Test executor with logging decorator."""
    import logging

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)

    with caplog.at_level(logging.INFO):
        executor = ExecutorBuilder(q_file).with_logging().build()
        executor.execute_cell("query_1")

    assert "Executing cell 'query_1'" in caplog.text
    assert "completed" in caplog.text


def test_executor_cell_not_found(user_table, tmp_path):
    """Test executor raises error for non-existent cell."""
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    with pytest.raises(ValueError, match="Cell 'nonexistent' not found"):
        executor.execute_cell("nonexistent")


def test_executor_validation_collects_all_errors(tmp_path):
    """Test that validation collects all errors before raising."""
    from quicksql.core.errors import QsqlConfigError

    # Create SQL with multiple invalid configs
    sql_text = dedent("""
        /*
        input: ./unknown1.xyz
        */
        -- name: query_1
        SELECT 1;

        -- name: query_2
        /*
        input: ./unknown2.abc
        */
        SELECT 2;
        """).strip()

    tmp_file = tmp_path / "test_validation.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)

    with pytest.raises(QsqlConfigError) as exc_info:
        ExecutorBuilder(q_file).build()

    error = exc_info.value
    # Should have collected errors from both cells
    assert len(error.errors) == 2
    assert "query_1" in str(error)
    assert "query_2" in str(error)


def test_executor_without_validation(tmp_path):
    """Test executor can skip validation."""
    # Create SQL with invalid config
    sql_text = dedent("""
        /*
        input: ./unknown.xyz
        */
        -- name: query_1
        SELECT 1;
        """).strip()

    tmp_file = tmp_path / "test_no_validation.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)

    # Should not raise when validation is skipped
    executor = ExecutorBuilder(q_file).without_validation().build()
    assert executor is not None


def test_executor_refresh_preserves_connections(user_table, tmp_path):
    """Test that refresh() preserves backend connections."""
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_refresh.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    # Execute to establish connection
    result1 = executor.execute_cell("query_1")
    assert len(result1) == 3

    # Get reference to connection cache (internal implementation detail)
    # We access _backend_conns to verify connections are preserved
    from quicksql.core.executor import BaseExecutor

    base_executor = executor
    while not isinstance(base_executor, BaseExecutor):
        base_executor = base_executor._wrapped  # type: ignore

    conn_cache_before = dict(base_executor._backend_conns)
    assert len(conn_cache_before) == 1

    # Modify the file and refresh
    sql_text_updated = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user WHERE name = 'Alice';
        """).strip()

    tmp_file.write_text(sql_text_updated)
    q_file_updated = QsqlFile(tmp_file)
    executor.refresh(q_file_updated)

    # Verify connection cache is preserved
    conn_cache_after = dict(base_executor._backend_conns)
    assert conn_cache_before == conn_cache_after

    # Execute again - should use same connection
    result2 = executor.execute_cell("query_1")
    assert len(result2) == 1  # Only Alice now


def test_executor_refresh_picks_up_new_cells(user_table, tmp_path):
    """Test that refresh() picks up newly added cells."""
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_refresh_new_cells.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).build()

    # Initially only query_1 exists
    result1 = executor.execute_cell("query_1")
    assert len(result1) == 3

    with pytest.raises(ValueError, match="Cell 'query_2' not found"):
        executor.execute_cell("query_2")

    # Add a new cell
    sql_text_updated = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;

        -- name: query_2
        SELECT COUNT(*) as cnt FROM user;
        """).strip()

    tmp_file.write_text(sql_text_updated)
    q_file_updated = QsqlFile(tmp_file)
    executor.refresh(q_file_updated)

    # Now query_2 should work
    result2 = executor.execute_cell("query_2")
    assert result2["cnt"][0] == 3


def test_executor_refresh_through_decorator(user_table, tmp_path):
    """Test that refresh() works through decorator chain."""
    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_refresh_decorator.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).with_logging().build()

    result1 = executor.execute_cell("query_1")
    assert len(result1) == 3

    # Modify and refresh
    sql_text_updated = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user WHERE name = 'Bob';
        """).strip()

    tmp_file.write_text(sql_text_updated)
    q_file_updated = QsqlFile(tmp_file)
    executor.refresh(q_file_updated)

    result2 = executor.execute_cell("query_1")
    assert len(result2) == 1


def test_executor_with_output_writes_parquet(user_table, tmp_path):
    """Test that output decorator writes results to parquet files."""
    import polars as pl

    output_dir = tmp_path / "output"

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        output: {output_dir}
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_output.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).with_output().build()

    # Execute - should write to parquet
    result = executor.execute_cell("query_1")
    assert len(result) == 3

    # Check parquet file was created
    parquet_file = output_dir / "query_1.parquet"
    assert parquet_file.exists()

    # Read back and verify contents
    df = pl.read_parquet(parquet_file)
    assert len(df) == 3
    assert "name" in df.columns


def test_executor_output_creates_directory(user_table, tmp_path):
    """Test that output decorator creates directory if it doesn't exist."""
    output_dir = tmp_path / "nested" / "output" / "dir"

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        output: {output_dir}
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_output_mkdir.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).with_output().build()

    # Directory shouldn't exist yet
    assert not output_dir.exists()

    # Execute - should create directory and write parquet
    executor.execute_cell("query_1")

    # Directory and file should now exist
    assert output_dir.exists()
    assert (output_dir / "query_1.parquet").exists()


def test_executor_output_cell_level_override(user_table, tmp_path):
    """Test that output can be configured at cell level."""
    import polars as pl

    output_dir_1 = tmp_path / "output1"
    output_dir_2 = tmp_path / "output2"

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        output: {output_dir_1}
        */
        -- name: query_1
        SELECT * FROM user;

        -- name: query_2
        /*
        output: {output_dir_2}
        */
        SELECT * FROM user WHERE name = 'Alice';
        """).strip()

    tmp_file = tmp_path / "test_output_override.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).with_output().build()

    executor.execute_many(["query_1", "query_2"])

    # query_1 should be in output_dir_1
    assert (output_dir_1 / "query_1.parquet").exists()
    df1 = pl.read_parquet(output_dir_1 / "query_1.parquet")
    assert len(df1) == 3

    # query_2 should be in output_dir_2
    assert (output_dir_2 / "query_2.parquet").exists()
    df2 = pl.read_parquet(output_dir_2 / "query_2.parquet")
    assert len(df2) == 1


def test_executor_no_output_when_not_configured(user_table, tmp_path):
    """Test that no output is written when output is not configured."""
    output_dir = tmp_path / "output"

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_no_output.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)
    executor = ExecutorBuilder(q_file).with_output().build()

    # Execute
    result = executor.execute_cell("query_1")
    assert len(result) == 3

    # No output directory should be created
    assert not output_dir.exists()


def test_executor_output_with_logging(user_table, tmp_path, caplog):
    """Test that output decorator works with logging decorator."""
    import logging
    import polars as pl

    output_dir = tmp_path / "output"

    sql_text = dedent(f"""
        /*
        input: {tmp_path}/test.ddb
        output: {output_dir}
        */
        -- name: query_1
        SELECT * FROM user;
        """).strip()

    tmp_file = tmp_path / "test_output_logging.sql"
    tmp_file.write_text(sql_text)

    q_file = QsqlFile(tmp_file)

    with caplog.at_level(logging.INFO):
        executor = ExecutorBuilder(q_file).with_logging().with_output().build()
        executor.execute_cell("query_1")

    # Check logging occurred
    assert "Executing cell 'query_1'" in caplog.text
    assert "completed" in caplog.text

    # Check output was written
    assert (output_dir / "query_1.parquet").exists()
    df = pl.read_parquet(output_dir / "query_1.parquet")
    assert len(df) == 3
