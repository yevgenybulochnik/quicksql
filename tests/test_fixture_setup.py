"""
Unit tests to verify module level fixture setup for the duckdb connection and
mock user table. Duckdb is setup to be in memory and the db gets reset every
test function run.
"""

import duckdb


def test_db_connection(db_conn: duckdb.DuckDBPyConnection) -> None:
    result = db_conn.sql("SELECT 1 + 1 ").fetchone()

    assert result is not None
    assert result[0] == 2


def test_user_table_setup(db_conn: duckdb.DuckDBPyConnection, user_table) -> None:
    result = db_conn.sql("SELECT * FROM user").fetchall()

    assert len(result) == 5
    assert result[0][1] == "Alice"
    assert result[4][2] == 40


def test_create_user(db_conn: duckdb.DuckDBPyConnection, user_table) -> None:
    db_conn.sql(
        """
        INSERT INTO user
        VALUES (
            6,
            'Eugene',
            37
        )
        """
    )
    result = db_conn.sql("SELECT * FROM user;").fetchall()

    assert len(result) == 6
    assert result[5][1] == "Eugene"


def test_create_user2(db_conn: duckdb.DuckDBPyConnection, user_table) -> None:
    """
    This test demonstrates the db is recreated on every test function call
    """
    db_conn.sql(
        """
        INSERT INTO user
        VALUES (
            6,
            'Matt',
            62
        )
        """
    )
    result = db_conn.sql("SELECT * FROM user;").fetchall()

    assert len(result) == 6
    assert result[5][1] == "Matt"


def test_qsql_example_file(qsql_file):
    qsql_string = """/*
output_dir: ./data
vars:
  my_var_1: Alice
  my_var_2: 25
  my_table: user
*/

-- Name: first_query
SELECT * FROM user;

-- Name: second_query
SELECT *
FROM user
WHERE name = {{ my_var_1 }};

-- Name: third_query
SELECT *
FROM user
WHERE age = {{ my_var_2 }};

-- Name: fourth_query
SELECT
  name
FROM {{ my_table }};
"""
    assert qsql_file == qsql_string
