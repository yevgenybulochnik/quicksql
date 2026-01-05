from quicksql.core.manager import QsqlExecutor
from quicksql.core.file import QsqlFile
from io import StringIO
from textwrap import dedent
import duckdb
import ibis
import pytest
from pprint import pprint


def test_qsql_executor_init(user_table, tmp_path):
    second_db_path = tmp_path / "another.ddb"
    conn = duckdb.connect(str(second_db_path))

    df = ibis.memtable(
        {
            "id": [1, 2, 3],
            "name": ["apple", "orange", "banana"],
        }
    )

    conn.sql(
        """
        CREATE TABLE fruits AS SELECT * FROM df
        """
    )

    print(tmp_path)

    sql_text = StringIO(
        dedent(f"""
        /*
        input:
            duckdb: {tmp_path}/test.ddb
        */
        -- name: query_1
        SELECT * FROM user;

        -- name: query_2
        /*
        input:
            duckdb: {tmp_path}/another.ddb
        */
        SELECT *
        FROM fruits
        WHERE name = 'apple';

        """)
    )

    tmp_file = tmp_path / "test_executor.sql"
    tmp_file.write_text(sql_text.read().strip())

    q_file = QsqlFile(tmp_file)

    executor = QsqlExecutor(q_file)

    results = executor.execute_many(["query_1", "query_2"])

    print(results)

    assert False
