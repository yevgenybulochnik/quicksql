from quicksql import QsqlManager


def test_qsql_manager_header_config(qsql_file_basic):
    q_manager = QsqlManager(qsql_file_basic)

    expected_header_dict = {"input": {"duckdb": "/tmp/test.ddb"}}
    assert q_manager.header == expected_header_dict
