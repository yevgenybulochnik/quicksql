"""Test Cases for Qsql File Class"""

from quicksql import QsqlFile


def test_qsql_file_lines(qsql_file_basic):
    q_file = QsqlFile(qsql_file_basic)

    assert len(q_file.lines) == 14
    assert q_file.lines[0] == (0, "/*")
    assert q_file.lines[1] == (1, "input:")
    assert q_file.lines[2] == (2, "  duckdb: /tmp/test.ddb")
    assert q_file.lines[3] == (3, "*/")
    assert q_file.lines[4] == (4, "-- name: query_1")
    assert q_file.lines[5] == (5, "SELECT * FROM user;")


def test_qsql_file_code_blocks(qsql_file_basic):
    q_file = QsqlFile(qsql_file_basic)

    assert len(q_file.cell_blocks) == 3

    # Check first cell block
    assert q_file.cell_blocks[0]["cell_name"] == "query_1"
    assert q_file.cell_blocks[0]["cell_start"] == 4
    assert q_file.cell_blocks[0]["cell_end"] == 6
    assert q_file.cell_blocks[0]["text"] == "-- name: query_1\nSELECT * FROM user;\n"

    # Check second cell block
    assert q_file.cell_blocks[1]["cell_name"] == "query_2"
    assert q_file.cell_blocks[1]["cell_start"] == 7
    assert q_file.cell_blocks[1]["cell_end"] == 11
    assert (
        q_file.cell_blocks[1]["text"]
        == "-- name: query_2\nSELECT *\nFROM user\nWHERE name = 'Alice';\n"
    )

    # Check third cell block
    assert q_file.cell_blocks[2]["cell_name"] == "query_3"
    assert q_file.cell_blocks[2]["cell_start"] == 12
    assert q_file.cell_blocks[2]["cell_end"] == 13
    assert (
        q_file.cell_blocks[2]["text"]
        == "-- name: query_3\nSELECT COUNT(*) AS user_count;"
    )


def test_qsql_file_header(qsql_file_basic):
    q_file = QsqlFile(qsql_file_basic)

    expected_header = "/*\ninput:\n  duckdb: /tmp/test.ddb\n*/"
    assert q_file.header == expected_header
