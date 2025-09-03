from textwrap import dedent
import pytest
from quicksql.qsql import CellParser, CellConfig, FileParser, QSqlRunner, Cell


cases = [
    (
        """
        -- name: test_sql_statement
        SELECT 1 + 1
        """,
        {
            "name": "test_sql_statement",
        },
    ),
    (
        """
        -- name: test_sql_statement random_additional string
        """,
        {
            "name": "test_sql_statement",
        },
    ),
    (
        """
        -- name: test_sql_statement random: key
        -- random_key: hello
        """,
        {"name": "test_sql_statement", "random_key": "hello"},
    ),
    (
        """
        -- name: test_sql_statement
        -- random comment
        """,
        {"name": "test_sql_statement"},
    ),
    (
        """
        -- name: test_sql_statement
        -- auto_run: false
        -- vars:
        /*
         my_var1: test
         my_var2: 1
         my_var3: ['a', 'b', 'c']
         my_var4:
            - 1
            - 2
            - 3
        */
        -- something random
        SELECT 1 + 1
        /* multline comment */
        /* additional
        multiline comment
        */ 
        """,
        {
            "name": "test_sql_statement",
            "auto_run": False,
            "vars": {
                "my_var1": "test",
                "my_var2": 1,
                "my_var3": ["a", "b", "c"],
                "my_var4": [1, 2, 3],
            },
        },
    ),
]


@pytest.mark.parametrize("config_string, expected", cases)
def test_parse_cell_config(config_string, expected):
    config_string = dedent(config_string.strip())

    config = CellParser()._parse_config(config_string)

    assert config == expected


file_content = """
-- Config:
/*
output_dir: ./data
*/

-- name: test_sql_statement
SELECT 1 + 1

-- name: test_sql_statement_2
-- auto_run: False
SELECT
    "a" AS col1,
    "b" as col2
"""


def test_parse_file_from_string():

    file_string = dedent(file_content).strip()
    file_parser = FileParser.from_string(file_string)
    cell_blocks = file_parser.cell_blocks

    assert len(cell_blocks) == 2
    assert cell_blocks[0] == {
        "cell_block_name": "test_sql_statement",
        "cell_start": 5,
        "cell_end": 8,
        "text": "-- name: test_sql_statement",
    }
    assert cell_blocks[1] == {
        "cell_block_name": "test_sql_statement_2",
        "cell_start": 8,
        "cell_end": 13,
        "text": "-- name: test_sql_statement_2",
    }


def test_parse_file_from_file(tmp_path):
    file_path = tmp_path / "test.sql"
    file_path.write_text(dedent(file_content).strip())
    file_parser = FileParser.from_file(file_path)
    cell_blocks = file_parser.cell_blocks

    assert len(file_parser.lines) == 13
    assert len(cell_blocks) == 2
    assert cell_blocks[0] == {
        "cell_block_name": "test_sql_statement",
        "cell_start": 5,
        "cell_end": 8,
        "text": "-- name: test_sql_statement",
    }
    assert cell_blocks[1] == {
        "cell_block_name": "test_sql_statement_2",
        "cell_start": 8,
        "cell_end": 13,
        "text": "-- name: test_sql_statement_2",
    }


def test_qsql_runner(tmp_path):
    file_content = """
        -- Config:
        /*
        output_dir: ./data
        */

        -- name: test_sql_statement
        SELECT 1 + 1

        -- name: test_sql_statement_2
        -- auto_run: False
        SELECT
            "a" AS col1,
            "b" as col2

        -- name: test_jinja_vars
        {% set my_var = 'Hello, World!' %}
        SELECT '{{ my_var }}' AS greeting

        -- name: test_jinja_custom_vars
        -- vars:
        /*
            my_var1: test
            my_var2: 1
        */
        SELECT '{{ my_var1 }}' AS col1, {{ my_var2 }} AS col2
    """
    file_path = tmp_path / "test.sql"
    file_path.write_text(dedent(file_content).strip())

    runner = QSqlRunner(
        file_path=file_path,
        file_parser=FileParser.from_file(file_path),
        cell_parser=CellParser(),
        cell_model=Cell,
        config=None,
        queue=[],
    )
    runner.create_cells()
    print(runner.cells[3].sql)

    assert False
