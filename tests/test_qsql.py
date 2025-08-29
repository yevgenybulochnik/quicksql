from textwrap import dedent
import pytest
from quicksql.qsql import CellParser, CellConfig, FileParser


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


def test_parse_file():
    file_string = """
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

    file_string = dedent(file_string).strip()

    file_parser = FileParser()

    cell_blocks = file_parser._parse_cell_blocks(file_string)
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
