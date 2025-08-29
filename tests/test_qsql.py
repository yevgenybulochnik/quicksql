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


