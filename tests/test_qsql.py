from textwrap import dedent
import pytest
from quicksql.qsql import CellParser, CellConfig


cases = [
    (
        """
        -- name: test_sql_statement
        SELECT 1 + 1
        """,
        {
            "name": "test_sql_statement",
            "auto_run": True,
            "vars": None,
        },
    ),
    (
        """
        -- name: test_sql_statement random_additional string
        """,
        {
            "name": "test_sql_statement",
            "auto_run": True,
            "vars": None,
        },
    ),
    (
        """
        -- name: test_sql_statement random: key
        """,
        {
            "name": "test_sql_statement",
            "auto_run": True,
            "vars": None,
        },
    ),
    (
        """
        -- name: test_sql_statement
        -- random comment
        """,
        {"name": "test_sql_statement", "auto_run": True, "vars": None},
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


@pytest.mark.parametrize("config_string, expected_json", cases)
def test_parse_cell_config(config_string, expected_json):
    config_string = dedent(config_string.strip())

    config = CellParser(CellConfig).parse_config(config_string)

    assert config.model_dump() == expected_json
