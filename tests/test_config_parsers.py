from quicksql.parsers import KeyValueParser, DictLikeParser


def test_key_value_parser():
    parser = KeyValueParser()

    result = parser.parse(
        """
        -- key1: value1
        -- key2: value2
        """
    )

    expected_output = {"key1": "value1", "key2": "value2"}

    assert result == expected_output


def test_dict_like_parser():
    parser = DictLikeParser()

    result = parser.parse(
        """
        /*
        input:
          duckdb: /tmp/test.ddb
        */
        """
    )

    expected_output = {"input": {"duckdb": "/tmp/test.ddb"}}

    assert result == expected_output
