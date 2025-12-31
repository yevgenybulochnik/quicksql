"""Test QSQLManager with explicit parser injection."""

from pathlib import Path
from quicksql import QsqlManager
from quicksql.parsers import KeyValueParser, DictLikeParser


def test_qsql_manager_with_explicit_parsers(qsql_file_basic: Path):
    """Test QSQLManager with explicitly provided parsers."""
    # Create specific parser instances
    parsers = [DictLikeParser(), KeyValueParser()]

    # Inject parsers explicitly
    q_manager = QsqlManager(qsql_file_basic, parsers=parsers)

    expected_header_dict = {"input": {"duckdb": "/tmp/test.ddb"}}
    assert q_manager.header == expected_header_dict


def test_qsql_manager_with_single_parser(qsql_file_basic: Path):
    """Test QSQLManager with only one parser."""
    # Use only DictLikeParser
    parsers = [DictLikeParser()]

    q_manager = QsqlManager(qsql_file_basic, parsers=parsers)

    expected_header_dict = {"input": {"duckdb": "/tmp/test.ddb"}}
    assert q_manager.header == expected_header_dict


def test_qsql_manager_empty_parsers(qsql_file_basic: Path):
    """Test QSQLManager with no parsers."""
    q_manager = QsqlManager(qsql_file_basic, parsers=[])

    # Should return empty dict since no parsers are applied
    assert q_manager.header == {}


def test_qsql_manager_parser_order(qsql_file_basic: Path):
    """Test that parser order affects result merging."""
    # Create parsers in different order
    parsers = [KeyValueParser(), DictLikeParser()]

    q_manager = QsqlManager(qsql_file_basic, parsers=parsers)

    # DictLikeParser result should override any key conflicts
    expected_header_dict = {"input": {"duckdb": "/tmp/test.ddb"}}
    assert q_manager.header == expected_header_dict
