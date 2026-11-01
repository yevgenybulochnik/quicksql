import pytest

from quicksql import sources
from quicksql.bootstrap import builtin_registries
from quicksql.errors import ConfigError
from quicksql.sources import sql_literal

READERS = builtin_registries().readers


def reader_for(path, type_=None):
    return sources.reader_for(path, type_, READERS)


def test_reader_inferred_from_extension() -> None:
    assert reader_for("seeds/users.csv").name == "csv"
    assert reader_for("data/x.parquet").name == "parquet"
    assert reader_for("logs.ndjson").name == "json"
    assert reader_for("sales.xlsx").name == "excel"


def test_reader_explicit_type_wins() -> None:
    assert reader_for("weird.dat", "csv").name == "csv"


def test_unknown_extension_raises() -> None:
    with pytest.raises(ConfigError, match="cannot infer source type"):
        reader_for("mystery.dat")


def test_excel_reader_declares_extension_and_opts() -> None:
    reader = reader_for("sales.xlsx")
    assert reader.requires == ["excel"]
    assert reader.expr("/abs/sales.xlsx", sheet="Q1") == "read_xlsx('/abs/sales.xlsx', sheet='Q1')"


def test_sql_literal_quoting() -> None:
    assert sql_literal("o'brien") == "'o''brien'"
    assert sql_literal(True) == "true"
    assert sql_literal(7) == "7"
