import json
from datetime import date, datetime, time
from decimal import Decimal

import polars as pl

from quicksql.models import RunResult


def _ok(preview: pl.DataFrame) -> RunResult:
    return RunResult(
        cell="users", status="ok", rows=3, target="data/users.parquet",
        elapsed=0.25, preview=preview,
    )


def test_to_dict_carries_the_outcome_and_no_preview_by_default() -> None:
    result = _ok(pl.DataFrame({"id": [1, 2, 3]}))
    assert result.to_dict() == {
        "cell": "users",
        "status": "ok",
        "rows": 3,
        "target": "data/users.parquet",
        "elapsed": 0.25,
        "error": None,
        "statement": None,
        "traceback": None,
    }


def test_to_dict_carries_the_failed_statement() -> None:
    failed = RunResult(cell="a", status="failed", error="boom", statement=2)
    assert failed.to_dict()["statement"] == 2


def test_to_dict_rows_adds_that_many_preview_rows() -> None:
    result = _ok(pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}))
    assert result.to_dict(rows=2)["preview"] == {
        "columns": ["id", "name"],
        "rows": [[1, "a"], [2, "b"]],
    }
    # more rows than the cached preview holds: all of it, no padding
    assert len(result.to_dict(rows=10)["preview"]["rows"]) == 3


def test_to_dict_failed_and_skipped_cells_have_a_null_preview() -> None:
    failed = RunResult(
        cell="a", status="failed", error="Catalog Error: nope", traceback="Traceback ..."
    )
    d = failed.to_dict(rows=5)
    assert d["preview"] is None
    assert d["error"] == "Catalog Error: nope"
    assert d["traceback"] == "Traceback ..."
    skipped = RunResult(cell="b", status="skipped", error="upstream 'a' failed")
    assert skipped.to_dict(rows=5)["preview"] is None


def test_to_dict_preview_values_are_json_safe() -> None:
    frame = pl.DataFrame(
        {
            "d": [date(2024, 1, 2)],
            "ts": [datetime(2024, 1, 2, 3, 4, 5)],
            "t": [time(3, 4, 5)],
            "amount": [Decimal("1.50")],
            "blob": [b"\x00ab"],
            "ratio": [float("nan")],
            "tags": [[date(2024, 1, 2)]],
            "meta": [{"at": date(2024, 1, 2)}],
        }
    )
    preview = _ok(frame).to_dict(rows=1)["preview"]
    # strict JSON: no default= hook, no NaN
    json.dumps(preview, allow_nan=False)
    assert preview["rows"] == [[
        "2024-01-02",
        "2024-01-02T03:04:05",
        "03:04:05",
        "1.50",  # exact, not a float
        "b'\\x00ab'",
        "nan",
        ["2024-01-02"],
        {"at": "2024-01-02"},
    ]]


def test_to_dict_elapsed_is_seconds_to_a_tenth_of_a_ms() -> None:
    result = RunResult(cell="a", status="ok", elapsed=0.123456789)
    assert result.to_dict()["elapsed"] == 0.1235
