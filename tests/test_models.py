import json
from datetime import date, datetime, time
from decimal import Decimal

from quicksql.models import Preview, RunResult


def _ok(preview: Preview) -> RunResult:
    return RunResult(
        cell="users", status="ok", rows=3, target="data/users.parquet",
        elapsed=0.25, preview=preview,
    )


def test_to_dict_carries_the_outcome_and_no_preview_by_default() -> None:
    result = _ok(Preview(("id",), ((1,), (2,), (3,))))
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
    result = _ok(Preview(("id", "name"), ((1, "a"), (2, "b"), (3, "c"))))
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
    head = Preview(
        ("d", "ts", "t", "amount", "blob", "ratio", "tags", "meta"),
        ((
            date(2024, 1, 2),
            datetime(2024, 1, 2, 3, 4, 5),
            time(3, 4, 5),
            Decimal("1.50"),
            b"\x00ab",
            float("nan"),
            [date(2024, 1, 2)],
            {"at": date(2024, 1, 2)},
        ),),
    )
    preview = _ok(head).to_dict(rows=1)["preview"]
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


def test_a_preview_is_plain_columns_and_rows() -> None:
    head = Preview(("id", "name"), ((1, "a"), (2, "b")))
    assert head.height == 2
    assert head.column("name") == ["a", "b"]


def test_a_run_event_to_dict_is_json_safe_and_carries_its_result() -> None:
    from quicksql.models import RunEvent

    started = RunEvent("run_started", detail="2 cell(s): a, b", cells=("a", "b"))
    assert started.to_dict() == {
        "kind": "run_started", "cell": None, "detail": "2 cell(s): a, b",
        "cells": ["a", "b"], "result": None,
    }
    result = RunResult(cell="a", status="ok", rows=1, preview=Preview(("x",), ((1,),)))
    finished = RunEvent("cell_finished", "a", result=result).to_dict(rows=1)
    assert finished["cells"] is None
    assert finished["result"]["status"] == "ok"
    assert finished["result"]["preview"] == {"columns": ["x"], "rows": [[1]]}
    json.dumps(finished)


def test_source_hash_is_a_short_stable_digest_of_the_text() -> None:
    from quicksql.models import source_hash

    assert source_hash("-- @cell a\nSELECT 1;\n") == source_hash("-- @cell a\nSELECT 1;\n")
    assert source_hash("SELECT 1;") != source_hash("SELECT 2;")
    assert len(source_hash("")) == 16
