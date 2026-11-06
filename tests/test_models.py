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


def test_a_preview_to_dict_is_json_safe_and_bounded() -> None:
    head = Preview(("d", "n"), ((date(2026, 1, 2), Decimal("1.50")), (None, Decimal("2"))))
    assert head.to_dict(1) == {"columns": ["d", "n"], "rows": [["2026-01-02", "1.50"]]}
    json.dumps(head.to_dict(10))


def test_human_duration_shows_milliseconds_under_a_second() -> None:
    from quicksql.models import human_duration

    assert human_duration(0.0) == "0 ms"
    assert human_duration(0.012) == "12 ms"
    assert human_duration(0.999) == "999 ms"
    assert human_duration(0.9994) == "999 ms"


def test_human_duration_rounding_carries_into_the_next_unit() -> None:
    """Each unit rounds at its own precision; a value that rounds up to the
    next unit's threshold shows in that unit, never as `1000 ms` or `60.0 s`."""
    from quicksql.models import human_duration

    assert human_duration(0.9996) == "1.0 s"
    assert human_duration(59.96) == "1m 00s"
    assert human_duration(3599.6) == "1h 00m"


def test_human_duration_shows_tenths_of_a_second_under_a_minute() -> None:
    from quicksql.models import human_duration

    assert human_duration(1.0) == "1.0 s"
    assert human_duration(1.44) == "1.4 s"
    assert human_duration(59.9) == "59.9 s"


def test_human_duration_shows_minutes_and_seconds_under_an_hour() -> None:
    from quicksql.models import human_duration

    assert human_duration(60.0) == "1m 00s"
    assert human_duration(84.0) == "1m 24s"
    assert human_duration(3599.0) == "59m 59s"


def test_human_duration_shows_hours_and_minutes_from_an_hour() -> None:
    from quicksql.models import human_duration

    assert human_duration(3600.0) == "1h 00m"
    assert human_duration(3720.0) == "1h 02m"
    assert human_duration(100 * 3600.0) == "100h 00m"
