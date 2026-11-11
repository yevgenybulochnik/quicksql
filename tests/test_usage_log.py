"""The usage log on disk: a file per process per day, compacted once the day
is over."""

import json
import os
import stat
import threading
from datetime import date

import pytest

from quicksql.usage_log import UsageLog, usage_dir

TODAY = date(2026, 11, 9)


def line(cell: str = "c", key: str = "k1", day: str = "2026-11-09", notebook: str = "/nb/a.qsql"):
    return {"v": 1, "day": day, "notebook": notebook, "cell": cell, "key": key, "tables": []}


def names(log: UsageLog) -> list[str]:
    return sorted(p.name for p in log.root.iterdir())


def test_the_log_lives_under_the_quicksql_home(qsql_home) -> None:
    assert usage_dir() == qsql_home / "usage"
    assert UsageLog().root == qsql_home / "usage"


def test_nothing_exists_until_something_is_appended(qsql_home) -> None:
    log = UsageLog()
    assert list(log.read()) == []
    assert log.keys("2026-11-09") == set()
    log.prune(TODAY)
    log.compact(TODAY)
    assert not (qsql_home / "usage").exists()


def test_append_writes_a_private_file_per_process_per_day(qsql_home) -> None:
    log = UsageLog()
    log.append([line("a"), line("b")], "2026-11-09")
    log.append([line("c")], "2026-11-09")
    log.append([line("d", day="2026-11-10")], "2026-11-10")
    assert names(log) == [f"2026-11-09.{os.getpid()}.ndjson", f"2026-11-10.{os.getpid()}.ndjson"]
    assert stat.S_IMODE(log.root.stat().st_mode) == 0o700
    for path in log.root.iterdir():
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert [entry["cell"] for entry in log.read()] == ["a", "b", "c", "d"]


def test_two_processes_never_share_a_file(qsql_home) -> None:
    UsageLog(pid=11).append([line("a")], "2026-11-09")
    UsageLog(pid=22).append([line("b")], "2026-11-09")
    log = UsageLog()
    assert names(log) == ["2026-11-09.11.ndjson", "2026-11-09.22.ndjson"]
    assert sorted(entry["cell"] for entry in log.read()) == ["a", "b"]


def test_an_append_with_no_lines_creates_nothing(qsql_home) -> None:
    UsageLog().append([], "2026-11-09")
    assert not (qsql_home / "usage").exists()


def test_read_is_bounded_by_day(qsql_home) -> None:
    log = UsageLog()
    for day in ("2026-11-07", "2026-11-08", "2026-11-09"):
        log.append([line(day, day=day)], day)
    assert [e["cell"] for e in log.read(since="2026-11-08")] == ["2026-11-08", "2026-11-09"]


def test_a_torn_or_unknown_line_is_skipped(qsql_home) -> None:
    log = UsageLog(pid=7)
    log.append([line("a")], "2026-11-09")
    path = log.root / "2026-11-09.7.ndjson"
    with path.open("a") as f:
        f.write(json.dumps({**line("future"), "v": 2}) + "\n")
        f.write("[1, 2]\n")
        f.write('{"v": 1, "day": "2026-11-09", "notebook": "/nb/a.qsql", "cell": "tor')
    assert [entry["cell"] for entry in log.read()] == ["a"]
    assert log.keys("2026-11-09") == {("/nb/a.qsql", "a", "k1")}


def test_files_that_arent_day_files_are_left_alone(qsql_home) -> None:
    log = UsageLog()
    log.append([line("a", day="2020-01-01")], "2020-01-01")
    (log.root / "curation.yaml").write_text("pin: []\n")
    (log.root / "notes.ndjson").write_text("{}\n")
    assert [entry["cell"] for entry in log.read()] == ["a"]
    log.prune(TODAY)
    log.compact(TODAY)
    assert names(log) == ["curation.yaml", "notes.ndjson"]


def test_todays_keys_are_what_every_process_logged_today(qsql_home) -> None:
    UsageLog(pid=11).append([line("a", "k1"), line("a", "k2")], "2026-11-09")
    UsageLog(pid=22).append([line("b", "k1", notebook="/nb/b.qsql")], "2026-11-09")
    UsageLog(pid=11).append([line("old", day="2026-11-08")], "2026-11-08")
    assert UsageLog().keys("2026-11-09") == {
        ("/nb/a.qsql", "a", "k1"),
        ("/nb/a.qsql", "a", "k2"),
        ("/nb/b.qsql", "b", "k1"),
    }


def test_prune_deletes_days_past_retention(qsql_home) -> None:
    log = UsageLog(pid=5)
    for day in ("2025-11-08", "2025-11-09", "2026-11-09"):
        log.append([line(day=day)], day)
    (log.root / "2025-01-01.ndjson").write_text(json.dumps(line(day="2025-01-01")) + "\n")
    log.prune(TODAY)  # 365 days: 2025-11-09 is the oldest day kept
    assert names(log) == ["2025-11-09.5.ndjson", "2026-11-09.5.ndjson"]
    log.prune(TODAY, keep_days=1)
    assert names(log) == ["2026-11-09.5.ndjson"]


def test_compact_folds_each_finished_day_into_one_file(qsql_home) -> None:
    for pid, cell in ((11, "a"), (22, "b"), (33, "a")):
        UsageLog(pid=pid).append([line(cell, day="2026-11-07")], "2026-11-07")
    UsageLog(pid=11).append([line("y", day="2026-11-08")], "2026-11-08")  # yesterday
    UsageLog(pid=11).append([line("t")], "2026-11-09")
    log = UsageLog()
    log.compact(TODAY)
    assert names(log) == ["2026-11-07.ndjson", "2026-11-08.11.ndjson", "2026-11-09.11.ndjson"]
    assert stat.S_IMODE((log.root / "2026-11-07.ndjson").stat().st_mode) == 0o600
    # the same (notebook, cell, key) from two processes is one line
    assert sorted(e["cell"] for e in log.read() if e["day"] == "2026-11-07") == ["a", "b"]


def test_compact_folds_late_files_into_a_day_already_compacted(qsql_home) -> None:
    UsageLog(pid=11).append([line("a", day="2026-11-01")], "2026-11-01")
    log = UsageLog()
    log.compact(TODAY)
    UsageLog(pid=22).append([line("b", day="2026-11-01"), line("a", day="2026-11-01")], "2026-11-01")
    log.compact(TODAY)
    assert names(log) == ["2026-11-01.ndjson"]
    assert sorted(e["cell"] for e in log.read()) == ["a", "b"]


def test_a_compacted_day_is_not_rewritten(qsql_home) -> None:
    UsageLog(pid=11).append([line("a", day="2026-11-01")], "2026-11-01")
    log = UsageLog()
    log.compact(TODAY)
    before = (log.root / "2026-11-01.ndjson").stat().st_mtime_ns
    log.compact(TODAY)
    assert (log.root / "2026-11-01.ndjson").stat().st_mtime_ns == before


def test_a_compactor_that_finds_a_listed_file_gone_leaves_the_day_alone(qsql_home) -> None:
    for pid, cell in ((11, "a"), (22, "b")):
        UsageLog(pid=pid).append([line(cell, day="2026-11-01")], "2026-11-01")
    log = UsageLog()
    listed = log.day_files("2026-11-01")
    # another process compacted the day between this one's listing and its opens
    UsageLog().compact(TODAY)
    assert names(log) == ["2026-11-01.ndjson"]
    before = (log.root / "2026-11-01.ndjson").read_bytes()
    assert log.compact_day("2026-11-01", listed) is False
    assert names(log) == ["2026-11-01.ndjson"]
    assert (log.root / "2026-11-01.ndjson").read_bytes() == before


def test_two_compactors_at_once_lose_no_line(qsql_home) -> None:
    for round_ in range(20):
        day = f"2026-10-{round_ + 1:02d}"
        for pid in range(1, 9):
            UsageLog(pid=pid).append(
                [line(f"c{pid}-{n}", key=f"k{n}", day=day) for n in range(5)], day
            )
        start = threading.Barrier(4)

        def compact() -> None:
            start.wait()
            UsageLog().compact(TODAY)

        threads = [threading.Thread(target=compact) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        log = UsageLog()
        assert len([e for e in log.read() if e["day"] == day]) == 40, day
        assert [n for n in names(log) if n.startswith(day)] == [f"{day}.ndjson"]


def test_an_unwritable_directory_raises_oserror(qsql_home) -> None:
    (qsql_home / "usage").write_text("a file where the directory should be")
    with pytest.raises(OSError):
        UsageLog().append([line()], "2026-11-09")
