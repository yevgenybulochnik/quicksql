"""The live host: the open TUI's state as an agent sees and drives it, tested
through a fake driver (no Textual, no socket)."""

from pathlib import Path

import pytest

from quicksql.bootstrap import compile_file
from quicksql.live.host import LiveHost
from quicksql.live.protocol import LiveError
from quicksql.models import RunEvent, RunResult

NOTEBOOK = """\
-- @cell a
SELECT range AS x FROM range(3);

-- @cell b
SELECT count(*) AS n FROM {{ ref('a') }};

-- @cell manual
-- @autorun: false
SELECT 1 AS one;
"""


@pytest.fixture
def nb(tmp_path) -> Path:
    path = tmp_path / "nb.qsql"
    path.write_text(NOTEBOOK)
    return path


@pytest.fixture
def host(nb, fake_driver):
    driver = fake_driver
    host = LiveHost(driver, nb)
    driver.host = host
    host.loaded(compile_file(nb))
    return host


def kinds(host: LiveHost) -> list[str]:
    return [e["kind"] for e in host.since(0)]


def ok(cell: str) -> RunResult:
    return RunResult(cell=cell, status="ok", rows=1, target=f"data/{cell}.parquet")


def run_through(host: LiveHost, run: int, project, results: list[RunResult]) -> None:
    """Feed the host one run's life, as the TUI's run controller reports it."""
    host.run_adopted(run, project)
    host.run_event(run, RunEvent("run_started", cells=tuple(r.cell for r in results)))
    for result in results:
        host.run_event(run, RunEvent("cell_started", result.cell))
        host.run_event(run, RunEvent("cell_finished", result.cell, result=result))
    host.run_event(run, RunEvent("run_finished", detail="done"))
    host.run_ended(run, results)


# ---------- controls ----------


def test_controls_are_set_not_toggled_and_say_who_set_them(host) -> None:
    host.set_armed(True, by="agent")
    host.set_armed(True, by="agent")  # setting twice is still on
    assert host.armed is True
    changes = [e for e in host.since(0) if e["kind"] == "controls_changed"]
    assert len(changes) == 1  # no change, no event
    assert changes[0]["armed"] is True and changes[0]["by"] == "agent"
    assert host.driver.logged == ["agent: arm"]
    assert host.driver.repaints == 1


def test_a_key_change_is_attributed_to_the_keys_and_not_logged_as_the_agent(host) -> None:
    host.toggle_armed(by="keys")
    assert host.armed is True
    assert host.since(0)[-1]["by"] == "keys"
    assert host.driver.logged == []


def test_session_autorun_switches_cells_off_and_back_on(host) -> None:
    host.set_autorun(["a"], False, by="agent")
    assert host.cell_autorun("a") is False
    assert host.autorun_of("a") == {"effective": False, "file": True, "session": False, "global": True}
    host.set_autorun(["a"], True, by="agent")
    assert host.cell_autorun("a") is True
    host.set_autorun_all(False, by="agent")
    assert host.cell_autorun("b") is False and host.controls()["autorun_global"] is False


def test_autorun_on_is_refused_for_a_cell_the_file_switches_off(host) -> None:
    with pytest.raises(LiveError) as err:
        host.set_autorun(["manual"], True, by="agent")
    assert err.value.code == "refused"
    assert "autorun: false" in err.value.message


def test_an_unknown_cell_is_refused_by_name(host) -> None:
    with pytest.raises(LiveError) as err:
        host.set_autorun(["nope"], False, by="agent")
    assert err.value.code == "unknown_cell"


def test_the_a_key_flips_the_session_switch_even_on_a_file_off_cell(host) -> None:
    host.toggle_autorun("manual", by="keys")  # as today: no refusal from a key
    assert "manual" in host.autorun_off
    assert host.cell_autorun("manual") is False


# ---------- saves, runs and outcomes ----------


def test_a_save_while_paused_settles_as_paused_with_what_changed(host, nb) -> None:
    nb.write_text(NOTEBOOK.replace("range(3)", "range(4)"))
    project = compile_file(nb)
    assert host.recompiled(project, ["a", "b"]) == []
    outcome = host.outcomes[project.source_hash]
    assert outcome["state"] == "paused" and outcome["changed"] == ["a", "b"]
    assert "settled" in kinds(host)


def test_an_armed_save_settles_when_its_rerun_ends(host, nb) -> None:
    host.set_armed(True, by="keys")
    nb.write_text(NOTEBOOK.replace("range(3)", "range(4)"))
    project = compile_file(nb)
    assert host.recompiled(project, ["a", "b", "manual"]) == ["a", "b"]  # manual: file-off
    assert project.source_hash not in host.outcomes  # its rerun hasn't run yet
    host.run_requested(1, project.source_hash)
    run_through(host, 1, project, [ok("a"), ok("b")])
    outcome = host.outcomes[project.source_hash]
    assert outcome["state"] == "ran"
    assert [r["cell"] for r in outcome["results"]] == ["a", "b"]
    assert outcome["summary"] == {"ok": 2, "failed": 0, "skipped": 0}


def test_an_armed_save_with_nothing_autorun_settles_at_once(host, nb) -> None:
    host.set_armed(True, by="keys")
    nb.write_text(NOTEBOOK.replace("SELECT 1 AS one", "SELECT 2 AS one"))
    project = compile_file(nb)
    assert host.recompiled(project, ["manual"]) == []
    assert host.outcomes[project.source_hash]["state"] == "nothing_to_rerun"


def test_a_compile_error_settles_its_hash_and_keeps_the_last_good_project(host) -> None:
    before = host.project
    host.compile_failed("deadbeefdeadbeef", [{"cell": "b", "line": 4, "message": "boom"}])
    assert host.project is before
    outcome = host.outcomes["deadbeefdeadbeef"]
    assert outcome["state"] == "compile_failed" and outcome["errors"][0]["message"] == "boom"
    assert host.snapshot()["compile_error"]["hash"] == "deadbeefdeadbeef"


def test_a_replaced_run_does_not_settle_its_save_the_replacing_run_does(host, nb) -> None:
    project = host.project
    host.run_requested(1, project.source_hash)
    host.run_requested(2, project.source_hash)  # e.g. you pressed r mid-run
    run_through(host, 1, project, [ok("a")])
    assert host.runs[1]["state"] == "replaced"
    run_through(host, 2, project, [ok("a"), ok("b")])
    assert host.outcomes[project.source_hash]["state"] == "ran"
    assert host.runs[2]["state"] == "finished"


def test_a_failed_cell_carries_its_lines_and_failing_statement(host) -> None:
    project = host.project
    failed = RunResult(cell="b", status="failed", error="Binder Error: nope", statement=1)
    host.run_requested(1, project.source_hash)
    run_through(host, 1, project, [ok("a"), failed])
    result = host.outcomes[project.source_hash]["results"][1]
    cell = project.cells["b"]
    assert (result["line"], result["line_end"]) == (cell.line, cell.line_end)
    assert result["statement_sql"].startswith("SELECT count(*) AS n FROM")
    finished = [e for e in host.since(0) if e["kind"] == "cell_finished"]
    assert finished[-1]["result"]["statement_sql"] == result["statement_sql"]


def test_a_dropped_run_ends_without_settling(host) -> None:
    host.run_requested(1, None)
    host.run_dropped(1, "replaced before it started")
    assert host.runs[1]["state"] == "dropped"
    assert not host.busy()


# ---------- snapshot ----------


def test_the_snapshot_shows_queued_and_running_cells_mid_run(host) -> None:
    project = host.project
    host.run_requested(1, project.source_hash)
    host.run_adopted(1, project)
    host.run_event(1, RunEvent("run_started", cells=("a", "b")))
    host.run_event(1, RunEvent("cell_started", "a"))
    snap = host.snapshot()
    status = {c["cell"]: c["status"] for c in snap["cells"]}
    assert status == {"a": "running", "b": "queued", "manual": "new"}
    assert snap["run"]["run_id"] == 1 and snap["run"]["current"] == "a"
    host.run_event(1, RunEvent("cell_finished", "a", result=ok("a")))
    status = {c["cell"]: c["status"] for c in host.snapshot()["cells"]}
    assert status["a"] == "ok" and status["b"] == "queued"


def test_the_snapshot_carries_controls_lines_and_autorun(host) -> None:
    snap = host.snapshot()
    assert snap["controls"] == {"armed": False, "autorun_global": True, "autorun_off": []}
    manual = next(c for c in snap["cells"] if c["cell"] == "manual")
    assert manual["autorun"]["file"] is False and manual["line"] > 0
    assert snap["hash"] == host.project.source_hash


# ---------- events ----------


def test_events_since_a_sequence_and_lost_past_the_ring(nb, fake_driver) -> None:
    driver = fake_driver
    host = LiveHost(driver, nb, ring=3)
    driver.host = host
    host.loaded(compile_file(nb))
    for on in (True, False, True, False):
        host.set_armed(on, by="keys")
    newest = host.seq
    assert [e["seq"] for e in host.since(newest - 2)] == [newest - 1, newest]
    assert host.since(newest) == []
    assert host.since(0) is None  # older than the ring holds: lost


def test_a_subscriber_that_falls_behind_is_dropped_and_told(host) -> None:
    class Sub:
        def __init__(self, room: int) -> None:
            self.room, self.got, self.lost_ = room, [], False

        def offer(self, event) -> bool:
            if len(self.got) >= self.room:
                return False
            self.got.append(event)
            return True

        def lost(self) -> None:
            self.lost_ = True

    slow = Sub(room=1)
    host.subscribe(slow)
    host.set_armed(True, by="keys")
    host.set_armed(False, by="keys")
    assert slow.lost_ and len(slow.got) == 1
    host.set_armed(True, by="keys")
    assert len(slow.got) == 1  # dropped: nothing more


# ---------- wait ----------


def test_wait_returns_the_outcome_of_the_saved_text(host, nb) -> None:
    project = host.project
    h = project.source_hash
    since = host.seq
    assert host.wait_outcome(h, since, disk_hash=h, disk_mtime=0)["state"] == "paused"


def test_wait_holds_while_a_run_of_that_text_is_under_way(host) -> None:
    h = host.project.source_hash
    host.run_requested(1, h)
    assert host.wait_outcome(h, host.seq, disk_hash=h, disk_mtime=0) is None
    run_through(host, 1, host.project, [ok("a")])
    assert host.wait_outcome(h, 0, disk_hash=h, disk_mtime=0)["state"] == "ran"


def test_wait_holds_for_a_save_the_session_has_not_seen_yet(host) -> None:
    assert host.wait_outcome("feedfeedfeedfeed", host.seq, "feedfeedfeedfeed", 0) is None


def test_wait_holds_when_the_file_was_saved_again_after_its_outcome(host) -> None:
    h = host.project.source_hash
    later = host.seen_at[h] + 10  # the same text saved again, not yet recompiled
    assert host.wait_outcome(h, host.seq, disk_hash=h, disk_mtime=later) is None


def test_wait_reports_superseded_when_the_file_moved_on(host, nb) -> None:
    mine = "feedfeedfeedfeed"  # what the agent saved; you saved over it first
    since = host.seq
    nb.write_text(NOTEBOOK.replace("range(3)", "range(9)"))
    yours = compile_file(nb)
    assert host.wait_outcome(mine, since, yours.source_hash, 0) is None  # nothing settled yet
    host.recompiled(yours, ["a"])
    outcome = host.wait_outcome(mine, since, yours.source_hash, 0)
    assert outcome["superseded"] is True and outcome["hash"] == yours.source_hash


# ---------- requests ----------


def test_an_agent_run_is_refused_while_a_run_is_under_way(host) -> None:
    host.run_requested(1, None)
    with pytest.raises(LiveError) as err:
        host.request_run(["a"], all_=False, cancel=False)
    assert err.value.code == "busy"
    assert host.request_run(["a"], all_=False, cancel=True)["run_id"] == 1 + 1
    assert host.driver.started[-1] == (["a"], True)


def test_an_agent_run_all_arms_like_R(host) -> None:
    host.request_run(None, all_=True, cancel=False)
    assert host.armed is True
    assert host.driver.started[-1] == (None, True)
    assert "agent: run all" in host.driver.logged


def test_cancel_stops_the_run_under_way(host) -> None:
    assert host.cancel()["cancelled"] is False  # nothing to cancel
    host.run_requested(1, None)
    host.run_event(1, RunEvent("run_started", cells=("a", "b")))
    host.run_event(1, RunEvent("cell_started", "a"))
    out = host.cancel()
    assert out == {"cancelled": True, "not_run": ["b"]}
    assert host.driver.cancelled == 1
