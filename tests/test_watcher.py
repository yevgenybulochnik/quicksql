import os
import queue
import threading

from quicksql.compiler import compile_file
from quicksql.runner import RunSession
from quicksql.watcher import hashes_of, plan_rerun, run_changed, touches, watch_events

V1 = """\
-- @cell a
SELECT 1 AS x;
-- @cell b
SELECT * FROM {{ ref('a') }};
-- @cell c
SELECT 9 AS z;
-- @cell d
-- @autorun: false
SELECT * FROM {{ ref('a') }};
"""

V2 = V1.replace("SELECT 1 AS x;", "SELECT 2 AS x;")


def test_only_changed_and_downstream_rerun_autorun_respected(tmp_path) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    hashes = hashes_of(compile_file(f))

    f.write_text(V2)
    project, results = run_changed(f, hashes)
    assert [r.cell for r in results] == ["a", "b"]  # c unchanged, d autorun:false
    assert all(r.ok for r in results), [r.error for r in results]
    assert (tmp_path / "data" / "b.parquet").exists()
    assert not (tmp_path / "data" / "c.parquet").exists()


def test_no_change_means_no_rerun(tmp_path) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    hashes = hashes_of(compile_file(f))
    _, results = run_changed(f, hashes)
    assert results == []


def test_new_cell_counts_as_changed(tmp_path) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    project = compile_file(f)
    hashes = hashes_of(project)
    f.write_text(V1 + "-- @cell e\nSELECT 5 AS w;\n")
    assert plan_rerun(hashes, compile_file(f)) == ["e"]


def test_touches_filters_directory_events(tmp_path) -> None:
    f = tmp_path / "base.sql"
    f.write_text("x")
    assert touches({(1, str(f))}, f)
    assert not touches({(1, str(tmp_path / "base.sql.swp"))}, f)


def test_watch_survives_atomic_replace_saves(tmp_path) -> None:
    # editors like nvim save via write-temp + rename, replacing the inode;
    # the watcher must keep firing across repeated saves
    f = tmp_path / "base.sql"
    f.write_text(V1)
    events: queue.Queue = queue.Queue()
    stop = threading.Event()

    def consume() -> None:
        for _project, results in watch_events(f, stop_event=stop):
            events.put([r.cell for r in results] if isinstance(results, list) else results)

    worker = threading.Thread(target=consume, daemon=True)
    worker.start()
    try:
        assert events.get(timeout=20) == ["a", "b", "c"]  # initial autorun pass

        def atomic_save(text: str) -> None:
            tmp = tmp_path / ".base.sql.tmp"
            tmp.write_text(text)
            os.replace(tmp, f)

        atomic_save(V1.replace("SELECT 1 AS x;", "SELECT 2 AS x;"))
        assert events.get(timeout=20) == ["a", "b"]
        atomic_save(V1.replace("SELECT 1 AS x;", "SELECT 3 AS x;"))
        assert events.get(timeout=20) == ["a", "b"]  # still alive after inode swap
    finally:
        stop.set()
        worker.join(timeout=10)


def test_config_only_edits_do_not_trigger(tmp_path) -> None:
    # change detection hashes SQL bodies only — a known limitation
    f = tmp_path / "base.sql"
    f.write_text(V1)
    hashes = hashes_of(compile_file(f))
    f.write_text(V1.replace("-- @autorun: false", "-- @autorun: true"))
    project, results = run_changed(f, hashes)
    assert results == []


def test_run_changed_streams_run_events(tmp_path) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    hashes = hashes_of(compile_file(f))
    f.write_text(V2)
    events = []
    _, results = run_changed(f, hashes, on_event=events.append)
    assert [r.cell for r in results] == ["a", "b"]
    assert [e.cell for e in events if e.kind == "cell_started"] == ["a", "b"]


def test_editing_only_the_downstream_reruns_its_failed_upstream_first(tmp_path) -> None:
    """Cross-context a broke on the last save; this save edits only b. a reruns
    (fails again) and b is skipped, not run on a's earlier parquet."""
    good = (
        "-- @cell a\n-- @input: {sqlite: \":memory:\"}\nSELECT 1 AS x;\n"
        "-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
    )
    bad = good.replace("SELECT 1 AS x", "SELECT * FROM nope")
    f = tmp_path / "base.sql"
    session = RunSession()
    try:
        f.write_text(good)
        project, results = run_changed(f, {}, session=session)  # every cell is new
        assert all(r.ok for r in results), [r.error for r in results]
        f.write_text(bad)
        project, results = run_changed(f, hashes_of(project), session=session)
        assert [(r.cell, r.status) for r in results] == [("a", "failed"), ("b", "skipped")]
        f.write_text(bad.replace("SELECT * FROM {{", "SELECT *, 1 AS y FROM {{"))
        _, results = run_changed(f, hashes_of(project), session=session)
        assert [(r.cell, r.status) for r in results] == [("a", "failed"), ("b", "skipped")]
    finally:
        session.close()


def test_config_only_edit_is_detected_but_not_rerun(tmp_path) -> None:
    from quicksql.watcher import config_only_changes

    f = tmp_path / "base.sql"
    f.write_text(V1)
    old = compile_file(f)
    f.write_text(V1.replace("-- @cell c\n", "-- @cell c\n-- @output: { type: duckdb }\n"))
    new = compile_file(f)
    assert config_only_changes(old, new) == ["c"]
    assert plan_rerun(hashes_of(old), new) == []  # body hash is blind to config edits
