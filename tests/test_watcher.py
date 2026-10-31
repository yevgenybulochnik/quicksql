import os
import queue
import threading

import pytest

from quicksql.compiler import compile_file
from quicksql.runner import RunSession
from quicksql.watcher import plan_rerun, run_changed, touches, watch_events

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


@pytest.fixture
def session():
    s = RunSession()
    yield s
    s.close()


def _seen(path, session) -> None:
    """The baseline a watch starts from: every cell as the file stands now."""
    session.see(compile_file(path))


def test_only_changed_and_downstream_rerun_autorun_respected(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)

    f.write_text(V2)
    project, results = run_changed(f, session)
    assert [r.cell for r in results] == ["a", "b"]  # c unchanged, d autorun:false
    assert all(r.ok for r in results), [r.error for r in results]
    assert (tmp_path / "data" / "b.parquet").exists()
    assert not (tmp_path / "data" / "c.parquet").exists()


def test_no_change_means_no_rerun(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)
    _, results = run_changed(f, session)
    assert results == []


def test_new_cell_counts_as_changed(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)
    f.write_text(V1 + "-- @cell e\nSELECT 5 AS w;\n")
    assert plan_rerun(compile_file(f), session) == ["e"]


MANUAL = """\
-- @cell a
SELECT 1 AS x;
-- @cell d
-- @autorun: false
SELECT * FROM {{ ref('a') }};
-- @cell e
SELECT * FROM {{ ref('d') }};
-- @cell f
SELECT * FROM {{ ref('a') }} JOIN {{ ref('d') }} USING (x);
"""


@pytest.mark.parametrize(
    ("edit", "planned"),
    [
        # d won't rerun, so e and f would read its old output: nothing reruns
        (("FROM {{ ref('a') }};\n-- @cell e", "FROM {{ ref('a') }} WHERE x > 0;\n-- @cell e"), []),
        # a reruns; e reads it only through d, which doesn't, so only f follows a
        (("SELECT 1 AS x;", "SELECT 2 AS x;"), ["a", "f"]),
    ],
    ids=["edit-manual-cell", "edit-through-manual-cell"],
)
def test_reruns_propagate_only_through_cells_that_rerun(tmp_path, session, edit, planned) -> None:
    f = tmp_path / "base.sql"
    f.write_text(MANUAL)
    _seen(f, session)
    old, new = edit
    assert MANUAL.count(old) == 1
    f.write_text(MANUAL.replace(old, new))
    assert plan_rerun(compile_file(f), session) == planned


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


# ---------- config edits rerun; fields that don't affect results don't ----------


def test_editing_a_cells_output_reruns_it(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)
    f.write_text(V1.replace("-- @cell c\n", "-- @cell c\n-- @output: { type: duckdb }\n"))
    _, results = run_changed(f, session)
    assert [r.cell for r in results] == ["c"]


def test_editing_vars_reruns_every_cell_they_reach(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text("-- @vars: { n: 1 }\n" + V1)
    _seen(f, session)
    f.write_text("-- @vars: { n: 2 }\n" + V1)
    _, results = run_changed(f, session)
    assert [r.cell for r in results] == ["a", "b", "c"]  # d is autorun: false


@pytest.mark.parametrize(
    ("old", "new"),
    [("-- @autorun: false", "-- @autorun: true"), ("-- @cell c\n", "-- @cell c\n-- @tags: [daily]\n")],
    ids=["autorun", "tags"],
)
def test_toggling_autorun_or_tags_reruns_nothing(tmp_path, session, old, new) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)
    f.write_text(V1.replace(old, new))
    _, results = run_changed(f, session)
    assert results == []


def test_run_changed_streams_run_events(tmp_path, session) -> None:
    f = tmp_path / "base.sql"
    f.write_text(V1)
    _seen(f, session)
    f.write_text(V2)
    events = []
    _, results = run_changed(f, session, on_event=events.append)
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
        project, results = run_changed(f, session)  # a fresh session: every cell is new
        assert all(r.ok for r in results), [r.error for r in results]
        f.write_text(bad)
        project, results = run_changed(f, session)
        assert [(r.cell, r.status) for r in results] == [("a", "failed"), ("b", "skipped")]
        f.write_text(bad.replace("SELECT * FROM {{", "SELECT *, 1 AS y FROM {{"))
        _, results = run_changed(f, session)
        assert [(r.cell, r.status) for r in results] == [("a", "failed"), ("b", "skipped")]
    finally:
        session.close()


def test_watch_compiles_once_at_startup(tmp_path, monkeypatch) -> None:
    """The CLI compiles to fail fast and load plugins; the watch reuses that project."""
    import threading

    from quicksql import watcher

    f = tmp_path / "base.sql"
    f.write_text(V1)
    project = compile_file(f)
    compiles = []
    monkeypatch.setattr(watcher, "compile_file", lambda *a, **k: compiles.append(a) or compile_file(*a, **k))
    stop = threading.Event()
    stop.set()
    events = watch_events(f, project=project, stop_event=stop)
    first_project, results = next(events)
    events.close()
    assert first_project is project and compiles == []
    assert [r.cell for r in results] == ["a", "b", "c"]
