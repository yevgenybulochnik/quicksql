import sqlite3

import polars as pl
import pytest

from quicksql.compiler import compile_text
from quicksql.plugins.base import Plugin
from quicksql.registry import plugin
from quicksql.runner import RunSession, run_project


def test_run_lands_parquet_and_previews(tmp_path) -> None:
    project = compile_text(
        "-- @cell users\nSELECT * FROM range(3) t(user_id);\n"
        "-- @cell active\nSELECT * FROM {{ ref('users') }} WHERE user_id > 0;",
        root=tmp_path,
    )
    results = project.run()
    assert [r.cell for r in results] == ["users", "active"]
    assert all(r.ok for r in results), [r.error for r in results]
    assert (tmp_path / "data" / "users.parquet").exists()
    assert (tmp_path / "data" / "active.parquet").exists()
    assert results[0].rows == 3 and results[1].rows == 2
    assert isinstance(results[1].preview, pl.DataFrame)
    assert results[1].elapsed > 0


def test_a_cell_whose_sink_lands_nothing_runs_every_statement(tmp_path, discard_sink) -> None:
    # effect-only is the sink's lands_output, not its name: the terminal INSERT
    # runs for effect instead of being wrapped as the result to land
    project = compile_text(
        "-- @cell load\n-- @output: { type: discard }\n"
        "CREATE TABLE staged (n INT);\nINSERT INTO staged VALUES (1), (2), (3);\n"
        "-- @cell counted\n-- @depends_on: [load]\n"
        "SELECT count(*) AS c FROM staged;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["load"].ok, results["load"].error
    assert (results["load"].rows, results["load"].target) == (0, "discarded")
    assert results["counted"].preview["c"].to_list() == [3]


def test_effect_only_cell_runs_for_side_effect_and_lands_nothing(tmp_path) -> None:
    project = compile_text(
        "-- @cell load\n-- @output: { type: none }\n"
        "CREATE TABLE staged (n INT);\nINSERT INTO staged VALUES (1), (2), (3);\n"
        "-- @cell counted\n-- @depends_on: [load]\n"
        "SELECT count(*) AS c FROM staged;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["load"].ok, results["load"].error
    assert results["load"].rows == 0 and results["load"].target == "none"
    # the effect-only cell actually ran its statements: a downstream cell sees the rows
    assert results["counted"].ok, results["counted"].error
    assert results["counted"].preview["c"].to_list() == [3]
    # nothing landed for the effect-only cell
    assert not (tmp_path / "data" / "load.parquet").exists()


def test_cross_engine_sqlite_duckdb_join(tmp_path) -> None:
    con = sqlite3.connect(tmp_path / "legacy.sqlite")
    con.execute("CREATE TABLE users (user_id INTEGER, name TEXT)")
    con.executemany("INSERT INTO users VALUES (?, ?)", [(1, "ada"), (2, "bob")])
    con.commit()
    con.close()

    project = compile_text(
        "-- @cell legacy_users\n"
        "/*@ input: { sqlite: legacy.sqlite } */\n"
        "SELECT user_id, name FROM users;\n"
        "-- @cell events\n"
        "SELECT 1 AS user_id, 'click' AS event UNION ALL SELECT 2, 'view';\n"
        "-- @cell joined\n"
        "SELECT u.name, e.event FROM {{ ref('legacy_users') }} u\n"
        "JOIN {{ ref('events') }} e USING (user_id);",
        root=tmp_path,
    )
    results = project.run()
    assert all(r.ok for r in results), [r.error for r in results]
    joined = pl.read_parquet(tmp_path / "data" / "joined.parquet")
    assert joined.height == 2
    assert set(joined["name"]) == {"ada", "bob"}


def test_duckdb_sink_cell_readable_downstream(tmp_path) -> None:
    project = compile_text(
        "-- @cell landed\n-- @output: { type: duckdb, path: wh.db }\n"
        "SELECT 1 AS id UNION ALL SELECT 2;\n"
        "-- @cell reader\nSELECT * FROM {{ ref('landed') }};",
        root=tmp_path,
    )
    results = project.run()
    assert all(r.ok for r in results), [r.error for r in results]
    assert (tmp_path / "wh.db").exists()
    assert pl.read_parquet(tmp_path / "data" / "reader.parquet").height == 2


def test_select_runs_upstream_closure_only(tmp_path) -> None:
    project = compile_text(
        "-- @cell users\nSELECT 1 AS id;\n"
        "-- @cell unrelated\nSELECT 2 AS id;\n"
        "-- @cell active\nSELECT * FROM {{ ref('users') }};",
        root=tmp_path,
    )
    results = project.run(select=["active"])
    assert [r.cell for r in results] == ["users", "active"]


def test_failed_cell_reports_error_and_run_continues(tmp_path) -> None:
    project = compile_text(
        "-- @cell bad\nSELECT * FROM no_such_table;\n"
        "-- @cell good\nSELECT 1 AS x;",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["bad"].ok is False
    assert "no_such_table" in results["bad"].error
    assert results["good"].ok is True


def test_failed_result_splits_message_from_traceback(tmp_path) -> None:
    """error is the engine's own message (self-describing for duckdb);
    the traceback rides separately for whoever wants to debug."""
    project = compile_text("-- @cell a\nSELECT * FROM nope;", root=tmp_path)
    [result] = project.run()
    assert result.status == "failed"
    assert result.error.startswith("Catalog Error: Table with name nope does not exist!")
    assert "LINE 1:" in result.error  # duckdb's context, kept for the terminal
    assert "Traceback" not in result.error
    assert "Traceback" in result.traceback


def test_plugin_bug_message_names_the_exception_type(tmp_path) -> None:
    @plugin
    class Buggy(Plugin):
        def run(self, cell, ctx, inner):
            raise KeyError("x")

    [result] = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path).run()
    assert result.status == "failed"
    assert result.error == "KeyError: 'x'"
    assert "Traceback" in result.traceback


# ---------- a failed upstream skips its downstream ----------


def test_failed_upstream_skips_downstream_across_contexts(tmp_path) -> None:
    """sqlite a fails -> duckdb b must not read run 1's still-landed parquet."""
    good = (
        "-- @cell a\n-- @input: {sqlite: \":memory:\"}\nSELECT 1 AS x\n"
        "-- @cell b\nSELECT * FROM {{ ref('a') }}\n"
    )
    assert all(r.ok for r in run_project(compile_text(good, root=tmp_path)))
    bad = good.replace("SELECT 1 AS x", "SELECT * FROM nope")
    results = {r.cell: r for r in run_project(compile_text(bad, root=tmp_path))}
    assert results["a"].ok is False
    assert results["b"].ok is False, "b ran on a's stale parquet"
    assert results["a"].status == "failed"
    assert results["b"].status == "skipped"
    assert results["b"].rows is None
    assert "upstream 'a' failed" in results["b"].error


def test_failed_upstream_skips_downstream_in_a_reused_session(tmp_path) -> None:
    """Same-context duckdb under one session (watch/TUI): b would read a's
    still-live temp table from run 1. A cell that didn't come out ok leaves
    session.materialized, so a later exact rerun re-includes it."""
    good = (
        "-- @cell a\nSELECT 1 AS x;\n"
        "-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
        "-- @cell c\nSELECT * FROM {{ ref('b') }};\n"
    )
    bad = good.replace("SELECT 1 AS x", "SELECT * FROM nope")
    session = RunSession()
    try:
        assert all(r.ok for r in run_project(compile_text(good, root=tmp_path), session=session))
        assert {"a", "b"} <= session.materialized
        results = {
            r.cell: r for r in run_project(compile_text(bad, root=tmp_path), session=session)
        }
        assert results["a"].ok is False
        assert results["b"].ok is False, "b ran on a's stale temp table"
        assert [results[n].status for n in "abc"] == ["failed", "skipped", "skipped"]
        assert not {"a", "b"} & session.materialized
    finally:
        session.close()


def test_depends_on_edge_alone_skips_downstream(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\nSELECT * FROM nope;\n"
        "-- @cell b\n-- @depends_on: [a]\nSELECT 1 AS x;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["a"].ok is False
    assert results["b"].ok is False, "b ran although its @depends_on upstream failed"
    assert results["b"].status == "skipped"
    assert "upstream 'a' failed" in results["b"].error
    assert not (tmp_path / "data" / "b.parquet").exists()


def test_skip_is_transitive_and_names_the_failed_root(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\nSELECT * FROM nope;\n"
        "-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
        "-- @cell c\nSELECT * FROM {{ ref('b') }};\n"
        "-- @cell d\nSELECT 1 AS x;\n",  # independent of a: still runs
        root=tmp_path,
    )
    events = []
    results = {r.cell: r for r in run_project(project, on_event=events.append)}
    assert "upstream 'a' failed" in (results["c"].error or "")
    assert [results[n].status for n in "abcd"] == ["failed", "skipped", "skipped", "ok"]
    # skipped cells never start, but each reports a cell_finished with its result
    started = {e.cell for e in events if e.kind == "cell_started"}
    assert started == {"a", "d"}
    finished = {e.cell: e.result for e in events if e.kind == "cell_finished"}
    assert finished["c"] is results["c"]
    assert events[-1].kind == "run_finished"
    assert "1 ok, 1 failed, 2 skipped" in events[-1].detail


# ---------- across exact reruns (watch/TUI), an upstream that wasn't ok reruns first ----------

CROSS = (
    "-- @cell a\n-- @input: {sqlite: \":memory:\"}\nSELECT 1 AS x\n"
    "-- @cell b\nSELECT * FROM {{ ref('a') }}\n"
)
CROSS_BROKEN = CROSS.replace("SELECT 1 AS x", "SELECT * FROM nope")


def test_exact_rerun_pulls_in_a_cross_context_upstream_that_failed(tmp_path) -> None:
    """Only b's body changed, so watch reruns just b. Cross-context a failed in
    the previous run, and b would read run 1's still-landed parquet: a reruns
    first, fails again, and b is skipped."""
    edited = CROSS_BROKEN.replace("SELECT * FROM {{", "SELECT *, 1 AS y FROM {{")
    session = RunSession()
    try:
        assert all(r.ok for r in run_project(compile_text(CROSS, root=tmp_path), session=session))
        run_project(compile_text(CROSS_BROKEN, root=tmp_path), session=session)
        results = run_project(
            compile_text(edited, root=tmp_path), select=["b"], closure=False, session=session
        )
        assert [(r.cell, r.status) for r in results] == [("a", "failed"), ("b", "skipped")]
    finally:
        session.close()


def test_exact_rerun_recovers_once_a_failed_upstreams_input_exists(tmp_path) -> None:
    """a failed on a missing table; the table then appears, a's SQL unchanged
    (the body hash can't see that). Rerunning b reruns a too: both on fresh data."""
    notebook = (
        "-- @cell a\n/*@ input: { sqlite: legacy.sqlite } */\nSELECT x FROM seed\n"
        "-- @cell b\nSELECT x * 10 AS y FROM {{ ref('a') }}\n"
    )
    session = RunSession()
    try:
        first = run_project(compile_text(notebook, root=tmp_path), session=session)
        assert [r.status for r in first] == ["failed", "skipped"]
        con = sqlite3.connect(tmp_path / "legacy.sqlite")
        con.execute("CREATE TABLE seed AS SELECT 7 AS x")
        con.commit()
        con.close()
        edited = notebook.replace("x * 10", "x * 100")
        results = run_project(
            compile_text(edited, root=tmp_path), select=["b"], closure=False, session=session
        )
        assert [(r.cell, r.status) for r in results] == [("a", "ok"), ("b", "ok")]
        assert pl.read_parquet(tmp_path / "data" / "b.parquet")["y"].to_list() == [700]
    finally:
        session.close()


def test_exact_rerun_pulls_in_a_skipped_upstream_and_its_failed_root(tmp_path) -> None:
    """a -> b -> c, over a cross-context ref() and an @depends_on edge (no temp
    table covers either): a failed, b was skipped; rerunning only c reruns both."""
    project = compile_text(
        CROSS_BROKEN + "-- @cell c\n-- @depends_on: [b]\nSELECT 1 AS z\n", root=tmp_path
    )
    session = RunSession()
    try:
        run_project(project, session=session)
        results = run_project(project, select=["c"], closure=False, session=session)
        assert [(r.cell, r.status) for r in results] == [
            ("a", "failed"),
            ("b", "skipped"),
            ("c", "skipped"),
        ]
        assert results[-1].error == "upstream 'a' failed"
    finally:
        session.close()


def test_exact_rerun_leaves_out_an_upstream_this_session_never_ran(tmp_path) -> None:
    """No status in this session (e.g. an autorun:false cell watch never ran):
    its landed output stands, as before."""
    project = compile_text(CROSS, root=tmp_path)
    assert all(r.ok for r in run_project(project))  # landed by a session of its own
    session = RunSession()
    try:
        results = run_project(project, select=["b"], closure=False, session=session)
        assert [(r.cell, r.status) for r in results] == [("b", "ok")]
    finally:
        session.close()


def test_session_remembers_each_cells_last_status_until_closed(tmp_path) -> None:
    session = RunSession()
    run_project(compile_text(CROSS, root=tmp_path), session=session)
    run_project(compile_text(CROSS_BROKEN, root=tmp_path), select=["a"], session=session)
    assert session.last_status == {"a": "failed", "b": "ok"}
    session.close()
    assert session.last_status == {}


def test_run_result_ok_is_derived_from_status() -> None:
    from quicksql.models import RunResult

    assert RunResult(cell="a", status="ok").ok is True
    assert RunResult(cell="a", status="failed").ok is False
    skipped = RunResult(cell="b", status="skipped", error="upstream 'a' failed")
    assert skipped.ok is False
    skipped.status = "ok"
    assert skipped.ok is True  # read back from status, never stored
    with pytest.raises(AttributeError):
        skipped.ok = False  # read-only
    with pytest.raises(TypeError):
        RunResult(cell="a", ok=True)  # status is the only constructor input


def test_extensions_directive_is_loaded_by_runner(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\n-- @extensions: [json]\nSELECT 1 AS x;", root=tmp_path
    )
    results = project.run()
    assert results[0].ok, results[0].error


def test_session_reuses_conduit_across_runs(tmp_path) -> None:
    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    session = RunSession()
    try:
        assert run_project(project, session=session)[0].ok
        first_conn = session.conn
        assert first_conn is not None
        assert run_project(project, session=session)[0].ok
        assert session.conn is first_conn  # no reconnect between runs
    finally:
        session.close()
    assert session.conn is None


def test_session_reconnects_when_conduit_target_changes(tmp_path) -> None:
    in_memory = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    on_disk = compile_text(
        "/*@ input: { duckdb: wh.db } */\n-- @cell a\nSELECT 1 AS x;", root=tmp_path
    )
    session = RunSession()
    try:
        run_project(in_memory, session=session)
        first_conn = session.conn
        run_project(on_disk, session=session)
        assert session.conn is not first_conn
        assert (tmp_path / "wh.db").exists()
    finally:
        session.close()


def test_session_caches_extension_loads(tmp_path, monkeypatch) -> None:
    import quicksql.runner as runner_mod

    loads: list[str] = []
    original = runner_mod._load_extension
    monkeypatch.setattr(
        runner_mod, "_load_extension",
        lambda conn, ext: (loads.append(ext), original(conn, ext))[1],
    )
    project = compile_text("-- @cell a\n-- @extensions: [json]\nSELECT 1;", root=tmp_path)
    session = RunSession()
    try:
        assert run_project(project, session=session)[0].ok
        assert run_project(project, session=session)[0].ok
    finally:
        session.close()
    assert loads.count("json") == 1  # second run hits the session cache


def test_without_session_each_run_owns_its_connection(tmp_path) -> None:
    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    assert project.run()[0].ok
    assert project.run()[0].ok  # independent runs still work


def test_context_keys_annotate_cells(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\nSELECT 1 AS x;\n"
        "-- @cell b\n/*@ input: { sqlite: legacy.db } */\nSELECT 2 AS x;\n"
        "-- @cell c\n/*@ input: { bigquery: { project: p } } */\nSELECT 3 AS x;",
        root=tmp_path,
    )
    assert project.cells["a"].context == "duckdb::memory:"
    assert project.cells["b"].context == "sqlite:legacy.db"
    assert project.cells["c"].context == "bigquery:p"


def test_same_context_run_lands_parent_and_child(tmp_path) -> None:
    # single execution, two consumers: the child reads the temp table while
    # the parent's parquet still lands for previews/VisiData
    project = compile_text(
        "-- @cell parent\nSELECT * FROM range(5) t(n);\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    assert results["child"].rows == 1
    assert pl.read_parquet(tmp_path / "data" / "parent.parquet").height == 5
    assert pl.read_parquet(tmp_path / "data" / "child.parquet")["c"][0] == 5


def test_exact_selection_expands_through_missing_temps(tmp_path) -> None:
    project = compile_text(
        "-- @cell parent\nSELECT 1 AS x;\n"
        "-- @cell child\nSELECT * FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    fresh = RunSession()
    try:  # cold session: the temp doesn't exist, so the parent must rerun
        results = run_project(project, select=["child"], closure=False, session=fresh)
        assert [r.cell for r in results] == ["parent", "child"]
        assert all(r.ok for r in results)
        # warm session: the temp is live, exact selection stays exact
        results = run_project(project, select=["child"], closure=False, session=fresh)
        assert [r.cell for r in results] == ["child"]
        assert results[0].ok, results[0].error
    finally:
        fresh.close()


def test_sqlite_cells_ref_each_other_in_context(tmp_path) -> None:
    con = sqlite3.connect(tmp_path / "legacy.sqlite")
    con.execute("CREATE TABLE nums (n INTEGER)")
    con.executemany("INSERT INTO nums VALUES (?)", [(1,), (2,), (3,)])
    con.commit()
    con.close()

    project = compile_text(
        "/*@ input: { sqlite: legacy.sqlite } */\n"
        "-- @cell parent\nSELECT n FROM nums WHERE n > 1;\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    assert project.cells["child"].engine == "sqlite"  # legal now: same context
    assert "FROM parent" in project.cells["child"].sql
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    assert pl.read_parquet(tmp_path / "data" / "parent.parquet").height == 2
    assert pl.read_parquet(tmp_path / "data" / "child.parquet")["c"][0] == 2


def test_sqlite_memory_context_shares_one_connection(tmp_path) -> None:
    # separate connections would each get their own :memory: db — the child
    # seeing the parent's temp table proves the context session is shared
    project = compile_text(
        "/*@ input: { sqlite: ':memory:' } */\n"
        "-- @cell seed\nSELECT 41 + 1 AS answer;\n"
        "-- @cell reader\nSELECT answer FROM {{ ref('seed') }};",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    assert pl.read_parquet(tmp_path / "data" / "reader.parquet")["answer"][0] == 42


def test_chain_wraps_outermost_first(tmp_path) -> None:
    calls: list[str] = []

    @plugin
    class InnerMost(Plugin):
        priority = 10

        def run(self, cell, ctx, inner):
            calls.append("in:start")
            result = inner(cell, ctx)
            calls.append("in:end")
            return result

    @plugin
    class OuterMost(Plugin):
        priority = -10

        def run(self, cell, ctx, inner):
            calls.append("out:start")
            result = inner(cell, ctx)
            calls.append("out:end")
            return result

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    assert project.run()[0].ok
    assert calls == ["out:start", "in:start", "in:end", "out:end"]


def test_retries_plugin_reinvokes_inner_on_error_result(tmp_path) -> None:
    attempts: list[int] = []

    @plugin
    class Retries(Plugin):
        def run(self, cell, ctx, inner):
            result = inner(cell, ctx)
            attempts.append(1)
            if not result.ok:
                result = inner(cell, ctx)
                attempts.append(2)
            return result

    project = compile_text("-- @cell bad\nSELECT * FROM nope;", root=tmp_path)
    results = project.run()
    assert results[0].ok is False
    assert attempts == [1, 2]


def test_buggy_plugin_degrades_to_error_result(tmp_path) -> None:
    @plugin
    class Buggy(Plugin):
        def run(self, cell, ctx, inner):
            raise RuntimeError("plugin exploded")

    project = compile_text(
        "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT 2 AS y;", root=tmp_path
    )
    results = project.run()
    assert all(r.ok is False for r in results)
    assert "plugin exploded" in results[0].error


def test_emit_sql_plugin_writes_rendered_sql(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\nSELECT 1 AS x;",
        root=tmp_path,
        overrides={"render_dir": "build/sql"},
    )
    assert project.run()[0].ok
    assert (tmp_path / "build" / "sql" / "a.sql").read_text().strip() == "SELECT 1 AS x;"


# ---------- run events: live started/step/finished stream ----------


def test_on_event_streams_the_cell_lifecycle_in_order(tmp_path) -> None:
    from quicksql.models import RunEvent

    project = compile_text(
        "-- @cell users\nSELECT * FROM range(3) t(user_id);\n"
        "-- @cell active\nSELECT * FROM {{ ref('users') }} WHERE user_id > 0;",
        root=tmp_path,
    )
    events: list[RunEvent] = []
    results = run_project(project, on_event=events.append)
    assert all(r.ok for r in results)

    kinds = [e.kind for e in events]
    assert kinds[0] == "run_started" and kinds[-1] == "run_finished"
    assert "2 cell" in events[0].detail

    # per cell: started -> fine-grained steps -> finished, in topo order
    def cell_events(name):
        return [e for e in events if e.cell == name]

    for name in ("users", "active"):
        seq = cell_events(name)
        assert seq[0].kind == "cell_started"
        assert "duckdb" in seq[0].detail and "parquet" in seq[0].detail
        steps = [e.detail for e in seq if e.kind == "cell_step"]
        assert any("executing on duckdb" in s for s in steps)
        assert any("landing via parquet" in s for s in steps)
        assert any("preview" in s for s in steps)
        assert seq[-1].kind == "cell_finished"
        assert seq[-1].result is not None and seq[-1].result.ok
    # all users events precede all active events (topo order, no interleave)
    assert events.index(cell_events("users")[-1]) < events.index(cell_events("active")[0])
    assert "2 ok, 0 failed" in events[-1].detail


def test_run_started_carries_the_post_expansion_cell_list(tmp_path) -> None:
    """run_started.cells is the authoritative list of what will actually run —
    including upstreams pulled in by _expand_stale_upstreams, which a consumer
    (the TUI cell table) cannot recover from its own select list."""
    from quicksql.models import RunEvent

    project = compile_text(
        "-- @cell parent\nSELECT 1 AS x;\n"
        "-- @cell child\nSELECT * FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    fresh = RunSession()
    try:  # cold session: the parent's temp is missing, so it joins the run
        events: list[RunEvent] = []
        run_project(project, select=["child"], closure=False, session=fresh, on_event=events.append)
        started = [e for e in events if e.kind == "run_started"]
        assert started and started[0].cells == ("parent", "child")
        # warm session: exact selection stays exact, and cells says so
        events.clear()
        run_project(project, select=["child"], closure=False, session=fresh, on_event=events.append)
        started = [e for e in events if e.kind == "run_started"]
        assert started and started[0].cells == ("child",)
    finally:
        fresh.close()


def test_run_started_lists_a_pulled_in_failed_upstream(tmp_path) -> None:
    from quicksql.models import RunEvent

    project = compile_text(CROSS_BROKEN, root=tmp_path)
    session = RunSession()
    try:
        run_project(project, session=session)
        events: list[RunEvent] = []
        run_project(project, select=["b"], closure=False, session=session, on_event=events.append)
        started = [e for e in events if e.kind == "run_started"]
        assert started and started[0].cells == ("a", "b")
    finally:
        session.close()


def test_on_event_reports_failures_and_run_continues(tmp_path) -> None:
    project = compile_text(
        "-- @cell bad\nSELECT * FROM missing_table;\n-- @cell good\nSELECT 1 AS x;",
        root=tmp_path,
    )
    events = []
    results = run_project(project, on_event=events.append)
    finished = {e.cell: e.result for e in events if e.kind == "cell_finished"}
    assert finished["bad"].ok is False and finished["good"].ok is True
    assert [r.cell for r in results] == ["bad", "good"]
    assert "1 ok, 1 failed" in [e for e in events if e.kind == "run_finished"][-1].detail


def test_broken_event_callback_never_kills_the_run(tmp_path) -> None:
    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)

    def boom(event) -> None:
        raise RuntimeError("consumer bug")

    results = run_project(project, on_event=boom)
    assert len(results) == 1 and results[0].ok  # the run is unharmed


def test_extension_load_step_is_attributed_to_the_cell(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\n-- @extensions: [json]\nSELECT 1 AS x;", root=tmp_path
    )
    events = []
    assert run_project(project, on_event=events.append)[0].ok
    loads = [
        e
        for e in events
        if e.kind == "cell_step" and "loading duckdb extension json" in e.detail
    ]
    assert loads, [e.detail for e in events]
    assert all(e.cell == "a" for e in loads)


def test_plugin_lifecycle_failures_surface_as_notes_before_run_finished(tmp_path) -> None:
    @plugin
    class BrokenAfterRun(Plugin):
        def after_run(self, project, results) -> None:
            raise RuntimeError("observer bug")

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    events = []
    assert run_project(project, on_event=events.append)[0].ok
    kinds = [e.kind for e in events]
    note = next(e for e in events if e.kind == "note" and "after_run failed" in e.detail)
    assert kinds[-1] == "run_finished"  # run_finished stays the terminal event
    assert events.index(note) < kinds.index("run_finished")


def test_run_event_line_renders_each_kind() -> None:
    from quicksql.models import RunEvent, RunResult

    ok = RunResult(cell="a", status="ok", rows=3, elapsed=0.012, target="data/a.parquet")
    assert RunEvent("cell_finished", "a", result=ok).line() == (
        "a: ok — 3 rows in 12 ms -> data/a.parquet"
    )
    bad = RunResult(
        cell="a",
        status="failed",
        error="Catalog Error: Table with name nope does not exist!\n"
        "\nLINE 1: SELECT * FROM nope\n                      ^",
    )
    assert RunEvent("cell_finished", "a", result=bad).line() == (
        "a: FAILED — Catalog Error: Table with name nope does not exist!"
    )
    assert RunEvent("cell_started", "a", "duckdb → parquet").line() == (
        "a: started (duckdb → parquet)"
    )
    assert RunEvent("cell_step", "a", "executing on duckdb").line() == "a: executing on duckdb"
    assert RunEvent("run_started", detail="2 cell(s): a, b").line() == "run started — 2 cell(s): a, b"
    assert RunEvent("run_finished", detail="2 ok, 0 failed in 0.1s").line() == (
        "run finished — 2 ok, 0 failed in 0.1s"
    )
    assert RunEvent("note", detail="plugin x after_run failed").line() == (
        "note: plugin x after_run failed"
    )


def test_run_event_line_renders_a_skipped_result() -> None:
    from quicksql.models import RunEvent, RunResult

    skipped = RunResult(cell="b", status="skipped", error="upstream 'a' failed")
    assert RunEvent("cell_finished", "b", result=skipped).line() == (
        "b: skipped — upstream 'a' failed"
    )


# ---------- the cell protocol: the runner drives every engine's session ----------


def _recording_session(calls: list):
    from quicksql.executors.base import EngineSession

    class Recording(EngineSession):
        """Records what the runner asks; a materialized temp reads back as 'READ <name>'."""

        def execute(self, sql, *, scoped=True):
            calls.append(("execute", sql, scoped))

        def materialize(self, name, sql):
            calls.append(("materialize", name, sql))
            return f"READ {name}"

        def fetch(self, sql, *, scoped=True):
            calls.append(("fetch", sql, scoped))
            return pl.DataFrame({"x": [1]})

        def close(self):
            calls.append(("close",))

    return Recording()


@pytest.fixture
def recording_engine():
    """Engine `rec`, whose sessions record the runner's calls; yields the calls."""
    from quicksql.executors.base import Executor, split_statements
    from quicksql.registry import executor

    calls: list = []

    @executor("rec")
    class Rec(Executor):
        supports_context_refs = True

        def split_statements(self, sql):
            return split_statements(sql, "duckdb")

        def open_session(self, config, ctx):
            calls.append(("open",))
            return _recording_session(calls)

    return calls


def test_a_plain_cell_runs_its_setup_then_extracts_its_terminal_statement(
    tmp_path, recording_engine
) -> None:
    project = compile_text(
        "-- @engine: rec\n-- @cell a\nCREATE TEMP TABLE s AS SELECT 1;\nSELECT * FROM s;",
        root=tmp_path,
    )
    [result] = project.run()
    assert result.ok, result.error
    assert recording_engine == [
        ("open",),
        ("execute", "CREATE TEMP TABLE s AS SELECT 1", False),
        ("fetch", "SELECT * FROM s", False),
        ("close",),  # with the run's private run session
    ]
    assert pl.read_parquet(tmp_path / "data" / "a.parquet")["x"].to_list() == [1]


def test_an_effect_only_cell_runs_every_statement_and_extracts_nothing(
    tmp_path, recording_engine
) -> None:
    project = compile_text(
        "-- @engine: rec\n-- @cell a\n-- @output: { type: none }\n"
        "CREATE TABLE t (n INT);\nINSERT INTO t VALUES (1);",
        root=tmp_path,
    )
    [result] = project.run()
    assert result.ok, result.error
    assert recording_engine == [
        ("open",),
        ("execute", "CREATE TABLE t (n INT)", False),
        ("execute", "INSERT INTO t VALUES (1)", False),
        ("close",),
    ]


def test_a_cell_reffed_in_context_materializes_once_and_lands_from_its_temp(
    tmp_path, recording_engine
) -> None:
    project = compile_text(
        "-- @engine: rec\n-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT * FROM {{ ref('a') }};",
        root=tmp_path,
    )
    results = project.run()
    assert all(r.ok for r in results), [r.error for r in results]
    assert recording_engine == [
        ("open",),  # one session for the context
        ("materialize", "a", "SELECT 1 AS x"),
        ("fetch", "READ a", True),  # a lands from its temp instead of running again
        ("fetch", "SELECT * FROM a", True),  # b reads the temp in-engine
        ("close",),
    ]


def test_without_a_run_session_a_cell_gets_a_private_session(tmp_path, recording_engine) -> None:
    import duckdb

    from quicksql.models import RunContext
    from quicksql.runner import execute_cell

    cell = compile_text("-- @engine: rec\n-- @cell a\nSELECT 1 AS x;", root=tmp_path).cells["a"]
    conn = duckdb.connect()
    try:
        view = execute_cell(cell, RunContext(conn=conn, root=tmp_path))
        assert conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(1,)]
    finally:
        conn.close()
    assert recording_engine == [("open",), ("fetch", "SELECT 1 AS x", False), ("close",)]
