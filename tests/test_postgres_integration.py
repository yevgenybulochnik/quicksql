"""Integration tests against the compose Postgres (docker compose up -d --wait).

Run with: uv run --extra postgres pytest -m postgres
Each test drops/recreates its own tables in quicksql_test, so runs are idempotent.
"""

import duckdb
import pytest

from quicksql.compiler import compile_text
from quicksql.errors import ExecutorError
from quicksql.models import RunContext
from quicksql.runner import execute_cell

pytestmark = pytest.mark.postgres


@pytest.fixture
def seeded(pg_dsn):
    """A small nums table in quicksql_test; yields the dsn."""
    import psycopg

    with psycopg.connect(pg_dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS nums")
        con.execute("CREATE TABLE nums (n int)")
        con.execute("INSERT INTO nums SELECT generate_series(1, 5)")
    return pg_dsn


def _cell_config(dsn: str) -> str:
    return f"/*@ input: {{ postgres: {{ dsn: '{dsn}' }} }} */\n"


def test_executor_runs_real_sql(seeded, tmp_path) -> None:
    from quicksql.config import resolve_cell
    from quicksql.models import RenderedCell

    cfg = resolve_cell({}, {"input": {"postgres": {"dsn": seeded}}})
    cell = RenderedCell(
        name="pg", config=cfg, sql_raw="", hash="",
        sql="SELECT n, n * n AS sq FROM nums ORDER BY n",
        engine="postgres", sink_type="parquet",
    )
    conn = duckdb.connect()
    try:
        view = execute_cell(cell, RunContext(conn=conn, root=tmp_path))
        assert conn.sql(f'SELECT * FROM "{view}"').fetchall() == [
            (1, 1), (2, 4), (3, 9), (4, 16), (5, 25)
        ]
    finally:
        conn.close()


def test_real_driver_errors_wrap_as_executor_error(seeded, tmp_path) -> None:
    """The offline test fakes the driver; only a real psycopg error proves the wrap."""
    from quicksql.config import resolve_cell
    from quicksql.models import RenderedCell

    cfg = resolve_cell({}, {"input": {"postgres": {"dsn": seeded}}})
    cell = RenderedCell(
        name="pg", config=cfg, sql_raw="", hash="", sql="SELECT * FROM no_such_table",
        engine="postgres", sink_type="parquet",
    )
    conn = duckdb.connect()
    try:
        with pytest.raises(ExecutorError, match="postgres.*no_such_table"):
            execute_cell(cell, RunContext(conn=conn, root=tmp_path))
    finally:
        conn.close()


def test_same_context_cells_share_session_temps(seeded, tmp_path) -> None:
    project = compile_text(
        _cell_config(seeded)
        + "-- @cell parent\nSELECT n FROM nums WHERE n <= 3;\n"
        + "-- @cell child\nSELECT count(*) AS c, sum(n) AS s FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    assert project.cells["child"].engine == "postgres"  # legal: same context
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    con = duckdb.connect()
    try:
        assert con.sql(
            f"SELECT c, s FROM read_parquet('{tmp_path / 'data' / 'child.parquet'}')"
        ).fetchall() == [(3, 6)]
    finally:
        con.close()


def test_multistatement_setup_then_terminal_lands_terminal(seeded, tmp_path) -> None:
    # a real psycopg connection: the setup statement runs for effect (temp table),
    # then the terminal SELECT is what lands.
    project = compile_text(
        _cell_config(seeded)
        + "-- @cell enriched\n"
        + "CREATE TEMP TABLE staging AS SELECT n FROM nums WHERE n <= 3;\n"
        + "SELECT n * 2 AS doubled FROM staging ORDER BY n;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["enriched"].ok, results["enriched"].error
    con = duckdb.connect()
    try:
        assert con.sql(
            f"SELECT doubled FROM read_parquet('{tmp_path / 'data' / 'enriched.parquet'}') ORDER BY doubled"
        ).fetchall() == [(2,), (4,), (6,)]
    finally:
        con.close()


def test_effect_only_cell_runs_and_lands_nothing(seeded, tmp_path) -> None:
    # an effect-only cell writes a real table via side effect and lands no parquet
    project = compile_text(
        _cell_config(seeded)
        + "-- @cell make_scratch\n-- @output: { type: none }\n"
        + "CREATE TABLE IF NOT EXISTS ms_scratch (n INT);\n"
        + "INSERT INTO ms_scratch VALUES (7), (8);\n",
        root=tmp_path,
    )
    try:
        result = project.run()[0]
        assert result.ok, result.error
        assert result.rows == 0 and result.target == "none"
        assert not (tmp_path / "data" / "make_scratch.parquet").exists()
        import psycopg

        with psycopg.connect(seeded, autocommit=True) as con:
            assert con.execute("SELECT count(*) FROM ms_scratch").fetchone()[0] == 2
    finally:
        import psycopg

        with psycopg.connect(seeded, autocommit=True) as con:
            con.execute("DROP TABLE IF EXISTS ms_scratch")


def test_failed_cell_does_not_poison_the_session(seeded, tmp_path) -> None:
    """watch/TUI keep one session across reruns: without autocommit a failed
    statement would leave it aborted and every later cell would fail too."""
    from quicksql.runner import RunSession, run_project

    session = RunSession()
    try:
        broken = compile_text(
            _cell_config(seeded) + "-- @cell broken\nSELECT * FROM no_such_table;",
            root=tmp_path,
        )
        assert not {r.cell: r for r in run_project(broken, session=session)}["broken"].ok

        # same session, same connection: a healthy cell must still run
        healthy = compile_text(
            _cell_config(seeded) + "-- @cell healthy\nSELECT count(*) AS c FROM nums;",
            root=tmp_path,
        )
        result = {r.cell: r for r in run_project(healthy, session=session)}["healthy"]
        assert result.ok, result.error
    finally:
        session.close()


def test_cross_engine_duckdb_reads_postgres_cell(seeded, tmp_path) -> None:
    project = compile_text(
        _cell_config(seeded)
        + "-- @cell extract\nSELECT n FROM nums;\n"
        + "-- @cell crunch\n-- @engine: duckdb\n"
        + "SELECT max(n) AS top FROM {{ ref('extract') }};",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    con = duckdb.connect()
    try:
        assert con.sql(
            f"SELECT top FROM read_parquet('{tmp_path / 'data' / 'crunch.parquet'}')"
        ).fetchall() == [(5,)]
    finally:
        con.close()


def test_round_trip_through_postgres_sink(seeded, tmp_path) -> None:
    """postgres engine cell lands back into Postgres via the existing sink
    (needs duckdb's postgres extension: network once, to install it)."""
    import psycopg

    project = compile_text(
        _cell_config(seeded)
        + f"-- @cell doubled\n/*@ output: {{ type: postgres, dsn: '{seeded}' }} */\n"
        + "SELECT n, n * 2 AS twice FROM nums ORDER BY n;",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    with psycopg.connect(seeded) as con:
        rows = con.execute("SELECT n, twice FROM public.doubled ORDER BY n").fetchall()
    assert rows == [(1, 2), (2, 4), (3, 6), (4, 8), (5, 10)]


def test_postgres_sink_reports_the_rows_each_write_landed(seeded, tmp_path) -> None:
    """replace reports the table's rows; append only the rows it inserted"""
    import psycopg

    from quicksql.models import RenderedCell
    from quicksql.registry import SINKS

    cell = RenderedCell(
        name="acc", config=None, sql_raw="", sql="", hash="",
        engine="duckdb", sink_type="postgres",
    )
    conn = duckdb.connect()
    try:
        conn.execute("LOAD postgres")
        conn.execute("CREATE TEMP VIEW v AS SELECT 1 AS x UNION ALL SELECT 2")
        replace = SINKS.get("postgres")({"dsn": seeded}, tmp_path)
        append = SINKS.get("postgres")({"dsn": seeded, "mode": "append"}, tmp_path)
        replace.prepare(conn)
        replaced, _ = replace.write(cell, "v", conn)
        appended, _ = append.write(cell, "v", conn)
    finally:
        conn.close()
    assert (replaced, appended) == (2, 2)
    with psycopg.connect(seeded) as con:
        assert con.execute("SELECT count(*) FROM public.acc").fetchone() == (4,)


def test_postgres_sink_append_mode_creates_its_table_on_the_first_run(seeded, tmp_path) -> None:
    import psycopg

    with psycopg.connect(seeded, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS public.events_log")
    project = compile_text(
        f"-- @cell events_log\n/*@ output: {{ type: postgres, dsn: '{seeded}', mode: append }} */\n"
        "SELECT 1 AS n UNION ALL SELECT 2;",
        root=tmp_path,
    )
    for _ in range(2):  # the first run creates the table, the second appends to it
        [result] = project.run()
        assert result.ok, result.error
        assert result.rows == 2
    with psycopg.connect(seeded) as con:
        assert con.execute("SELECT count(*) FROM public.events_log").fetchone() == (4,)


def test_materializing_a_cell_never_drops_a_real_table_of_its_name(seeded, tmp_path) -> None:
    """A reffed cell's temp shares its name with whatever the search_path holds:
    replacing the temp with an unqualified DROP dropped a real table of that name."""
    import psycopg

    with psycopg.connect(seeded, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS public.parent")
        con.execute("CREATE TABLE public.parent AS SELECT 42 AS keep")
    try:
        project = compile_text(
            _cell_config(seeded)
            + "-- @cell parent\nSELECT n FROM nums;\n"
            + "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
            root=tmp_path,
        )
        results = {r.cell: r for r in project.run()}
        assert all(r.ok for r in results.values()), [r.error for r in results.values()]
        with psycopg.connect(seeded) as con:
            assert con.execute("SELECT keep FROM public.parent").fetchall() == [(42,)]
    finally:
        with psycopg.connect(seeded, autocommit=True) as con:
            con.execute("DROP TABLE IF EXISTS public.parent")
