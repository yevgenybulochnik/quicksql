import sqlite3
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest

from quicksql.config import resolve_cell, resolve_engine, resolve_sink_type
from quicksql.errors import ExecutorError
from quicksql.executors.bigquery_exec import BigQueryExecutor, BigQuerySession
from quicksql.executors.postgres_exec import PostgresExecutor
from quicksql.models import RenderedCell, RunContext
from quicksql.registry import EXECUTORS
from quicksql.runner import execute_cell


def _cell(name: str, sql: str, cell_raw: dict | None = None) -> RenderedCell:
    cfg = resolve_cell({}, cell_raw or {})
    return RenderedCell(
        name=name, config=cfg, sql_raw=sql, sql=sql, hash="",
        engine=resolve_engine(cfg), sink_type=resolve_sink_type(cfg),
    )


@pytest.fixture
def ctx(tmp_path):
    conn = duckdb.connect()
    yield RunContext(conn=conn, root=tmp_path)
    conn.close()


def test_split_statements_splits_on_top_level_semicolons() -> None:
    from quicksql.executors.base import split_statements

    assert split_statements("SELECT 1; SELECT 2", "duckdb") == ["SELECT 1", "SELECT 2"]


def test_split_statements_preserves_original_text_verbatim() -> None:
    from quicksql.executors.base import split_statements

    # sqlglot re-rendering would uppercase LIST_TRANSFORM and space out x+1;
    # slicing the original text must return it byte-for-byte.
    sql = "SELECT list_transform(xs, x -> x+1) AS ys"
    assert split_statements(sql, "duckdb") == [sql]


def test_split_statements_ignores_semicolons_in_strings_and_comments() -> None:
    from quicksql.executors.base import split_statements

    assert split_statements("SELECT 'a;b' AS x", "duckdb") == ["SELECT 'a;b' AS x"]
    assert split_statements("SELECT 1 /* a;b */; SELECT 2", "duckdb") == [
        "SELECT 1 /* a;b */",
        "SELECT 2",
    ]


def test_split_statements_drops_trailing_semicolon_and_blanks() -> None:
    from quicksql.executors.base import split_statements

    assert split_statements("SELECT 1;", "duckdb") == ["SELECT 1"]
    assert split_statements("SELECT 1;;\n; SELECT 2;", "duckdb") == ["SELECT 1", "SELECT 2"]


def test_split_statements_falls_back_on_unparseable() -> None:
    from quicksql.executors.base import split_statements

    # an unterminated string literal makes the tokenizer raise; treat the whole
    # body as one statement rather than blowing up.
    assert split_statements("SELECT 'oops", "duckdb") == ["SELECT 'oops"]


def test_split_statements_drops_comment_only_tail() -> None:
    from quicksql.executors.base import split_statements

    # a trailing semicolon followed by only comments (e.g. a rendered {% if %}
    # that collapsed, then help comments) must not become a phantom statement.
    body = "SELECT u.name\nFROM users u\n;\n\n-- Things to try:\n--   * something\n"
    assert split_statements(body, "duckdb") == ["SELECT u.name\nFROM users u"]


def test_statement_spans_returns_raw_offsets_that_tile_the_source() -> None:
    from quicksql.executors.base import statement_spans

    sql = "SELECT 1; SELECT 2"
    spans = statement_spans(sql, "duckdb")
    assert spans == [(0, 8), (9, 18)]  # raw segments (leading ws kept), split on the ';'
    assert [sql[a:b].strip() for a, b in spans] == ["SELECT 1", "SELECT 2"]


def test_statement_spans_excludes_comment_only_tail() -> None:
    from quicksql.executors.base import statement_spans

    sql = "SELECT 1;\n-- trailing comment"
    assert [sql[a:b].strip() for a, b in statement_spans(sql, "duckdb")] == ["SELECT 1"]


def test_statement_spans_empty_on_tokenizer_error() -> None:
    from quicksql.executors.base import statement_spans

    assert statement_spans("SELECT 'oops", "duckdb") == []


def test_executor_split_policy_bigquery_never_splits() -> None:
    body = "CREATE TEMP TABLE t AS SELECT 1; SELECT * FROM t"
    assert EXECUTORS.get("duckdb").split_statements(body) == [
        "CREATE TEMP TABLE t AS SELECT 1",
        "SELECT * FROM t",
    ]
    # bigquery has native scripting; splitting client-side would lose session
    # state, so it sends the whole body as one job.
    assert EXECUTORS.get("bigquery").split_statements(body) == [body]


def test_multistatement_materialization_capability() -> None:
    assert EXECUTORS.get("duckdb").supports_multistatement_materialization is True
    assert EXECUTORS.get("sqlite").supports_multistatement_materialization is True
    assert EXECUTORS.get("postgres").supports_multistatement_materialization is True
    assert EXECUTORS.get("bigquery").supports_multistatement_materialization is False


def test_duckdb_executor_creates_view(ctx) -> None:
    view = execute_cell(_cell("a", "SELECT 42 AS x;"), ctx)
    assert ctx.conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(42,)]


def test_sqlite_executor_registers_result(ctx, tmp_path) -> None:
    db = tmp_path / "nums.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE nums (n INTEGER)")
    con.executemany("INSERT INTO nums VALUES (?)", [(1,), (2,), (3,)])
    con.commit()
    con.close()

    cell = _cell("sq", "SELECT n FROM nums ORDER BY n", {"input": {"sqlite": "nums.sqlite"}})
    view = execute_cell(cell, ctx)
    assert ctx.conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(1,), (2,), (3,)]


def test_sqlite_executor_bad_sql_raises(ctx) -> None:
    cell = _cell("sq", "SELECT * FROM missing", {"input": {"sqlite": ":memory:"}})
    with pytest.raises(ExecutorError, match="sqlite"):
        execute_cell(cell, ctx)


def test_sqlite_failure_message_does_not_repeat_the_cell_name(tmp_path) -> None:
    """The CLI prints "FAIL a: <message>": the engine prefix is news, the
    cell name isn't."""
    from quicksql.compiler import compile_text

    project = compile_text(
        "-- @cell a\n-- @input: {sqlite: \":memory:\"}\nSELECT * FROM nope\n", root=tmp_path
    )
    [result] = project.run()
    assert result.error == "sqlite: no such table: nope"


def test_duckdb_setup_then_terminal_lands_terminal(ctx) -> None:
    cell = _cell(
        "enriched",
        "CREATE TEMP TABLE staging AS SELECT 1 AS n;\nSELECT n * 2 AS m FROM staging",
    )
    view = execute_cell(cell, ctx)
    assert ctx.conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(2,)]


def test_sqlite_setup_then_terminal_lands_terminal(ctx) -> None:
    cell = _cell(
        "sq",
        "CREATE TEMP TABLE staging AS SELECT 5 AS n;\nSELECT n + 1 AS m FROM staging",
        {"input": {"sqlite": ":memory:"}},
    )
    view = execute_cell(cell, ctx)
    assert ctx.conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(6,)]


def test_sqlite_terminal_without_result_set_raises(ctx) -> None:
    cell = _cell(
        "sq",
        "CREATE TABLE t (n INT);\nINSERT INTO t VALUES (1)",  # terminal INSERT: no result set
        {"input": {"sqlite": ":memory:"}},
    )
    with pytest.raises(ExecutorError, match="^statement 2 of 2: sqlite query returned no result set$"):
        execute_cell(cell, ctx)


def test_duckdb_effect_only_runs_all_statements_lands_nothing(ctx) -> None:
    cell = _cell(
        "load",
        "CREATE TABLE t (n INT);\nINSERT INTO t VALUES (1), (2)",
        {"output": {"type": "none"}},
    )
    view = execute_cell(cell, ctx)
    assert view == ""  # effect-only: nothing to land
    assert ctx.conn.sql("SELECT count(*) FROM t").fetchone() == (2,)


def test_bigquery_same_context_cells_share_a_session(tmp_path, monkeypatch) -> None:
    from types import SimpleNamespace

    from quicksql.compiler import compile_text

    calls: list[tuple[str, object]] = []
    events: list[str] = []  # submit/wait interleaving: sessions forbid concurrency

    class FakeJob:
        def __init__(self, sql: str, first: bool) -> None:
            self._sql = sql
            self.session_info = SimpleNamespace(session_id="sess-1") if first else None

        def result(self):
            events.append("wait")
            return self

        def to_arrow(self):
            return pl.DataFrame({"n": [1, 2, 3]})

    class FakeClient:
        def query(self, sql, job_config=None):
            events.append("submit")
            calls.append((sql, job_config))
            return FakeJob(sql, first=len(calls) == 1)

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    monkeypatch.setattr(BigQuerySession, "_job_config", lambda self: {"session": self.session_id})
    project = compile_text(
        "/*@ input: { bigquery: { project: p } } */\n"
        "-- @cell parent\nSELECT * FROM src;\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    assert project.cells["child"].engine == "bigquery"  # legal: same context
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]

    sqls = [sql for sql, _ in calls]
    assert any(s.startswith("CREATE OR REPLACE TEMP TABLE parent AS") for s in sqls)
    assert "SELECT * FROM parent" in sqls          # extraction reads the temp once
    assert sum("FROM src" in s for s in sqls) == 1  # the parent query ran exactly once
    assert any("count(*)" in s and "FROM parent" in s for s in sqls)  # child in-engine
    # every job after the first rides the captured session id
    assert calls[0][1] == {"session": None}
    assert all(cfg == {"session": "sess-1"} for _, cfg in calls[1:])
    # regression: real sessions reject concurrent jobs — every submit must be
    # waited on before the next one goes in
    assert events == ["submit", "wait"] * len(calls)
    # landing-by-default: the parent stays inspectable as parquet
    assert (tmp_path / "data" / "parent.parquet").exists()
    assert (tmp_path / "data" / "child.parquet").exists()


def test_bigquery_reffed_cte_parent_materializes_unwrapped(tmp_path, monkeypatch) -> None:
    """The reffed body goes into CTAS bare, as on the other engines: wrapped in
    parens, a trailing line comment swallowed the closing one (BigQuery: Syntax
    error: Expected ")" but got end of statement)."""
    import sqlglot

    from quicksql.compiler import compile_text

    sqls: list[str] = []

    class FakeJob:
        session_info = SimpleNamespace(session_id="sess-1")

        def result(self):
            return self

        def to_arrow(self):
            return pl.DataFrame({"n": [1]})

    class FakeClient:
        def query(self, sql, job_config=None):
            sqls.append(sql)
            return FakeJob()

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    monkeypatch.setattr(BigQuerySession, "_job_config", lambda self: None)
    body = "WITH totals AS (\n  SELECT n FROM src\n)\nSELECT n FROM totals  -- trailing note"
    project = compile_text(
        "/*@ input: { bigquery: { project: p } } */\n"
        f"-- @cell parent\n{body}\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]

    ctas = next(s for s in sqls if s.startswith("CREATE OR REPLACE TEMP TABLE parent"))
    assert ctas == f"CREATE OR REPLACE TEMP TABLE parent AS {body}"
    sqlglot.parse_one(ctas, read="bigquery")  # no paren left for the comment to eat


def test_bigquery_executor_with_fake_client(ctx, monkeypatch) -> None:
    frame = pl.DataFrame({"user_id": [1, 2], "event": ["click", "view"]})

    class FakeJob:
        def to_arrow(self):
            # stands in for a pyarrow.Table; duckdb registers either the same way
            return frame

    class FakeClient:
        def query(self, sql):
            assert "events" in sql
            return FakeJob()

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    cell = _cell("bq", "SELECT * FROM events", {"input": {"bigquery": {"project": "p"}}})
    view = execute_cell(cell, ctx)
    assert ctx.conn.sql(f'SELECT count(*) FROM "{view}"').fetchone() == (2,)


def test_bigquery_emulator_endpoint_disables_retries(tmp_path, monkeypatch) -> None:
    """goccy reports every execution failure with a retryable-looking reason
    (jobInternalError), so the client's default 600s job retry re-submits a
    failing query for ten minutes — wedging the TUI's run worker; a down
    emulator blocks the same way via the API retry. Emulator endpoints must
    fail fast: retry=None and job_retry=None on every query. Specs without
    an endpoint (real BigQuery) keep the client defaults."""
    from quicksql.compiler import compile_text

    captured: list[dict] = []

    class FakeJob:
        session_info = None

        def result(self):
            return self

        def to_arrow(self):
            return pl.DataFrame({"x": [1]})

    class FakeClient:
        def query(self, sql, job_config=None, **kwargs):
            captured.append(kwargs)
            return FakeJob()

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    monkeypatch.setattr(BigQuerySession, "_job_config", lambda self: None)
    project = compile_text(
        "/*@ input: { bigquery: { project: p, endpoint: http://localhost:9050 } } */\n"
        "-- @cell parent\nSELECT 1 AS x;\n"
        "-- @cell child\nSELECT * FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    assert captured  # both the session path and the plain path submit jobs
    assert all(
        "retry" in k and k["retry"] is None and "job_retry" in k and k["job_retry"] is None
        for k in captured
    ), captured


def test_bigquery_make_client_honors_emulator_endpoint() -> None:
    """An `endpoint` in the input spec points the client at an emulator: no
    real credentials, no ADC lookup."""
    pytest.importorskip(
        "google.cloud.bigquery", reason="needs the 'bigquery' extra"
    )
    from google.auth.credentials import AnonymousCredentials

    client = BigQueryExecutor().make_client(
        {"project": "p", "endpoint": "http://localhost:9050"}
    )
    assert client._connection.API_BASE_URL == "http://localhost:9050"
    assert isinstance(client._credentials, AnonymousCredentials)


@pytest.mark.bigquery
def test_bigquery_cell_runs_against_the_emulator(tmp_path, bq_emulator) -> None:
    from quicksql.compiler import compile_text

    project = compile_text(
        f"/*@ input: {{ bigquery: {{ project: quicksql-test, endpoint: {bq_emulator} }} }} */\n"
        "-- @cell nums\nSELECT 1 AS n UNION ALL SELECT 2 AS n;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["nums"].ok, results["nums"].error
    landed = pl.read_parquet(tmp_path / "data" / "nums.parquet")
    assert sorted(landed["n"].to_list()) == [1, 2]


@pytest.mark.bigquery
def test_bigquery_multistatement_script_lands_terminal(tmp_path, bq_emulator) -> None:
    from quicksql.compiler import compile_text

    # bigquery runs the whole script server-side; the terminal SELECT's rows land
    project = compile_text(
        f"/*@ input: {{ bigquery: {{ project: quicksql-test, endpoint: {bq_emulator} }} }} */\n"
        "-- @cell scripted\n"
        "CREATE TEMP TABLE staging AS SELECT 3 AS n UNION ALL SELECT 4 AS n;\n"
        "SELECT n * 10 AS m FROM staging;\n",
        root=tmp_path,
    )
    results = {r.cell: r for r in project.run()}
    assert results["scripted"].ok, results["scripted"].error
    landed = pl.read_parquet(tmp_path / "data" / "scripted.parquet")
    assert sorted(landed["m"].to_list()) == [30, 40]


class FakePgCursor:
    """Stands in for a psycopg cursor: .description (objects with .name) + rows."""

    def __init__(self, sql: str) -> None:
        head = sql.lstrip().split(None, 1)[0].upper()
        if head in ("DROP", "CREATE"):  # statements: no result set
            self.description = None
            self._rows: list[tuple] = []
        else:
            self.description = [SimpleNamespace(name="n")]
            self._rows = [(1,), (2,), (3,)]

    def fetchall(self) -> list[tuple]:
        return self._rows


class FakePgConnection:
    def __init__(self) -> None:
        self.sqls: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> FakePgCursor:
        self.sqls.append(sql)
        if "boom" in sql:
            raise RuntimeError("relation missing")
        return FakePgCursor(sql)

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_pg(monkeypatch):
    """Route make_connection to fakes; yields the list of connections created."""
    created: list[FakePgConnection] = []

    def make(self, dsn):
        created.append(FakePgConnection())
        return created[-1]

    monkeypatch.setattr(PostgresExecutor, "make_connection", make)
    return created


def test_postgres_executor_registers_result(ctx, fake_pg) -> None:
    cell = _cell("pg", "SELECT n FROM nums;", {"input": {"postgres": {"dsn": "postgresql://x"}}})
    view = execute_cell(cell, ctx)
    assert ctx.conn.sql(f'SELECT * FROM "{view}"').fetchall() == [(1,), (2,), (3,)]
    # trailing semicolon stripped; no session -> the ephemeral connection is closed
    assert fake_pg[0].sqls == ["SELECT n FROM nums"]
    assert fake_pg[0].closed


def test_postgres_setup_then_terminal_runs_in_order(ctx, fake_pg) -> None:
    cell = _cell(
        "pg",
        "CREATE TEMP TABLE staging AS SELECT n FROM src;\nSELECT n FROM staging",
        {"input": {"postgres": {"dsn": "postgresql://x"}}},
    )
    execute_cell(cell, ctx)
    # setup statement runs for effect, then the terminal statement is the result
    assert fake_pg[0].sqls == [
        "CREATE TEMP TABLE staging AS SELECT n FROM src",
        "SELECT n FROM staging",
    ]


def test_postgres_executor_string_shorthand_and_env_dsn(monkeypatch) -> None:
    monkeypatch.setenv("PG_DSN", "postgresql://quicksql@localhost/quicksql")
    cfg = resolve_cell({}, {"input": {"postgres": "$PG_DSN"}})
    key = EXECUTORS.get("postgres").context_key(cfg)
    assert key == "postgres:postgresql://quicksql@localhost/quicksql"


def test_bigquery_string_shorthand_names_the_project() -> None:
    cfg = resolve_cell({}, {"input": {"bigquery": "my-proj"}})
    assert EXECUTORS.get("bigquery").context_key(cfg) == "bigquery:my-proj"


def test_postgres_executor_missing_dsn_raises(ctx, fake_pg) -> None:
    cell = _cell("pg", "SELECT 1", {"engine": "postgres"})
    with pytest.raises(ExecutorError, match="^postgres engine needs input.postgres.dsn$"):
        execute_cell(cell, ctx)


def test_postgres_executor_wraps_driver_errors(ctx, fake_pg) -> None:
    cell = _cell("pg", "SELECT boom", {"input": {"postgres": {"dsn": "postgresql://x"}}})
    with pytest.raises(ExecutorError, match="^postgres: relation missing$"):
        execute_cell(cell, ctx)


def test_postgres_same_context_cells_share_a_connection(tmp_path, fake_pg) -> None:
    from quicksql.compiler import compile_text

    project = compile_text(
        "/*@ input: { postgres: { dsn: 'postgresql://x' } } */\n"
        "-- @cell parent\nSELECT n FROM src;\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    assert project.cells["child"].engine == "postgres"  # legal: same context
    results = {r.cell: r for r in project.run()}
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]

    assert len(fake_pg) == 1  # one shared session connection for the context
    sqls = fake_pg[0].sqls
    # the reffed parent materializes once as a session temp, then extracts from it
    assert 'CREATE TEMP TABLE "parent" AS SELECT n FROM src' in sqls
    assert 'SELECT * FROM "parent"' in sqls
    assert sum("FROM src" in s for s in sqls) == 1
    # the child consumes the temp by bare name, in postgres
    assert any("count(*)" in s and "FROM parent" in s for s in sqls)
    # landing-by-default: both cells still inspectable as parquet
    assert (tmp_path / "data" / "parent.parquet").exists()
    assert (tmp_path / "data" / "child.parquet").exists()
