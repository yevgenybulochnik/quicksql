import os

import pytest

from quicksql.bootstrap import builtin_registries


@pytest.fixture
def registries():
    """This test's own registries: the builtins, to add scratch plugins to
    (``registries.add(MyPlugin)``) and compile with (``registries=registries``).
    Decorators only mark classes, so nothing leaks between tests."""
    return builtin_registries()


@pytest.fixture
def add_plugin(registries):
    """``@add_plugin`` on a scratch plugin class: ``@plugin``, then added to
    this test's ``registries``."""
    from quicksql.registry import plugin

    def decorate(cls):
        registries.add(plugin(cls))
        return cls

    return decorate


@pytest.fixture(autouse=True)
def qsql_home(tmp_path_factory, monkeypatch):
    """Keep every test out of the real ~/.quicksql: runs write state files
    there, and the notebook picker reads user templates from it."""
    home = tmp_path_factory.mktemp("qsql-home")
    monkeypatch.setenv("QSQL_HOME", str(home))
    return home


@pytest.fixture
def pg_dsn():
    """DSN of the compose Postgres's test database (docker compose up -d --wait);
    postgres-marked tests skip when psycopg or the server is unavailable."""
    dsn = os.environ.get(
        "QUICKSQL_TEST_PG_DSN", "postgresql://quicksql:quicksql@localhost:5432/quicksql_test"
    )
    psycopg = pytest.importorskip(
        "psycopg", reason="postgres tests need the 'postgres' extra"
    )
    try:
        psycopg.connect(dsn, connect_timeout=2).close()
    except psycopg.OperationalError as exc:
        pytest.skip(f"no postgres server at {dsn}: {exc}")
    return dsn


@pytest.fixture
def bq_emulator():
    """Endpoint of the compose bigquery-emulator (docker compose up -d --wait;
    project ``quicksql-test``); bigquery-marked tests skip when the 'bigquery'
    extra or the emulator is unavailable."""
    import urllib.error
    import urllib.request

    endpoint = os.environ.get("QUICKSQL_TEST_BQ_ENDPOINT", "http://localhost:9050")
    pytest.importorskip(
        "google.cloud.bigquery", reason="bigquery tests need the 'bigquery' extra"
    )
    try:
        urllib.request.urlopen(endpoint, timeout=2)
    except urllib.error.HTTPError:
        pass  # any HTTP response means the emulator is listening
    except OSError as exc:
        pytest.skip(f"no bigquery emulator at {endpoint}: {exc}")
    return endpoint


@pytest.fixture
def project_dir(tmp_path):
    (tmp_path / "seeds").mkdir()
    (tmp_path / "seeds" / "users.csv").write_text(
        "user_id,name,active\n1,ada,true\n2,bob,false\n3,cyd,true\n"
    )
    return tmp_path


@pytest.fixture
def discard_sink(registries):
    """A third-party effect-only sink: lands_output = False, but not named 'none'.
    Added to the test's ``registries``."""
    from quicksql.registry import sink
    from quicksql.sinks.base import Sink

    @sink("discard")
    class Discard(Sink):
        lands_output = False

        def write(self, cell, view, conn):
            return 0, "discarded"

        def ref_expr(self, cell_name):
            return "nowhere"

    registries.add(Discard)
    return Discard


@pytest.fixture
def tsql_engine(registries):
    """A third-party engine ``mssql`` written in T-SQL, added to the test's
    ``registries`` with no core edits: it declares ``dialect = "tsql"`` (its
    name is no sqlglot dialect) and a catalog of one database, ``wh``, holding
    ``dbo.t (x INT)``. Its sessions record each statement they run; the
    fixture's value is that list."""
    import polars as pl

    from quicksql.executors.base import EngineSession, Executor, field_path_frame
    from quicksql.models import CatalogNode
    from quicksql.registry import executor

    ran: list[str] = []

    class Session(EngineSession):
        def execute(self, sql, *, scoped=True):
            ran.append(sql)

        def fetch(self, sql, *, scoped=True):
            ran.append(sql)
            return pl.DataFrame({"x": [1]})

    def table(schema: str, name: str) -> CatalogNode | None:
        if (schema, name) != ("dbo", "t"):
            return None
        return CatalogNode(
            title=f"{schema}.{name}",
            load=lambda: field_path_frame([("x", "x", "INT", "", "")]),
        )

    def database(db: str) -> CatalogNode:
        return CatalogNode(
            title=f"mssql:{db}",
            load=lambda: pl.DataFrame({"schema": ["dbo"], "table": ["t"]}),
            child=lambda row: table(row["schema"], row["table"]),
        )

    @executor("mssql")
    class MsSql(Executor):
        dialect = "tsql"
        supports_context_refs = True

        def context_key(self, config):
            return "mssql:wh"

        def open_session(self, config, ctx):
            return Session()

        def catalog(self, config, root):
            return database("wh")

        def catalog_target(self, target, root):
            return f"mssql:{target}", database(target)

        def catalog_table(self, config, root, parts):
            return table(parts[-2] if len(parts) >= 2 else "dbo", parts[-1])

    registries.add(MsSql)
    return ran
