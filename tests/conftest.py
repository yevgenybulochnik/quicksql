import os

import pytest

from quicksql import registry
from quicksql.bootstrap import load_builtins


@pytest.fixture(autouse=True)
def fresh_registries():
    """Snapshot every registry so tests can register scratch plugins freely."""
    load_builtins()
    snaps = [(r, r.snapshot()) for r in registry.ALL_REGISTRIES]
    yield
    for reg, snap in snaps:
        reg.restore(snap)


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
def discard_sink():
    """A third-party effect-only sink: lands_output = False, but not named 'none'."""
    from quicksql.registry import sink
    from quicksql.sinks.base import Sink

    @sink("discard")
    class Discard(Sink):
        lands_output = False

        def write(self, cell, view, conn):
            return 0, "discarded"

        def ref_expr(self, cell_name):
            return "nowhere"

    return Discard
