"""``quicksql live …`` and the live-aware commands, against a live session
served from a thread (as the open TUI serves it)."""

import json
import os
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from typer.testing import CliRunner

from quicksql.cli import app
from quicksql.live.protocol import file_hash
from quicksql.models import RunEvent, RunResult
from quicksql.runner import run_project
from test_live_server import NOTEBOOK, Live, feed_run, ok

runner = CliRunner()


@pytest.fixture
def nb(tmp_path) -> Path:
    path = tmp_path / "nb.qsql"
    path.write_text(NOTEBOOK)
    return path


@pytest.fixture
def live(nb, fake_driver):
    session = Live(nb, fake_driver)
    assert session.why is None
    yield session
    session.close()


def cli(*args: str):
    return runner.invoke(app, list(args))


# ---------- no session ----------


def test_a_live_command_without_a_tui_exits_4_and_says_how(nb) -> None:
    result = cli("live", "arm", str(nb))
    assert result.exit_code == 4
    assert "quicksql tui" in result.output


def test_its_json_error_is_one_document(nb) -> None:
    result = cli("live", "wait", str(nb), "--json")
    assert result.exit_code == 4
    assert json.loads(result.stdout)["error"]["code"] == "no_session"


# ---------- control ----------


def test_arm_and_disarm(live) -> None:
    assert cli("live", "arm", str(live.nb)).exit_code == 0
    assert live.host.armed is True
    result = cli("live", "disarm", str(live.nb), "--json")
    assert result.exit_code == 0 and json.loads(result.stdout)["armed"] is False


def test_autorun_per_cell_and_for_all(live) -> None:
    assert cli("live", "autorun", "off", str(live.nb), "-s", "a").exit_code == 0
    assert live.host.autorun_off == {"a"}
    assert cli("live", "autorun", "off", str(live.nb), "--all").exit_code == 0
    assert live.host.autorun_global is False
    assert cli("live", "autorun", "off", str(live.nb), "-s", "ghost").exit_code == 3
    assert cli("live", "autorun", "off", str(live.nb)).exit_code == 2  # which cells?
    assert cli("live", "autorun", "maybe", str(live.nb), "--all").exit_code == 2


def test_run_waits_for_its_results(live) -> None:
    def finish_when_started() -> None:  # the TUI's run, as it reports to the host
        for _ in range(200):
            if live.driver.started:
                break
            threading.Event().wait(0.02)
        live.on_loop(lambda: feed_run(live.host, live.host.latest_run, [ok("a"), ok("b")]))

    feeder = threading.Thread(target=finish_when_started)
    feeder.start()
    result = cli("live", "run", str(live.nb), "-s", "b", "--wait", "--json")
    feeder.join()
    assert result.exit_code == 0, result.output
    doc = json.loads(result.stdout)
    assert doc["status"] == "ok" and [c["cell"] for c in doc["cells"]] == ["a", "b"]


def test_run_while_a_run_is_under_way_exits_5(live) -> None:
    live.on_loop(lambda: live.host.run_requested(1, None))
    result = cli("live", "run", str(live.nb), "-s", "a")
    assert result.exit_code == 5 and "--cancel" in result.output


def test_cancel(live) -> None:
    result = cli("live", "cancel", str(live.nb), "--json")
    assert result.exit_code == 0 and json.loads(result.stdout)["cancelled"] is False


def test_focus_and_notify(live) -> None:
    assert cli("live", "focus", "b", str(live.nb), "--tab", "data").exit_code == 0
    assert cli("live", "notify", "look at b", str(live.nb)).exit_code == 0
    assert live.driver.focused == [("b", "data")]
    assert live.driver.toasts == [("look at b", "information")]


# ---------- watching ----------


def test_wait_reports_a_paused_session(live) -> None:
    result = cli("live", "wait", str(live.nb))
    assert result.exit_code == 0
    assert "paused" in result.output and "quicksql live arm" in result.output


def test_wait_exits_3_on_a_compile_error(live) -> None:
    errors = [{"cell": "b", "line": 4, "message": "unknown ref"}]
    live.nb.write_text(NOTEBOOK + "\n-- @cell c\nSELECT * FROM {{ ref('ghost') }};\n")
    live.on_loop(lambda: live.host.compile_failed(file_hash(live.nb), errors))
    result = cli("live", "wait", str(live.nb), "--json")
    assert result.exit_code == 3
    assert json.loads(result.stdout)["errors"] == errors


def test_wait_exits_1_when_a_cell_failed(live) -> None:
    failed = RunResult(cell="b", status="failed", error="Binder Error: nope\nLINE 1: ...", statement=1)
    live.on_loop(lambda: (live.host.run_requested(1, live.host.project.source_hash),
                          feed_run(live.host, 1, [ok("a"), failed])))
    result = cli("live", "wait", str(live.nb))
    assert result.exit_code == 1
    assert "FAIL b" in result.output and "Binder Error: nope" in result.output
    assert "line" in result.output  # where to look in the file


def test_events_prints_ndjson(live) -> None:
    live.on_loop(lambda: live.host.set_armed(True, by="keys"))
    result = cli("live", "events", str(live.nb))
    assert result.exit_code == 0
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert events[-1]["kind"] == "controls_changed"
    since = events[-1]["seq"]
    assert cli("live", "events", str(live.nb), "--since", str(since)).stdout == ""


def test_status_is_live_when_a_tui_has_the_notebook(live) -> None:
    live.on_loop(lambda: live.host.set_armed(True, by="keys"))
    result = cli("status", str(live.nb), "--json")
    assert result.exit_code == 0
    doc = json.loads(result.stdout)
    assert doc["live"] is True and doc["controls"]["armed"] is True
    assert [c["cell"] for c in doc["cells"]] == ["a", "b"]
    text = cli("status", str(live.nb)).output
    assert "live" in text and "armed" in text


def test_status_offline_says_so(nb) -> None:
    doc = json.loads(cli("status", str(nb), "--json").stdout)
    assert doc["live"] is False


# ---------- data ----------


@pytest.fixture
def landed(live):
    assert all(r.ok for r in run_project(live.host.project))
    return live


def test_head_reads_through_the_session(landed) -> None:
    result = cli("head", "a", str(landed.nb), "--rows", "2", "--json")
    assert result.exit_code == 0, result.output
    doc = json.loads(result.stdout)
    assert doc["rows"] == [[0], [1]] and doc["source"] == "live"


def test_head_reads_offline_without_a_tui(nb, registries) -> None:
    from quicksql.bootstrap import compile_file

    assert all(r.ok for r in run_project(compile_file(nb)))
    result = cli("head", "a", str(nb), "--json")
    assert result.exit_code == 0, result.output
    doc = json.loads(result.stdout)
    assert doc["rows"] == [[0], [1], [2]] and doc["source"] == "offline"
    assert "x" in cli("head", "a", str(nb)).output


def test_query_and_its_refusals(landed) -> None:
    result = cli("live", "query", "SELECT sum(x) AS total FROM a", str(landed.nb), "--json")
    assert result.exit_code == 0 and json.loads(result.stdout)["rows"] == [[3]]
    assert "total" in cli("live", "query", "SELECT sum(x) AS total FROM a", str(landed.nb)).output
    assert cli("live", "query", "DROP VIEW a", str(landed.nb)).exit_code == 5
    assert cli("live", "query", "SELECT nope FROM a", str(landed.nb)).exit_code == 1


# ---------- one runner per notebook ----------


def test_run_and_watch_refuse_while_a_tui_has_the_notebook(live) -> None:
    result = cli("run", str(live.nb))
    assert result.exit_code == 5 and "quicksql live run" in result.output
    doc = json.loads(cli("run", str(live.nb), "--json").stdout)
    assert doc["status"] == "refused"
    assert cli("watch", str(live.nb)).exit_code == 5


# ---------- the catalog ----------


def test_catalog_prints_a_level_and_reads_a_leaf(landed) -> None:
    result = cli("live", "catalog", str(landed.nb))
    assert result.exit_code == 0, result.output
    assert "duckdb::memory:" in result.output and "output" in result.output
    doc = json.loads(cli("live", "catalog", str(landed.nb), "a", "--json").stdout)
    assert doc["leaf"] is True and doc["path"] == ["a"] and doc["rows"][0][1] == "x"


def test_catalog_pages_with_rows_and_offset(landed) -> None:
    doc = json.loads(cli("live", "catalog", str(landed.nb), "-n", "1", "--offset", "1", "--json").stdout)
    assert doc["names"] == ["a"] and doc["next_offset"] == 2
    assert "--offset 1" in cli("live", "catalog", str(landed.nb), "-n", "1").output  # the next page


def test_catalog_match_and_columns(landed) -> None:
    result = cli("live", "catalog", str(landed.nb), "--match", "A", "--columns", "kind,name", "--json")
    doc = json.loads(result.stdout)  # any case: only "a" has an a ("duckdb::memory:" hasn't)
    assert doc["columns"] == ["kind", "name"] and doc["names"] == ["a"]


def test_an_unknown_catalog_path_exits_3(landed) -> None:
    result = cli("live", "catalog", str(landed.nb), "nope")
    assert result.exit_code == 3 and "nope" in result.output


def test_catalog_without_a_tui_exits_4(nb) -> None:
    assert cli("live", "catalog", str(nb)).exit_code == 4


def test_catalog_text_shows_each_name_once_and_whole(tmp_path, fake_driver) -> None:
    """A row's name is what to type next: never cut, and not repeated in a
    column of its own when a column already holds it."""
    import sqlite3

    con = sqlite3.connect(tmp_path / "s.db")
    con.execute("CREATE TABLE t (x INTEGER)")
    con.close()
    dsn = "postgresql://reader:pw@a-rather-long-hostname.example.internal:5432/warehouse"
    nb = tmp_path / "nb.qsql"
    nb.write_text(
        f"/*@ catalog: {{ postgres: ['{dsn}'] }} */\n"
        "-- @cell a\n-- @engine: sqlite\n/*@ input: { sqlite: s.db } */\nSELECT 1 AS x;\n"
    )
    session = Live(nb, fake_driver)
    try:
        top = cli("live", "catalog", str(nb)).output
        assert "postgres:postgresql://reader:***@a-rather-long-hostname.example.internal:5432/warehouse" in top
        level = cli("live", "catalog", str(nb), "sqlite:s.db").output.splitlines()
        assert level[1].split() == ["table"]  # the names are the table column itself
    finally:
        session.close()


# ---------- the client loads neither Polars nor Textual ----------

WITHOUT = "import sys; sys.modules.update(polars=None, textual=None)\nfrom quicksql.cli import app\napp()"


def test_live_commands_need_neither_polars_nor_textual(live) -> None:
    env = {**os.environ}
    for args in (["live", "arm", str(live.nb)], ["live", "wait", str(live.nb), "--json"],
                 ["status", str(live.nb), "--json"], ["live", "catalog", str(live.nb), "--json"]):
        done = subprocess.run(
            [sys.executable, "-c", WITHOUT, *args], capture_output=True, text=True, env=env,
            timeout=60,
        )
        assert done.returncode == 0, done.stderr
