"""The live session over its socket: a host served on an event loop in a
thread (as the TUI serves it on its own), talked to with the CLI's client."""

import asyncio
import json
import os
import socket
import stat
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from quicksql.bootstrap import compile_file
from quicksql.live.client import LiveClient, probe
from quicksql.live.host import LiveHost
from quicksql.live.protocol import PROTOCOL, LiveError, encode, file_hash, socket_path
from quicksql.live.server import LiveServer
from quicksql.models import RunEvent, RunResult
from quicksql.runner import run_project

NOTEBOOK = """\
-- @cell a
SELECT range AS x FROM range(3);

-- @cell b
SELECT count(*) AS n FROM {{ ref('a') }};
"""


class Live:
    """A host and its server on a loop in a thread; ``on_loop`` runs host
    code where the TUI would, on that loop."""

    def __init__(self, nb: Path, driver, *, screen: bool = True, queue: int = 1000) -> None:
        self.nb = nb
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()
        self.driver = driver
        self.host = LiveHost(driver, nb)
        driver.host = self.host
        self.on_loop(lambda: self.host.loaded(compile_file(nb)))
        self.server = self.on_loop(lambda: LiveServer(self.host, nb, screen=screen, queue=queue))
        self.why = self.wait(self.server.start())

    def wait(self, coro):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(15)

    def on_loop(self, fn):
        async def call():
            return fn()

        return self.wait(call())

    def close(self) -> None:
        self.on_loop(self.server.close)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)

    def client(self) -> LiveClient:
        return LiveClient.connect(self.nb)


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


def ok(cell: str) -> RunResult:
    return RunResult(cell=cell, status="ok", rows=1, target=f"data/{cell}.parquet")


def feed_run(host: LiveHost, run: int, results: list[RunResult]) -> None:
    project = host.project
    host.run_adopted(run, project)
    host.run_event(run, RunEvent("run_started", cells=tuple(r.cell for r in results)))
    for result in results:
        host.run_event(run, RunEvent("cell_started", result.cell))
        host.run_event(run, RunEvent("cell_finished", result.cell, result=result))
    host.run_ended(run, results)


# ---------- the socket ----------


def test_hello_then_a_snapshot(live) -> None:
    with live.client() as client:
        assert client.hello["protocol"] == PROTOCOL
        assert client.hello["notebook"] == str(live.nb)
        assert "snapshot" in client.hello["methods"]
        snap = client.call("snapshot")
    assert [c["cell"] for c in snap["cells"]] == ["a", "b"]
    assert snap["controls"]["armed"] is False


def test_the_socket_is_the_users_alone(live) -> None:
    path = socket_path(live.nb)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700


def test_a_client_of_another_protocol_is_told_to_restart_the_tui(live) -> None:
    raw = socket.socket(socket.AF_UNIX)
    raw.connect(str(socket_path(live.nb)))
    raw.sendall(encode({"id": 1, "method": "hello", "params": {"protocol": PROTOCOL + 1}}))
    reply = json.loads(raw.makefile("rb").readline())
    assert reply["error"]["code"] == "protocol" and "restart the TUI" in reply["error"]["message"]
    raw.close()


def test_hello_comes_first(live) -> None:
    raw = socket.socket(socket.AF_UNIX)
    raw.connect(str(socket_path(live.nb)))
    raw.sendall(encode({"id": 1, "method": "snapshot", "params": {}}))
    assert json.loads(raw.makefile("rb").readline())["error"]["code"] == "protocol"
    raw.close()


def test_no_session_without_a_socket(tmp_path) -> None:
    other = tmp_path / "other.qsql"
    other.write_text(NOTEBOOK)
    with pytest.raises(LiveError) as err:
        LiveClient.connect(other)
    assert err.value.code == "no_session" and "quicksql tui" in err.value.message
    assert probe(other) is None


def test_probe_finds_the_session(live) -> None:
    assert probe(live.nb)["notebook"] == str(live.nb)


def test_a_stale_socket_is_replaced(nb, fake_driver) -> None:
    path = socket_path(nb)
    path.parent.mkdir(parents=True, exist_ok=True)
    dead = socket.socket(socket.AF_UNIX)
    dead.bind(str(path))  # bound, never listening: what a crashed TUI leaves
    dead.close()
    session = Live(nb, fake_driver)
    try:
        assert session.why is None
        with session.client() as client:
            assert client.call("snapshot")["cells"]
    finally:
        session.close()


def test_a_second_session_for_the_notebook_stays_off(live, fake_driver) -> None:
    from conftest import FakeDriver

    second = Live(live.nb, FakeDriver())
    try:
        assert second.why and "open" in second.why
        with live.client() as client:  # the first one still answers
            assert client.call("snapshot")
    finally:
        second.loop.call_soon_threadsafe(second.loop.stop)
        second.thread.join(5)


def test_a_long_home_moves_the_socket_to_the_runtime_dir(tmp_path, monkeypatch, fake_driver) -> None:
    deep = tmp_path / ("h" * 60) / ("o" * 60)
    monkeypatch.setenv("QSQL_HOME", str(deep))
    runtime = tmp_path / "rt"
    runtime.mkdir()
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(runtime))
    nb = tmp_path / "nb.qsql"
    nb.write_text(NOTEBOOK)
    path = socket_path(nb)
    assert path.parent == runtime / "quicksql" and len(os.fsencode(path)) <= 100
    session = Live(nb, fake_driver)
    try:
        assert session.why is None
        with session.client() as client:
            assert client.call("snapshot")
    finally:
        session.close()


def test_closing_removes_the_socket(nb, fake_driver) -> None:
    session = Live(nb, fake_driver)
    path = socket_path(nb)
    assert path.exists()
    session.close()
    assert not path.exists()


# ---------- events ----------


def test_events_since_a_sequence(live) -> None:
    seq = live.host.seq
    live.on_loop(lambda: live.host.set_armed(True, by="keys"))
    with live.client() as client:
        got = client.call("events.since", seq=seq)
    assert [e["kind"] for e in got["events"]] == ["controls_changed"]
    assert got["seq"] == live.host.seq


def test_a_subscription_streams_the_backlog_then_what_happens(live) -> None:
    seq = live.host.seq
    live.on_loop(lambda: live.host.set_armed(True, by="keys"))
    client = live.client()
    stream = client.events(since=seq, kinds=["controls_changed"])
    first = next(stream)
    assert first["armed"] is True and first["seq"] == seq + 1
    live.on_loop(lambda: live.host.set_armed(False, by="agent"))
    second = next(stream)
    assert second["armed"] is False and second["by"] == "agent"
    client.close()


def test_a_subscriber_that_falls_behind_is_told_events_lost(nb, fake_driver) -> None:
    session = Live(nb, fake_driver, queue=2)
    try:
        client = session.client()
        stream = client.events()
        session.on_loop(lambda: session.host.set_armed(True, by="keys"))
        assert next(stream)["kind"] == "controls_changed"  # subscribed and draining

        def burst() -> None:  # many events with no await between: the queue overflows
            for on in (False, True, False, True, False):
                session.host.set_armed(on, by="keys")

        session.on_loop(burst)
        with pytest.raises(LiveError) as err:
            for _ in stream:
                pass
        assert err.value.code == "events_lost"
        client.close()
    finally:
        session.close()


# ---------- wait ----------


def test_wait_answers_at_once_for_a_settled_save(live) -> None:
    with live.client() as client:
        outcome = client.call("wait", hash=file_hash(live.nb), timeout=5)
    assert outcome["state"] == "paused" and outcome["superseded"] is False


def test_wait_holds_until_the_saves_rerun_ends(live) -> None:
    host = live.host
    live.on_loop(lambda: host.set_armed(True, by="keys"))
    live.nb.write_text(NOTEBOOK.replace("range(3)", "range(4)"))
    h = file_hash(live.nb)
    with ThreadPoolExecutor(1) as pool, live.client() as client:
        waiting = pool.submit(client.call, "wait", hash=h, timeout=10)

        def save_and_rerun() -> None:
            project = compile_file(live.nb)
            to_run = host.recompiled(project, ["a", "b"])
            host.run_requested(1, project.source_hash)
            feed_run(host, 1, [ok(c) for c in to_run])

        live.on_loop(save_and_rerun)
        outcome = waiting.result(10)
    assert outcome["state"] == "ran" and outcome["hash"] == h
    assert outcome["summary"]["ok"] == 2


def test_wait_times_out(live) -> None:
    with live.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("wait", hash="0123456789abcdef", timeout=0.3)
    assert err.value.code == "timeout"


# ---------- control ----------


def test_an_agent_arms_and_switches_autorun(live) -> None:
    with live.client() as client:
        assert client.call("arm")["armed"] is True
        assert client.call("autorun", cells=["a"], on=False)["autorun_off"] == ["a"]
        assert client.call("autorun", all=True, on=False)["autorun_global"] is False
        with pytest.raises(LiveError) as err:
            client.call("autorun", cells=["nope"], on=False)
        assert err.value.code == "unknown_cell"
    assert live.driver.logged[:2] == ["agent: arm", "agent: autorun a off"]


def test_an_agent_run_and_its_wait(live) -> None:
    with live.client() as client:
        run_id = client.call("run", cells=["b"])["run_id"]
        assert live.driver.started[-1] == (["b"], True)
        with pytest.raises(LiveError) as err:
            client.call("run", cells=["a"])
        assert err.value.code == "busy"
        live.on_loop(lambda: feed_run(live.host, run_id, [ok("a"), ok("b")]))
        record = client.call("run.wait", run_id=run_id, timeout=5)
    assert record["state"] == "finished" and record["summary"]["ok"] == 2
    assert [c["cell"] for c in record["cells"]] == ["a", "b"]


def test_focus_and_notify_reach_the_screen(live) -> None:
    with live.client() as client:
        client.call("focus", cell="b", tab="data")
        client.call("notify", message="look at b", severity="warning")
    assert live.driver.focused == [("b", "data")]
    assert live.driver.toasts == [("look at b", "warning")]


def test_a_session_without_a_screen_has_no_focus(nb, fake_driver) -> None:
    session = Live(nb, fake_driver, screen=False)
    try:
        with session.client() as client:
            assert "focus" not in client.hello["methods"]
            with pytest.raises(LiveError) as err:
                client.call("focus", cell="a")
        assert err.value.code == "unsupported"
    finally:
        session.close()


# ---------- reading data through the session ----------


@pytest.fixture
def landed(live):
    assert all(r.ok for r in run_project(live.host.project))
    return live


def test_head_reads_a_landed_output(landed) -> None:
    with landed.client() as client:
        head = client.call("head", cell="a", rows=2)
    assert head == {"cell": "a", "columns": ["x"], "rows": [[0], [1]]}


def test_head_of_a_running_cell_is_busy(landed) -> None:
    host = landed.host
    landed.on_loop(lambda: (host.run_requested(1, None),
                            host.run_event(1, RunEvent("run_started", cells=("a",))),
                            host.run_event(1, RunEvent("cell_started", "a"))))
    with landed.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("head", cell="a")
    assert err.value.code == "busy"


def test_query_names_cells_as_views(landed) -> None:
    with landed.client() as client:
        got = client.call("query", sql="SELECT sum(x) AS total FROM a", rows=5)
    assert got == {"columns": ["total"], "rows": [[3]]}


@pytest.mark.parametrize(
    "sql",
    ["DROP TABLE a", "SELECT 1; DROP VIEW a", "CREATE TABLE t AS SELECT 1", "COPY a TO 'x.csv'"],
)
def test_query_takes_one_select_only(landed, sql) -> None:
    with landed.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("query", sql=sql)
    assert err.value.code == "refused"


def test_a_failing_query_says_why(landed) -> None:
    with landed.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("query", sql="SELECT nope FROM a")
    assert err.value.code == "query_failed" and "nope" in err.value.message


def test_a_bug_in_a_handler_answers_internal_and_the_connection_lives(live, monkeypatch) -> None:
    def broken():
        raise RuntimeError("a bug")

    monkeypatch.setattr(live.host, "snapshot", broken)
    with live.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("snapshot")
        assert err.value.code == "internal" and "a bug" in err.value.message
        assert client.call("arm")["armed"] is True  # same connection, still answering


# ---------- the catalog ----------


def test_the_catalog_lists_a_level_by_its_path(landed) -> None:
    with landed.client() as client:
        assert "catalog" in client.hello["methods"]
        top = client.call("catalog", path=[])
        leaf = client.call("catalog", path=["a"])
    assert top["names"] == ["duckdb::memory:", "a", "b"]
    assert leaf["leaf"] is True and [row[1] for row in leaf["rows"]] == ["x"]


def test_an_unknown_catalog_path_answers_unknown_path(live) -> None:
    with live.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("catalog", path=["nope"])
    assert err.value.code == "unknown_path" and "nope" in err.value.message


@pytest.mark.parametrize("params", [
    {"path": "a"}, {"path": [1]}, {"rows": "x"}, {"offset": True}, {"match": 3},
    {"columns": "kind"}, {"top_level": "yes"}, {"timeout": 0},
])
def test_catalog_params_are_checked(live, params) -> None:
    with live.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("catalog", **params)
    assert err.value.code == "invalid"


def test_a_slow_catalog_load_times_out_and_carries_on(live, monkeypatch) -> None:
    import polars as pl

    from quicksql.catalog import CatalogNode

    gate = threading.Event()
    frame = pl.DataFrame({"name": ["x"]})

    def load():
        gate.wait(10)
        return frame

    slow = CatalogNode(title="slow", load=load)
    monkeypatch.setattr(live.host, "catalog_root", lambda project=None: slow)
    with live.client() as client:
        with pytest.raises(LiveError) as err:
            client.call("catalog", path=[], timeout=0.3)
        assert err.value.code == "timeout" and "again" in err.value.message
        gate.set()
        got = client.call("catalog", path=[], timeout=5)
    assert got["names"] == ["x"]
