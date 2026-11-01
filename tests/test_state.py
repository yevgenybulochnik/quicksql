"""Fingerprints and the state file: what a cell's result depends on, and what
each landed output was built from."""

import json

import pytest

from quicksql.compiler import compile_file, compile_text
from quicksql.runner import RunSession, run_project
from quicksql.state import Landed, current_fingerprints, read_state, statuses

BASE = "-- @cell a\nSELECT 1 AS x;\n"


def _fingerprint(tmp_path, text: str, landed: dict | None = None) -> str:
    project = compile_text(text, root=tmp_path)
    return current_fingerprints(project, landed or {})["a"]


@pytest.mark.parametrize(
    "edited",
    [
        BASE.replace("SELECT 1", "SELECT 2"),
        "-- @output: { type: duckdb }\n" + BASE,
        "-- @vars: { n: 1 }\n" + BASE,
        "-- @engine: sqlite\n" + BASE,
    ],
    ids=["sql", "output", "vars", "engine"],
)
def test_a_fingerprint_follows_what_the_result_depends_on(tmp_path, edited) -> None:
    assert _fingerprint(tmp_path, edited) != _fingerprint(tmp_path, BASE)


@pytest.mark.parametrize(
    "edited",
    ["-- @tags: [daily]\n" + BASE, BASE.replace("-- @cell a\n", "-- @cell a\n-- @autorun: false\n")],
    ids=["tags", "autorun"],
)
def test_a_fingerprint_ignores_fields_that_dont_affect_results(tmp_path, edited) -> None:
    assert _fingerprint(tmp_path, edited) == _fingerprint(tmp_path, BASE)


def test_a_fingerprint_follows_the_landed_fingerprints_of_its_upstreams(tmp_path) -> None:
    text = "-- @cell up\nSELECT 1 AS x;\n-- @cell a\nSELECT * FROM {{ ref('up') }};\n"
    before = _fingerprint(tmp_path, text, {"up": Landed("f1", "own", {})})
    after = _fingerprint(tmp_path, text, {"up": Landed("f2", "own", {})})
    assert before != after
    # an edit to the upstream's SQL alone changes nothing downstream until it lands
    edited = text.replace("SELECT 1", "SELECT 2")
    assert _fingerprint(tmp_path, edited, {"up": Landed("f1", "own", {})}) == before


# ---------- the state file ----------


def _notebook(tmp_path, text: str):
    path = tmp_path / "base.qsql"
    path.write_text(text)
    return path


def _state_files(qsql_home) -> list:
    return sorted((qsql_home / "state").glob("*.json"))


def test_each_run_writes_the_state_file_under_the_quicksql_home(tmp_path, qsql_home) -> None:
    path = _notebook(tmp_path, "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT * FROM {{ ref('a') }};\n")
    project = compile_file(path)
    assert all(r.ok for r in run_project(project))
    [state] = _state_files(qsql_home)
    assert state.name.startswith("base.qsql-")
    doc = json.loads(state.read_text())
    assert doc["notebook"] == str(path.resolve())  # so a prune can find orphans
    entries = doc["cells"]
    assert set(entries) == {"a", "b"}
    landed = {name: cell.landed for name, cell in read_state(project).items()}
    current = current_fingerprints(project, landed)
    assert {name: e["status"] for name, e in entries.items()} == {"a": "ok", "b": "ok"}
    assert {name: e["landed"]["fingerprint"] for name, e in entries.items()} == current
    # nothing lands next to the notebook
    assert sorted(p.name for p in tmp_path.iterdir()) == ["base.qsql", "data"]


def test_each_notebook_has_its_own_state_file(tmp_path, qsql_home) -> None:
    run_project(compile_file(_notebook(tmp_path, BASE)))
    other = tmp_path / "other.qsql"
    other.write_text("-- @cell z\nSELECT 9 AS z;\n")
    run_project(compile_file(other))
    assert len(_state_files(qsql_home)) == 2
    assert set(read_state(compile_file(other))) == {"z"}


def test_two_spellings_of_one_notebook_share_its_state(tmp_path, qsql_home) -> None:
    path = _notebook(tmp_path, BASE)
    (tmp_path / "sub").mkdir()
    run_project(compile_file(path))
    assert set(read_state(compile_file(tmp_path / "sub" / ".." / "base.qsql"))) == {"a"}
    assert len(_state_files(qsql_home)) == 1


def test_the_quicksql_home_defaults_to_dot_quicksql_in_the_users_home(tmp_path, monkeypatch) -> None:
    from quicksql.state import quicksql_home

    monkeypatch.delenv("QSQL_HOME")
    monkeypatch.setenv("HOME", str(tmp_path))
    assert quicksql_home() == tmp_path / ".quicksql"


def test_a_run_keeps_the_state_of_cells_it_didnt_run(tmp_path) -> None:
    path = _notebook(tmp_path, "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT 2 AS y;\n")
    run_project(compile_file(path))
    run_project(compile_file(path), select=["a"])
    assert set(read_state(compile_file(path))) == {"a", "b"}


def test_a_notebook_compiled_from_text_writes_no_state(tmp_path, qsql_home) -> None:
    run_project(compile_text(BASE, root=tmp_path))
    assert _state_files(qsql_home) == []
    assert not (tmp_path / ".quicksql").exists()


# ---------- status: ok / failed / skipped / stale / new ----------


def _statuses(path) -> dict[str, tuple[str, str]]:
    return {s.cell: (s.status, s.reason) for s in statuses(compile_file(path))}


def test_status_reports_each_cells_last_outcome(tmp_path) -> None:
    path = _notebook(
        tmp_path,
        "-- @cell good\nSELECT 1 AS x;\n"
        "-- @cell bad\nSELECT * FROM nope;\n"
        "-- @cell after_bad\nSELECT * FROM {{ ref('bad') }};\n"
        "-- @cell never\nSELECT 3 AS z;\n",
    )
    run_project(compile_file(path), select=["good", "after_bad"])
    assert {name: status for name, (status, _) in _statuses(path).items()} == {
        "good": "ok", "bad": "failed", "after_bad": "skipped", "never": "new",
    }


def test_an_upstream_edit_marks_downstream_stale_until_it_reruns(tmp_path) -> None:
    text = "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
    path = _notebook(tmp_path, text)
    run_project(compile_file(path))
    path.write_text(text.replace("SELECT 1", "SELECT 2"))
    assert _statuses(path)["a"] == ("stale", "its SQL or config changed")
    assert _statuses(path)["b"][0] == "ok"  # a hasn't landed anything new yet
    run_project(compile_file(path), select=["a"], closure=False)
    assert _statuses(path) == {"a": ("ok", ""), "b": ("stale", "upstream 'a' changed")}
    run_project(compile_file(path), select=["b"], closure=False)
    assert _statuses(path)["b"] == ("ok", "")


def test_rerunning_an_edited_manual_upstream_by_hand_leaves_its_downstream_stale(tmp_path) -> None:
    text = "-- @cell a\n-- @autorun: false\nSELECT 1 AS x;\n-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
    path = _notebook(tmp_path, text)
    session = RunSession()
    try:
        run_project(compile_file(path), session=session)
        path.write_text(text.replace("SELECT 1", "SELECT 2"))
        assert _statuses(path)["b"][0] == "ok"
        run_project(compile_file(path), select=["a"], closure=False, session=session)
        assert _statuses(path)["b"] == ("stale", "upstream 'a' changed")
    finally:
        session.close()


def test_a_fingerprint_is_the_same_in_every_process(tmp_path) -> None:
    """status compares fingerprints across processes: a set-valued field (a
    plugin's) must not hash in the hash-seeded order Python iterates it in."""
    import os
    import subprocess
    import sys
    import textwrap

    script = textwrap.dedent(f"""
        from pydantic import BaseModel
        from quicksql.compiler import compile_text
        from quicksql.plugins.base import Plugin, qfield
        from quicksql.registry import plugin
        from quicksql.state import current_fingerprints

        @plugin
        class Labels(Plugin):
            class Config(BaseModel):
                labels: set[str] = qfield(set())

        text = "-- @labels: [alpha, beta, gamma, delta, epsilon]\\n-- @cell a\\nSELECT 1;"
        print(current_fingerprints(compile_text(text, root={str(tmp_path)!r}), {{}})["a"])
    """)
    prints = {
        subprocess.run(
            [sys.executable, "-c", script], env={**os.environ, "PYTHONHASHSEED": str(seed)},
            capture_output=True, text=True, check=True,
        ).stdout
        for seed in (1, 2, 3)
    }
    assert len(prints) == 1, prints
