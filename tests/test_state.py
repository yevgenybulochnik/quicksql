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


def test_each_run_writes_the_state_file(tmp_path) -> None:
    path = _notebook(tmp_path, "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT * FROM {{ ref('a') }};\n")
    project = compile_file(path)
    assert all(r.ok for r in run_project(project))
    state = tmp_path / ".quicksql" / "state.json"
    entries = json.loads(state.read_text())["notebooks"]["base.qsql"]
    assert set(entries) == {"a", "b"}
    landed = {name: cell.landed for name, cell in read_state(project).items()}
    current = current_fingerprints(project, landed)
    assert {name: e["status"] for name, e in entries.items()} == {"a": "ok", "b": "ok"}
    assert {name: e["landed"]["fingerprint"] for name, e in entries.items()} == current
    # the directory ignores itself, wherever the notebook's repository is
    assert (tmp_path / ".quicksql" / ".gitignore").read_text().strip() == "*"


def test_a_run_keeps_the_state_of_cells_it_didnt_run(tmp_path) -> None:
    path = _notebook(tmp_path, "-- @cell a\nSELECT 1 AS x;\n-- @cell b\nSELECT 2 AS y;\n")
    run_project(compile_file(path))
    run_project(compile_file(path), select=["a"])
    assert set(read_state(compile_file(path))) == {"a", "b"}


def test_a_notebook_compiled_from_text_writes_no_state(tmp_path) -> None:
    run_project(compile_text(BASE, root=tmp_path))
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
