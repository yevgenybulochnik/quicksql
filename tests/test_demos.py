"""The shipped demos stay compilable offline, and the sources demo runnable.

Ported from tests/test_examples.py on feat/qsql-notebook-engine. Every
demo/**/*.qsql is collected by glob, so a new demo is covered with no edit here.
"""

from __future__ import annotations

import shutil
import socket
import sqlite3
from pathlib import Path

import duckdb
import pytest

from quicksql.compiler import compile_file
from quicksql.errors import QsqlError
from quicksql.parser import parse_file
from quicksql.runner import run_project

DEMO = Path(__file__).parent.parent / "demo"
NOTEBOOKS = sorted(DEMO.glob("**/*.qsql"))
# what sources.qsql lands (warehouse.db* takes a crashed run's .wal too);
# .gitignore lists them, so a hand run of the demo leaves them unseen
SOURCES_OUTPUTS = ("data", "warehouse.db*")


@pytest.fixture
def connections(monkeypatch) -> list[str]:
    """Refuse and record every socket and database connection. Kitchen-sink
    names Postgres and the BigQuery emulator, which may well be up on this box;
    compiling must not reach them. Recorded too, in case a caller swallows the
    refusal."""
    attempts: list[str] = []

    def refuse(what: str):
        def connect(*args, **kwargs):
            attempts.append(f"{what}{args[-1:]}")
            raise ConnectionRefusedError(f"{what} refused: compiling must stay offline")

        return connect

    monkeypatch.setattr(socket.socket, "connect", refuse("socket.connect"))
    monkeypatch.setattr(socket.socket, "connect_ex", refuse("socket.connect_ex"))
    monkeypatch.setattr(duckdb, "connect", refuse("duckdb.connect"))
    monkeypatch.setattr(sqlite3, "connect", refuse("sqlite3.connect"))
    try:
        import psycopg  # libpq connects in C, below the socket patch
    except ImportError:  # without the postgres extra nothing can reach Postgres
        pass
    else:
        monkeypatch.setattr(psycopg, "connect", refuse("psycopg.connect"))
    return attempts


@pytest.mark.parametrize(
    "notebook", NOTEBOOKS, ids=[p.relative_to(DEMO).as_posix() for p in NOTEBOOKS]
)
def test_demo_compiles_offline(notebook: Path, connections: list[str]) -> None:
    try:
        project = compile_file(notebook)
    except QsqlError as exc:  # the message names each failing cell; no chain, no trace
        raise pytest.fail.Exception(str(exc), pytrace=False) from None
    assert not connections, f"compiling connected: {connections}"
    declared = [blk.name for blk in parse_file(notebook)[1:]]
    assert sorted(project.order) == sorted(declared)
    empty = [name for name in declared if not project.cells[name].sql.strip()]
    assert not empty, f"cells rendered no SQL: {empty}"


def test_sources_demo_runs_offline(tmp_path: Path, monkeypatch) -> None:
    copy = tmp_path / "sources"
    # without a hand run's outputs, which the landed-output checks below would
    # otherwise find even if this run landed nothing
    shutil.copytree(DEMO / "sources", copy, ignore=shutil.ignore_patterns(*SOURCES_OUTPUTS))
    stale = sorted(str(p.relative_to(copy)) for pat in SOURCES_OUTPUTS for p in copy.rglob(pat))
    assert not stale, f"the copy holds outputs from an earlier run: {stale}"
    # an output resolved against the cwd instead of the notebook lands here
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)

    project = compile_file(copy / "sources.qsql")
    results = {r.cell: r for r in run_project(project)}

    not_ok = [f"{r.cell} ({r.status}): {r.error}" for r in results.values() if not r.ok]
    assert not not_ok, "\n".join(not_ok)
    assert list(results) == project.order
    missing = [
        name
        for name in results
        if name != "sales_by_segment" and not (copy / "data" / f"{name}.parquet").exists()
    ]
    assert not missing, f"no parquet landed under data/ for: {missing}"
    assert (copy / "warehouse.db").exists(), "sales_by_segment landed no warehouse.db"
    with duckdb.connect(str(copy / "warehouse.db"), read_only=True) as con:
        (rows,) = con.execute("SELECT count(*) FROM sales_by_segment").fetchone()
    assert rows == results["sales_by_segment"].rows
    assert not list(cwd.iterdir())
