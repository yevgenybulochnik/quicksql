"""The recorder: lineage across a notebook's cells, and the UsagePlugin."""

import os

from quicksql.plugins.usage import CellScan, Source, lineage
from quicksql.sqlscan import Rel, scan

FUNCTIONS = frozenset({"read_csv", "read_csv_auto", "read_parquet"})
SUFFIXES = frozenset({".csv", ".parquet"})


def cell(sql, refs=(), engine="duckdb", context="mem", usage=True, landed=None, dialect="duckdb"):
    """A cell as lineage sees it; ``refs`` are cells it reads by bare name."""
    on_conduit = engine == "duckdb"
    found = scan(
        sql,
        dialect,
        {name: name for name in refs},
        FUNCTIONS if on_conduit else frozenset(),
        SUFFIXES if on_conduit else frozenset(),
    )
    return CellScan(found, engine, context, usage, landed)


def run(cells, **kwargs):
    return lineage(cells, list(cells), **kwargs)


def columns(reads) -> dict[str, set[str]]:
    return {".".join((*src.parts, col)): roles for (src, col), roles in reads.columns.items()}


def tables(reads) -> dict[str, bool]:
    return {".".join(src.parts): star for src, star in reads.tables.items()}


def src(*parts, engine="duckdb", context="mem") -> Source:
    return Source(engine, context, "table", parts)


def test_a_cell_that_reads_no_ref_reads_what_its_scan_says() -> None:
    reads = run({"a": cell("select o.amount from orders o join c on o.cid = c.id where o.x")})["a"]
    assert columns(reads) == {
        "orders.amount": {"select"},
        "orders.cid": {"join"},
        "c.id": {"join"},
        "orders.x": {"where"},
    }
    assert reads.tables == {src("orders"): False, src("c"): False}
    assert reads.joins == {((src("c"), "id"), (src("orders"), "cid"))}


def test_a_ref_column_is_read_through_its_producers_pass_through() -> None:
    reads = run(
        {
            "stage": cell("select id, amount as amt from orders"),
            "top": cell("select amt from stage where id > 1", refs=["stage"]),
        }
    )
    assert columns(reads["top"]) == {"orders.amount": {"select"}, "orders.id": {"where"}}
    assert tables(reads["top"]) == {"orders": False}
    assert reads["top"].unresolved == {}


def test_columns_read_through_a_select_star_staging_cell_count() -> None:
    reads = run(
        {
            "stage": cell("select * from members"),
            "by_member": cell("select member_id, count(*) as n from stage group by 1", refs=["stage"]),
        }
    )
    assert tables(reads["stage"]) == {"members": True}
    assert columns(reads["stage"]) == {}
    assert columns(reads["by_member"]) == {"members.member_id": {"select", "group"}}
    assert tables(reads["by_member"]) == {"members": False}


def test_a_star_over_a_ref_is_a_star_over_what_it_passes_through() -> None:
    reads = run(
        {
            "stage": cell("select * from members"),
            "again": cell("select * from stage", refs=["stage"]),
            "third": cell("select * from again", refs=["again"]),
        }
    )
    assert tables(reads["third"]) == {"members": True}


def test_a_chain_of_refs_is_followed_to_the_table() -> None:
    reads = run(
        {
            "a": cell("select * from orders"),
            "b": cell("select total as t, id from a", refs=["a"]),
            "c": cell("select t as value from b order by id", refs=["b"]),
            "d": cell("select value from c", refs=["c"]),
        }
    )
    assert columns(reads["d"]) == {"orders.total": {"select"}}
    assert columns(reads["c"]) == {"orders.total": {"select"}, "orders.id": {"order"}}


def test_a_computed_column_stops_the_trace() -> None:
    reads = run(
        {
            "stage": cell("select id, amount * qty as revenue from lines"),
            "top": cell("select id, revenue from stage", refs=["stage"]),
        }
    )
    assert columns(reads["top"]) == {"lines.id": {"select"}}


def test_a_column_through_a_star_over_a_join_is_unresolved() -> None:
    reads = run(
        {
            "stage": cell("select * from orders o join customers c on o.cid = c.id"),
            "top": cell("select total from stage", refs=["stage"]),
        }
    )
    assert columns(reads["top"]) == {}
    assert reads["top"].unresolved == {
        ("total", (src("orders"), src("customers")), False): {"select"}
    }
    assert tables(reads["top"]) == {"orders": False, "customers": False}


def test_a_ref_candidate_is_ruled_in_by_its_producers_outputs() -> None:
    reads = run(
        {
            "stage": cell("select id, total as amount from payments"),
            "top": cell("select amount from stage r join orders o on r.id = o.id", refs=["stage"]),
        }
    )
    assert columns(reads["top"])["payments.total"] == {"select"}
    assert reads["top"].unresolved == {}


def test_a_ref_candidate_is_ruled_out_leaving_the_one_table() -> None:
    reads = run(
        {
            "stage": cell("select id from payments"),
            "top": cell("select amount from stage r join orders o on r.id = o.id", refs=["stage"]),
        }
    )
    assert columns(reads["top"])["orders.amount"] == {"select"}
    assert reads["top"].unresolved == {}


def test_a_ref_candidate_with_a_star_stays_a_candidate() -> None:
    reads = run(
        {
            "stage": cell("select * from payments"),
            "top": cell("select amount from stage r join orders o on r.id = o.id", refs=["stage"]),
        }
    )
    assert reads["top"].unresolved == {
        ("amount", (src("payments"), src("orders")), False): {"select"}
    }


def test_a_correlated_subquerys_column_stays_unresolved() -> None:
    reads = run(
        {
            "stage": cell("select oid from payments"),
            "top": cell(
                "select o.total from orders o where exists (select 1 from stage where oid = id)",
                refs=["stage"],
            ),
        }
    )
    assert columns(reads["top"]) == {"orders.total": {"select"}}
    assert set(reads["top"].unresolved) == {
        ("oid", (None, src("orders")), True),
        ("id", (None, src("orders")), True),
    }


def test_a_renamed_column_and_one_the_producers_star_excludes() -> None:
    reads = run(
        {
            "stage": cell("select * exclude (ssn) replace (upper(name) as name) from members"),
            "top": cell("select ssn, name, dob as born from stage", refs=["stage"]),
            "again": cell("select born from top", refs=["top"]),
        }
    )
    assert columns(reads["top"]) == {"members.dob": {"select"}}
    assert columns(reads["again"]) == {"members.dob": {"select"}}


def test_a_producer_with_usage_off_stops_the_trace() -> None:
    reads = run(
        {
            "stage": cell("select * from members", usage=False),
            "top": cell("select member_id from stage join plans p using (plan_id)", refs=["stage"]),
            "all": cell("select * from stage", refs=["stage"]),
        }
    )
    assert columns(reads["top"]) == {"plans.plan_id": {"join"}}
    # the cell that opted out may have it: nothing says, so it stays open
    assert set(reads["top"].unresolved) == {("member_id", (None, src("plans")), False)}
    assert tables(reads["top"]) == {"plans": False}
    assert reads["top"].joins == set()
    assert tables(reads["all"]) == {}


def test_a_resolved_column_carries_its_producers_context() -> None:
    reads = run(
        {
            "claims": cell(
                "select member_id, paid from public.claims",
                engine="postgres",
                context="pg1",
                dialect="postgres",
            ),
            "paid": cell(
                "select c.paid, m.name from claims c join members m on c.member_id = m.id",
                refs=["claims"],
            ),
        }
    )
    claims = Source("postgres", "pg1", "table", ("public", "claims"))
    assert reads["paid"].columns == {
        (claims, "paid"): {"select"},
        (claims, "member_id"): {"join"},
        (src("members"), "name"): {"select"},
        (src("members"), "id"): {"join"},
    }
    assert reads["paid"].joins == {((src("members"), "id"), (claims, "member_id"))}


def test_a_table_any_cell_creates_is_not_a_source() -> None:
    reads = run(
        {
            "setup": cell("create or replace table staged as select * from raw"),
            "top": cell("select a from staged join main.STAGED s2 using (a) join other using (a)"),
        }
    )
    assert tables(reads["setup"]) == {"raw": True}
    assert tables(reads["top"]) == {"other": False}
    assert columns(reads["top"]) == {"other.a": {"join", "select"}}


def test_a_relation_a_sink_lands_is_not_a_source_to_a_sibling() -> None:
    reads = run(
        {
            "stage": cell(
                "select * from members",
                landed=Rel("table", ("qsql_db_1a2b3c4d", "main", "stage")),
            ),
            "file": cell("select * from plans", landed=Rel("file", ("/nb/data/file.parquet",))),
            "top": cell(
                "select a from wh.main.stage join read_parquet('/nb/data/file.parquet') using (a)"
                " join other.stage using (a)"
            ),
        }
    )
    assert tables(reads["top"]) == {"other.stage": False}


def test_a_join_through_a_ref_is_a_join_of_the_source_tables() -> None:
    reads = run(
        {
            "stage": cell("select member_id as id from members"),
            "top": cell("select 1 from stage s join orders o on s.id = o.sid", refs=["stage"]),
        }
    )
    assert reads["top"].joins == {((src("members"), "member_id"), (src("orders"), "sid"))}


def test_a_file_is_keyed_by_its_resolved_path_and_has_no_context() -> None:
    reads = run(
        {"a": cell("select x from read_csv('seeds/a.csv') join '/abs/b.parquet' using (x)")},
        resolve_file=lambda path: path if path.startswith("/") else "/cwd/" + path,
    )["a"]
    a = Source(None, None, "file", ("/cwd/seeds/a.csv",))
    b = Source(None, None, "file", ("/abs/b.parquet",))
    assert reads.tables == {a: False, b: False}
    assert set(reads.columns) == {(a, "x"), (b, "x")}


def test_a_scans_errors_are_the_cells() -> None:
    reads = run({"a": cell("select 'x' from where; call foo(1)")})["a"]
    assert reads.errors == ((1, "parse"), (2, "unsupported:Command"))


# ---- the plugin: QSQL_USAGE=1 records what a run's ok cells read

import hashlib  # noqa: E402
import json  # noqa: E402
import subprocess  # noqa: E402
import sys  # noqa: E402
from datetime import date, timedelta  # noqa: E402

import duckdb  # noqa: E402
import pytest  # noqa: E402

from quicksql.bootstrap import compile_file, compile_text  # noqa: E402
from quicksql.plugins import usage as usage_plugin  # noqa: E402
from quicksql.runner import run_project  # noqa: E402
from quicksql.usage_log import UsageLog  # noqa: E402

HEADER = "-- @input: {duckdb: wh.db}\n"


@pytest.fixture
def on(monkeypatch):
    monkeypatch.setenv("QSQL_USAGE", "1")


@pytest.fixture
def nb(tmp_path):
    """A notebook directory with a DuckDB warehouse: orders and customers."""
    root = tmp_path / "nb"
    root.mkdir()
    with duckdb.connect(str(root / "wh.db")) as con:
        con.execute("create table orders as select 1 as id, 9.5 as amount, 7 as cid, 'x' as status")
        con.execute("create table customers as select 7 as id, 'ada' as name")
    return root


def write(root, text, name="nb.qsql"):
    path = root / name
    path.write_text(text)
    return path


def run_file(path):
    """Compile and run a notebook: (results by cell, the run's notes)."""
    notes: list[str] = []
    results = run_project(
        compile_file(path),
        on_event=lambda e: notes.append(e.detail) if e.kind == "note" else None,
    )
    return {r.cell: r for r in results}, notes


def logged() -> list[dict]:
    return list(UsageLog().read())


def context_of(identity: str) -> str:
    return hashlib.sha256(identity.encode()).hexdigest()[:12]


def test_the_usage_directive_defaults_on_and_inherits(tmp_path) -> None:
    project = compile_text("-- @cell a\nselect 1;\n", root=tmp_path)
    assert project.cells["a"].config.usage is True
    project = compile_text(
        "-- @usage: false\n-- @cell a\nselect 1;\n-- @cell b\n-- @usage: true\nselect 2;\n",
        root=tmp_path,
    )
    assert (project.cells["a"].config.usage, project.cells["b"].config.usage) == (False, True)


def test_the_usage_directive_never_reruns_a_cell(tmp_path) -> None:
    from quicksql.state import own_hash

    one = compile_text("-- @cell a\nselect 1;\n", root=tmp_path)
    two = compile_text("-- @cell a\n-- @usage: false\nselect 1;\n", root=tmp_path)
    assert own_hash(one.cells["a"], one.config) == own_hash(two.cells["a"], two.config)


@pytest.mark.parametrize("value", [None, "", "0", "true", "yes", "11", " 1"])
def test_recording_is_off_unless_the_variable_is_exactly_1(
    nb, qsql_home, monkeypatch, value
) -> None:
    if value is not None:
        monkeypatch.setenv("QSQL_USAGE", value)
    results, notes = run_file(write(nb, HEADER + "-- @cell a\n-- @usage: true\nselect id from orders;\n"))
    assert results["a"].ok
    assert not (qsql_home / "usage").exists()
    assert notes == []


def test_a_run_with_recording_off_imports_no_scanner(nb, qsql_home) -> None:
    path = write(nb, HEADER + "-- @cell a\nselect id from orders;\n")
    code = (
        "import sys\n"
        "from quicksql.bootstrap import compile_file\n"
        "from quicksql.runner import run_project\n"
        f"assert all(r.ok for r in run_project(compile_file({str(path)!r})))\n"
        "print(sorted(m for m in sys.modules if m in"
        " ('quicksql.sqlscan', 'quicksql.usage_log', 'sqlglot.optimizer.qualify')))\n"
    )

    def modules(**env) -> str:
        base = {k: v for k, v in os.environ.items() if k != "QSQL_USAGE"}
        done = subprocess.run(
            [sys.executable, "-c", code], env={**base, **env}, capture_output=True, text=True
        )
        assert done.returncode == 0, done.stderr
        return done.stdout.strip()

    assert modules() == "[]"
    assert not (qsql_home / "usage").exists()
    assert modules(QSQL_USAGE="1") == (
        "['quicksql.sqlscan', 'quicksql.usage_log', 'sqlglot.optimizer.qualify']"
    )
    assert (qsql_home / "usage").is_dir()


def test_a_logged_line_says_what_the_cell_read_and_no_sql(nb, on) -> None:
    path = write(
        nb,
        HEADER
        + "-- @cell paid\n"
        "select o.amount, c.name from orders o join customers c on c.id = o.cid\n"
        "where o.status <> 'secret-literal';\n",
    )
    results, notes = run_file(path)
    assert results["paid"].ok and notes == []
    (entry,) = logged()
    context = context_of(f"duckdb:{nb / 'wh.db'}")
    customers = {"engine": "duckdb", "context": context, "kind": "table", "parts": ["customers"], "star": False}
    orders = {**customers, "parts": ["orders"]}
    assert entry.pop("at").startswith(str(date.today().year))
    assert len(entry.pop("key")) == 16
    assert entry == {
        "v": 1,
        "day": date.today().isoformat(),
        "notebook": str(path),
        "cell": "paid",
        "engine": "duckdb",
        "context": context,
        "tables": [customers, orders],
        "columns": [
            {"table": 0, "column": "id", "roles": ["join"]},
            {"table": 0, "column": "name", "roles": ["select"]},
            {"table": 1, "column": "amount", "roles": ["select"]},
            {"table": 1, "column": "cid", "roles": ["join"]},
            {"table": 1, "column": "status", "roles": ["where"]},
        ],
        "joins": [[[0, "id"], [1, "cid"]]],
        "unresolved": [],
        "errors": [],
    }
    raw = b"".join(p.read_bytes() for p in UsageLog().root.iterdir())
    assert b"secret-literal" not in raw and b"select" not in raw.lower()


def test_unresolved_columns_and_scan_errors_are_logged(nb, on) -> None:
    path = write(
        nb,
        HEADER
        + "-- @cell a\n-- @output: {type: none}\n"
        "pivot orders on status using sum(amount);\n"
        "select amount, name from orders o join customers c on c.id = o.cid;\n",
    )
    results, _ = run_file(path)
    assert results["a"].ok, results["a"].error
    (entry,) = logged()
    assert entry["errors"] == [[1, "unsupported:Pivot"]]
    assert [t["parts"] for t in entry["tables"]] == [["customers"], ["orders"]]
    assert entry["unresolved"] == [
        {"column": "amount", "roles": ["select"], "candidates": [1, 0], "nested": False},
        {"column": "name", "roles": ["select"], "candidates": [1, 0], "nested": False},
    ]


def test_only_ok_cells_are_logged(nb, on) -> None:
    path = write(
        nb,
        HEADER
        + "-- @cell good\nselect id from orders;\n"
        "-- @cell bad\nselect nope from orders;\n"
        "-- @cell after\nselect * from {{ ref('bad') }};\n",
    )
    results, _ = run_file(path)
    assert {name: r.status for name, r in results.items()} == {
        "good": "ok",
        "bad": "failed",
        "after": "skipped",
    }
    assert [entry["cell"] for entry in logged()] == ["good"]


def test_usage_false_in_the_header_logs_nothing(nb, on, qsql_home) -> None:
    results, notes = run_file(
        write(nb, "-- @usage: false\n" + HEADER + "-- @cell a\nselect id from orders;\n")
    )
    assert results["a"].ok and notes == []
    assert logged() == []


def test_usage_false_on_a_cell_logs_no_line_and_stops_what_reads_through_it(nb, on) -> None:
    path = write(
        nb,
        HEADER
        + "-- @cell stage\n-- @usage: false\nselect * from orders;\n"
        "-- @cell top\nselect s.amount, c.name from {{ ref('stage') }} s"
        " join customers c on c.id = s.cid;\n",
    )
    run_file(path)
    (entry,) = logged()
    assert entry["cell"] == "top"
    assert [t["parts"] for t in entry["tables"]] == [["customers"]]
    assert [c["column"] for c in entry["columns"]] == ["id", "name"]


def test_a_notebook_without_a_file_logs_nothing(nb, on, qsql_home) -> None:
    project = compile_text(HEADER + "-- @cell a\nselect id from orders;\n", root=nb)
    assert all(r.ok for r in run_project(project))
    assert not (qsql_home / "usage").exists()


def test_a_ref_across_contexts_is_logged_on_its_producers_context(tmp_path, on) -> None:
    import sqlite3

    with sqlite3.connect(tmp_path / "app.db") as con:
        con.execute("create table events (id integer, kind text)")
        con.execute("insert into events values (1, 'click')")
    path = write(
        tmp_path,
        "-- @cell raw\n-- @input: {sqlite: app.db}\nselect id, kind from events;\n"
        "-- @cell kinds\nselect kind, count(*) as n from {{ ref('raw') }} group by kind;\n",
    )
    results, _ = run_file(path)
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    by_cell = {entry["cell"]: entry for entry in logged()}
    events = {
        "engine": "sqlite",
        "context": context_of(f"sqlite:{tmp_path / 'app.db'}"),
        "kind": "table",
        "parts": ["events"],
        "star": False,
    }
    assert by_cell["kinds"]["tables"] == [events]
    assert by_cell["kinds"]["columns"] == [
        {"table": 0, "column": "kind", "roles": ["group", "select"]}
    ]
    # the line is the duckdb cell's; the table is where its producer read it
    assert by_cell["kinds"]["engine"] == "duckdb"
    assert by_cell["kinds"]["context"] != events["context"]


def test_a_file_is_logged_by_the_path_duckdb_reads(nb, on, tmp_path, monkeypatch) -> None:
    (nb / "seeds").mkdir()
    (nb / "seeds" / "users.csv").write_text("user_id,name\n1,ada\n")
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "seeds").mkdir(parents=True)
    (elsewhere / "seeds" / "plans.csv").write_text("plan_id\n1\n")
    path = write(
        nb,
        "-- @cell users\nselect user_id from {{ source('seeds/users.csv') }};\n"
        # hand-written and relative: DuckDB reads it against the process's cwd
        "-- @cell plans\nselect plan_id from read_csv('seeds/plans.csv');\n",
    )
    monkeypatch.chdir(elsewhere)
    results, _ = run_file(path)
    assert all(r.ok for r in results.values()), [r.error for r in results.values()]
    tables = {entry["cell"]: entry["tables"] for entry in logged()}
    assert tables["users"] == [
        {"kind": "file", "parts": [str(nb / "seeds" / "users.csv")], "star": False}
    ]
    assert tables["plans"] == [
        {"kind": "file", "parts": [str(elsewhere / "seeds" / "plans.csv")], "star": False}
    ]


def test_in_memory_contexts_of_two_notebooks_hash_apart(tmp_path, on) -> None:
    text = "-- @cell t\nselect table_name from information_schema.tables;\n"
    for name in ("one", "two"):
        (tmp_path / name).mkdir()
        run_file(write(tmp_path / name, text))
    run_file(write(tmp_path / "one", text, name="again.qsql"))
    contexts = {entry["notebook"]: entry["tables"][0]["context"] for entry in logged()}
    assert len(set(contexts.values())) == 3
    one = tmp_path / "one" / "nb.qsql"
    assert contexts[str(one)] == context_of(f"duckdb:{one}")


def test_two_notebooks_that_reach_one_database_share_its_context(nb, on) -> None:
    (nb / "sub").mkdir()
    run_file(write(nb, HEADER + "-- @cell a\nselect id from orders;\n"))
    run_file(
        write(nb / "sub", "-- @input: {duckdb: ../wh.db}\n-- @cell b\nselect id from orders;\n")
    )
    assert len({entry["tables"][0]["context"] for entry in logged()}) == 1


def test_an_unwritable_usage_directory_is_reported_once(nb, on, qsql_home) -> None:
    (qsql_home / "usage").write_text("a file where the directory should be")
    path = write(nb, HEADER + "-- @cell a\nselect id from orders;\n")
    results, first = run_file(path)
    assert results["a"].ok
    assert len(first) == 1 and "usage" in first[0] and str(qsql_home / "usage") in first[0]
    results, second = run_file(path)
    assert results["a"].ok and second == []


def test_an_unchanged_rerun_adds_no_line(nb, on) -> None:
    path = write(
        nb, HEADER + "-- @cell a\nselect id from orders;\n-- @cell b\nselect name from customers;\n"
    )
    run_file(path)
    run_file(path)
    assert sorted(entry["cell"] for entry in logged()) == ["a", "b"]
    usage_plugin.forget()  # as a new process starts: nothing remembered
    run_file(path)
    assert sorted(entry["cell"] for entry in logged()) == ["a", "b"]


def test_an_edited_cell_adds_one_line(nb, on) -> None:
    text = HEADER + "-- @cell a\nselect id from orders;\n-- @cell b\nselect name from customers;\n"
    run_file(write(nb, text))
    run_file(write(nb, text.replace("select id from", "select id, amount from")))
    assert sorted(entry["cell"] for entry in logged()) == ["a", "a", "b"]
    # an edit that reads the same columns adds none
    run_file(write(nb, text.replace("select id from", "select id,  amount from")))
    assert len(logged()) == 3
    # going back to the first version adds none: that read is logged for today
    run_file(write(nb, text))
    assert len(logged()) == 3


def test_an_upstream_edit_that_changes_a_downstream_cells_reads_logs_it_anew(nb, on) -> None:
    text = (
        HEADER
        + "-- @cell stage\nselect id, amount from orders;\n"
        "-- @cell top\nselect amount from {{ ref('stage') }};\n"
    )
    run_file(write(nb, text))
    assert sorted(entry["cell"] for entry in logged()) == ["stage", "top"]
    run_file(write(nb, text.replace("id, amount from", "id, amount * 2 as amount from")))
    # top's SQL is unchanged, but it no longer reads orders.amount
    assert sorted(entry["cell"] for entry in logged()) == ["stage", "top", "top"]
    assert [entry["columns"] for entry in logged() if entry["cell"] == "top"][-1] == []


def test_a_raising_recorder_leaves_the_run_ok_and_says_so(nb, on, monkeypatch) -> None:
    import quicksql.sqlscan

    def boom(*args, **kwargs):
        raise RuntimeError("scanner blew up")

    monkeypatch.setattr(quicksql.sqlscan, "scan", boom)
    results, notes = run_file(write(nb, HEADER + "-- @cell a\nselect id from orders;\n"))
    assert results["a"].ok
    assert len(notes) == 1
    assert "plugin 'usage' after_run failed" in notes[0] and "scanner blew up" in notes[0]
    assert logged() == []


def test_the_first_write_prunes_and_compacts(nb, on, qsql_home) -> None:
    usage = qsql_home / "usage"
    usage.mkdir()
    ago = lambda days: (date.today() - timedelta(days=days)).isoformat()  # noqa: E731
    old = {"v": 1, "day": ago(3), "notebook": "/x.qsql", "cell": "c", "key": "k"}
    for pid in (11, 22):
        (usage / f"{ago(3)}.{pid}.ndjson").write_text(json.dumps(old) + "\n")
    (usage / f"{ago(400)}.11.ndjson").write_text(json.dumps({**old, "day": ago(400)}) + "\n")
    run_file(write(nb, HEADER + "-- @cell a\nselect id from orders;\n"))
    assert sorted(p.name for p in usage.iterdir()) == sorted(
        [f"{ago(3)}.ndjson", f"{date.today().isoformat()}.{os.getpid()}.ndjson"]
    )
    assert [entry["cell"] for entry in logged()] == ["c", "a"]
