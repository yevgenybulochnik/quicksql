"""The usage report: aggregation, rendering, notebooks, enrichment, curation."""

from datetime import date

import pytest

from quicksql.usage import collect, rank

TODAY = date(2026, 11, 9)
CLAIMS = ("postgres", "ctx1", "table", ("public", "claims"))
MEMBERS = ("postgres", "ctx1", "table", ("public", "members"))


def table(*parts, star=False, engine="postgres", context="ctx1"):
    return {"engine": engine, "context": context, "kind": "table", "parts": list(parts), "star": star}


def col(index, name, *roles):
    return {"table": index, "column": name, "roles": sorted(roles or ("select",))}


def entry(cell="c", day="2026-11-09", notebook="/nb/a.qsql", tables=(), columns=(), joins=(),
          unresolved=(), key="k"):
    return {
        "v": 1, "day": day, "notebook": notebook, "cell": cell, "key": key,
        "engine": "postgres", "context": "ctx1", "tables": list(tables),
        "columns": list(columns), "joins": list(joins), "unresolved": list(unresolved),
        "errors": [],
    }  # fmt: skip


def ranked(entries, **kwargs):
    kwargs.setdefault("today", TODAY)
    days = kwargs.pop("days", 90)
    return rank(collect(entries, today=kwargs["today"], days=days), **kwargs)


def by_name(tables):
    return {t.name: t for t in tables}


def test_a_cells_day_is_the_union_of_its_lines() -> None:
    claims = [table("public", "claims")]
    a = entry(tables=claims, columns=[col(0, "paid")], key="A")
    b = entry(tables=claims, columns=[col(0, "billed")], key="B")
    # A, then an edit to B, then back to A (which logs no third line)
    (claims_usage,) = ranked([a, b])
    assert {c.name: c.uses for c in claims_usage.columns} == {"paid": 1, "billed": 1}
    assert claims_usage.uses == 1


def test_a_use_is_a_notebook_day() -> None:
    claims = [table("public", "claims")]
    lines = [
        # a column passed through three staging cells of one notebook, logged by each
        entry("stage", tables=claims, columns=[col(0, "member_id")]),
        entry("clean", tables=claims, columns=[col(0, "member_id", "where")]),
        entry("top", tables=claims, columns=[col(0, "member_id", "group")]),
        entry("top", tables=claims, columns=[col(0, "member_id")], notebook="/nb/b.qsql"),
        entry("top", tables=claims, columns=[col(0, "member_id")], day="2026-11-08"),
    ]
    (usage,) = ranked(lines)
    (member_id,) = usage.columns
    assert (usage.uses, member_id.uses) == (3, 3)
    assert member_id.roles == ["group", "select", "where"]


def test_score_halves_every_half_life() -> None:
    claims = [table("public", "claims")]
    lines = [
        entry(tables=claims, columns=[col(0, "paid")], day=day)
        for day in ("2026-11-09", "2026-10-10", "2026-09-10")
    ]
    (usage,) = ranked(lines)
    assert usage.score == pytest.approx(1.75)
    assert usage.columns[0].score == pytest.approx(1.75)
    (usage,) = ranked(lines, half_life=60)
    assert usage.score == pytest.approx(1 + 0.5**0.5 + 0.5)


def test_tables_and_columns_rank_by_score_then_name() -> None:
    both = [table("public", "claims"), table("public", "members")]
    lines = [
        entry(tables=both, columns=[col(0, "paid"), col(0, "billed"), col(1, "id")]),
        entry(tables=both[:1], columns=[col(0, "paid")], day="2026-11-08"),
        # older, so it scores less than a fresh use though it's one use too
        entry(tables=[table("public", "zips")], columns=[col(0, "zip")], day="2026-08-20"),
    ]
    tables = ranked(lines)
    assert [t.name for t in tables] == ["public.claims", "public.members", "public.zips"]
    assert [c.name for c in tables[0].columns] == ["paid", "billed"]
    assert [t.uses for t in tables] == [2, 1, 1]


def test_used_in_lists_notebooks_by_their_uses() -> None:
    claims = [table("public", "claims")]
    lines = [
        entry(tables=claims, notebook="/nb/a.qsql", day="2026-11-09"),
        entry(tables=claims, notebook="/nb/b.qsql", day="2026-11-09"),
        entry(tables=claims, notebook="/nb/b.qsql", day="2026-11-08"),
        entry(tables=claims, notebook="/nb/c.qsql", day="2026-11-07"),
    ]
    (usage,) = ranked(lines)
    assert usage.notebooks == [("/nb/b.qsql", 2), ("/nb/a.qsql", 1), ("/nb/c.qsql", 1)]


def test_joins_count_by_notebook_day_and_show_on_both_tables() -> None:
    both = [table("public", "claims"), table("public", "members")]
    join = [[[0, "member_id"], [1, "id"]]]
    lines = [
        entry("a", tables=both, joins=join),
        entry("b", tables=both, joins=join),
        entry("a", tables=both, joins=join, day="2026-11-08"),
    ]
    tables = by_name(ranked(lines))
    for name in ("public.claims", "public.members"):
        (found,) = tables[name].joins
        assert (found.left, found.right, found.uses) == (
            (CLAIMS, "member_id"),
            (MEMBERS, "id"),
            2,
        )


def test_select_star_counts_the_table_not_its_columns() -> None:
    lines = [
        entry("stage", tables=[table("public", "claims", star=True)]),
        entry("top", tables=[table("public", "claims")], columns=[col(0, "paid")]),
        entry("stage", tables=[table("public", "claims", star=True)], day="2026-11-08"),
    ]
    (usage,) = ranked(lines)
    assert (usage.uses, usage.stars) == (2, 2)
    assert [(c.name, c.uses) for c in usage.columns] == [("paid", 1)]


def test_tables_on_different_contexts_or_engines_stay_apart() -> None:
    lines = [
        entry(tables=[table("orders", context="ctx1")]),
        entry(tables=[table("orders", context="ctx2")]),
        entry(tables=[table("orders", engine="duckdb")]),
        entry(tables=[{"kind": "file", "parts": ["/data/orders.parquet"], "star": False}]),
    ]
    tables = ranked(lines)
    assert sorted((t.engine or "", t.context or "", t.kind) for t in tables) == [
        ("", "", "file"),
        ("duckdb", "ctx1", "table"),
        ("postgres", "ctx1", "table"),
        ("postgres", "ctx2", "table"),
    ]
    (file,) = [t for t in tables if t.kind == "file"]
    assert file.name == "/data/orders.parquet"


def test_an_unresolved_column_shows_on_each_candidate() -> None:
    both = [table("public", "claims"), table("public", "members")]
    open_ = [{"column": "status", "roles": ["where"], "candidates": [0, 1, None], "nested": False}]
    tables = by_name(ranked([entry(tables=both, unresolved=open_)]))
    for name in ("public.claims", "public.members"):
        (found,) = tables[name].unresolved
        assert (found.name, found.uses, found.roles) == ("status", 1, ["where"])
        assert tables[name].columns == []


def test_days_bounds_what_is_read() -> None:
    claims = [table("public", "claims")]
    lines = [
        entry(tables=claims, columns=[col(0, "paid")], day="2026-11-09"),
        entry(tables=claims, columns=[col(0, "billed")], day="2026-08-12"),  # 89 days ago
        entry(tables=claims, columns=[col(0, "old")], day="2026-08-10"),  # 91
    ]
    (usage,) = ranked(lines)
    assert {c.name for c in usage.columns} == {"paid", "billed"}
    (usage,) = ranked(lines, days=7)
    assert {c.name for c in usage.columns} == {"paid"}


def test_engine_filters_tables() -> None:
    lines = [
        entry(tables=[table("public", "claims")]),
        entry(tables=[table("orders", engine="duckdb")]),
        entry(tables=[{"kind": "file", "parts": ["/data/x.csv"], "star": False}]),
    ]
    assert [t.name for t in ranked(lines, engine="duckdb")] == ["orders"]
    assert [t.name for t in ranked(lines, engine="file")] == ["/data/x.csv"]


def test_a_line_that_doesnt_fit_is_skipped() -> None:
    claims = [table("public", "claims")]
    lines = [
        entry(tables=claims, columns=[col(0, "paid")]),
        entry(tables=claims, columns=[col(7, "nope")], notebook="/nb/b.qsql"),
        {"v": 1, "day": "2026-11-09", "notebook": "/nb/c.qsql", "cell": "c", "key": "k"},
        entry(tables=claims, columns=[col(0, "paid")], day="not-a-day"),
    ]
    (usage,) = ranked(lines)
    assert (usage.uses, [c.name for c in usage.columns]) == (1, ["paid"])


# ---- quicksql usage, without a notebook: every context, by its hash

import json  # noqa: E402

from typer.testing import CliRunner  # noqa: E402

from quicksql.cli import app  # noqa: E402
from quicksql.usage_log import UsageLog  # noqa: E402

runner = CliRunner()
NOW = date.today().isoformat()


def log(*entries) -> None:
    by_day: dict[str, list] = {}
    for item in entries:
        by_day.setdefault(item["day"], []).append(item)
    for day, items in by_day.items():
        UsageLog().append(items, day)


def cli(*args: str, code: int = 0):
    result = runner.invoke(app, ["usage", *args])
    assert result.exit_code == code, result.output
    return result.output


def claims_log() -> None:
    both = [table("public", "claims"), table("public", "members")]
    log(
        entry(
            "paid",
            NOW,
            "/home/me/claims/claims.qsql",
            tables=both,
            columns=[
                col(0, "member_id", "select", "join", "group"),
                col(0, "paid_amount"),
                col(1, "id", "join"),
            ],
            joins=[[[0, "member_id"], [1, "id"]]],
            unresolved=[
                {"column": "status", "roles": ["where"], "candidates": [0, 1], "nested": False}
            ],
        ),
        entry(
            "denied",
            NOW,
            "/home/me/claims/denials.qsql",
            tables=[table("public", "claims", star=True)],
        ),
        entry(
            "seeds",
            NOW,
            "/home/me/claims/claims.qsql",
            tables=[{"kind": "file", "parts": ["/home/me/claims/seeds/plans.csv"], "star": False}],
            columns=[col(0, "plan_id")],
        ),
    )


@pytest.mark.parametrize("flag", [None, "--markdown"])
def test_an_empty_log_says_how_to_turn_recording_on(flag) -> None:
    out = cli(*([flag] if flag else []))
    assert "no usage recorded" in out and "QSQL_USAGE=1" in out


def test_an_empty_log_as_json_is_an_empty_report() -> None:
    doc = json.loads(cli("--json"))
    assert doc["tables"] == [] and "QSQL_USAGE=1" in doc["hint"]


def test_the_report_reads_what_was_logged_whether_or_not_recording_is_on(monkeypatch) -> None:
    claims_log()
    assert "public.claims" in cli()
    monkeypatch.setenv("QSQL_USAGE", "1")
    assert "public.claims" in cli()


def test_markdown_is_a_section_per_table() -> None:
    claims_log()
    assert cli("--markdown") == (
        "# quicksql usage: last 90 days\n"
        "\n"
        "A use is a (notebook, day) that read it; score weighs recent uses"
        " (half-life 30 days).\n"
        "\n"
        "## postgres · public.claims — context ctx1 · 2 notebooks, score 2.0\n"
        "Used in: claims/claims.qsql (1), claims/denials.qsql (1)\n"
        "| column | type | uses | read as | note |\n"
        "|---|---|---|---|---|\n"
        "| member_id | | 1 | select, join, group | |\n"
        "| paid_amount | | 1 | select | |\n"
        "Joins: claims.member_id = members.id (1)\n"
        "Read with `select *`: 1\n"
        "Unresolved (this table or another in the query has it): status (1)\n"
        "\n"
        "## postgres · public.members — context ctx1 · 1 notebook, score 1.0\n"
        "Used in: claims/claims.qsql (1)\n"
        "| column | type | uses | read as | note |\n"
        "|---|---|---|---|---|\n"
        "| id | | 1 | join | |\n"
        "Joins: claims.member_id = members.id (1)\n"
        "Unresolved (this table or another in the query has it): status (1)\n"
        "\n"
        "## file · /home/me/claims/seeds/plans.csv · 1 notebook, score 1.0\n"
        "Used in: claims/claims.qsql (1)\n"
        "| column | type | uses | read as | note |\n"
        "|---|---|---|---|---|\n"
        "| plan_id | | 1 | select | |\n"
    )


def test_text_lists_each_table_with_its_columns() -> None:
    claims_log()
    out = cli()
    assert "postgres · public.claims — context ctx1 · 2 notebooks, score 2.0" in out
    assert "used in: claims/claims.qsql (1), claims/denials.qsql (1)" in out
    lines = [line.split() for line in out.splitlines()]
    assert ["member_id", "1", "select,", "join,", "group"] in lines
    assert "joins: claims.member_id = members.id (1)" in out
    assert "select *: 1" in out
    assert "unresolved: status (1)" in out


def test_json_is_one_document() -> None:
    claims_log()
    doc = json.loads(cli("--json"))
    assert (doc["days"], doc["half_life"], doc["notebooks"]) == (90, 30.0, [])
    claims, members, plans = doc["tables"]
    assert (claims["engine"], claims["context"], claims["kind"], claims["name"]) == (
        "postgres",
        "ctx1",
        "table",
        "public.claims",
    )
    assert (claims["uses"], claims["stars"], claims["score"]) == (2, 1, 2.0)
    assert claims["notebooks"] == [
        {"path": "/home/me/claims/claims.qsql", "uses": 1},
        {"path": "/home/me/claims/denials.qsql", "uses": 1},
    ]
    assert claims["columns"][0] == {
        "name": "member_id",
        "score": 1.0,
        "uses": 1,
        "roles": ["select", "join", "group"],
        "type": "",
        "description": "",
        "note": "",
        "pinned": False,
        "missing": False,
    }
    assert claims["joins"] == [
        {
            "left": {"table": "public.claims", "column": "member_id"},
            "right": {"table": "public.members", "column": "id"},
            "uses": 1,
            "score": 1.0,
        }
    ]
    assert claims["unresolved"] == [{"name": "status", "uses": 1, "roles": ["where"]}]
    assert (plans["kind"], plans["engine"], plans["context"]) == ("file", None, None)
    assert members["name"] == "public.members"


def test_top_and_columns_cut_the_report_and_say_so() -> None:
    claims_log()
    out = cli("--markdown", "--top", "1", "--columns", "1")
    assert out.count("\n## ") == 1
    assert "| member_id |" in out and "| paid_amount |" not in out
    assert "+1 more column" in out
    assert "+2 more tables" in out
    doc = json.loads(cli("--json", "--top", "1", "--columns", "1"))
    assert len(doc["tables"]) == 1 and len(doc["tables"][0]["columns"]) == 1


def test_engine_and_days_filter_the_report() -> None:
    claims_log()
    log(entry("old", "2020-01-01", tables=[table("public", "ancient")]))
    out = cli("--markdown", "--engine", "file")
    assert out.count("\n## ") == 1 and "plans.csv" in out
    assert "ancient" not in cli("--days", "365")
    assert "no usage recorded in the last 90 days" in cli("--engine", "sqlite")


@pytest.mark.parametrize(
    "args",
    [
        ["--days", "0"],
        ["--top", "0"],
        ["--columns", "-1"],
        ["--half-life", "0"],
        ["--json", "--markdown"],
        ["--nope"],
    ],
)
def test_bad_flags_exit_2(args) -> None:
    cli(*args, code=2)


# ---- quicksql usage NOTEBOOK: what every logged notebook read on its contexts

import duckdb  # noqa: E402

from quicksql.bootstrap import compile_file  # noqa: E402
from quicksql.runner import run_project  # noqa: E402


def warehouse(root, name="wh.db"):
    root.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(root / name)) as con:
        con.execute("create table orders as select 1 as id, 9.5 as amount, 7 as cid")
        con.execute("create table customers as select 7 as id, 'ada' as name")


def record(path, text):
    """Write a notebook and run it with recording on, as `quicksql run` would."""
    path.write_text(text)
    results = run_project(compile_file(path))
    assert all(r.ok for r in results), [r.error for r in results]
    return path


@pytest.fixture
def recorded(tmp_path, monkeypatch):
    """Three notebooks' usage: two reach one warehouse, one has its own; one
    reads a seed file next to it."""
    monkeypatch.setenv("QSQL_USAGE", "1")
    shop, other = tmp_path / "shop", tmp_path / "other"
    warehouse(shop)
    warehouse(other)
    (shop / "seeds").mkdir()
    (shop / "seeds" / "plans.csv").write_text("plan_id\n1\n")
    header = "-- @input: {duckdb: wh.db}\n"
    sales = record(
        shop / "sales.qsql",
        header
        + "-- @cell by_customer\nselect cid, sum(amount) as total from orders group by cid;\n"
        "-- @cell plans\nselect plan_id from {{ source('seeds/plans.csv') }};\n",
    )
    (shop / "reports").mkdir()
    record(
        shop / "reports" / "names.qsql",
        "-- @input: {duckdb: ../wh.db}\n-- @cell names\nselect name from customers;\n",
    )
    record(other / "theirs.qsql", header + "-- @cell t\nselect id from orders;\n")
    monkeypatch.delenv("QSQL_USAGE")
    return sales


def test_a_notebooks_report_keeps_the_tables_on_its_contexts(recorded) -> None:
    doc = json.loads(cli(str(recorded), "--json"))
    assert doc["notebooks"] == [str(recorded)]
    found = {t["name"]: t for t in doc["tables"]}
    # what every logged notebook read on its warehouse; not the other warehouse's orders
    assert sorted(found) == sorted(
        ["orders", "customers", str(recorded.parent / "seeds" / "plans.csv")]
    )
    assert found["orders"]["notebooks"] == [{"path": str(recorded), "uses": 1}]
    assert [c["name"] for c in found["orders"]["columns"]] == ["amount", "cid"]
    assert found["customers"]["notebooks"] == [
        {"path": str(recorded.parent / "reports" / "names.qsql"), "uses": 1}
    ]
    everything = json.loads(cli("--json"))
    assert len([t for t in everything["tables"] if t["name"] == "orders"]) == 2


def test_a_notebooks_contexts_are_named_by_its_cells(recorded) -> None:
    out = cli(str(recorded), "--markdown")
    assert "## duckdb · orders — context of by_customer, plans · 1 notebook, score 1.0" in out
    assert "## file · " + str(recorded.parent / "seeds" / "plans.csv") + " · 1 notebook" in out
    doc = json.loads(cli(str(recorded), "--json"))
    assert {t["context_label"] for t in doc["tables"] if t["kind"] == "table"} == {
        "context of by_customer, plans"
    }


def test_a_file_under_a_named_notebooks_directory_is_kept(recorded, tmp_path) -> None:
    theirs = tmp_path / "other" / "theirs.qsql"
    log(
        entry(
            "x",
            NOW,
            "/elsewhere/x.qsql",
            tables=[
                {"kind": "file", "parts": [str(theirs.parent / "data" / "in.csv")], "star": False},
                {"kind": "file", "parts": ["/far/away.csv"], "star": False},
            ],
        )
    )
    names = {t["name"] for t in json.loads(cli(str(theirs), "--json"))["tables"]}
    assert names == {"orders", str(theirs.parent / "data" / "in.csv")}


def test_several_notebooks_report_the_union_of_their_contexts(recorded, tmp_path) -> None:
    doc = json.loads(cli(str(recorded), str(tmp_path / "other" / "theirs.qsql"), "--json"))
    assert len([t for t in doc["tables"] if t["name"] == "orders"]) == 2
    assert len(doc["notebooks"]) == 2


def test_set_overrides_name_the_context_the_run_had(recorded, tmp_path) -> None:
    out = cli(str(recorded), "--set", f"input.duckdb={tmp_path / 'other' / 'wh.db'}", "--json")
    found = {t["name"]: t for t in json.loads(out)["tables"]}
    assert found["orders"]["notebooks"] == [
        {"path": str(tmp_path / "other" / "theirs.qsql"), "uses": 1}
    ]
    assert "customers" not in found


def test_plugins_load_for_a_named_notebook(recorded, tmp_path) -> None:
    plug = tmp_path / "plug.py"
    plug.write_text(
        "from pydantic import BaseModel\n"
        "from quicksql.plugins import Plugin, qfield\n"
        "from quicksql.registry import plugin\n"
        "@plugin\nclass Owner(Plugin):\n"
        "    class Config(BaseModel):\n        owner: str | None = qfield(None)\n"
    )
    recorded.write_text("-- @owner: me\n" + recorded.read_text())
    cli(str(recorded), code=3)  # an unknown directive without the plugin
    assert "orders" in cli(str(recorded), "--plugins", str(plug))


def test_a_notebook_that_doesnt_compile_exits_3(recorded, tmp_path) -> None:
    bad = tmp_path / "bad.qsql"
    bad.write_text("-- @cell a\nselect * from {{ ref('nope') }};\n")
    assert "nope" in cli(str(bad), code=3)
    cli(str(tmp_path / "missing.qsql"), code=3)


def test_a_logged_notebook_nobody_named_never_compiles(recorded) -> None:
    # its rc would raise if anything imported it
    (recorded.parent / "reports" / "quicksqlrc.py").write_text("raise RuntimeError('rc ran')\n")
    out = cli(str(recorded), "--markdown")
    assert "reports/names.qsql (1)" in out
    assert "reports/names.qsql" in cli("--markdown")


def test_a_notebook_with_nothing_logged_on_its_contexts_says_so(tmp_path) -> None:
    warehouse(tmp_path)
    path = tmp_path / "new.qsql"
    path.write_text("-- @input: {duckdb: wh.db}\n-- @cell a\nselect id from orders;\n")
    assert "no usage recorded" in cli(str(path))
