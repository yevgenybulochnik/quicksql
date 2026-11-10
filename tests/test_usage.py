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
