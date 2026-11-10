"""The recorder: lineage across a notebook's cells, and the UsagePlugin."""

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
