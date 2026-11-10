"""The scanner: what one cell's rendered SQL reads, statement by statement.

The sqlglot behaviours pinned here were probed on 30.12.0 (docs/usage-plan.md);
a newer sqlglot that shifts one fails a test rather than the usage log."""

import pytest

from quicksql.sqlscan import Rel, Scan, scan

FUNCTIONS = frozenset(
    {
        "read_csv",
        "read_csv_auto",
        "read_parquet",
        "parquet_scan",
        "read_json",
        "read_json_auto",
        "read_ndjson",
        "read_ndjson_auto",
        "read_xlsx",
    }
)
SUFFIXES = frozenset({".csv", ".tsv", ".parquet", ".json", ".ndjson", ".xlsx", ".xls"})


def duck(sql: str, refs: dict[str, str] | None = None) -> Scan:
    return scan(sql, "duckdb", refs or {}, FUNCTIONS, SUFFIXES)


def off_conduit(sql: str, dialect: str | None, refs: dict[str, str] | None = None) -> Scan:
    return scan(sql, dialect, refs or {}, frozenset(), frozenset())


def table(*parts: str) -> Rel:
    return Rel("table", parts)


def file(path: str) -> Rel:
    return Rel("file", (path,))


def ref(cell: str) -> Rel:
    return Rel("ref", (cell,))


def rels(found: Scan) -> list[Rel]:
    return [rel for rel, _ in found.tables]


def reads_nothing(found: Scan) -> bool:
    return (found.tables, found.columns, found.unresolved, found.joins, found.star) == ((),) * 5


# ---- statements


def test_an_empty_body_scans_to_nothing() -> None:
    assert duck("") == Scan()
    assert duck("-- only a comment\n") == Scan()


def test_each_statement_is_scanned() -> None:
    found = duck("select a from orders; select b from customers;")
    assert rels(found) == [table("orders"), table("customers")]
    assert found.errors == ()


def test_one_unreadable_statement_leaves_the_others_scanned() -> None:
    found = duck("select a from orders; select from where garbage; select b from customers")
    assert rels(found) == [table("orders"), table("customers")]
    assert found.errors == ((2, "parse"),)


def test_an_unterminated_literal_is_one_parse_error() -> None:
    assert duck("select 'abc from orders") == Scan(errors=((1, "parse"),))


def test_an_error_is_an_index_and_a_kind_never_the_statement() -> None:
    found = duck("select 'ssn 078-05-1120' from where")
    assert found.errors == ((1, "parse"),)
    assert "078" not in repr(found)


@pytest.mark.parametrize(
    "dialect, sql",
    [
        ("duckdb", "call foo(1)"),
        ("postgres", "vacuum t"),
        ("bigquery", "if x then select 1"),
    ],
)
def test_a_command_fallback_is_unsupported_and_silent(dialect, sql, capfd, caplog) -> None:
    found = off_conduit(sql, dialect)
    assert found == Scan(errors=((1, "unsupported:Command"),))
    out, err = capfd.readouterr()
    assert (out, err, caplog.text) == ("", "", "")


def test_the_sqlglot_logger_is_left_as_it_was() -> None:
    import logging

    logger = logging.getLogger("sqlglot")
    before = (logger.level, logger.disabled)
    duck("call foo(1)")
    assert (logger.level, logger.disabled) == before


def test_a_body_over_the_cap_is_not_scanned() -> None:
    big = "select a from orders where b in (" + ", ".join(["1"] * 60_000) + ")"
    assert duck(big) == Scan(errors=((0, "too large"),))


def test_a_statement_that_reads_no_relation_records_nothing() -> None:
    body = """
        create table t (a int);
        drop table if exists u;
        insert into t values (1);
        set threads = 2;
        attach 'x.db' as y;
        load httpfs;
    """
    found = duck(body)
    assert rels(found) == []
    assert found.errors == ()
    assert found.created == frozenset({("t",)})


def test_a_kind_with_no_scope_is_unsupported() -> None:
    assert duck("pivot t on a using sum(b)").errors == ((1, "unsupported:Pivot"),)


# ---- tables


def test_tables_are_named_with_their_qualifiers_first() -> None:
    found = off_conduit("select a from claims.members m join public.plans p on true", "postgres")
    assert rels(found) == [table("claims", "members"), table("public", "plans")]


def test_names_are_normalized_as_the_dialect_folds_them() -> None:
    assert rels(off_conduit('select a from Orders, "Lines"', "postgres")) == [
        table("orders"),
        table("Lines"),
    ]


def test_a_bigquery_backticked_path_is_three_parts() -> None:
    assert rels(off_conduit("select a from `proj.ds.tbl`", "bigquery")) == [
        table("proj", "ds", "tbl")
    ]


def test_no_dialect_scans_as_generic_sql() -> None:
    assert rels(off_conduit("select a from s.t", None)) == [table("s", "t")]


def test_a_table_read_twice_is_listed_once() -> None:
    assert rels(duck("select 1 from orders a join orders b on a.id = b.parent")) == [
        table("orders")
    ]


def test_a_cte_is_not_a_table() -> None:
    found = duck("with lines as (select a from order_lines) select a from lines")
    assert rels(found) == [table("order_lines")]


def test_a_subquerys_tables_count() -> None:
    found = duck("select a from orders where id in (select order_id from payments)")
    assert rels(found) == [table("payments"), table("orders")]


# ---- files


def test_a_reader_function_with_a_path_literal_is_a_file() -> None:
    found = duck("select a from read_csv_auto('/data/x.csv', header=true)")
    assert rels(found) == [file("/data/x.csv")]


@pytest.mark.parametrize("function", sorted(FUNCTIONS))
def test_every_reader_function_is_recognized(function: str) -> None:
    assert rels(duck(f"select a from {function}('x.dat')")) == [file("x.dat")]


def test_a_list_of_path_literals_is_a_file_each() -> None:
    found = duck("select a from read_parquet(['a.parquet', 'b.parquet'])")
    assert rels(found) == [file("a.parquet"), file("b.parquet")]


def test_a_replacement_scan_is_a_file() -> None:
    assert rels(duck("select a from 'data/x.parquet'")) == [file("data/x.parquet")]
    assert rels(duck('select a from "data/X.CSV"')) == [file("data/X.CSV")]


def test_a_quoted_name_without_a_readers_extension_is_a_table() -> None:
    assert rels(duck('select a from "my table"')) == [table("my table")]


def test_a_urls_userinfo_and_query_are_stripped() -> None:
    found = duck("select a from read_parquet('s3://key:secret@bucket/x.parquet?token=abc#frag')")
    assert rels(found) == [file("s3://bucket/x.parquet")]
    assert "secret" not in repr(found) and "abc" not in repr(found)


def test_a_non_reader_table_function_is_no_source_and_its_arguments_stay_out() -> None:
    found = duck(
        "select a from generate_series(1, 10) g(a);"
        " select b from postgres_scan('host=db password=hunter2', 'public', 'orders')"
    )
    assert reads_nothing(found) and found.errors == ()
    assert "hunter2" not in repr(found)


def test_a_reader_function_without_a_literal_path_is_no_source() -> None:
    assert reads_nothing(duck("select a from read_csv(getvariable('p'))"))


def test_files_count_on_the_conduits_engine_only() -> None:
    found = off_conduit("select a from read_csv('x.csv')", "postgres")
    assert rels(found) == []
    assert rels(off_conduit('select a from "x.csv"', "postgres")) == [table("x.csv")]


# ---- refs


def test_a_bare_cell_name_is_a_ref() -> None:
    found = duck("select a from stage join orders using (id)", {"stage": "stage"})
    assert rels(found) == [ref("stage"), table("orders")]


def test_a_parquet_sinks_read_parquet_is_a_ref_not_a_file() -> None:
    expr = "read_parquet('/nb/data/stage.parquet')"
    assert rels(duck(f"select a from {expr}", {expr: "stage"})) == [ref("stage")]
    assert rels(duck("select a from read_parquet('/nb/data/other.parquet')", {expr: "stage"})) == [
        file("/nb/data/other.parquet")
    ]


def test_an_attached_sink_table_is_a_ref() -> None:
    expr = 'qsql_db_1a2b3c4d."main"."stage"'
    found = duck(f"select a from {expr} s", {expr: "stage"})
    assert rels(found) == [ref("stage")]


def test_a_ref_matches_however_the_cell_spelled_it() -> None:
    assert rels(duck("select a from STAGE", {"stage": "stage"})) == [ref("stage")]


# ---- created


def test_a_table_the_cell_creates_is_not_a_source() -> None:
    found = duck(
        "create temp table tmp as select a from orders;"
        " create or replace view v as select a from tmp;"
        " select a from tmp join v using (a) join customers using (a)"
    )
    assert found.created == frozenset({("tmp",), ("v",)})
    assert rels(found) == [table("orders"), table("customers")]


def test_created_names_fold_case_and_match_by_their_last_parts() -> None:
    found = duck("create table main.Staged as select 1 as a; select a from staged")
    assert found.created == frozenset({("main", "staged")})
    assert rels(found) == []


# ---- statements that read through something other than a SELECT


def test_an_update_reads_its_table_and_its_from() -> None:
    found = duck("update orders set amount = s.amount from staged s where orders.id = s.id")
    assert rels(found) == [table("orders"), table("staged")]
    assert found.errors == ()


def test_a_delete_reads_its_table_and_its_using() -> None:
    found = duck("delete from orders using voided v where orders.id = v.id")
    assert rels(found) == [table("orders"), table("voided")]


def test_a_merge_reads_its_target_and_its_source() -> None:
    found = off_conduit(
        "merge into orders o using staged s on o.id = s.id"
        " when matched then update set amount = s.amount"
        " when not matched then insert (id, amount) values (s.id, s.amount)",
        "postgres",
    )
    assert rels(found) == [table("orders"), table("staged")]
    assert found.errors == ()


def test_create_as_insert_select_and_copy_scan_as_they_are() -> None:
    found = duck(
        "create table t as select a from s1;"
        " insert into t select a from s2;"
        " copy (select a from s3) to 'out.parquet'"
    )
    assert rels(found) == [table("s1"), table("s2"), table("s3")]
    assert found.errors == ()


# ---- columns: the probe tables of docs/usage-plan.md, per dialect

from quicksql.sqlscan import Unresolved  # noqa: E402

DIALECTS = ["duckdb", "postgres", "bigquery"]
EXPANDS_ALIASES = ["duckdb", "postgres"]  # BigQuery reads a WHERE alias as a column


def columns(found: Scan) -> dict[str, set[str]]:
    """``table.column`` -> roles."""
    return {".".join((*use.rel.parts, use.column)): set(use.roles) for use in found.columns}


def unresolved(found: Scan) -> dict[str, tuple]:
    return {u.column: u.candidates for u in found.unresolved}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_columns_count_on_the_table_they_are_read_from(dialect: str) -> None:
    found = off_conduit(
        "select o.amount * o.qty as revenue, upper(c.name) as customer"
        " from orders o join customers c on c.id = o.customer_id",
        dialect,
    )
    assert columns(found) == {
        "orders.amount": {"select"},
        "orders.qty": {"select"},
        "customers.name": {"select"},
        "customers.id": {"join"},
        "orders.customer_id": {"join"},
    }
    assert found.unresolved == ()


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_ctes_outputs_are_not_columns(dialect: str) -> None:
    found = off_conduit(
        "with lines as (select order_id, amount * qty as revenue from order_lines)"
        " select order_id, sum(revenue) as revenue from lines group by order_id",
        dialect,
    )
    assert set(columns(found)) == {
        "order_lines.order_id",
        "order_lines.amount",
        "order_lines.qty",
    }


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_derived_column_of_the_same_name_counts_once(dialect: str) -> None:
    found = off_conduit(
        "with adj as (select amount * 1.1 as amount from orders) select amount from adj", dialect
    )
    assert columns(found) == {"orders.amount": {"select"}}


@pytest.mark.parametrize("dialect", EXPANDS_ALIASES)
def test_a_where_alias_reference_counts_its_inputs(dialect: str) -> None:
    # in Postgres WHERE can't see a select alias, so this read a real column
    # `revenue`; sqlglot credits the alias's inputs all the same (the plan's risk)
    found = off_conduit(
        "select amount * qty as revenue from orders where revenue > 100", dialect
    )
    assert columns(found) == {
        "orders.amount": {"select", "where"},
        "orders.qty": {"select", "where"},
    }


def test_bigquery_reads_a_where_alias_reference_as_a_column() -> None:
    found = off_conduit(
        "select amount * qty as revenue from orders where revenue > 100", "bigquery"
    )
    assert columns(found) == {
        "orders.amount": {"select"},
        "orders.qty": {"select"},
        "orders.revenue": {"where"},
    }


@pytest.mark.parametrize("dialect", EXPANDS_ALIASES)
def test_a_lateral_alias_reference_counts_its_inputs(dialect: str) -> None:
    found = off_conduit(
        "select amount * qty as revenue, revenue * 0.1 as tax from orders", dialect
    )
    assert columns(found) == {"orders.amount": {"select"}, "orders.qty": {"select"}}


def test_bigquery_reads_a_lateral_alias_reference_as_a_column() -> None:
    found = off_conduit(
        "select amount * qty as revenue, revenue * 0.1 as tax from orders", "bigquery"
    )
    assert set(columns(found)) == {"orders.amount", "orders.qty", "orders.revenue"}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_having_or_order_by_alias_reference_yields_no_column(dialect: str) -> None:
    found = off_conduit("select a as z from s group by z having z > 1 order by z", dialect)
    assert columns(found) == {"s.a": {"select", "group"}}
    assert found.unresolved == ()


@pytest.mark.parametrize("dialect", DIALECTS)
def test_unqualified_columns_over_two_tables_wait_for_a_schema(dialect: str) -> None:
    found = off_conduit("select id, amount from orders join payments using (order_id)", dialect)
    assert columns(found) == {"orders.order_id": {"join"}, "payments.order_id": {"join"}}
    both = (table("orders"), table("payments"))
    assert unresolved(found) == {"id": both, "amount": both}
    assert {u.roles for u in found.unresolved} == {frozenset({"select"})}
    assert not any(u.nested for u in found.unresolved)


def test_a_bigquery_path_and_a_postgres_schema_carry_their_columns() -> None:
    assert columns(off_conduit("select a from `proj.ds.tbl` where b", "bigquery")) == {
        "proj.ds.tbl.a": {"select"},
        "proj.ds.tbl.b": {"where"},
    }
    assert columns(off_conduit("select m.a from claims.members m", "postgres")) == {
        "claims.members.a": {"select"}
    }


def test_bigquery_pipe_syntax_scans_like_any_query() -> None:
    found = off_conduit("FROM t |> WHERE x > 1 |> SELECT a, b", "bigquery")
    assert found.errors == ()
    assert set(columns(found)) == {"t.a", "t.b", "t.x"}
    assert columns(found)["t.x"] == {"where"}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_correlated_column_counts_on_the_enclosing_source(dialect: str) -> None:
    found = off_conduit(
        "select o.total from orders o"
        " where exists (select 1 from payments p where p.oid = o.id)",
        dialect,
    )
    assert columns(found) == {
        "orders.total": {"select"},
        "payments.oid": {"where"},
        "orders.id": {"where"},
    }
    assert found.unresolved == ()


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_unqualified_column_in_a_correlated_subquery_is_unresolved(dialect: str) -> None:
    # sqlglot credits `id` to payments, though it may be the outer table's
    found = off_conduit(
        "select o.total from orders o where exists (select 1 from payments where oid = id)",
        dialect,
    )
    assert columns(found) == {"orders.total": {"select"}}
    inner_first = (table("payments"), table("orders"))
    assert unresolved(found) == {"oid": inner_first, "id": inner_first}
    assert all(u.nested and u.roles == {"where"} for u in found.unresolved)


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_subquery_with_no_enclosing_relation_resolves(dialect: str) -> None:
    found = off_conduit("select (select max(a) from t) as top", dialect)
    assert columns(found) == {"t.a": {"select"}}
    assert found.unresolved == ()


@pytest.mark.parametrize("dialect", ["duckdb", "bigquery"])
def test_a_struct_root_with_one_relation_in_scope_is_its_column(dialect: str) -> None:
    found = off_conduit("select address.city from customers", dialect)
    assert columns(found) == {"customers.address": {"select"}}


@pytest.mark.parametrize("dialect", ["duckdb", "bigquery"])
def test_a_struct_root_with_two_relations_in_scope_is_unresolved(dialect: str) -> None:
    # bare, sqlglot reads this as column `city` of a source `address`
    found = off_conduit(
        "select address.city from customers c join orders o on c.id = o.cid", dialect
    )
    assert set(columns(found)) == {"customers.id", "orders.cid"}
    assert found.unresolved == (
        Unresolved("address", frozenset({"select"}), (table("customers"), table("orders"))),
    )


def test_a_nested_field_records_its_top_level_column() -> None:
    found = off_conduit("select o.address.city, o.items[1].sku from orders o", "duckdb")
    assert set(columns(found)) == {"orders.address", "orders.items"}


def test_a_column_read_in_two_clauses_is_one_use_with_both_roles() -> None:
    found = duck("select a from t where a > 1 group by a having sum(b) > 2 order by sum(c)")
    assert columns(found) == {
        "t.a": {"select", "where", "group"},
        "t.b": {"having"},
        "t.c": {"order"},
    }


def test_an_update_counts_what_it_reads_not_what_it_assigns() -> None:
    assert columns(duck("update orders set amount = 0 where status = 'void'")) == {
        "orders.status": {"where"}
    }
    found = duck("update orders set amount = s.amount * 2 from staged s where orders.id = s.id")
    assert columns(found) == {
        "staged.amount": {"select"},
        "orders.id": {"where"},
        "staged.id": {"where"},
    }


def test_a_delete_counts_its_condition() -> None:
    found = duck("delete from orders using voided v where orders.id = v.id and v.flag")
    assert columns(found) == {
        "orders.id": {"where"},
        "voided.id": {"where"},
        "voided.flag": {"where"},
    }


def test_a_merge_counts_its_on_and_the_values_it_writes() -> None:
    found = off_conduit(
        "merge into orders o using staged s on o.id = s.id"
        " when matched and s.live then update set amount = s.amount"
        " when not matched then insert (id, amount) values (s.id, s.total)",
        "postgres",
    )
    assert columns(found) == {
        "orders.id": {"join"},
        "staged.id": {"join", "select"},
        "staged.live": {"select"},
        "staged.amount": {"select"},
        "staged.total": {"select"},
    }


def test_a_files_columns_count_on_the_file() -> None:
    found = duck("select a from read_csv_auto('/x.csv', header=true) where b > 1")
    assert columns(found) == {"/x.csv.a": {"select"}, "/x.csv.b": {"where"}}
    found = duck("select a from read_parquet(['p.parquet', 'q.parquet'])")
    assert columns(found) == {"p.parquet.a": {"select"}, "q.parquet.a": {"select"}}
    assert columns(duck("select a from 'data/x.parquet'")) == {"data/x.parquet.a": {"select"}}


def test_columns_of_a_non_source_or_a_created_table_do_not_count() -> None:
    assert reads_nothing(duck("select a from generate_series(1, 10) g(a)"))
    found = duck("create temp table tmp as select a from orders; select a, b from tmp")
    assert columns(found) == {"orders.a": {"select"}}
    assert found.unresolved == ()


def test_a_refs_columns_are_kept_for_the_recorder() -> None:
    found = duck("select s.a, o.b from stage s join orders o on s.id = o.id", {"stage": "stage"})
    assert columns(found) == {
        "stage.a": {"select"},
        "stage.id": {"join"},
        "orders.b": {"select"},
        "orders.id": {"join"},
    }
    assert {use.rel for use in found.columns} == {ref("stage"), table("orders")}


# ---- roles


def test_a_windows_partition_and_order_are_read_as_window() -> None:
    found = duck(
        "select a, sum(b) over (partition by c order by d) as running from t"
        " qualify row_number() over w = 1 window w as (partition by e order by f)"
    )
    assert columns(found) == {
        "t.a": {"select"},
        "t.b": {"select"},
        "t.c": {"window"},
        "t.d": {"window"},
        "t.e": {"window"},
        "t.f": {"window"},
    }


def test_a_column_read_inside_from_is_read_as_from() -> None:
    found = duck("select i from orders o, unnest(o.items) as u(i)")
    assert columns(found)["orders.items"] == {"from"}
    found = duck("select g.n from orders o join generate_series(1, o.qty) g(n) on true")
    assert columns(found) == {"orders.qty": {"from"}}


# ---- joins


def joins(found: Scan) -> set[tuple[str, str]]:
    def side(end) -> str:
        rel, column = end
        return ".".join((*rel.parts, column))

    return {(side(a), side(b)) for a, b in found.joins}


def test_an_on_equality_is_a_join() -> None:
    found = duck(
        "select 1 from orders o join customers c on c.id = o.customer_id and c.live"
        " left join payments p on (p.order_id = o.id)"
    )
    assert joins(found) == {
        ("customers.id", "orders.customer_id"),
        ("orders.id", "payments.order_id"),
    }


def test_a_join_is_recorded_once_whichever_way_round_it_is_written() -> None:
    found = duck(
        "select 1 from a join b on a.x = b.y; select 2 from b join a on b.y = a.x"
    )
    assert joins(found) == {("a.x", "b.y")}


def test_a_using_column_joins_both_sides_and_a_bare_read_of_it_credits_both() -> None:
    found = duck("select order_id from orders join payments using (order_id)")
    assert joins(found) == {("orders.order_id", "payments.order_id")}
    assert columns(found) == {
        "orders.order_id": {"join", "select"},
        "payments.order_id": {"join", "select"},
    }
    assert found.unresolved == ()


def test_a_comma_joins_where_equality_is_a_join() -> None:
    found = duck(
        "select 1 from orders o, payments p where o.id = p.oid and o.total > 5 and o.a = o.b"
    )
    assert joins(found) == {("orders.id", "payments.oid")}
    assert columns(found)["orders.id"] == {"where"}


def test_a_correlated_equality_is_not_a_join() -> None:
    found = duck("select 1 from orders o where exists (select 1 from payments p where p.oid = o.id)")
    assert found.joins == ()


def test_a_self_join_is_a_join() -> None:
    found = duck("select 1 from emp e join emp m on e.manager_id = m.id")
    assert joins(found) == {("emp.id", "emp.manager_id")}


def test_a_natural_joins_columns_stay_unresolved() -> None:
    found = duck("select a from x natural join y")
    assert found.joins == ()
    assert unresolved(found) == {"a": (table("x"), table("y"))}


def test_a_join_through_a_ref_keeps_the_ref_side_for_the_recorder() -> None:
    found = duck("select 1 from stage s join orders o on s.id = o.id", {"stage": "stage"})
    assert found.joins == (((table("orders"), "id"), (ref("stage"), "id")),)


# ---- pass-through inside the cell


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_column_passed_through_a_cte_is_read_where_it_is_finally_read(dialect: str) -> None:
    found = off_conduit(
        "with lines as (select order_id, amount * qty as revenue from order_lines)"
        " select order_id, sum(revenue) as revenue from lines group by order_id",
        dialect,
    )
    assert columns(found) == {
        "order_lines.order_id": {"select", "group"},
        "order_lines.amount": {"select"},
        "order_lines.qty": {"select"},
    }


def test_a_renamed_column_passes_through() -> None:
    found = duck("select amt from (select amount as amt, qty from orders) s where amt > 1")
    assert columns(found) == {"orders.amount": {"select", "where"}, "orders.qty": {"select"}}


def test_a_computed_column_stops_the_trace() -> None:
    found = duck(
        "with adj as (select amount * 1.1 as amount from orders) select amount from adj where amount > 1"
    )
    assert columns(found) == {"orders.amount": {"select"}}


@pytest.mark.parametrize("dialect", DIALECTS)
def test_columns_read_through_a_star_count_on_its_relation(dialect: str) -> None:
    found = off_conduit(
        "with base as (select * from orders where status = 'x') select amount, qty from base",
        dialect,
    )
    assert columns(found) == {
        "orders.status": {"where"},
        "orders.amount": {"select"},
        "orders.qty": {"select"},
    }


def test_a_chain_of_stars_and_ctes_is_followed_to_the_table() -> None:
    found = duck(
        "with a as (select * from orders), b as (select * from a), c as (select total as t from b)"
        " select t from c order by t"
    )
    assert columns(found) == {"orders.total": {"select"}}


def test_a_star_excludes_and_replaces() -> None:
    found = duck(
        "with base as (select * exclude (x) replace (a + 1 as amount) from orders)"
        " select x, amount, qty from base"
    )
    assert columns(found) == {"orders.a": {"select"}, "orders.qty": {"select"}}
    found = off_conduit(
        "with base as (select * except (x) from orders) select x, qty from base", "bigquery"
    )
    assert columns(found) == {"orders.qty": {"select"}}


def test_a_qualified_star_covers_its_relation_only() -> None:
    found = duck(
        "with j as (select o.*, c.name from orders o join customers c on o.cid = c.id)"
        " select total, name from j"
    )
    assert columns(found) == {
        "orders.cid": {"join"},
        "customers.id": {"join"},
        "customers.name": {"select"},
        "orders.total": {"select"},
    }


def test_a_column_through_a_star_over_several_relations_is_unresolved() -> None:
    found = duck(
        "with j as (select * from orders o join customers c on o.cid = c.id) select total from j"
    )
    assert set(columns(found)) == {"orders.cid", "customers.id"}
    assert found.unresolved == (
        Unresolved("total", frozenset({"select"}), (table("orders"), table("customers"))),
    )


def test_a_union_cte_passes_each_branchs_column_through() -> None:
    found = duck(
        "with u as (select id, a as v from t1 union all select id, b from t2)"
        " select v from u where id > 1"
    )
    assert columns(found) == {
        "t1.id": {"select", "where"},
        "t1.a": {"select"},
        "t2.id": {"select", "where"},
        "t2.b": {"select"},
    }


def test_a_recursive_cte_ends() -> None:
    found = duck(
        "with recursive r as (select id, parent from nodes union all"
        " select n.id, n.parent from nodes n join r on n.parent = r.id) select id from r"
    )
    assert set(columns(found)) == {"nodes.id", "nodes.parent"}
    assert found.errors == ()


def test_a_join_through_a_cte_is_a_join_of_the_tables() -> None:
    found = duck(
        "with o as (select id as oid, cid from orders) select 1 from o join customers c on o.cid = c.id"
    )
    assert joins(found) == {("customers.id", "orders.cid")}
    assert columns(found)["orders.cid"] == {"select", "join"}


# ---- unresolved candidates


def test_a_derived_relation_that_names_its_columns_is_ruled_in_or_out() -> None:
    # `b` isn't one of c's columns, so it is u's; `a` is c's (qualify places it)
    found = duck("with c as (select a, k from t) select a, b from c join u on c.k = u.k")
    assert columns(found)["u.b"] == {"select"}
    assert columns(found)["t.a"] == {"select"}
    assert found.unresolved == ()


def test_a_derived_relation_with_a_star_stands_for_what_its_star_covers() -> None:
    found = duck("with c as (select * from t) select b from c join u on c.k = u.k")
    assert unresolved(found) == {"b": (table("t"), table("u"))}


def test_a_relation_whose_columns_arent_known_is_a_null_candidate() -> None:
    found = duck("select a from t, unnest(t.items) as i")
    assert unresolved(found) == {"a": (table("t"), None)}
    found = duck("create temp table tmp as select 1 as a; select a from tmp, orders")
    assert unresolved(found) == {"a": (None, table("orders"))}


def test_a_ref_is_a_candidate() -> None:
    found = duck("select amount from stage r join orders o on r.id = o.id", {"stage": "stage"})
    assert unresolved(found) == {"amount": (ref("stage"), table("orders"))}


def test_a_correlated_subquery_over_a_cte_that_names_the_column_resolves() -> None:
    found = duck(
        "with p as (select oid from payments) select o.total from orders o"
        " where exists (select 1 from p where oid = o.id)"
    )
    assert columns(found) == {
        "payments.oid": {"select", "where"},
        "orders.total": {"select"},
        "orders.id": {"where"},
    }
    assert found.unresolved == ()


def test_a_lateral_subquerys_bare_column_is_unresolved_not_a_struct() -> None:
    # bare, sqlglot rewrites `b` to a field of a column `u` on t
    found = duck("select t.a from t, lateral (select b from u where u.k = t.k) l")
    assert columns(found) == {"t.a": {"select"}, "u.k": {"where"}, "t.k": {"where"}}
    assert [(u.column, u.candidates[:2], u.nested) for u in found.unresolved] == [
        ("b", (table("u"), table("t")), True)
    ]


# ---- select *


def test_a_star_counts_the_table_not_its_columns() -> None:
    found = duck("select * from orders")
    assert found.tables == ((table("orders"), True),)
    assert found.columns == ()


def test_a_star_that_stops_in_a_cte_doesnt_reach_the_result() -> None:
    found = duck("with base as (select * from orders) select amount from base")
    assert found.tables == ((table("orders"), False),)
    found = duck("with base as (select * from orders) select * from base join c using (k)")
    assert dict(found.tables) == {table("orders"): True, table("c"): True}


def test_a_star_reaches_the_result_of_any_statement() -> None:
    found = duck("create table t as select o.* from orders o join c on o.k = c.k; select 1")
    assert dict(found.tables) == {table("orders"): True, table("c"): False}


# ---- the terminal statement's shape, for cells that ref() this one


def test_outputs_name_each_columns_pass_through_origin() -> None:
    found = duck(
        "create temp table x as select 1 as a;"
        " select o.id, o.amount as amt, o.amount * 2 as doubled, c.name"
        " from orders o join (select id, name from customers) c on o.cid = c.id"
    )
    assert found.outputs == (
        ("id", (table("orders"), "id")),
        ("amt", (table("orders"), "amount")),
        ("doubled", None),
        ("name", (table("customers"), "name")),
    )
    assert found.star == ()


def test_only_the_terminal_statement_has_outputs() -> None:
    found = duck("select a from t; create table z as select b from u")
    assert found.outputs == (("b", (table("u"), "b")),)
    assert duck("select a from t; drop table z").outputs == ()


def test_a_terminal_star_names_the_relations_it_passes_through() -> None:
    found = duck("select * exclude (ssn) replace (upper(name) as name) from members")
    assert found.star == ((table("members"), frozenset({"ssn", "name"})),)
    assert found.outputs == (("name", None),)


def test_a_terminal_star_over_a_derived_relation_passes_its_columns() -> None:
    found = duck(
        "with base as (select id, amount * 2 as doubled from orders)"
        " select * exclude (doubled), 1 as one from base"
    )
    assert found.outputs == (("id", (table("orders"), "id")), ("one", None))
    assert found.star == ()
    found = duck("with a as (select * exclude (x) from orders) select * exclude (y) from a")
    assert found.star == ((table("orders"), frozenset({"x", "y"})),)


def test_a_terminal_star_over_a_join_lists_each_relation() -> None:
    found = duck("select * from stage s join orders o on s.id = o.id", {"stage": "stage"})
    assert found.star == ((ref("stage"), frozenset()), (table("orders"), frozenset()))


def test_an_output_from_a_ref_keeps_the_ref_for_the_recorder() -> None:
    found = duck("select member_id as id from stage", {"stage": "stage"})
    assert found.outputs == (("id", (ref("stage"), "member_id")),)


def test_an_unresolved_or_union_output_has_no_origin() -> None:
    assert duck("select a from x natural join y").outputs == (("a", None),)
    assert duck("select a from t1 union all select b from t2").outputs == (("a", None),)
