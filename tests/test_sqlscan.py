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
    assert found == Scan()
    assert "hunter2" not in repr(found)


def test_a_reader_function_without_a_literal_path_is_no_source() -> None:
    assert duck("select a from read_csv(getvariable('p'))") == Scan()


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
    assert duck("select a from generate_series(1, 10) g(a)") == Scan()
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
