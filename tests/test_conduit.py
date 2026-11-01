import duckdb

from quicksql.bootstrap import compile_text
from quicksql.conduit import landed_relation, read_landed
from quicksql.runner import run_project


def test_read_landed_reads_a_parquet_output_back_as_a_preview(tmp_path, registries) -> None:
    project = compile_text(
        "-- @cell a\nSELECT range AS x FROM range(5);\n", root=tmp_path, registries=registries
    )
    assert run_project(project)[0].ok
    conn = duckdb.connect()
    head = read_landed(conn, landed_relation(project, "a"), limit=2)
    assert head is not None and head.columns == ("x",) and head.rows == ((0,), (1,))


def test_read_landed_is_none_for_an_output_never_landed(tmp_path, registries) -> None:
    project = compile_text("-- @cell a\nSELECT 1 AS x;\n", root=tmp_path, registries=registries)
    assert read_landed(duckdb.connect(), landed_relation(project, "a"), limit=5) is None


def test_an_effect_only_cell_has_no_landed_relation(tmp_path, registries) -> None:
    project = compile_text(
        "-- @cell a\n-- @output: {type: none}\nSELECT 1 AS x;\n", root=tmp_path,
        registries=registries,
    )
    assert landed_relation(project, "a") is None


def test_a_database_output_reads_only_where_its_database_is_attached(
    tmp_path, registries
) -> None:
    project = compile_text(
        "-- @cell a\n-- @output: {type: duckdb}\nSELECT 1 AS x;\n", root=tmp_path,
        registries=registries,
    )
    assert run_project(project)[0].ok
    # a fresh connection hasn't ATTACHed the sink's file, and read_landed never does
    assert read_landed(duckdb.connect(), landed_relation(project, "a"), limit=5) is None
