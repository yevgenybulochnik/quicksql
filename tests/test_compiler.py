import pytest

from quicksql.compiler import compile_text
from quicksql.errors import ConfigError

PIPELINE = """\
-- @engine: duckdb
-- @vars: { n: 3 }

-- @cell users
SELECT * FROM range({{ var('n') }}) t(user_id);

-- @cell events
SELECT range AS user_id, 'click' AS event FROM range(2);

-- @cell active
-- @depends_on: [events]
SELECT u.user_id FROM {{ ref('users') }} u;
"""


def test_project_topo_order_and_edges(tmp_path) -> None:
    project = compile_text(PIPELINE, root=tmp_path)
    assert project.order == ["users", "events", "active"]
    assert project.cells["active"].depends_on == ["events", "users"]


def test_engine_resolution_explicit_and_inferred(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\n-- @engine: sqlite\nSELECT 1 AS x;\n"
        "-- @cell b\n/*@ input: { sqlite: db.db } */\nSELECT 2 AS x;\n"
        "-- @cell c\nSELECT 3 AS x;",
        root=tmp_path,
    )
    assert project.cells["a"].engine == "sqlite"
    assert project.cells["b"].engine == "sqlite"
    assert project.cells["c"].engine == "duckdb"


def test_depends_on_unknown_cell_raises(tmp_path) -> None:
    with pytest.raises(ConfigError, match="unknown cell"):
        compile_text("-- @cell a\n-- @depends_on: [ghost]\nSELECT 1;", root=tmp_path)


def test_non_duckdb_cell_with_ref_trips_guardrail(tmp_path) -> None:
    with pytest.raises(ConfigError, match="must run on duckdb"):
        compile_text(
            "-- @cell a\nSELECT 1 AS x;\n"
            "-- @cell b\n-- @engine: sqlite\nSELECT * FROM {{ ref('a') }};",
            root=tmp_path,
        )


def test_non_duckdb_cell_with_source_trips_guardrail(tmp_path) -> None:
    with pytest.raises(ConfigError, match="must run on duckdb"):
        compile_text(
            "-- @cell a\n-- @engine: sqlite\nSELECT * FROM {{ source('x.csv') }};",
            root=tmp_path,
        )


def test_extensions_directive_trips_guardrail_on_non_duckdb_cell(tmp_path) -> None:
    with pytest.raises(ConfigError, match="must run on duckdb"):
        compile_text(
            "-- @cell a\n-- @engine: sqlite\n-- @extensions: [excel]\nSELECT 1;",
            root=tmp_path,
        )


def test_ref_to_none_sink_cell_rejected(tmp_path) -> None:
    with pytest.raises(ConfigError, match="lands nothing"):
        compile_text(
            "-- @cell effect\n-- @output: { type: none }\nCREATE TABLE t AS SELECT 1 AS n;\n"
            "-- @cell reader\nSELECT * FROM {{ ref('effect') }};",
            root=tmp_path,
        )


def test_ref_to_any_sink_that_lands_nothing_rejected(tmp_path, discard_sink) -> None:
    # lands_output, not the sink's name, says whether there is anything to read back
    with pytest.raises(ConfigError, match="cannot ref\\('effect'\\): it uses sink 'discard' and lands nothing"):
        compile_text(
            "-- @cell effect\n-- @output: { type: discard }\nCREATE TABLE t AS SELECT 1 AS n;\n"
            "-- @cell reader\nSELECT * FROM {{ ref('effect') }};",
            root=tmp_path,
        )


def test_a_duckdb_cell_with_its_own_duckdb_input_is_rejected(tmp_path) -> None:
    # every duckdb cell runs on the one conduit the header names; a cell's own
    # input.duckdb used to be ignored silently, running it in memory
    with pytest.raises(
        ConfigError,
        match=r"cell 'a' \(line 1\): input.duckdb 'other.db' isn't the conduit's \(':memory:'\)",
    ):
        compile_text("-- @cell a\n-- @input: { duckdb: other.db }\nSELECT 1;", root=tmp_path)


def test_a_duckdb_cell_naming_the_conduits_database_compiles(tmp_path) -> None:
    project = compile_text(
        "-- @input: { duckdb: wh.db }\n"
        "-- @cell inherits\nSELECT 1;\n"
        "-- @cell repeats\n-- @input: { duckdb: ./wh.db }\nSELECT 2;\n",
        root=tmp_path,
    )
    assert list(project.cells) == ["inherits", "repeats"]


def test_depends_on_to_none_sink_cell_allowed(tmp_path) -> None:
    # ordering-only dependency reads no data, so it's fine to depend on an effect-only cell
    project = compile_text(
        "-- @cell effect\n-- @output: { type: none }\nCREATE TABLE t AS SELECT 1 AS n;\n"
        "-- @cell after\n-- @depends_on: [effect]\nSELECT 2 AS m;",
        root=tmp_path,
    )
    assert "effect" in project.cells["after"].depends_on


def test_reffed_multistatement_bigquery_rejected(tmp_path) -> None:
    with pytest.raises(ConfigError, match="single statement"):
        compile_text(
            "/*@ input: { bigquery: { project: p } } */\n"
            "-- @cell parent\nCREATE TEMP TABLE s AS SELECT 1 AS n; SELECT n FROM s;\n"
            "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
            root=tmp_path,
        )


def test_reffed_single_statement_bigquery_compiles(tmp_path) -> None:
    project = compile_text(
        "/*@ input: { bigquery: { project: p } } */\n"
        "-- @cell parent\nSELECT 1 AS n;\n"
        "-- @cell child\nSELECT count(*) AS c FROM {{ ref('parent') }};",
        root=tmp_path,
    )
    assert project.cells["parent"].reffed_in_context is True


def test_extensions_union_config_and_render_collected(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\n-- @extensions: [spatial]\nSELECT * FROM {{ source('s.xlsx') }};",
        root=tmp_path,
    )
    assert set(project.cells["a"].extensions) == {"spatial", "excel"}


def test_all_config_errors_reported_together_with_lines(tmp_path) -> None:
    from quicksql.errors import ConfigErrorGroup

    text = (
        "-- @cell good\nSELECT 1;\n"
        "-- @cell bad_directive\n-- @bogus: 1\nSELECT 2;\n"
        "-- @cell bad_sink\n-- @output: { type: carrier_pigeon }\nSELECT 3;\n"
    )
    with pytest.raises(ConfigErrorGroup) as exc:
        compile_text(text, root=tmp_path)
    msg = str(exc.value)
    assert "cell 'bad_directive' (line 3)" in msg
    assert "@bogus" in msg
    assert "cell 'bad_sink' (line 6)" in msg
    assert "carrier_pigeon" in msg
    assert [e.cell for e in exc.value.errors] == ["bad_directive", "bad_sink"]


def test_all_render_errors_reported_together(tmp_path) -> None:
    from quicksql.errors import ConfigErrorGroup

    text = (
        "-- @cell a\nSELECT * FROM {{ ref('ghost') }};\n"
        "-- @cell b\nSELECT {{ var('nope') }};\n"
    )
    with pytest.raises(ConfigErrorGroup) as exc:
        compile_text(text, root=tmp_path)
    msg = str(exc.value)
    assert "cell 'a' (line 1)" in msg and "ghost" in msg
    assert "cell 'b' (line 3)" in msg and "undefined var" in msg


def test_single_error_still_matches_substring(tmp_path) -> None:
    with pytest.raises(ConfigError, match="unknown directive: @bogus"):
        compile_text("-- @cell a\n-- @bogus: 1\nSELECT 1;", root=tmp_path)


def test_cycle_reported(tmp_path) -> None:
    from quicksql.errors import CycleError

    with pytest.raises(CycleError):
        compile_text(
            "-- @cell a\nSELECT * FROM {{ ref('b') }};\n"
            "-- @cell b\nSELECT * FROM {{ ref('a') }};",
            root=tmp_path,
        )


def test_global_config_reaches_cells_and_overrides_apply(tmp_path) -> None:
    project = compile_text(
        "-- @autorun: false\n-- @cell a\nSELECT 1;",
        root=tmp_path,
        overrides={"autorun": True},
    )
    assert project.cells["a"].config.autorun is True
    assert project.config.autorun is True
