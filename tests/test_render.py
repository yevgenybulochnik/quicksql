import pytest

from quicksql.compiler import compile_text
from quicksql.errors import ConfigError
from quicksql.render import RenderContext


def test_render_context_capability_verbs(tmp_path) -> None:
    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    rctx = RenderContext(
        name="probe", config=project.cells["a"].config, root=tmp_path,
        producers={},
    )
    with pytest.raises(ConfigError, match="unknown cell"):
        rctx.add_edge("ghost")
    with pytest.raises(ConfigError, match="unknown cell"):
        rctx.producer_expr("ghost")
    rctx.require_extensions(["excel", "excel", "spatial"])
    assert rctx.extensions == ["excel", "spatial"]
    assert not rctx.used_source
    rctx.mark_source_used()
    assert rctx.used_source
    assert rctx.resolve_path("seeds/x.csv") == str(tmp_path / "seeds" / "x.csv")
    assert rctx.resolve_path("/abs/x.csv") == "/abs/x.csv"


def test_same_context_ref_is_a_bare_temp_table_name(tmp_path) -> None:
    project = compile_text(
        "-- @cell users\nSELECT 1 AS id;\n"
        "-- @cell active\nSELECT * FROM {{ ref('users') }};",
        root=tmp_path,
    )
    active = project.cells["active"]
    assert "FROM users" in active.sql          # in-engine, dialect-neutral
    assert "read_parquet" not in active.sql    # no conduit round-trip
    assert active.depends_on == ["users"]
    assert active.context_refs == ["users"]
    assert project.cells["users"].reffed_in_context


def test_cross_context_ref_emits_producer_parquet_ref_expr(tmp_path) -> None:
    project = compile_text(
        "-- @cell legacy\n-- @engine: sqlite\nSELECT 1 AS id;\n"
        "-- @cell active\nSELECT * FROM {{ ref('legacy') }};",
        root=tmp_path,
    )
    active = project.cells["active"]
    assert f"read_parquet('{tmp_path}/data/legacy.parquet')" in active.sql
    assert active.external_refs == ["legacy"]
    assert not project.cells["legacy"].reffed_in_context


def test_cross_context_ref_uses_the_producers_own_sink(tmp_path) -> None:
    project = compile_text(
        "-- @cell landed\n-- @engine: sqlite\n"
        "-- @output: { type: duckdb, path: wh.db }\n"
        "SELECT 1 AS id;\n"
        "-- @cell reader\nSELECT * FROM {{ ref('landed') }};",
        root=tmp_path,
    )
    sql = project.cells["reader"].sql
    assert '."main"."landed"' in sql
    assert "read_parquet" not in sql


def test_ref_unknown_cell_raises(tmp_path) -> None:
    with pytest.raises(ConfigError, match="unknown cell"):
        compile_text("-- @cell a\nSELECT * FROM {{ ref('ghost') }};", root=tmp_path)


def test_named_source_resolves_reader_and_records_extension(tmp_path) -> None:
    project = compile_text(
        "/*@\nsources:\n  sales: { path: data/sales.xlsx, sheet: Q1 }\n*/\n"
        "-- @cell q1\nSELECT * FROM {{ source('sales') }};",
        root=tmp_path,
    )
    q1 = project.cells["q1"]
    assert f"read_xlsx('{tmp_path}/data/sales.xlsx', sheet='Q1')" in q1.sql
    assert "excel" in q1.extensions


def test_inline_path_source(tmp_path) -> None:
    project = compile_text(
        "-- @cell users\nSELECT * FROM {{ source('seeds/users.csv') }};",
        root=tmp_path,
    )
    assert f"read_csv_auto('{tmp_path}/seeds/users.csv')" in project.cells["users"].sql


def test_var_and_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QUICKSQL_TEST_TOKEN", "sekret")
    project = compile_text(
        "-- @vars: { data_dir: ./seeds }\n"
        "-- @cell a\nSELECT '{{ var(\"data_dir\") }}' AS d, '{{ env(\"QUICKSQL_TEST_TOKEN\") }}' AS t,"
        " {{ var('missing', 42) }} AS fallback;",
        root=tmp_path,
    )
    sql = project.cells["a"].sql
    assert "'./seeds' AS d" in sql
    assert "'sekret' AS t" in sql
    assert "42 AS fallback" in sql


def test_var_missing_without_default_raises(tmp_path) -> None:
    with pytest.raises(ConfigError, match="undefined var"):
        compile_text("-- @cell a\nSELECT {{ var('nope') }};", root=tmp_path)


def test_jinja_only_touches_sql_not_config(tmp_path) -> None:
    project = compile_text(
        '-- @vars: { greeting: "{{ not rendered }}" }\n-- @cell a\nSELECT 1;',
        root=tmp_path,
    )
    assert project.config.vars["greeting"] == "{{ not rendered }}"


# ---------- sandbox: compiling never runs Python ----------

_ESCAPES = {
    # a default global's function globals hold the os module
    "global": '{{ cycler.__init__.__globals__.os.popen("touch MARKER").read() }}',
    # every class hangs off a literal's MRO; os._wrap_close's globals hold popen
    "mro": (
        "{% for c in ''.__class__.__mro__[1].__subclasses__()"
        " if c.__name__ == '_wrap_close' %}"
        '{{ c.__init__.__globals__.popen("touch MARKER").read() }}{% endfor %}'
    ),
}


@pytest.mark.parametrize("payload", _ESCAPES.values(), ids=_ESCAPES.keys())
def test_template_cannot_reach_python_internals(tmp_path, payload) -> None:
    marker = tmp_path / "escaped"
    text = f"-- @cell a\nSELECT '{payload.replace('MARKER', str(marker))}' AS x;"
    with pytest.raises(ConfigError, match=r"cell 'a' \(line 1\): template error: .*unsafe"):
        compile_text(text, root=tmp_path)
    assert not marker.exists()


def test_loops_conditionals_filters_and_macros_still_render(tmp_path) -> None:
    project = compile_text(
        "-- @vars: { cols: [id, name], dedupe: true }\n"
        "-- @cell a\n"
        "{% macro quoted(n) %}\"{{ n | lower }}\"{% endmacro %}"
        "SELECT {% if var('dedupe') %}DISTINCT {% endif %}"
        "{{ var('cols') | map('upper') | join(', ') }}, {{ quoted('Total') }}\n"
        "FROM {% for t in ['x', 'y'] %}{{ t }}{% if not loop.last %} CROSS JOIN {% endif %}"
        "{% endfor %};",
        root=tmp_path,
    )
    assert project.cells["a"].sql.strip() == (
        'SELECT DISTINCT ID, NAME, "total"\nFROM x CROSS JOIN y;'
    )
