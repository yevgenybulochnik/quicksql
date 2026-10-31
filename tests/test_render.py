import pytest

from quicksql.compiler import compile_text
from quicksql.errors import ConfigError, ExecutorError
from quicksql.plugins.base import Plugin
from quicksql.registry import plugin
from quicksql.render import RenderContext, render_sql


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


def test_template_cannot_call_config_model_methods(tmp_path) -> None:
    secret = tmp_path / "secret.json"
    secret.write_text('{"tags": ["read-from-disk"]}')
    text = "-- @cell a\nSELECT '{{ config.parse_file(\"SECRET\").tags[0] }}' AS x;"
    with pytest.raises(ConfigError, match=r"template error: .*'parse_file'.*unsafe") as excinfo:
        compile_text(text.replace("SECRET", str(secret)), root=tmp_path)
    assert "read-from-disk" not in str(excinfo.value)


def test_config_fields_still_render(tmp_path) -> None:
    project = compile_text(
        "-- @cell a\n-- @engine: duckdb\nSELECT '{{ config.engine }}' AS e;", root=tmp_path
    )
    assert "'duckdb' AS e" in project.cells["a"].sql


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


# ---------- any exception while rendering is a template error ----------


def _render(tmp_path, body: str):
    config = compile_text("-- @cell a\nSELECT 1;", root=tmp_path).cells["a"].config
    return render_sql("a", body, config, producers={}, root=tmp_path)


_PYTHON_ERRORS = {
    "zero-division": ("SELECT {{ 1/0 }};", ZeroDivisionError, "division by zero"),
    # the sandbox caps range() with a plain OverflowError, not a TemplateError
    "range-cap": (
        "SELECT 1{% for i in range(100001) %}{% endfor %};", OverflowError, "Range too big"
    ),
}


@pytest.mark.parametrize(
    ("body", "exc_type", "detail"), _PYTHON_ERRORS.values(), ids=_PYTHON_ERRORS.keys()
)
def test_a_python_error_while_rendering_is_a_type_prefixed_template_error(
    tmp_path, body, exc_type, detail
) -> None:
    with pytest.raises(ConfigError) as excinfo:
        _render(tmp_path, body)
    assert str(excinfo.value).startswith(
        f"cell 'a': template error: {exc_type.__name__}: {detail}"
    )
    assert isinstance(excinfo.value.__cause__, exc_type)


def test_a_jinja_error_while_rendering_keeps_its_unprefixed_message(tmp_path) -> None:
    with pytest.raises(ConfigError) as excinfo:
        _render(tmp_path, "SELECT {{ nope }};")
    assert str(excinfo.value) == "cell 'a': template error: 'nope' is undefined"


def test_an_unknown_ref_keeps_its_own_message_not_a_template_error(tmp_path) -> None:
    with pytest.raises(ConfigError) as excinfo:
        _render(tmp_path, "SELECT * FROM {{ ref('ghost') }};")
    assert type(excinfo.value) is ConfigError
    assert str(excinfo.value) == "cell 'a': unknown cell 'ghost' in ref()"
    assert excinfo.value.__cause__ is None  # raised as-is, not re-wrapped


@pytest.mark.parametrize(
    "error",
    [ConfigError("cell 'a': no lookup table"), ExecutorError("warehouse unreachable")],
    ids=["config-error", "executor-error"],
)
def test_a_quicksql_error_from_a_contributed_global_passes_through(tmp_path, error) -> None:
    @plugin
    class Lookups(Plugin):
        def render_context(self, rctx):
            def lookup():
                raise error

            return {"lookup": lookup}

    with pytest.raises(type(error)) as excinfo:
        _render(tmp_path, "SELECT {{ lookup() }};")
    assert excinfo.value is error  # the very exception, not a wrapper around it
