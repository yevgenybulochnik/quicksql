"""quicksql language server — analysis layer (no editor transport needed)."""

from __future__ import annotations

import shutil

import pytest

# the lsp extra (sqlglot + pygls) is optional; skip this whole module without it
pytest.importorskip("sqlglot", reason="install the 'lsp' extra to run LSP tests")

from quicksql.bootstrap import compile_file
from quicksql.lsp.mask import Opaque, Ref, Source, mask_jinja


def test_mask_preserves_length_and_line_structure() -> None:
    sql = "SELECT *\nFROM {{ ref('users') }} u\nWHERE {% if flag %}x{% endif %}"
    masked, spans = mask_jinja(sql)
    assert len(masked) == len(sql)  # offsets map 1:1
    assert masked.count("\n") == sql.count("\n")  # line numbers preserved
    # non-Jinja text is byte-identical
    assert masked.startswith("SELECT *\nFROM ")
    assert " u\nWHERE " in masked


def test_ref_becomes_a_same_length_sql_identifier_mapped_to_the_cell() -> None:
    sql = "FROM {{ ref('users') }} u"
    masked, spans = mask_jinja(sql)
    ref_spans = [s for s in spans if isinstance(s.tag, Ref)]
    assert len(ref_spans) == 1
    span = ref_spans[0]
    assert span.tag == Ref("users")
    # the placeholder sits exactly where the Jinja was and is a bare identifier
    placeholder = masked[span.start : span.end]
    assert span.end - span.start == len("{{ ref('users') }}")
    assert placeholder.isidentifier()
    # ...so sqlglot reads `FROM <placeholder> u` as a table aliased u
    assert masked == f"FROM {placeholder} u"


def test_source_and_opaque_expressions_are_tagged() -> None:
    sql = "SELECT {{ var('x') }} FROM {{ source('seeds/u.csv') }}"
    _, spans = mask_jinja(sql)
    tags = [s.tag for s in spans]
    assert Source("seeds/u.csv") in tags
    assert any(isinstance(t, Opaque) for t in tags)  # var(...) is opaque


def test_opaque_expression_masks_to_a_parseable_placeholder() -> None:
    """`> {{ var('startdate') }}` blanked to spaces left a dangling operator,
    so sqlglot reported a false `Required keyword: 'expression' missing` on a
    line that is fine at runtime. Opaque expressions must read as a value."""
    import sqlglot

    sql = "SELECT * FROM orders WHERE order_date > {{ var('startdate') }}"
    masked, _ = mask_jinja(sql)
    assert len(masked) == len(sql)
    sqlglot.parse(masked, dialect="duckdb")  # must not raise


def test_known_var_value_substitutes_in_place_when_it_fits() -> None:
    """With the cell's vars available, a bare {{ var('key') }} becomes the
    value the runtime render would produce (space-padded to the run's length),
    so the LSP parses what the engine will actually see."""
    sql = "SELECT * FROM t WHERE order_date > {{ var('startdate') }}"
    masked, _ = mask_jinja(sql, {"startdate": "2026-01-01"})
    assert len(masked) == len(sql)
    assert "order_date > 2026-01-01" in masked


def test_var_value_longer_than_its_run_falls_back_to_placeholder() -> None:
    sql = "SELECT * FROM t WHERE a > {{ var('x') }}"
    masked, _ = mask_jinja(sql, {"x": "y" * 50})
    assert len(masked) == len(sql)
    assert "y" * 50 not in masked


def test_control_and_multiline_blocks_blank_to_spaces_keeping_newlines() -> None:
    sql = "SELECT 1\n{% if a\n   and b %}\nWHERE x{% endif %}"
    masked, spans = mask_jinja(sql)
    assert len(masked) == len(sql)
    assert masked.count("\n") == sql.count("\n")
    # a multi-line {% %} becomes spaces (not an identifier) but newlines survive
    block = next(s for s in spans if s.start == sql.index("{% if"))
    assert set(masked[block.start : block.end]) <= {" ", "\n"}


def test_name_to_tag_round_trip_for_scope_lookup() -> None:
    sql = "FROM {{ ref('a') }} x JOIN {{ ref('b') }} y ON x.k = y.k"
    masked, spans = mask_jinja(sql)
    name_to_tag = {s.name: s.tag for s in spans if s.name}
    # every placeholder identifier resolves back to its ref
    assert set(name_to_tag.values()) == {Ref("a"), Ref("b")}
    for name in name_to_tag:
        assert name in masked


# ---------- schema.py: columns per relation ----------


def _write_run(tmp_path, body):
    from quicksql.runner import run_project

    nb = tmp_path / "n.qsql"
    nb.write_text(body)
    project = compile_file(nb)
    run_project(project)  # land the sinks so refs are introspectable
    return compile_file(nb)


def test_ref_columns_come_from_the_landed_cell(tmp_path) -> None:
    from quicksql.lsp.schema import ref_columns

    project = _write_run(
        tmp_path,
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS id, 'ada' AS name, true AS active;\n",
    )
    cols = dict(ref_columns(project, "users"))
    assert set(cols) == {"id", "name", "active"}


def test_source_columns_read_a_csv_header(tmp_path) -> None:
    from quicksql.lsp.schema import source_columns

    (tmp_path / "u.csv").write_text("member_id,plan_code\n1,AB\n")
    project = _write_run(
        tmp_path,
        "-- @engine: duckdb\n-- @cell probe\nSELECT 1 AS x;\n",
    )
    names = [c for c, _ in source_columns(project, "u.csv")]
    assert names == ["member_id", "plan_code"]


@pytest.mark.postgres
def test_table_columns_introspect_live_postgres(tmp_path) -> None:
    from quicksql.lsp.schema import table_columns

    dsn = "postgresql://quicksql:quicksql@localhost:5432/quicksql_claims"
    nb = tmp_path / "n.qsql"
    nb.write_text(
        "-- @engine: postgres\n"
        f"/*@ input: {{ postgres: {{ dsn: '{dsn}' }} }} */\n"
        "-- @cell probe\nSELECT 1 AS x;\n"
    )
    project = compile_file(nb)
    cell = project.cells["probe"]
    names = [c for c, _ in table_columns(project, cell, "claims.medical_claims")]
    assert "diagnosis_code" in names and "paid_amount" in names


# ---------- analysis.py: diagnostics + completions ----------


def test_diagnostics_flag_unknown_ref_at_the_cell_line(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = "-- @engine: duckdb\n-- @cell a\nSELECT * FROM {{ ref('nope') }};\n"
    diags = Analyzer().diagnostics(text, tmp_path)
    assert any("nope" in d.message for d in diags)
    assert any(d.line == 1 for d in diags)  # the `-- @cell a` line (0-indexed)


def test_diagnostics_flag_cross_context_source_on_non_duckdb(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = "-- @engine: sqlite\n-- @cell a\nSELECT * FROM {{ source('u.csv') }};\n"
    diags = Analyzer().diagnostics(text, tmp_path)
    assert any("duckdb" in d.message for d in diags)


def test_diagnostics_flag_a_template_escaping_the_sandbox_without_running_it(
    tmp_path,
) -> None:
    from quicksql.lsp.analysis import Analyzer

    marker = tmp_path / "escaped"
    text = (
        "-- @engine: duckdb\n-- @cell a\n"
        "SELECT '{{ cycler.__init__.__globals__.os.popen(\"touch MARKER\").read() }}' AS x;\n"
        .replace("MARKER", str(marker))
    )
    diags = Analyzer().diagnostics(text, tmp_path)
    flagged = [(d.line, d.severity) for d in diags if "template error" in d.message]
    assert flagged == [(1, "error")]  # the `-- @cell a` line (0-indexed)
    assert not marker.exists()


def test_diagnostics_flag_a_render_hook_that_raises_instead_of_crashing(
    tmp_path, add_plugin, registries
) -> None:
    from quicksql.lsp.analysis import Analyzer
    from quicksql.plugins.base import Plugin

    @add_plugin
    class Boom(Plugin):
        def after_render(self, rctx, sql):
            raise KeyError("x")

    text = "-- @engine: duckdb\n-- @cell a\nSELECT 1 AS x;\n"
    diags = Analyzer(registries=registries).diagnostics(text, tmp_path)
    assert [(d.line, d.message) for d in diags] == [
        (1, "template error: plugin 'boom' after_render: KeyError: 'x'")
    ]


def test_diagnostics_surface_sqlglot_syntax_errors(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = "-- @engine: duckdb\n-- @cell a\nSELECT * FROM WHERE 1;\n"
    diags = Analyzer().diagnostics(text, tmp_path)
    assert any(d.source == "sqlglot" for d in diags)


def test_diagnostics_check_a_third_party_engine_in_its_declared_dialect(
    tmp_path, tsql_engine, registries
) -> None:
    from quicksql.lsp.analysis import Analyzer

    analyzer = Analyzer(registries=registries)
    # TOP is T-SQL's own: generic SQL rejects it
    valid = "-- @engine: mssql\n-- @cell a\nSELECT TOP 1 x FROM dbo.t;\n"
    assert analyzer.diagnostics(valid, tmp_path) == []
    invalid = "-- @engine: mssql\n-- @cell a\nSELECT * FROM WHERE 1;\n"
    assert [(d.line, d.source) for d in analyzer.diagnostics(invalid, tmp_path)] == [(2, "sqlglot")]


def test_table_columns_come_from_a_third_party_engines_catalog(
    tmp_path, tsql_engine, registries
) -> None:
    from quicksql.bootstrap import compile_text
    from quicksql.lsp.schema import table_columns

    project = compile_text(
        "-- @engine: mssql\n-- @cell a\nSELECT 1 AS x;\n", root=tmp_path, registries=registries
    )
    cell = project.cells["a"]
    assert table_columns(project, cell, "dbo.t") == [("x", "INT")]
    assert table_columns(project, cell, "t") == [("x", "INT")]  # the engine's default schema
    assert table_columns(project, cell, "dbo.nope") == []


def test_completion_offers_a_third_party_tables_columns(tmp_path, tsql_engine, registries) -> None:
    from quicksql.lsp.analysis import Analyzer

    line = "SELECT TOP 1 e. FROM dbo.t AS e"
    text = f"-- @engine: mssql\n-- @cell a\n{line}\n"
    comps = Analyzer(registries=registries).completions(text, tmp_path, 2, len("SELECT TOP 1 e."))
    assert [(c.label, c.detail) for c in comps if c.kind == "field"] == [("x", "INT")]


def test_diagnostics_accept_var_in_operand_position(tmp_path) -> None:
    """A var used as a comparison operand must not produce a syntax error;
    the cell's merged vars feed the mask so the parsed SQL carries the value."""
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @vars: {startdate: '2026-01-01'}\n"
        "-- @cell a\n"
        "SELECT * FROM orders WHERE order_date > {{ var('startdate') }};\n"
    )
    assert Analyzer().diagnostics(text, tmp_path) == []


def test_diagnostics_warn_on_repeated_directive_keys(tmp_path) -> None:
    """A repeated `input:` silently replaces the earlier one (the demo-file
    footgun) — the LSP must flag the later occurrence with a warning."""
    from quicksql.lsp.analysis import Analyzer

    text = (
        "/*@ input: { bigquery: { project: p } } */\n"
        "/*@ input: { postgres: { dsn: 'postgresql://u@h/db' } } */\n"
        "-- @engine: bigquery\n"
        "-- @cell a\nSELECT 1;\n"
    )
    diags = Analyzer().diagnostics(text, tmp_path)
    dups = [d for d in diags if "repeated" in d.message]
    assert len(dups) == 1
    assert dups[0].severity == "warning" and dups[0].source == "quicksql"
    assert dups[0].line == 1  # the *second* occurrence (0-indexed row)
    assert "input" in dups[0].message and "line 2" in dups[0].message


# ---------- analysis.py: googlesql-backed bigquery diagnostics ----------

# body line 1 = file line 3; `|> SET` is valid BigQuery that sqlglot rejects
_PIPE_TEXT = "-- @engine: bigquery\n-- @cell a\nFROM t\n|> SET x = 2\n"


def _fake_execute_query(tmp_path, stdout: str = "", exit_code: int = 0):
    """A stand-in `execute_query`: swallow stdin, print a canned parse result."""
    payload = tmp_path / "payload.txt"
    payload.write_text(stdout)
    script = tmp_path / "fake_execute_query"
    script.write_text(f"#!/bin/sh\ncat >/dev/null\ncat {payload}\nexit {exit_code}\n")
    script.chmod(0o755)
    return script


def test_googlesql_errors_map_to_cell_relative_positions(tmp_path, monkeypatch) -> None:
    from quicksql.lsp.analysis import Analyzer

    fake = _fake_execute_query(
        tmp_path, "ERROR: Syntax error: boom [at 3:5]\n|> SET x = 2\n    ^\n"
    )
    monkeypatch.setenv("QSQL_EXECUTE_QUERY", str(fake))
    diags = Analyzer().diagnostics(_PIPE_TEXT, tmp_path)
    # cell.source line 1 is the `-- @cell a` line itself, so input line 3 =
    # file line 4 = row 3 (0-indexed); col 5 -> character 4
    assert [(d.line, d.character, d.source) for d in diags] == [(3, 4, "googlesql")]
    assert "boom" in diags[0].message


def test_googlesql_clean_parse_accepts_pipe_operators_sqlglot_rejects(
    tmp_path, monkeypatch
) -> None:
    from quicksql.lsp.analysis import Analyzer

    fake = _fake_execute_query(tmp_path, "QueryStatement [0-19]\n")
    monkeypatch.setenv("QSQL_EXECUTE_QUERY", str(fake))
    assert Analyzer().diagnostics(_PIPE_TEXT, tmp_path) == []


def test_missing_binary_falls_back_to_sqlglot_and_downgrades_pipe_errors(
    tmp_path, monkeypatch
) -> None:
    from quicksql.lsp.analysis import Analyzer

    monkeypatch.delenv("QSQL_EXECUTE_QUERY", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))  # no execute_query discoverable
    diags = Analyzer().diagnostics(_PIPE_TEXT, tmp_path)
    assert [(d.severity, d.source) for d in diags] == [("warning", "sqlglot")]
    assert "pipe" in diags[0].message.lower()


def test_broken_binary_falls_back_to_sqlglot(tmp_path, monkeypatch) -> None:
    from quicksql.lsp.analysis import Analyzer

    fake = _fake_execute_query(tmp_path, "panic", exit_code=1)
    monkeypatch.setenv("QSQL_EXECUTE_QUERY", str(fake))
    diags = Analyzer().diagnostics(_PIPE_TEXT, tmp_path)
    assert [(d.severity, d.source) for d in diags] == [("warning", "sqlglot")]


@pytest.mark.skipif(
    shutil.which("execute_query") is None,
    reason="googlesql execute_query not on PATH",
)
def test_googlesql_real_binary_parses_pipe_syntax(tmp_path, monkeypatch) -> None:
    from quicksql.lsp.analysis import Analyzer

    monkeypatch.delenv("QSQL_EXECUTE_QUERY", raising=False)
    text = "-- @engine: bigquery\n-- @cell a\nFROM t\n|> SET x = 2\n|> DROP y\n"
    assert Analyzer().diagnostics(text, tmp_path) == []


def test_completion_alias_scopes_to_a_fake_bigquery_table(tmp_path, monkeypatch) -> None:
    """Live-table completion for bigquery cells, offline: the catalog chain
    only ever calls list_datasets/list_tables/get_table on the client."""
    from types import SimpleNamespace

    from quicksql.executors.bigquery_exec import BigQueryExecutor
    from quicksql.lsp.analysis import Analyzer

    def field(name, type_):
        return SimpleNamespace(
            name=name, field_type=type_, mode="", fields=(), description=""
        )

    class FakeClient:
        def list_tables(self, dataset_id):
            return [SimpleNamespace(table_id="events", table_type="TABLE")]

        def get_table(self, path):
            assert path == "analytics.events"
            return SimpleNamespace(schema=[field("event_id", "INT64"), field("name", "STRING")])

    monkeypatch.setattr(BigQueryExecutor, "make_client", lambda self, spec: FakeClient())
    text = (
        "-- @input: { bigquery: { project: p } }\n"
        "-- @cell c\nSELECT e. FROM analytics.events AS e\n"
    )
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT e."))
    fields = [c.label for c in comps if c.kind == "field"]
    assert "event_id" in fields and "name" in fields


def _relation_fake_client(monkeypatch, projects_seen):
    """FakeClient serving datasets p:{analytics,staging} / other-proj:{ds1},
    tables analytics:{events,users}; records which project each call used."""
    from types import SimpleNamespace

    from quicksql.executors.bigquery_exec import BigQueryExecutor

    datasets = {"p": ["analytics", "staging"], "other-proj": ["ds1"]}

    def make(self, spec):
        class FakeClient:
            def list_datasets(self):
                projects_seen.append(("datasets", spec.get("project")))
                return [
                    SimpleNamespace(dataset_id=d)
                    for d in datasets.get(spec.get("project"), [])
                ]

            def list_tables(self, ds):
                projects_seen.append(("tables", spec.get("project")))
                if spec.get("project") != "p" or ds != "analytics":
                    raise RuntimeError(f"unknown dataset {ds}")
                return [
                    SimpleNamespace(table_id="events", table_type="TABLE"),
                    SimpleNamespace(table_id="users", table_type="VIEW"),
                ]

        return FakeClient()

    monkeypatch.setattr(BigQueryExecutor, "make_client", make)


def test_completion_offers_datasets_after_from(tmp_path, monkeypatch) -> None:
    from quicksql.lsp.analysis import Analyzer

    seen: list = []
    _relation_fake_client(monkeypatch, seen)
    text = "-- @input: { bigquery: { project: p } }\n-- @cell c\nSELECT 1 FROM \n"
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT 1 FROM "))
    assert [c.label for c in comps if c.kind == "table"] == ["analytics", "staging"]


def test_completion_offers_tables_after_a_dataset_dot(tmp_path, monkeypatch) -> None:
    from quicksql.lsp.analysis import Analyzer

    seen: list = []
    _relation_fake_client(monkeypatch, seen)
    text = "-- @input: { bigquery: { project: p } }\n-- @cell c\nSELECT 1 FROM analytics.\n"
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT 1 FROM analytics."))
    got = [(c.label, c.detail) for c in comps if c.kind == "table"]
    assert got == [("events", "TABLE"), ("users", "VIEW")]


def test_completion_falls_back_to_a_projects_datasets(tmp_path, monkeypatch) -> None:
    """`FROM other-proj.` — not a dataset of the default project, so offer
    that *project's* datasets instead."""
    from quicksql.lsp.analysis import Analyzer

    seen: list = []
    _relation_fake_client(monkeypatch, seen)
    text = "-- @input: { bigquery: { project: p } }\n-- @cell c\nSELECT 1 FROM other-proj.\n"
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT 1 FROM other-proj."))
    assert [c.label for c in comps if c.kind == "table"] == ["ds1"]
    assert ("datasets", "other-proj") in seen


def test_completion_offers_cross_project_tables_inside_backticks(
    tmp_path, monkeypatch
) -> None:
    from quicksql.lsp.analysis import Analyzer

    seen: list = []
    _relation_fake_client(monkeypatch, seen)
    # `p.analytics.` spelled fully — three segments means project.dataset.
    text = (
        "-- @input: { bigquery: { project: whatever } }\n"
        "-- @cell c\nSELECT 1 FROM `p.analytics.\n"
    )
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT 1 FROM `p.analytics."))
    assert [c.label for c in comps if c.kind == "table"] == ["events", "users"]
    assert ("tables", "p") in seen


@pytest.fixture
def bq_analytics_events(bq_emulator):
    """analytics.events seeded in the emulator; the emulator is in-memory, so
    every test that reads it must seed it — never lean on another test having
    run first. goccy answers duplicate creates with a retryable 500 rather
    than the 409 exists_ok expects, so probe-then-create instead of
    create(exists_ok=True)."""
    from google.cloud import bigquery as bq
    from google.cloud.exceptions import NotFound

    from quicksql.executors.bigquery_exec import BigQueryExecutor

    client = BigQueryExecutor().make_client(
        {"project": "quicksql-test", "endpoint": bq_emulator}
    )
    try:
        client.get_dataset("analytics")
    except NotFound:
        client.create_dataset("analytics")
    try:
        client.get_table("quicksql-test.analytics.events")
    except NotFound:
        client.create_table(
            bq.Table(
                "quicksql-test.analytics.events",
                schema=[
                    bq.SchemaField("event_id", "INT64"),
                    bq.SchemaField("name", "STRING"),
                ],
            )
        )
    return bq_emulator


@pytest.mark.bigquery
def test_completion_offers_emulator_datasets_and_tables(tmp_path, bq_analytics_events) -> None:
    from quicksql.lsp.analysis import Analyzer

    endpoint = bq_analytics_events
    text = (
        f"-- @input: {{ bigquery: {{ project: quicksql-test, endpoint: {endpoint} }} }}\n"
        "-- @cell c\nSELECT 1 FROM analytics.\n"
    )
    a = Analyzer()
    comps = a.completions(text, tmp_path, 2, len("SELECT 1 FROM analytics."))
    assert "events" in [c.label for c in comps if c.kind == "table"]
    text2 = text.replace("FROM analytics.", "FROM ")
    comps = a.completions(text2, tmp_path, 2, len("SELECT 1 FROM "))
    assert "analytics" in [c.label for c in comps if c.kind == "table"]


def test_completion_uses_the_query_project_for_cross_project_tables(
    tmp_path, monkeypatch
) -> None:
    """`other-proj.analytics.events` must introspect other-proj — not silently
    resolve `analytics.events` against the cell's default project."""
    from types import SimpleNamespace

    from quicksql.executors.bigquery_exec import BigQueryExecutor
    from quicksql.lsp.analysis import Analyzer

    seen: list[tuple[str | None, str]] = []  # (client's project, get_table path)

    def make(self, spec):
        class FakeClient:
            def get_table(self, path):
                seen.append((spec.get("project"), path))
                if spec.get("project") != "other-proj":
                    raise RuntimeError(f"no such table in project {spec.get('project')}")
                return SimpleNamespace(
                    schema=[
                        SimpleNamespace(
                            name="event_id", field_type="INT64", mode="",
                            fields=(), description="",
                        )
                    ]
                )

        return FakeClient()

    monkeypatch.setattr(BigQueryExecutor, "make_client", make)
    text = (
        "-- @input: { bigquery: { project: p } }\n"
        "-- @cell c\nSELECT e. FROM `other-proj.analytics.events` AS e\n"
    )
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT e."))
    assert [c.label for c in comps if c.kind == "field"] == ["event_id"]
    assert seen == [("other-proj", "analytics.events")]


@pytest.mark.bigquery
def test_completion_alias_scopes_to_an_emulator_bigquery_table(
    tmp_path, bq_analytics_events
) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        f"-- @input: {{ bigquery: {{ project: quicksql-test, endpoint: {bq_analytics_events} }} }}\n"
        "-- @cell c\nSELECT e. FROM analytics.events AS e\n"
    )
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT e."))
    fields = [c.label for c in comps if c.kind == "field"]
    assert "event_id" in fields and "name" in fields


def test_completion_alias_scope_survives_unsupported_pipe_operators(tmp_path) -> None:
    """sqlglot can't parse `|> SET`, but _resolve_scope parses at
    ErrorLevel.IGNORE and the partial tree still carries the FROM tables —
    alias-scoped column completion must keep working in such cells."""
    import polars as pl

    from quicksql.lsp.analysis import Analyzer

    (tmp_path / "data").mkdir()
    pl.DataFrame({"x": [1], "y": [2]}).write_parquet(tmp_path / "data" / "up.parquet")
    text = (
        "-- @engine: bigquery\n"
        "-- @cell up\nSELECT 1 AS x, 2 AS y\n"
        "-- @cell down\nFROM {{ ref('up') }} AS o\n|> SET x = 9\n|> WHERE o.\n"
    )
    comps = Analyzer().completions(text, tmp_path, 6, len("|> WHERE o."))
    assert [c.label for c in comps if c.kind == "field"] == ["x", "y"]


def _land_users(tmp_path, text):
    from quicksql.runner import run_project

    nb = tmp_path / "n.qsql"
    nb.write_text(text)
    run_project(compile_file(nb), select=["users"])  # land users.parquet only


def test_completion_offers_ref_columns_and_alias_scopes_them(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS id, 'ada' AS name, true AS active;\n"
        "-- @cell down\nSELECT u.id FROM {{ ref('users') }} u\n"
    )
    _land_users(tmp_path, text)
    a = Analyzer()
    down_line = 5  # `SELECT u.id FROM ...` (0-indexed)

    # bare position after SELECT -> union of the cell's columns + keywords
    flat = {c.label for c in a.completions(text, tmp_path, down_line, len("SELECT "))}
    assert {"id", "name", "active"} <= flat
    assert "FROM" in flat  # keywords too

    # after `u.` -> scoped to users' columns
    scoped = a.completions(text, tmp_path, down_line, len("SELECT u."))
    labels = {c.label for c in scoped if c.kind == "field"}
    assert {"id", "name", "active"} <= labels


def test_completion_in_ref_call_offers_cell_names(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS id;\n"
        "-- @cell down\nSELECT * FROM {{ ref('users') }}\n"
    )
    _land_users(tmp_path, text)
    down_line = 5
    prefix = "SELECT * FROM {{ ref('"  # cursor right after ref('
    items = Analyzer().completions(text, tmp_path, down_line, len(prefix))
    names = {c.label for c in items if c.kind == "reference"}
    assert "users" in names


def _land_cells(tmp_path, text, cells):
    from quicksql.runner import run_project

    nb = tmp_path / "n.qsql"
    nb.write_text(text)
    run_project(compile_file(nb), select=cells)


def test_completion_alias_collision_resolves_to_the_cursor_statement(tmp_path) -> None:
    """Same alias in two statements: completing it uses the cursor's statement,
    not whichever assignment won the merged whole-cell parse."""
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell a\nSELECT 1 AS aid, 2 AS aname;\n"
        "-- @cell b\nSELECT 3 AS bid, 4 AS bname;\n"
        "-- @cell down\n"
        "SELECT t. FROM {{ ref('a') }} t;\n"     # line 7: alias t -> a
        "SELECT bid FROM {{ ref('b') }} t\n"      # line 8: alias t -> b (same name)
    )
    _land_cells(tmp_path, text, ["a", "b"])
    fields = {
        c.label
        for c in Analyzer().completions(text, tmp_path, 7, len("SELECT t."))
        if c.kind == "field"
    }
    assert {"aid", "aname"} <= fields  # statement 1's ref('a')
    assert "bid" not in fields and "bname" not in fields  # not statement 2's colliding t


def test_completion_does_not_pollute_with_other_statements_columns(tmp_path) -> None:
    """Bare column completion in one statement must not offer columns from a
    sibling statement's FROM clause (the merged parse used to union them)."""
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell a\nSELECT 1 AS aid, 2 AS aname;\n"
        "-- @cell b\nSELECT 3 AS bid, 4 AS bname;\n"
        "-- @cell down\n"
        "SELECT aid FROM {{ ref('a') }} x;\n"    # line 7
        "SELECT  FROM {{ ref('b') }} y\n"         # line 8: bare cursor after 'SELECT '
    )
    _land_cells(tmp_path, text, ["a", "b"])
    fields = {
        c.label
        for c in Analyzer().completions(text, tmp_path, 8, len("SELECT "))
        if c.kind == "field"
    }
    assert {"bid", "bname"} <= fields  # statement 2's ref('b')
    assert "aid" not in fields and "aname" not in fields  # not statement 1's


def test_completion_just_after_trailing_semicolon_reuses_previous_scope(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell a\nSELECT 1 AS aid;\n"
        "-- @cell down\nSELECT aid FROM {{ ref('a') }} a;\n"  # line 5
    )
    _land_cells(tmp_path, text, ["a"])
    stmt = "SELECT aid FROM {{ ref('a') }} a;"
    labels = {c.label for c in Analyzer().completions(text, tmp_path, 5, len(stmt))}
    assert "aid" in labels  # cursor just past ';' reuses the previous statement's scope


def test_completion_survives_multi_statement_edges(tmp_path) -> None:
    """A trailing blank line and a {% %} control block across statements must
    not crash completion (bounds guard + degraded-parse tolerance)."""
    from quicksql.lsp.analysis import Analyzer

    a = Analyzer()
    blank_tail = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell x\nSELECT 1 AS n;\nSELECT 2 AS m;\n\n"  # trailing blank line at EOF
    )
    assert isinstance(a.completions(blank_tail, tmp_path, 5, 0), list)

    control = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell x\nSELECT 1 AS n;\n{% if true %}SELECT m {% endif %}\n"
    )
    assert isinstance(a.completions(control, tmp_path, 4, len("{% if true %}SELECT m")), list)


@pytest.mark.postgres
def test_completion_alias_scopes_to_a_live_postgres_table(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    dsn = "postgresql://quicksql:quicksql@localhost:5432/quicksql_claims"
    text = (
        "-- @engine: postgres\n"
        f"/*@ input: {{ postgres: {{ dsn: '{dsn}' }} }} */\n"
        "-- @cell probe\nSELECT m.paid_amount FROM claims.medical_claims m\n"
    )
    (tmp_path / "n.qsql").write_text(text)
    probe_line = 3  # the SELECT line (0-indexed)
    scoped = Analyzer().completions(text, tmp_path, probe_line, len("SELECT m."))
    labels = {c.label for c in scoped if c.kind == "field"}
    assert "paid_amount" in labels and "diagnosis_code" in labels


def test_definition_jumps_from_ref_to_the_defining_cell(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS id;\n"
        "-- @cell down\nSELECT * FROM {{ ref('users') }}\n"
    )
    _land_users(tmp_path, text)
    # cursor inside ref('users') on the `down` cell (line 5)
    target = Analyzer().definition(text, tmp_path, 5, len("SELECT * FROM {{ ref('us"))
    assert target == (2, 0)  # the `-- @cell users` line, 0-indexed


def test_document_symbols_outline_every_cell(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @cell a\nSELECT 1;\n-- @cell b\nSELECT 2;\n"
    )
    syms = Analyzer().document_symbols(text, tmp_path)
    names = [s[0] for s in syms]
    assert names == ["a", "b"]
    assert all(s[3] == "duckdb" for s in syms)


# ---------- server.py: pygls wiring ----------


def test_server_registers_the_lsp_features_and_maps_diagnostics() -> None:
    pytest.importorskip("pygls", reason="install the 'lsp' extra")
    from lsprotocol import types

    from quicksql.lsp.analysis import Diagnostic
    from quicksql.lsp.server import _to_lsp_diagnostic, create_server

    server = create_server()
    registered = set(server.protocol.fm.features)
    assert {
        types.TEXT_DOCUMENT_DID_OPEN,
        types.TEXT_DOCUMENT_DID_CHANGE,
        types.TEXT_DOCUMENT_COMPLETION,
        types.TEXT_DOCUMENT_DEFINITION,
        types.TEXT_DOCUMENT_DOCUMENT_SYMBOL,
    } <= registered

    lsp_diag = _to_lsp_diagnostic(Diagnostic(2, 0, 2, 9, "boom", "error", "sqlglot"))
    assert lsp_diag.range.start.line == 2
    assert lsp_diag.severity == types.DiagnosticSeverity.Error
    assert lsp_diag.source == "sqlglot" and lsp_diag.message == "boom"


def test_opening_a_document_with_a_python_error_in_a_template_publishes_a_diagnostic(
    tmp_path,
) -> None:
    pytest.importorskip("pygls", reason="install the 'lsp' extra")
    from types import SimpleNamespace

    from lsprotocol import types

    from quicksql.lsp.server import create_server

    nb = tmp_path / "n.qsql"
    text = "-- @engine: duckdb\n-- @cell a\nSELECT {{ 1/0 }} AS x;\n"
    published: list = []
    ls = SimpleNamespace(  # just what the handler touches: the open document, publishing
        workspace=SimpleNamespace(
            get_text_document=lambda uri: SimpleNamespace(source=text, path=str(nb))
        ),
        text_document_publish_diagnostics=published.append,
    )
    # pygls binds each handler to its server with functools.partial
    did_open = create_server().protocol.fm.features[types.TEXT_DOCUMENT_DID_OPEN].func
    did_open(ls, types.DidOpenTextDocumentParams(
        types.TextDocumentItem(uri=nb.as_uri(), language_id="qsql", version=1, text=text)
    ))
    [params] = published
    assert [(d.range.start.line, d.message) for d in params.diagnostics] == [
        (1, "template error: ZeroDivisionError: division by zero")  # the `-- @cell a` line
    ]


def test_completion_scopes_each_join_alias_to_its_own_ref(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS user_id, 'ada' AS name;\n"
        "-- @cell events\nSELECT 1 AS user_id, 'click' AS event;\n"
        "-- @cell down\n"
        "SELECT e. FROM {{ ref('users') }} u JOIN {{ ref('events') }} e USING (user_id)\n"
    )
    from quicksql.runner import run_project

    nb = tmp_path / "n.qsql"
    nb.write_text(text)
    run_project(compile_file(nb), select=["users", "events"])
    scoped = Analyzer().completions(text, tmp_path, 7, len("SELECT e."))
    labels = {c.label for c in scoped if c.kind == "field"}
    assert labels == {"user_id", "event"}  # events only — not users' columns


def test_completion_derives_cte_alias_columns_from_its_projection(tmp_path) -> None:
    from quicksql.lsp.analysis import Analyzer

    text = (
        "-- @engine: duckdb\n-- @output: { type: parquet, dir: data/ }\n"
        "-- @cell users\nSELECT 1 AS user_id, 'ada' AS name;\n"
        "-- @cell cte_cell\n"
        "WITH w AS (SELECT user_id, name AS member_name FROM {{ ref('users') }})\n"
        "SELECT w. FROM w\n"
    )
    from quicksql.runner import run_project

    nb = tmp_path / "n.qsql"
    nb.write_text(text)
    run_project(compile_file(nb), select=["users"])
    scoped = Analyzer().completions(text, tmp_path, 6, len("SELECT w."))
    labels = {c.label for c in scoped if c.kind == "field"}
    # the CTE's own projected columns, output alias included
    assert labels == {"user_id", "member_name"}


def test_completion_offers_dialect_function_vocabulary(tmp_path) -> None:
    """The completion word list is dialect-aware: a bigquery cell offers
    BigQuery function names as kind "function" — both sqlglot-known ones
    (RANGE_BUCKET) and the range-family TVFs BigQuery registers on top of
    ZetaSQL that sqlglot's tables don't carry (RANGE_SESSIONIZE) — while a
    duckdb cell gets duckdb's vocabulary without bigquery-only names."""
    from quicksql.lsp.analysis import Analyzer

    text = "-- @engine: bigquery\n-- @cell c\nSELECT \n"
    comps = Analyzer().completions(text, tmp_path, 2, len("SELECT "))
    funcs = {c.label for c in comps if c.kind == "function"}
    assert "RANGE_SESSIONIZE" in funcs
    assert "RANGE_BUCKET" in funcs
    assert "SELECT" in {c.label for c in comps if c.kind == "keyword"}

    text2 = "-- @engine: duckdb\n-- @cell c\nSELECT \n"
    comps2 = Analyzer().completions(text2, tmp_path, 2, len("SELECT "))
    funcs2 = {c.label for c in comps2 if c.kind == "function"}
    assert funcs2  # duckdb has its own function vocabulary
    assert "RANGE_SESSIONIZE" not in funcs2


def test_completion_vocab_excludes_internal_and_cross_dialect_names() -> None:
    """The function vocabulary must not offer names the engine doesn't have.
    sqlglot's parser registry is a pool shared across dialects, so its base
    carries BigQuery-only names (SAFE_DIVIDE, GAP_FILL, GENERATE_UUID) and even
    sqlglot's internal abstract-wrapper classes (SAFE_FUNC) — none of which are
    valid in a duckdb cell. duckdb's vocabulary comes from its own embedded
    catalog instead, so those are absent while real duckdb functions the shared
    pool would otherwise mask (ARRAY_APPEND, ARGMAX) survive. The internal
    wrappers are dropped from every dialect, but genuinely-BigQuery functions
    stay for a bigquery cell."""
    from quicksql.lsp import vocab

    duck = set(vocab.functions("duckdb"))
    bq = set(vocab.functions("bigquery"))

    # sqlglot-internal abstract wrappers are not SQL in any dialect
    assert "SAFE_FUNC" not in duck
    assert "SAFE_FUNC" not in bq

    # BigQuery-exclusive names must not leak into a duckdb cell...
    for name in ("SAFE_DIVIDE", "GAP_FILL", "GENERATE_UUID"):
        assert name not in duck, f"{name} leaked into duckdb vocab"
    # ...while remaining for bigquery, where they are real functions
    assert "SAFE_DIVIDE" in bq
    assert "GENERATE_UUID" in bq

    # real duckdb functions survive — guard against over-filtering
    assert {"ARRAY_APPEND", "ARGMAX", "COUNT"} <= duck


def test_server_sorts_columns_above_keywords() -> None:
    """Clients sort by sortText when present; fields must rank above tables,
    refs, and keywords regardless of label alphabetics, keeping server order
    within each kind."""
    pytest.importorskip("pygls", reason="install the 'lsp' extra")
    from quicksql.lsp.analysis import Completion
    from quicksql.lsp.server import _to_lsp_completion

    items = [
        _to_lsp_completion(c, i)
        for i, c in enumerate(
            [
                Completion("zulu_col", "field", "INT64"),
                Completion("alpha_col", "field", "STRING"),
                Completion("a_cell", "reference"),
                Completion("AND", "keyword"),
            ]
        )
    ]
    ordered = sorted(items, key=lambda it: it.sort_text)
    assert [it.label for it in ordered] == ["zulu_col", "alpha_col", "a_cell", "AND"]
