"""Decision/aggregation hooks: the remaining builtins own their behavior."""

import pytest

from quicksql.bootstrap import compile_file, compile_text
from quicksql.errors import ConfigError
from quicksql.plugins.base import Plugin
from quicksql.registry import plugin
from quicksql.runner import RunSession
from quicksql.watcher import plan_rerun


def test_first_result_returns_highest_priority_answer(registries) -> None:
    @plugin
    class Loud(Plugin):
        priority = -50

        def resolve_engine(self, config):
            return "sqlite"

    @plugin
    class Quiet(Plugin):
        priority = 50

        def resolve_engine(self, config):
            return "bigquery"

    from quicksql.config import resolve_cell, resolve_engine

    registries.add(Loud, Quiet)
    cfg = resolve_cell({}, {}, registries=registries)
    assert resolve_engine(cfg, registries.plugins) == "sqlite"


def test_engine_resolution_owners_split(tmp_path) -> None:
    # Engine answers for explicit @engine; Input infers from its own key
    project = compile_text(
        "-- @cell explicit\n-- @engine: sqlite\nSELECT 1;\n"
        "-- @cell inferred\n/*@ input: { sqlite: db.db } */\nSELECT 2;\n"
        "-- @cell fallback\nSELECT 3;",
        root=tmp_path,
    )
    assert project.cells["explicit"].engine == "sqlite"
    assert project.cells["inferred"].engine == "sqlite"
    assert project.cells["fallback"].engine == "duckdb"


def test_dropping_depends_on_plugin_removes_its_edges(tmp_path, registries) -> None:
    text = "-- @cell a\nSELECT 1;\n-- @cell b\n-- @depends_on: [a]\nSELECT 2;"
    assert compile_text(text, root=tmp_path, registries=registries).cells["b"].depends_on == ["a"]
    registries.plugins.remove("depends_on")
    with pytest.raises(ConfigError, match="unknown directive: @depends_on"):
        compile_text(text, root=tmp_path, registries=registries)  # directive gone with the plugin
    bare = compile_text(
        "-- @cell a\nSELECT 1;\n-- @cell b\nSELECT 2;", root=tmp_path, registries=registries
    )
    assert bare.cells["b"].depends_on == []


def test_dropping_autorun_plugin_defaults_to_rerun_everything(tmp_path, registries) -> None:
    f = tmp_path / "base.sql"
    f.write_text("-- @cell a\n-- @autorun: false\nSELECT 1;")
    project = compile_file(f, registries=registries)
    assert plan_rerun(project, RunSession()) == []  # autorun:false filters it out
    registries.plugins.remove("autorun")
    f.write_text("-- @cell a\nSELECT 1;")
    project = compile_file(f, registries=registries)
    assert plan_rerun(project, RunSession()) == ["a"]  # no plugin -> default: rerun


def test_schema_directive_reaches_sink_config(tmp_path) -> None:
    project = compile_text(
        "-- @cell t\n-- @schema: analytics\n-- @output: { type: duckdb, path: wh.db }\n"
        "SELECT 1 AS id;",
        root=tmp_path,
    )
    result = project.run()[0]
    assert result.ok, result.error
    assert result.target.endswith("::analytics.t")


def test_extensions_plugin_loads_via_before_execute(tmp_path, monkeypatch) -> None:
    import quicksql.conduit as conduit

    loads: list[str] = []
    original = conduit.load_extension
    monkeypatch.setattr(
        conduit, "load_extension",
        lambda conn, ext: (loads.append(ext), original(conn, ext))[1],
    )
    project = compile_text("-- @cell a\n-- @extensions: [json]\nSELECT 1;", root=tmp_path)
    assert project.run()[0].ok
    assert "json" in loads
    assert "extensions" in [p.name for p in project.registries.plugins.chain()]  # it joined the chain
