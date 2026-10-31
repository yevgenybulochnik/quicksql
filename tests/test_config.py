from collections import Counter

import pytest
from pydantic import BaseModel, field_validator

from quicksql import config
from quicksql.compiler import compile_text
from quicksql.config import (
    build_models,
    resolve_cell,
    resolve_engine,
    resolve_global,
    resolve_sink_type,
)
from quicksql.errors import ConfigError
from quicksql.models import Scope
from quicksql.plugins.base import Plugin, qfield
from quicksql.registry import PLUGINS, plugin


def test_build_models_fields_and_defaults() -> None:
    GlobalConfig, CellConfig = build_models()
    g = GlobalConfig()
    assert g.engine is None
    assert g.output == {"type": "parquet", "dir": "data/"}
    assert g.autorun is True
    assert g.vars == {}
    c = CellConfig()
    assert c.depends_on == []
    assert "depends_on" not in GlobalConfig.model_fields


def test_mutable_defaults_are_isolated() -> None:
    _, CellConfig = build_models()
    a, b = CellConfig(), CellConfig()
    a.vars["x"] = 1
    assert b.vars == {}


def test_unknown_directive_raises() -> None:
    with pytest.raises(ConfigError, match="unknown directive: @bogus"):
        resolve_global({"bogus": 1})


def test_scope_violation_raises() -> None:
    with pytest.raises(ConfigError, match="@depends_on is not allowed in global scope"):
        resolve_global({"depends_on": ["a"]})


def test_merge_precedence_global_cell_override() -> None:
    cfg = resolve_cell(
        {"engine": "duckdb", "vars": {"a": 1, "b": 2}},
        {"engine": "sqlite", "vars": {"b": 3}},
        overrides={"vars": {"c": 4}},
    )
    assert cfg.engine == "sqlite"                     # OVERRIDE: cell beats global
    assert cfg.vars == {"a": 1, "b": 3, "c": 4}       # DEEP merge across layers


def test_deep_merge_output_keeps_unset_keys() -> None:
    cfg = resolve_cell({"output": {"dir": "out/"}}, {}, overrides={"output": {"type": "duckdb"}})
    assert cfg.output["dir"] == "out/"
    assert cfg.output["type"] == "duckdb"


def test_extend_merge_concatenates_lists() -> None:
    cfg = resolve_cell({"extensions": ["excel"]}, {"extensions": ["postgres"]})
    assert cfg.extensions == ["excel", "postgres"]


def test_validator_rejects_unknown_sink_type() -> None:
    with pytest.raises(ConfigError, match="output.type"):
        resolve_cell({}, {"output": {"type": "carrier_pigeon"}})


def test_validator_rejects_unknown_engine() -> None:
    with pytest.raises(ConfigError, match="engine"):
        resolve_cell({}, {"engine": "abacus"})


def test_validator_rejects_non_string_extensions() -> None:
    with pytest.raises(ConfigError):
        resolve_cell({}, {"extensions": [1, 2]})


def test_resolve_engine_explicit_inferred_default() -> None:
    assert resolve_engine(resolve_cell({}, {"engine": "sqlite"})) == "sqlite"
    assert resolve_engine(resolve_cell({}, {"input": {"sqlite": "db.db"}})) == "sqlite"
    assert resolve_engine(resolve_cell({}, {})) == "duckdb"


def test_resolve_engine_ambiguous_input_raises() -> None:
    cfg = resolve_cell({}, {"input": {"sqlite": "a", "duckdb": "b"}})
    with pytest.raises(ConfigError, match="infer"):
        resolve_engine(cfg)


def test_resolve_sink_type_default_and_override() -> None:
    assert resolve_sink_type(resolve_cell({}, {})) == "parquet"
    assert resolve_sink_type(resolve_cell({}, {"output": {"type": "duckdb"}})) == "duckdb"


def test_schema_alias_field() -> None:
    cfg = resolve_cell({}, {"schema": "analytics"})
    assert cfg.schema_ == "analytics"


def test_third_party_config_plugin_extends_models() -> None:
    @plugin
    class RowLimit(Plugin):
        scope = Scope.CELL

        class Config(BaseModel):
            row_limit: int | None = qfield(None)

            @field_validator("row_limit")
            @classmethod
            def positive(cls, v: int | None) -> int | None:
                if v is not None and v <= 0:
                    raise ValueError("row_limit must be positive")
                return v

    cfg = resolve_cell({}, {"row_limit": 10})
    assert cfg.row_limit == 10
    with pytest.raises(ConfigError, match="positive"):
        resolve_cell({}, {"row_limit": -1})
    with pytest.raises(ConfigError, match="not allowed in global scope"):
        resolve_global({"row_limit": 5})


def test_duplicate_validator_names_across_plugins_are_rejected() -> None:
    # pydantic composes plugin Configs by inheritance, so a validator whose
    # attribute name repeats an existing one (e.g. Sources' _shape) silently
    # shadows it in the MRO — the guard must fail loudly instead
    @plugin
    class Shadowing(Plugin):
        class Config(BaseModel):
            widgets: list[str] = qfield([])

            @field_validator("widgets")
            @classmethod
            def _shape(cls, v: list[str]) -> list[str]:  # clashes with Sources
                return v

    for _ in range(2):  # a failed compose must not be cached
        with pytest.raises(ConfigError, match="_shape"):
            build_models()


def test_override_keys_scoped_to_target_model() -> None:
    # global-only override keys are dropped when resolving cells, not errors
    @plugin
    class Auditish(Plugin):
        scope = Scope.GLOBAL

        class Config(BaseModel):
            audit_dir: str | None = qfield(None)

    g = resolve_global({}, overrides={"audit_dir": "build/"})
    assert g.audit_dir == "build/"
    c = resolve_cell({}, {}, overrides={"audit_dir": "build/"})
    assert not hasattr(c, "audit_dir")


def test_unknown_override_key_raises() -> None:
    with pytest.raises(ConfigError, match="unknown directive: @zap"):
        resolve_cell({}, {}, overrides={"zap": 1})


NOTEBOOK = """\
-- @vars: { n: 2 }

-- @cell a
SELECT 1 AS x;

-- @cell b
SELECT * FROM {{ ref('a') }};

-- @cell c
-- @depends_on: [b]
SELECT {{ var('n') }} AS y;
"""


@pytest.fixture
def composed(monkeypatch) -> list[str]:
    """Names of the config models composed (``create_model`` calls) from here on."""
    made: list[str] = []
    real = config.create_model

    def spy(name, **kwargs):
        made.append(name)
        return real(name, **kwargs)

    monkeypatch.setattr(config, "create_model", spy)
    return made


def _register_row_limit(where: Scope = Scope.CELL, *, positive: bool = False) -> None:
    """Register a scratch plugin owning @row_limit; each call defines fresh classes."""

    @plugin
    class RowLimit(Plugin):
        scope = where

        class Config(BaseModel):
            row_limit: int | None = qfield(None)

            @field_validator("row_limit")
            @classmethod
            def _row_limit_sign(cls, v: int | None) -> int | None:
                if positive and v is not None and v <= 0:
                    raise ValueError("row_limit must be positive")
                return v


def test_compile_composes_each_config_model_once(tmp_path, composed) -> None:
    _register_row_limit(Scope.BOTH)  # fresh bases for both models: no compile has seen them
    compile_text(NOTEBOOK, root=tmp_path)
    assert Counter(composed) == {"GlobalConfig": 1, "CellConfig": 1}


def test_recompile_with_same_registry_composes_nothing(tmp_path, composed) -> None:
    compile_text(NOTEBOOK, root=tmp_path)
    composed.clear()
    compile_text(NOTEBOOK, root=tmp_path)
    assert composed == []


def test_registering_a_plugin_between_compiles_composes_anew(tmp_path, composed) -> None:
    compile_text(NOTEBOOK, root=tmp_path)
    composed.clear()
    _register_row_limit()  # cell scope: only CellConfig's bases change
    project = compile_text(
        NOTEBOOK.replace("-- @cell a\n", "-- @cell a\n-- @row_limit: 10\n"), root=tmp_path
    )
    assert composed == ["CellConfig"]
    assert project.cells["a"].config.row_limit == 10


def test_unregistering_a_plugin_between_compiles_rejects_its_directive(tmp_path) -> None:
    snap = PLUGINS.snapshot()  # what the autouse fixture restores between tests
    _register_row_limit()
    text = "-- @cell a\n-- @row_limit: 10\nSELECT 1;\n"
    assert compile_text(text, root=tmp_path).cells["a"].config.row_limit == 10
    PLUGINS.restore(snap)
    with pytest.raises(ConfigError, match="unknown directive: @row_limit"):
        compile_text(text, root=tmp_path)
    cfg = compile_text("-- @cell a\nSELECT 1;\n", root=tmp_path).cells["a"].config
    assert "row_limit" not in type(cfg).model_fields


def test_reregistering_a_plugin_name_composes_its_new_config(tmp_path) -> None:
    # same plugin name and field key, new Config class: the composed model must
    # follow the class, not the name
    snap = PLUGINS.snapshot()
    _register_row_limit()
    text = "-- @cell a\n-- @row_limit: -1\nSELECT 1;\n"
    assert compile_text(text, root=tmp_path).cells["a"].config.row_limit == -1
    PLUGINS.restore(snap)
    _register_row_limit(positive=True)
    with pytest.raises(ConfigError, match="positive"):
        compile_text(text, root=tmp_path)


# ---------- input_spec: an engine's input as a dict ----------


@pytest.mark.parametrize(
    ("engine", "raw", "spec"),
    [
        ("duckdb", "wh.db", {"path": "wh.db"}),
        ("sqlite", "x.sqlite", {"path": "x.sqlite"}),
        ("postgres", "$PG_DSN", {"dsn": "$PG_DSN"}),
        ("bigquery", "my-proj", {"project": "my-proj"}),
        ("sqlite", {"path": "x.sqlite"}, {"path": "x.sqlite"}),
        ("bigquery", {"project": "p", "endpoint": "e"}, {"project": "p", "endpoint": "e"}),
    ],
    ids=["duckdb-str", "sqlite-str", "postgres-str", "bigquery-str", "sqlite-dict", "bigquery-dict"],
)
def test_input_spec_expands_the_engines_string_shorthand(engine, raw, spec) -> None:
    from quicksql.config import input_spec

    assert input_spec(resolve_cell({}, {"input": {engine: raw}}), engine) == spec


def test_input_spec_of_an_absent_or_empty_input_is_empty() -> None:
    from quicksql.config import input_spec

    assert input_spec(resolve_cell({}, {}), "duckdb") == {}
    assert input_spec(resolve_cell({}, {"input": {"duckdb": None}}), "duckdb") == {}
    assert input_spec(resolve_cell({}, {"input": {"sqlite": "x.db"}}), "duckdb") == {}
