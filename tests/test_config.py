from collections import Counter

import pytest
from pydantic import BaseModel, field_validator

from quicksql import config
from quicksql.bootstrap import builtin_registries, compile_text
from quicksql.errors import ConfigError
from quicksql.models import Scope
from quicksql.plugins.base import Plugin, qfield
from quicksql.registry import plugin


# the config functions with the builtins unless a test passes its own registries
def build_models(registries=None):
    return config.build_models((registries or builtin_registries()).plugins)


def resolve_global(raw, overrides=None, *, registries=None):
    return config.resolve_global(raw, overrides, registries=registries or builtin_registries())


def resolve_cell(global_raw, cell_raw, overrides=None, *, registries=None):
    return config.resolve_cell(
        global_raw, cell_raw, overrides, registries=registries or builtin_registries()
    )


def resolve_engine(cfg):
    return config.resolve_engine(cfg, builtin_registries().plugins)


def resolve_sink_type(cfg):
    return config.resolve_sink_type(cfg, builtin_registries().plugins)


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


def test_third_party_config_plugin_extends_models(registries) -> None:
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

    registries.add(RowLimit)
    cfg = resolve_cell({}, {"row_limit": 10}, registries=registries)
    assert cfg.row_limit == 10
    with pytest.raises(ConfigError, match="positive"):
        resolve_cell({}, {"row_limit": -1}, registries=registries)
    with pytest.raises(ConfigError, match="not allowed in global scope"):
        resolve_global({"row_limit": 5}, registries=registries)


def test_duplicate_validator_names_across_plugins_are_rejected(registries) -> None:
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

    registries.add(Shadowing)
    for _ in range(2):  # a failed compose must not be cached
        with pytest.raises(ConfigError, match="_shape"):
            build_models(registries)


def test_override_keys_scoped_to_target_model(registries) -> None:
    # global-only override keys are dropped when resolving cells, not errors
    @plugin
    class Auditish(Plugin):
        scope = Scope.GLOBAL

        class Config(BaseModel):
            audit_dir: str | None = qfield(None)

    registries.add(Auditish)
    g = resolve_global({}, overrides={"audit_dir": "build/"}, registries=registries)
    assert g.audit_dir == "build/"
    c = resolve_cell({}, {}, overrides={"audit_dir": "build/"}, registries=registries)
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


def _register_row_limit(registries, where: Scope = Scope.CELL, *, positive: bool = False) -> None:
    """Add a scratch plugin owning @row_limit; each call defines fresh classes."""

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

    registries.add(RowLimit)


def test_compile_composes_each_config_model_once(tmp_path, composed, registries) -> None:
    # fresh bases for both models: no compile has seen them
    _register_row_limit(registries, Scope.BOTH)
    compile_text(NOTEBOOK, root=tmp_path, registries=registries)
    assert Counter(composed) == {"GlobalConfig": 1, "CellConfig": 1}


def test_recompile_with_same_registry_composes_nothing(tmp_path, composed) -> None:
    compile_text(NOTEBOOK, root=tmp_path)
    composed.clear()
    compile_text(NOTEBOOK, root=tmp_path)
    assert composed == []


def test_registering_a_plugin_between_compiles_composes_anew(
    tmp_path, composed, registries
) -> None:
    compile_text(NOTEBOOK, root=tmp_path, registries=registries)
    composed.clear()
    _register_row_limit(registries)  # cell scope: only CellConfig's bases change
    project = compile_text(
        NOTEBOOK.replace("-- @cell a\n", "-- @cell a\n-- @row_limit: 10\n"),
        root=tmp_path,
        registries=registries,
    )
    assert composed == ["CellConfig"]
    assert project.cells["a"].config.row_limit == 10


def test_unregistering_a_plugin_between_compiles_rejects_its_directive(
    tmp_path, registries
) -> None:
    _register_row_limit(registries)
    text = "-- @cell a\n-- @row_limit: 10\nSELECT 1;\n"
    assert compile_text(text, root=tmp_path, registries=registries).cells["a"].config.row_limit == 10
    registries.plugins.remove("row_limit")
    with pytest.raises(ConfigError, match="unknown directive: @row_limit"):
        compile_text(text, root=tmp_path, registries=registries)
    cfg = compile_text("-- @cell a\nSELECT 1;\n", root=tmp_path, registries=registries)
    cfg = cfg.cells["a"].config
    assert "row_limit" not in type(cfg).model_fields


def test_reregistering_a_plugin_name_composes_its_new_config(tmp_path, registries) -> None:
    # same plugin name and field key, new Config class: the composed model must
    # follow the class, not the name
    _register_row_limit(registries)
    text = "-- @cell a\n-- @row_limit: -1\nSELECT 1;\n"
    assert compile_text(text, root=tmp_path, registries=registries).cells["a"].config.row_limit == -1
    registries.plugins.remove("row_limit")
    _register_row_limit(registries, positive=True)
    with pytest.raises(ConfigError, match="positive"):
        compile_text(text, root=tmp_path, registries=registries)


# ---------- an executor's input_spec: its engine's input as a dict ----------


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
    executor = builtin_registries().executors.get(engine)
    assert executor.input_spec(resolve_cell({}, {"input": {engine: raw}})) == spec


def test_input_spec_of_an_absent_or_empty_input_is_empty() -> None:
    duckdb = builtin_registries().executors.get("duckdb")
    assert duckdb.input_spec(resolve_cell({}, {})) == {}
    assert duckdb.input_spec(resolve_cell({}, {"input": {"duckdb": None}})) == {}
    assert duckdb.input_spec(resolve_cell({}, {"input": {"sqlite": "x.db"}})) == {}
