import pytest
from pydantic import BaseModel

from quicksql.errors import ConfigError
from quicksql.models import Scope
from quicksql.plugins.base import Plugin, qfield
from quicksql.registry import PLUGINS, Registry, plugin


def test_registry_get_unknown_raises() -> None:
    reg = Registry("widget")
    with pytest.raises(ConfigError, match="unknown widget: nope"):
        reg.get("nope")


def test_registry_duplicate_name_raises() -> None:
    reg = Registry("widget")
    reg.register("a", object())
    with pytest.raises(ConfigError, match="duplicate widget"):
        reg.register("a", object())


def test_registry_snapshot_restore() -> None:
    reg = Registry("widget")
    reg.register("a", 1)
    snap = reg.snapshot()
    reg.register("b", 2)
    reg.restore(snap)
    assert "b" not in reg
    assert reg.get("a") == 1


def test_plugin_decorator_registers_with_derived_name() -> None:
    @plugin
    class Sparkle(Plugin):
        class Config(BaseModel):
            sparkle_level: int = qfield(0)

    assert PLUGINS.get("sparkle") is not None
    assert PLUGINS.field_map()["sparkle_level"].plugin.name == "sparkle"


def test_builtin_field_map_covers_directives() -> None:
    fields = PLUGINS.field_map()
    for key in ("engine", "input", "sources", "extensions", "output",
                "autorun", "vars", "depends_on", "schema", "tags"):
        assert key in fields, key


def test_duplicate_config_field_raises() -> None:
    with pytest.raises(ConfigError, match="field 'engine'"):
        @plugin
        class Impostor(Plugin):
            class Config(BaseModel):
                engine: str | None = qfield(None)


def test_by_scope_filters() -> None:
    names = {p.name for p in PLUGINS.by_scope(Scope.GLOBAL, Scope.BOTH)}
    assert "engine" in names
    assert "depends_on" not in names  # cell-only


def test_chain_orders_by_priority_then_registration() -> None:
    calls: list[str] = []

    class Tracer(Plugin):
        def run(self, cell, ctx, inner):
            calls.append(self.name)
            return inner(cell, ctx)

    @plugin
    class Late(Tracer):
        priority = 10

    @plugin
    class Early(Tracer):
        priority = -10

    @plugin
    class AlsoLate(Tracer):
        priority = 10

    chain = [p.name for p in PLUGINS.chain() if p.name in {"early", "late", "also_late"}]
    assert chain == ["early", "late", "also_late"]
