import types

import pytest
from pydantic import BaseModel

from quicksql.errors import ConfigError
from quicksql.models import Scope
from quicksql.plugins.base import Plugin, qfield
from quicksql.registry import Registries, Registry, marked, plugin, sink
from quicksql.sinks.base import Sink


def test_registry_get_unknown_raises() -> None:
    reg = Registry("widget")
    with pytest.raises(ConfigError, match="unknown widget: nope"):
        reg.get("nope")


def test_registry_duplicate_name_raises() -> None:
    reg = Registry("widget")
    reg.register("a", object())
    with pytest.raises(ConfigError, match="duplicate widget"):
        reg.register("a", object())


def test_a_registry_copy_is_independent() -> None:
    reg = Registry("widget")
    reg.register("a", 1)
    copy = reg.copy()
    copy.register("b", 2)
    copy.remove("a")
    assert reg.names() == ["a"]
    assert copy.names() == ["b"]


def test_the_plugin_decorator_marks_without_registering(registries) -> None:
    @plugin
    class Sparkle(Plugin):
        class Config(BaseModel):
            sparkle_level: int = qfield(0)

    assert Sparkle.name == "sparkle"
    assert "sparkle" not in registries.plugins
    registries.add(Sparkle)
    assert registries.plugins.field_map()["sparkle_level"].plugin.name == "sparkle"


def test_adding_a_sink_registers_its_class(registries) -> None:
    @sink("void")
    class Void(Sink):
        def write(self, cell, view, conn):
            return 0, "void"

        def ref_expr(self, cell_name):
            return "void"

    registries.add(Void)
    assert registries.sinks.get("void") is Void


def test_adding_the_same_class_twice_is_harmless_another_is_a_duplicate(registries) -> None:
    @plugin
    class Sparkle(Plugin):
        pass

    registries.add(Sparkle, Sparkle)
    assert registries.plugins.names().count("sparkle") == 1

    @plugin
    class Sparkle(Plugin):  # noqa: F811 — another class, same name
        pass

    with pytest.raises(ConfigError, match="duplicate plugin registration: 'sparkle'"):
        registries.add(Sparkle)


def test_adding_an_unmarked_class_says_how_to_mark_it(registries) -> None:
    class Bare(Plugin):
        pass

    with pytest.raises(ConfigError, match="decorate it with @plugin"):
        registries.add(Bare)


def test_marked_collects_the_classes_a_module_defines_not_imports() -> None:
    module = types.ModuleType("scratch_plugins")
    exec(
        "from quicksql.plugins.base import Plugin\n"
        "from quicksql.plugins.emit_sql import EmitSql\n"  # imported: not this module's
        "from quicksql.registry import plugin\n"
        "@plugin\nclass First(Plugin): pass\n"
        "class Helper(Plugin): pass\n"  # not decorated
        "@plugin\nclass Second(Plugin): pass\n"
        "class Child(First): pass\n",  # inherits the mark, isn't decorated itself
        module.__dict__,
    )
    assert [cls.__name__ for cls in marked(module)] == ["First", "Second"]


def test_builtin_field_map_covers_directives(registries) -> None:
    fields = registries.plugins.field_map()
    for key in ("engine", "input", "sources", "extensions", "output",
                "autorun", "vars", "depends_on", "schema", "tags"):
        assert key in fields, key


def test_duplicate_config_field_raises(registries) -> None:
    @plugin
    class Impostor(Plugin):
        class Config(BaseModel):
            engine: str | None = qfield(None)

    with pytest.raises(ConfigError, match="field 'engine'"):
        registries.add(Impostor)


def test_by_scope_filters(registries) -> None:
    names = {p.name for p in registries.plugins.by_scope(Scope.GLOBAL, Scope.BOTH)}
    assert "engine" in names
    assert "depends_on" not in names  # cell-only


def test_chain_orders_by_priority_then_registration(registries) -> None:
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

    registries.add(Late, Early, AlsoLate)
    chain = [p.name for p in registries.plugins.chain() if p.name in {"early", "late", "also_late"}]
    assert chain == ["early", "late", "also_late"]


def test_hook_participants_follow_registrations(registries) -> None:
    before = [p.name for p in registries.plugins.overriding("after_compile")]

    @plugin
    class Checker(Plugin):
        def after_compile(self, project):
            pass

    registries.add(Checker)
    assert [p.name for p in registries.plugins.overriding("after_compile")] == [*before, "checker"]
    registries.plugins.remove("checker")
    assert [p.name for p in registries.plugins.overriding("after_compile")] == before


def test_registries_copy_leaves_the_original_untouched(registries) -> None:
    @plugin
    class Sparkle(Plugin):
        pass

    copy = registries.copy()
    copy.add(Sparkle)
    assert "sparkle" in copy.plugins
    assert "sparkle" not in registries.plugins
    assert isinstance(copy, Registries)
