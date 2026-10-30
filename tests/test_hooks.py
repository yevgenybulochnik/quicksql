"""Behavior-hook seams: execute sugar, render seams, lifecycle notifications."""

from dataclasses import replace

import pytest
from pydantic import BaseModel

from quicksql.compiler import compile_text
from quicksql.errors import ConfigError
from quicksql.plugins.base import Plugin
from quicksql.registry import PLUGINS, plugin

# ---------- before_execute / after_execute sugar ----------


def test_before_and_after_execute_called_around_cell(tmp_path) -> None:
    calls: list[str] = []

    @plugin
    class Timing(Plugin):
        def before_execute(self, cell, ctx):
            calls.append(f"before:{cell.name}")

        def after_execute(self, cell, ctx, result):
            calls.append(f"after:{cell.name}:{result.ok}")

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    assert project.run()[0].ok
    assert calls == ["before:a", "after:a:True"]


def test_after_execute_can_replace_the_result(tmp_path) -> None:
    @plugin
    class Censor(Plugin):
        def after_execute(self, cell, ctx, result):
            return replace(result, target="[redacted]")

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    assert project.run()[0].target == "[redacted]"


def test_after_execute_returning_none_keeps_result(tmp_path) -> None:
    @plugin
    class Observer(Plugin):
        def after_execute(self, cell, ctx, result):
            return None

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    result = project.run()[0]
    assert result.ok and result.rows == 1


def test_sugar_only_plugin_joins_the_run_chain() -> None:
    @plugin
    class OnlyBefore(Plugin):
        def before_execute(self, cell, ctx):
            pass

    assert "only_before" in [p.name for p in PLUGINS.chain()]


def test_render_context_plugin_contributes_jinja_globals(tmp_path) -> None:
    @plugin
    class Macros(Plugin):
        def render_context(self, rctx):
            return {"greeting": "hello"}

    project = compile_text("-- @cell a\nSELECT '{{ greeting }}' AS g;", root=tmp_path)
    assert "'hello' AS g" in project.cells["a"].sql


class _Helpers:
    _secret = "leaked"

    def quote(self, name: str) -> str:
        return f'"{name}"'


def test_contributed_callables_and_methods_render_in_the_sandbox(tmp_path) -> None:
    @plugin
    class Macros(Plugin):
        def render_context(self, rctx):
            return {"helpers": _Helpers(), "shout": lambda s: s.upper()}

    project = compile_text(
        "-- @cell a\nSELECT {{ helpers.quote('id') }}, '{{ shout('hi') }}';", root=tmp_path
    )
    assert "SELECT \"id\", 'HI';" in project.cells["a"].sql


def test_contributed_globals_hide_their_underscore_attributes(tmp_path) -> None:
    @plugin
    class Macros(Plugin):
        def render_context(self, rctx):
            return {"helpers": _Helpers()}

    with pytest.raises(ConfigError, match="template error: .*'_secret'.*unsafe"):
        compile_text("-- @cell a\nSELECT '{{ helpers._secret }}';", root=tmp_path)


class _Settings(BaseModel):
    schema_name: str = "analytics"


def test_contributed_pydantic_models_expose_only_their_fields(tmp_path) -> None:
    secret = tmp_path / "secret.json"
    secret.write_text('{"schema_name": "read-from-disk"}')

    @plugin
    class Settings(Plugin):
        def render_context(self, rctx):
            return {"settings": _Settings()}

    project = compile_text(
        "-- @cell a\nSELECT * FROM {{ settings.schema_name }}.t;", root=tmp_path
    )
    assert "FROM analytics.t" in project.cells["a"].sql
    text = "-- @cell a\nSELECT '{{ settings.parse_file(\"SECRET\").schema_name }}';"
    with pytest.raises(ConfigError, match="template error: .*'parse_file'.*unsafe") as excinfo:
        compile_text(text.replace("SECRET", str(secret)), root=tmp_path)
    assert "read-from-disk" not in str(excinfo.value)


def test_render_context_receives_capabilities(tmp_path) -> None:
    @plugin
    class UsesCtx(Plugin):
        def render_context(self, rctx):
            return {"whoami": f"{rctx.name}@{rctx.config.autorun}"}

    project = compile_text("-- @cell a\nSELECT '{{ whoami }}' AS w;", root=tmp_path)
    assert "'a@True'" in project.cells["a"].sql


def test_contributing_a_taken_key_is_a_compile_error(tmp_path) -> None:
    @plugin
    class Hijack(Plugin):
        def render_context(self, rctx):
            return {"ref": lambda n: "HACKED"}

    with pytest.raises(ConfigError, match="ref.*hijack|hijack.*ref"):
        compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)


def test_two_plugins_colliding_on_a_key_names_both(tmp_path) -> None:
    @plugin
    class One(Plugin):
        def render_context(self, rctx):
            return {"shared": 1}

    @plugin
    class Two(Plugin):
        def render_context(self, rctx):
            return {"shared": 2}

    with pytest.raises(ConfigError, match="one") as excinfo:
        compile_text("-- @cell a\nSELECT 1;", root=tmp_path)
    assert "two" in str(excinfo.value)


def test_deleting_the_vars_plugin_removes_the_var_global(tmp_path) -> None:
    # behavior truly lives in the plugin: without it, var() is undefined
    snap = PLUGINS.snapshot()
    PLUGINS.restore({k: v for k, v in snap.items() if k != "vars"})
    with pytest.raises(ConfigError, match="template error"):
        compile_text("-- @cell a\nSELECT {{ var('x', 1) }};", root=tmp_path)


def test_after_render_transformers_compose_in_priority_order(tmp_path) -> None:
    @plugin
    class Footer(Plugin):
        priority = 10

        def after_render(self, rctx, sql):
            return sql + "\n-- footer"

    @plugin
    class Header(Plugin):
        priority = -10

        def after_render(self, rctx, sql):
            return "-- header\n" + sql

    sql = compile_text("-- @cell a\nSELECT 1;", root=tmp_path).cells["a"].sql
    assert sql == "-- header\nSELECT 1;\n-- footer"


def test_dev_limit_is_no_longer_a_directive(tmp_path) -> None:
    # removed: limits belong in the cell's SQL, not a wrapper around the body
    with pytest.raises(ConfigError, match="unknown directive: @dev_limit"):
        compile_text("-- @dev_limit: 4\n-- @cell a\nSELECT 1;", root=tmp_path)


# ---------- lifecycle notifications ----------


def test_after_compile_sees_the_project(tmp_path) -> None:
    seen: list[list[str]] = []

    @plugin
    class Lint(Plugin):
        def after_compile(self, project):
            seen.append(list(project.cells))

    compile_text("-- @cell a\nSELECT 1;\n-- @cell b\nSELECT 2;", root=tmp_path)
    assert seen == [["a", "b"]]


def test_after_compile_can_reject_the_project(tmp_path) -> None:
    @plugin
    class TagsRequired(Plugin):
        def after_compile(self, project):
            untagged = [n for n, c in project.cells.items() if not c.config.tags]
            if untagged:
                raise ConfigError(f"cells missing @tags: {', '.join(untagged)}")

    with pytest.raises(ConfigError, match="missing @tags: a"):
        compile_text("-- @cell a\nSELECT 1;", root=tmp_path)
    project = compile_text("-- @tags: [ok]\n-- @cell a\nSELECT 1;", root=tmp_path)
    assert project.cells["a"].config.tags == ["ok"]


def test_before_and_after_run_notifications(tmp_path) -> None:
    events: list[str] = []

    @plugin
    class Notify(Plugin):
        def before_run(self, project, ctx):
            events.append("start")

        def after_run(self, project, results):
            events.append(f"end:{len(results)}:{all(r.ok for r in results)}")

    project = compile_text("-- @cell a\nSELECT 1;\n-- @cell b\nSELECT 2;", root=tmp_path)
    project.run()
    assert events == ["start", "end:2:True"]


def test_failing_lifecycle_notification_is_contained(tmp_path) -> None:
    @plugin
    class Boom(Plugin):
        def before_run(self, project, ctx):
            raise RuntimeError("boom before")

        def after_run(self, project, results):
            raise RuntimeError("boom after")

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    results = project.run()
    assert results[0].ok  # the run itself is unaffected


def test_sugar_respects_chain_priority(tmp_path) -> None:
    calls: list[str] = []

    @plugin
    class Inner(Plugin):
        priority = 10

        def before_execute(self, cell, ctx):
            calls.append("inner")

    @plugin
    class Outer(Plugin):
        priority = -10

        def before_execute(self, cell, ctx):
            calls.append("outer")

    project = compile_text("-- @cell a\nSELECT 1 AS x;", root=tmp_path)
    project.run()
    assert calls == ["outer", "inner"]
