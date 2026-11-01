"""A registry per notebook: the builtins come from one list, and a notebook's
quicksqlrc.py (or --plugins) loads into that notebook's registries only."""

import json

from typer.testing import CliRunner

from quicksql.cli import app

runner = CliRunner()


def _rc_plugin(cls: str, field: str) -> str:
    return (
        "from pydantic import BaseModel\n"
        "from quicksql.plugins import Plugin, qfield\n"
        "from quicksql.registry import plugin\n\n\n"
        "@plugin\n"
        f"class {cls}(Plugin):\n"
        "    class Config(BaseModel):\n"
        f"        {field}: int = qfield(0)\n"
    )


def _notebook(directory, text: str, rc: str | None = None):
    directory.mkdir(parents=True, exist_ok=True)
    if rc is not None:
        (directory / "quicksqlrc.py").write_text(rc)
    path = directory / "nb.qsql"
    path.write_text(text)
    return path


def _compile(path, *args: str):
    return runner.invoke(app, ["compile", str(path), *args])


def test_two_notebooks_with_different_rcs_dont_see_each_others_directives(tmp_path) -> None:
    text = "-- @sparkle: 3\n-- @cell x\nSELECT 1 AS x;\n"
    a = _notebook(tmp_path / "a", text, rc=_rc_plugin("Sparkle", "sparkle"))
    b = _notebook(tmp_path / "b", text)  # no rc: @sparkle means nothing here
    assert _compile(a).exit_code == 0
    result = _compile(b)
    assert result.exit_code == 3
    assert "unknown directive: @sparkle" in result.output


def test_two_rc_files_may_define_a_plugin_of_the_same_name(tmp_path) -> None:
    a = _notebook(tmp_path / "a", "-- @sparkle: 1\n-- @cell x\nSELECT 1 AS x;\n",
                  rc=_rc_plugin("Sparkle", "sparkle"))
    b = _notebook(tmp_path / "b", "-- @glitter: 2\n-- @cell x\nSELECT 1 AS x;\n",
                  rc=_rc_plugin("Sparkle", "glitter"))
    for path in (a, b, a):
        result = _compile(path)
        assert result.exit_code == 0, result.output


def test_an_edited_rc_reloads(tmp_path) -> None:
    path = _notebook(tmp_path, "-- @sparkle: 1\n-- @cell x\nSELECT 1 AS x;\n",
                     rc=_rc_plugin("Sparkle", "sparkle"))
    assert _compile(path).exit_code == 0
    (tmp_path / "quicksqlrc.py").write_text(_rc_plugin("Sparkle", "glitter"))
    path.write_text("-- @glitter: 2\n-- @cell x\nSELECT 1 AS x;\n")
    result = _compile(path)
    assert result.exit_code == 0, result.output


def test_a_plugins_module_loads_for_its_run_only(tmp_path) -> None:
    (tmp_path / "myplug.py").write_text(_rc_plugin("Greeting", "greeting"))
    path = _notebook(tmp_path / "nb", "-- @greeting: 1\n-- @cell x\nSELECT 1 AS x;\n")
    assert _compile(path, "--plugins", str(tmp_path / "myplug.py")).exit_code == 0
    assert _compile(path).exit_code == 3


def test_loading_an_rc_leaves_the_builtin_registries_untouched(tmp_path) -> None:
    from quicksql.bootstrap import builtin_registries, registries_for

    path = _notebook(tmp_path, "-- @cell x\nSELECT 1 AS x;\n", rc=_rc_plugin("Sparkle", "sparkle"))
    assert "sparkle" in registries_for(path).plugins
    assert "sparkle" not in builtin_registries().plugins
    assert "duckdb" in builtin_registries().executors


def test_explain_shows_the_notebooks_own_plugins(tmp_path) -> None:
    a = _notebook(tmp_path / "a", "-- @cell x\nSELECT 1 AS x;\n",
                  rc=_rc_plugin("Sparkle", "sparkle") + (
                      "\n    def after_compile(self, project):\n        pass\n"))
    b = _notebook(tmp_path / "b", "-- @cell x\nSELECT 1 AS x;\n")
    assert "sparkle" in runner.invoke(app, ["explain", str(a)]).output
    assert "sparkle" not in runner.invoke(app, ["explain", str(b)]).output


def test_run_json_uses_the_rc_plugins(tmp_path) -> None:
    path = _notebook(tmp_path, "-- @sparkle: 1\n-- @cell x\nSELECT 1 AS x;\n",
                     rc=_rc_plugin("Sparkle", "sparkle"))
    result = runner.invoke(app, ["run", str(path), "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["status"] == "ok"
