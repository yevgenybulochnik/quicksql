from pathlib import Path

from typer.testing import CliRunner

from quicksql.cli import app

runner = CliRunner()


def _init(tmp_path) -> Path:
    f = tmp_path / "base.qsql"
    result = runner.invoke(app, ["init", str(f)])
    assert result.exit_code == 0, result.output
    return f


def test_init_scaffolds_and_refuses_overwrite(tmp_path) -> None:
    f = _init(tmp_path)
    assert f.exists()
    again = runner.invoke(app, ["init", str(f)])
    assert again.exit_code != 0
    assert "refus" in again.output.lower()


def test_bare_invocation_scaffolds_qsql_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, [])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "base.qsql").exists()


def test_run_lands_outputs(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "data" / "users.parquet").exists()
    assert "active_user_events" in result.output


def test_run_set_overrides_sink(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f), "--set", "output.type=duckdb"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "warehouse.db").exists()


def test_run_select_subset(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f), "--select", "users"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "data" / "users.parquet").exists()
    assert not (tmp_path / "data" / "events.parquet").exists()


def test_run_failure_exits_nonzero(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @cell broken\nSELECT * FROM no_such_table;")
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 1
    assert "broken" in result.output


def test_run_summary_reports_skipped_downstream(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text(
        "-- @cell broken\nSELECT * FROM no_such_table;\n"
        "-- @cell downstream\nSELECT * FROM {{ ref('broken') }};"
    )
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 1
    assert "skip downstream: upstream 'broken' failed" in result.output
    assert "FAIL downstream" not in result.output


def test_list_shows_topo_engine_sink_autorun(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["list", str(f)])
    assert result.exit_code == 0, result.output
    lines = result.output.strip().splitlines()
    assert lines[0].startswith("users")
    assert "duckdb" in result.output
    assert "parquet" in result.output
    assert "autorun:on" in result.output


def test_show_prints_rendered_sql(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["show", "active_user_events", str(f)])
    assert result.exit_code == 0, result.output
    assert "FROM users" in result.output  # same-context ref: bare temp name
    assert "{{" not in result.output


def test_show_unknown_cell_errors(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["show", "ghost", str(f)])
    assert result.exit_code == 1


def test_compile_prints_all_cells(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["compile", str(f)])
    assert result.exit_code == 0, result.output
    assert "-- cell: users" in result.output
    assert "-- cell: active_user_events" in result.output


def test_compile_error_reported(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @bogus: 1\n-- @cell a\nSELECT 1;")
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 1
    assert "unknown directive" in result.output


def test_explain_shows_chain_hooks_and_cells(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["explain", str(f)])
    assert result.exit_code == 0, result.output
    assert "run chain" in result.output
    assert "emit_sql(-100)" in result.output   # builtin wrapper, with priority
    assert "after_render" in result.output
    for name in ("refs", "sources", "vars", "env"):  # plugin-owned globals
        assert name in result.output
    assert "users" in result.output            # cells listed with engine -> sink
    assert "duckdb → parquet" in result.output


PLUGIN_MODULE = """\
from pydantic import BaseModel

from quicksql.plugins import Plugin, qfield
from quicksql.registry import plugin


@plugin
class Greeting(Plugin):
    class Config(BaseModel):
        greeting: str | None = qfield(None)
"""

GREETING_FILE = "-- @greeting: hello\n-- @cell a\nSELECT 1 AS x;\n"


def test_plugins_flag_loads_module_from_path(tmp_path) -> None:
    (tmp_path / "myplug.py").write_text(PLUGIN_MODULE)
    f = tmp_path / "pipeline.sql"
    f.write_text(GREETING_FILE)
    without = runner.invoke(app, ["run", str(f)])
    assert without.exit_code == 1  # @greeting unknown without the plugin
    result = runner.invoke(
        app, ["run", str(f), "--plugins", str(tmp_path / "myplug.py")]
    )
    assert result.exit_code == 0, result.output


def test_quicksqlrc_next_to_file_autoloads(tmp_path) -> None:
    (tmp_path / "quicksqlrc.py").write_text(PLUGIN_MODULE)
    f = tmp_path / "pipeline.sql"
    f.write_text(GREETING_FILE)
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 0, result.output


def test_run_streams_progress_lines(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 0, result.output
    assert "users: executing on duckdb" in result.output
    assert "landing via parquet" in result.output
