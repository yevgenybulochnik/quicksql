import json
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


def test_run_failure_prints_the_engine_message_and_its_context(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @cell a\nSELECT * FROM nope;")
    result = runner.invoke(app, ["run", str(f)])
    assert result.exit_code == 1
    lines = result.output.splitlines()
    fail = next(line for line in lines if "FAIL a:" in line)
    assert fail.strip() == "FAIL a: Catalog Error: Table with name nope does not exist!"
    # duckdb's LINE/caret context follows, indented as a block so the caret
    # still points at the missing table
    context = lines[lines.index(fail) + 1 :]
    sql_line = next(line for line in context if "LINE 1:" in line)
    caret = context[context.index(sql_line) + 1]
    assert caret.strip() == "^"
    assert caret.index("^") == sql_line.index("nope")


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
    assert result.exit_code == 3  # an invalid selection, like an unknown --select


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
    assert result.exit_code == 3  # compile errors exit 3; 1 means a cell failed
    assert "unknown directive" in result.output


def test_compile_rejects_a_template_escaping_the_sandbox(tmp_path) -> None:
    marker = tmp_path / "escaped"
    f = tmp_path / "evil.qsql"
    f.write_text(
        "-- @cell a\n"
        "SELECT '{{ cycler.__init__.__globals__.os.popen(\"touch MARKER\").read() }}' AS x;\n"
        .replace("MARKER", str(marker))
    )
    result = runner.invoke(app, ["compile", str(f)])
    assert result.exit_code == 3, result.output
    assert "cell 'a' (line 1): template error" in result.output
    assert "-- cell:" not in result.output
    assert [p.name for p in tmp_path.iterdir()] == ["evil.qsql"]  # nothing written


def test_compile_reports_a_python_error_in_a_template_as_a_template_error(tmp_path) -> None:
    f = tmp_path / "bad.qsql"
    f.write_text("-- @cell a\nSELECT {{ 1/0 }} AS x;\n")
    result = runner.invoke(app, ["compile", str(f)])
    assert result.exit_code == 3, result.output
    assert (
        "cell 'a' (line 1): template error: ZeroDivisionError: division by zero"
        in result.output
    )
    assert "Traceback" not in result.output
    assert "-- cell:" not in result.output


def test_compile_error_exits_3_for_every_command(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @cell a\n-- @bogus: 1\nSELECT 1;")
    for command in ("run", "list", "compile", "explain"):
        result = runner.invoke(app, [command, str(f)])
        assert result.exit_code == 3, (command, result.output)
    result = runner.invoke(app, ["show", "a", str(f)])
    assert result.exit_code == 3, result.output


def test_run_unknown_select_exits_3(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f), "--select", "ghost"])
    assert result.exit_code == 3
    assert "unknown cell(s) in --select: ghost" in result.output


def test_compile_unknown_select_exits_3(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["compile", str(f), "--select", "ghost"])
    assert result.exit_code == 3
    assert "unknown cell(s) in --select: ghost" in result.output


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
    assert without.exit_code == 3  # @greeting unknown without the plugin
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


def _json_run(f: Path, *args: str):
    result = runner.invoke(app, ["run", str(f), "--json", *args])
    # stdout is exactly one JSON document: no progress lines, no colours
    return result, json.loads(result.stdout)


def test_run_json_all_ok(tmp_path) -> None:
    f = _init(tmp_path)
    result, doc = _json_run(f)
    assert result.exit_code == 0, result.output
    assert doc["file"] == str(f)
    assert doc["status"] == "ok"
    assert doc["errors"] == []
    assert doc["summary"] == {"ok": 3, "failed": 0, "skipped": 0}
    assert isinstance(doc["elapsed"], float)
    assert [c["cell"] for c in doc["cells"]] == ["users", "events", "active_user_events"]
    users = doc["cells"][0]
    assert users["status"] == "ok"
    assert users["rows"] == 3
    assert users["target"].endswith("data/users.parquet")
    assert "preview" not in users  # only with --rows


def test_run_json_piped_is_one_compact_line(tmp_path) -> None:
    f = _init(tmp_path)
    result, doc = _json_run(f, "--rows", "2")
    # a pipe (an agent, jq) gets one line; a terminal gets it indented
    assert result.stdout.count("\n") == 1
    assert doc["elapsed"] == round(doc["elapsed"], 4)  # seconds, to 0.1 ms


def test_run_json_failed_skipped_and_independent_cells(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text(
        "-- @cell broken\nSELECT * FROM no_such_table;\n"
        "-- @cell downstream\nSELECT * FROM {{ ref('broken') }};\n"
        "-- @cell independent\nSELECT 1 AS x;\n"
    )
    result, doc = _json_run(f)
    assert result.exit_code == 1
    assert doc["status"] == "failed"
    assert doc["summary"] == {"ok": 1, "failed": 1, "skipped": 1}
    cells = {c["cell"]: c for c in doc["cells"]}
    assert {name: c["status"] for name, c in cells.items()} == {
        "broken": "failed", "downstream": "skipped", "independent": "ok",
    }
    broken = cells["broken"]
    # the engine's message, not the traceback's last line
    assert broken["error"].splitlines()[0] == (
        "Catalog Error: Table with name no_such_table does not exist!"
    )
    assert "Traceback" in broken["traceback"]
    assert cells["downstream"]["error"] == "upstream 'broken' failed"
    assert doc["errors"] == []  # cell failures live on the cells


def test_run_json_rows_adds_previews(tmp_path) -> None:
    f = _init(tmp_path)
    _, doc = _json_run(f, "--rows", "2")
    for cell in doc["cells"]:
        assert len(cell["preview"]["rows"]) == 2, cell["cell"]
    assert doc["cells"][0]["preview"]["columns"][0] == "user_id"


def test_run_json_preview_values_survive_json(tmp_path) -> None:
    f = tmp_path / "types.sql"
    f.write_text(
        "-- @cell typed\n"
        "SELECT DATE '2024-01-02' AS d, 1.50::DECIMAL(4, 2) AS amount, 'nan'::DOUBLE AS r;\n"
    )
    result, doc = _json_run(f, "--rows", "1")
    assert result.exit_code == 0, result.output
    assert doc["cells"][0]["preview"]["rows"] == [["2024-01-02", "1.50", "nan"]]


def test_run_json_compile_error_lists_cell_and_line(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @cell a\nSELECT 1;\n-- @cell b\n-- @bogus: 1\nSELECT 2;\n")
    result, doc = _json_run(f)
    assert result.exit_code == 3
    assert doc["status"] == "compile_error"
    assert doc["errors"] == [{"cell": "b", "line": 3, "message": "unknown directive: @bogus"}]
    assert doc["cells"] == []
    assert doc["summary"] == {"ok": 0, "failed": 0, "skipped": 0}


def test_run_json_reports_a_python_error_in_a_template_under_its_cell(tmp_path) -> None:
    f = tmp_path / "bad.qsql"
    f.write_text("-- @cell a\nSELECT {{ 1/0 }} AS x;\n")
    result, doc = _json_run(f)
    assert result.exit_code == 3
    assert doc["status"] == "compile_error"
    assert doc["errors"] == [{
        "cell": "a",
        "line": 1,
        "message": "template error: ZeroDivisionError: division by zero",
    }]
    assert doc["cells"] == []


def test_run_json_other_compile_errors_have_no_cell_or_line(tmp_path) -> None:
    f = tmp_path / "bad.sql"
    f.write_text("-- @bogus: 1\n-- @cell a\nSELECT 1;\n")  # header: no cell group
    result, doc = _json_run(f)
    assert result.exit_code == 3
    assert doc["errors"] == [{"cell": None, "line": None, "message": "unknown directive: @bogus"}]
    ok = _init(tmp_path)
    result, doc = _json_run(ok, "--select", "ghost")
    assert result.exit_code == 3
    assert doc["status"] == "compile_error"
    assert doc["errors"] == [
        {"cell": None, "line": None, "message": "unknown cell(s) in --select: ghost"}
    ]


def test_run_json_other_errors_before_cells_still_print_one_document(tmp_path) -> None:
    f = _init(tmp_path)
    result, doc = _json_run(f, "--plugins", "no_such_module")
    assert result.exit_code == 1
    assert doc["status"] == "error"
    assert doc["cells"] == []
    assert doc["summary"] == {"ok": 0, "failed": 0, "skipped": 0}
    assert doc["errors"] == [{
        "cell": None,
        "line": None,
        "message": "ModuleNotFoundError: No module named 'no_such_module'",
    }]


def test_run_json_keeps_compile_warnings_on_stderr(tmp_path) -> None:
    f = tmp_path / "warn.sql"
    f.write_text("-- @cell a\n-- @autorun: true\n/*@ autorun: false */\nSELECT 1 AS x;\n")
    result, doc = _json_run(f)
    assert result.exit_code == 0, result.output
    assert doc["status"] == "ok"
    assert "warning: directive 'autorun' at line 3" in result.stderr


def test_run_rows_needs_json(tmp_path) -> None:
    f = _init(tmp_path)
    result = runner.invoke(app, ["run", str(f), "--rows", "2"])
    assert result.exit_code == 2  # a usage error
    assert "--json" in result.output
