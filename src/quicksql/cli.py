"""The quicksql CLI: init / run / watch / tui / list / show / compile."""

from __future__ import annotations

import json
import sys
import textwrap
import time
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer

from .bootstrap import compile_file
from .compiler import Project
from .errors import QsqlError, error_dicts
from .live import protocol as live_protocol
from .live.client import LiveClient, probe
from .live.protocol import LiveError, file_hash
from .overrides import gather_overrides
from .runner import error_message, run_project
from .scaffold import write_scaffold
from .state import statuses

app = typer.Typer(
    add_completion=False,
    no_args_is_help=False,
    invoke_without_command=True,
    help="quicksql — a notebook for SQL: one file, named cells, comment directives.",
)

FILE_ARG = typer.Argument(Path("base.qsql"), help="The .qsql/.sql notebook file")
SET_OPT = typer.Option(None, "--set", help="Override config: key.path=value (repeatable)")
PLUGINS_OPT = typer.Option(
    None, "--plugins", help="Extra plugin module (dotted name or path/to/file.py, repeatable)"
)

# 2 is click's usage error
EXIT_FAILED = 1  # a cell failed (a skip always has a failed root), or another runtime failure
EXIT_COMPILE = 3  # the notebook didn't compile, or the selection names an unknown cell
EXIT_NO_SESSION = 4  # no TUI has the notebook open (or a protocol mismatch, or a timeout)
EXIT_REFUSED = 5  # the live session refused: busy, the file says otherwise, a replaced run


@app.callback()
def main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        _init(Path("base.qsql"))


@app.command()
def init(file: Path = FILE_ARG) -> None:
    """Scaffold a starter notebook (refuses to overwrite)."""
    _init(file)


def _init(file: Path) -> None:
    try:
        write_scaffold(file)
    except FileExistsError:
        typer.secho(f"refusing to overwrite existing {file}", fg="red")
        raise typer.Exit(1)
    typer.echo(f"scaffolded {file} — try: quicksql run {file}")


def _compile(file: Path, sets: list[str] | None, plugins: list[str] | None) -> Project:
    """Compile with the notebook's own registries (the builtins, its
    quicksqlrc.py, --plugins), warnings to stderr; a QsqlError propagates."""
    project = compile_file(file, gather_overrides(list(sets or [])), plugins or ())
    for line, message in project.warnings:
        typer.secho(f"warning: {message}", fg="yellow", err=True)
    return project


def _project(file: Path, sets: list[str] | None, plugins: list[str] | None) -> Project:
    try:
        return _compile(file, sets, plugins)
    except QsqlError as exc:
        typer.secho(str(exc), fg="red")
        raise typer.Exit(EXIT_COMPILE)


def _print_results(results: list) -> bool:
    return _print_cells([r.to_dict() for r in results])


def _print_cells(cells: list[dict[str, Any]]) -> bool:
    """Each result (``RunResult.to_dict``, or a live session's, which adds
    the cell's lines and failing statement) as a line; True if one failed."""
    failed = False
    for r in cells:
        if r["status"] == "ok":
            rows = r["rows"] if r["rows"] is not None else "?"
            typer.secho(
                f"  ok {r['cell']:<24} {rows:>8} rows"
                f"  {r['elapsed'] * 1000:8.1f} ms  -> {r['target']}",
                fg="green",
            )
        elif r["status"] == "skipped":
            typer.secho(f"  skip {r['cell']}: {r['error']}", fg="yellow")
        else:
            failed = True
            first, *rest = (r["error"] or "unknown error").strip().splitlines()
            where = _where(r)
            typer.secho(f"  FAIL {r['cell']}{where}: {first}", fg="red")
            if rest:
                # e.g. duckdb's LINE/caret context; one indent for the whole
                # block keeps the caret under what it points at
                typer.secho(textwrap.indent("\n".join(rest), "      "), fg="red")
    return failed


def _where(r: dict[str, Any]) -> str:
    """`` (lines 4–9, statement 2)``: where a live session's failure is in
    the file. Plain run results carry no lines, and print as they always have."""
    if not r.get("line"):
        return ""
    where = f"lines {r['line']}–{r['line_end']}"
    if r.get("statement"):
        where += f", statement {r['statement']}"
    return f" ({where})"


@app.command()
def run(
    file: Path = FILE_ARG,
    select: Optional[list[str]] = typer.Option(None, "--select", "-s", help="Run these cells (+ upstreams)"),
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
    json_out: bool = typer.Option(
        False, "--json", help="Print one JSON document on stdout instead of progress and a summary"
    ),
    rows: int = typer.Option(
        0, "--rows", min=0,
        help="With --json: add up to N preview rows per ok cell (at most 100, the cached preview)",
    ),
) -> None:
    """Compile and run cells in dependency order, landing each via its sink.

    Exit codes: 0 all ok, 1 a cell failed, 3 the notebook didn't compile or
    --select names an unknown cell (2 is a usage error)."""
    if rows and not json_out:
        raise typer.BadParameter("only applies with --json", param_hint="'--rows'")
    _refuse_while_live(file, json_out)
    selected = list(select) if select else None
    if json_out:
        _run_json(file, selected, set_, plugins, rows)
        return
    project = _project(file, set_, plugins)
    try:
        results = run_project(project, select=selected, on_event=_live_event)
    except QsqlError as exc:  # an unknown --select cell
        typer.secho(str(exc), fg="red")
        raise typer.Exit(EXIT_COMPILE)
    if _print_results(results):
        raise typer.Exit(EXIT_FAILED)


def _run_json(
    file: Path,
    select: list[str] | None,
    sets: list[str] | None,
    plugins: list[str] | None,
    rows: int,
) -> None:
    """``run --json``: stdout gets the one document and nothing else (no
    progress events), whatever goes wrong; compile warnings stay on stderr."""
    started = time.perf_counter()
    results = []
    errors: list[dict[str, Any]] = []
    status = "ok"
    try:
        results = run_project(_compile(file, sets, plugins), select=select)
    except QsqlError as exc:  # before any cell ran: compile, or an unknown --select
        status, errors = "compile_error", error_dicts(exc)
    except Exception as exc:  # before any cell ran: a plugin that won't import, a locked db
        status, errors = "error", [{"cell": None, "line": None, "message": error_message(exc)}]
    counts = Counter(r.status for r in results)
    if status == "ok" and counts["failed"]:
        status = "failed"
    doc = {
        "file": str(file),
        "status": status,
        "summary": {s: counts[s] for s in ("ok", "failed", "skipped")},
        "elapsed": round(time.perf_counter() - started, 4),
        "errors": errors,
        "cells": [r.to_dict(rows) for r in results],
    }
    # indented for a terminal, one line for a pipe (an agent, jq) — like gh.
    # to_dict is JSON-safe; default=str covers a plugin's odd target (a Path, ...)
    indent = 2 if sys.stdout.isatty() else None
    typer.echo(json.dumps(doc, indent=indent, default=str))
    if status != "ok":
        raise typer.Exit(EXIT_COMPILE if status == "compile_error" else EXIT_FAILED)


def _refuse_while_live(file: Path, json_out: bool = False) -> None:
    """One runner per notebook: while a TUI has it open, runs go through the
    TUI (its keys, or ``quicksql live run``), never a second process racing
    its sinks, its state file and the DuckDB files it holds."""
    if probe(file) is None:
        return
    message = (
        f"a TUI has {file} open: run cells there (r / R) or with `quicksql live run {file}`"
    )
    if json_out:
        doc = {
            "file": str(file), "status": "refused",
            "summary": {"ok": 0, "failed": 0, "skipped": 0}, "elapsed": 0.0,
            "errors": [{"cell": None, "line": None, "message": message}], "cells": [],
        }
        typer.echo(json.dumps(doc))
    else:
        typer.secho(message, fg="red")
    raise typer.Exit(EXIT_REFUSED)


def _live_event(event) -> None:
    """Stream run progress; results get their green/red summary from
    _print_results, so only the in-flight kinds print here."""
    if event.kind in ("cell_started", "cell_step"):
        typer.secho(f"  {event.line()}", dim=True)
    elif event.kind == "note":
        typer.secho(f"  {event.line()}", fg="yellow")


@app.command("list")
def list_cells(
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Cells in topo order with engine, sink, autorun, and dependencies."""
    project = _project(file, set_, plugins)
    for name in project.order:
        cell = project.cells[name]
        auto = "on" if cell.config.autorun else "off"
        deps = ", ".join(cell.depends_on) or "-"
        typer.echo(
            f"{name:<26} {cell.engine:>8} -> {cell.sink_type:<8} autorun:{auto}  deps: {deps}"
        )


@app.command()
def show(
    cell: str,
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Print one cell's rendered SQL."""
    project = _project(file, set_, plugins)
    if cell not in project.cells:
        typer.secho(f"unknown cell: {cell}", fg="red")
        raise typer.Exit(EXIT_COMPILE)
    typer.echo(project.cells[cell].sql)


@app.command("compile")
def compile_cmd(
    file: Path = FILE_ARG,
    select: Optional[list[str]] = typer.Option(None, "--select", "-s"),
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Print rendered SQL for all (or selected) cells."""
    project = _project(file, set_, plugins)
    unknown = [s for s in select or [] if s not in project.cells]
    if unknown:
        typer.secho(f"unknown cell(s) in --select: {', '.join(unknown)}", fg="red")
        raise typer.Exit(EXIT_COMPILE)
    for name in project.order:
        if select and name not in select:
            continue
        typer.echo(f"-- cell: {name}")
        typer.echo(project.cells[name].sql.strip())
        typer.echo("")


@app.command()
def explain(
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Show the effective plugin chain and hook participants for this project."""
    project = _project(file, set_, plugins)
    registry = project.registries.plugins

    def names(plugs: list) -> str:
        return " → ".join(f"{p.name}({p.priority})" for p in plugs) or "(none)"

    typer.echo(f"run chain:      {names(registry.chain())} → [execute on engine, land via sink]")
    for hook in (
        "render_context", "after_render", "resolve_engine", "resolve_sink",
        "collect_edges", "sink_config", "should_rerun",
        "after_compile", "before_run", "after_run",
    ):
        typer.echo(f"{hook + ':':<16}{names(registry.overriding(hook))}")
    typer.echo("cells:")
    for name in project.order:
        cell = project.cells[name]
        deps = ", ".join(cell.depends_on) or "-"
        typer.echo(
            f"  {name:<24} {cell.engine} → {cell.sink_type}  ctx: {cell.context}  deps: {deps}"
        )


@app.command()
def watch(
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Re-run changed cells (+ downstream dependents) on every save."""
    from .watcher import watch_events

    _refuse_while_live(file)
    project = _project(file, set_, plugins)  # load plugins + fail fast on compile errors
    typer.echo(f"watching {file} — Ctrl+C to stop")
    try:
        for project, event in watch_events(
            file, overrides=gather_overrides(list(set_ or [])), on_event=_live_event,
            project=project, plugins=plugins or (),
        ):
            _watch_cycle(project, event)
    except KeyboardInterrupt:
        pass


def _watch_cycle(project: Project | None, event: Any) -> None:
    """One save's outcome, ending in exactly one final status line: the
    compile error, "nothing to rerun", or the rerun's counts after its results."""
    if project is None:
        typer.secho(f"  compile error: {event}", fg="red")
    elif not event:
        typer.secho("  nothing to rerun", dim=True)
    else:
        _print_results(event)
        counts = Counter(r.status for r in event)
        typer.echo(f"  {counts['ok']} ok, {counts['failed']} failed, {counts['skipped']} skipped")


_STATUS_COLORS = {"ok": "green", "failed": "red", "skipped": "yellow", "stale": "yellow"}


@app.command()
def status(
    file: Path = FILE_ARG,
    json_out: bool = typer.Option(False, "--json", help="Print one JSON document instead"),
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Each cell's state: ok, failed or skipped (its last run), stale, or new
    (never run). Stale: its landed output was built from something else than
    the cell would be built from now, because its SQL or result-affecting
    config changed, or an upstream landed a new output since it ran.

    Stale is relative to the notebook: source files and remote tables aren't
    fingerprinted, so a changed CSV or warehouse table leaves a cell ok.

    While a TUI has the notebook open, the status is its live session's: also
    queued and running cells, each cell's autorun, and whether saves rerun."""
    if probe(file) is not None:
        _live_status(file, json_out)
        return
    if json_out:  # like run --json: stdout gets one document, whatever goes wrong
        try:
            doc = {"file": str(file), "live": False, "errors": [], "cells": [
                vars(row) for row in statuses(_compile(file, set_, plugins))
            ]}
        except QsqlError as exc:
            doc = {"file": str(file), "live": False, "errors": error_dicts(exc), "cells": []}
        typer.echo(json.dumps(doc, indent=2 if sys.stdout.isatty() else None))
        if doc["errors"]:
            raise typer.Exit(EXIT_COMPILE)
        return
    rows = statuses(_project(file, set_, plugins))
    for row in rows:
        line = f"  {row.status:<7} {row.cell:<24} {row.reason}".rstrip()
        typer.secho(line, fg=_STATUS_COLORS.get(row.status))


def _live_status(file: Path, json_out: bool) -> None:
    snap = _live_call(file, json_out, lambda c: c.call("snapshot"))
    errors = (snap["compile_error"] or {}).get("errors", [])
    if json_out:
        doc = {
            "file": str(file), "live": True, "errors": errors, "hash": snap["hash"],
            "controls": snap["controls"], "run": snap["run"], "cells": snap["cells"],
        }
        typer.echo(json.dumps(doc, indent=2 if sys.stdout.isatty() else None, default=str))
        if errors:
            raise typer.Exit(EXIT_COMPILE)
        return
    controls, run = snap["controls"], snap["run"]
    head = "live · " + ("armed: saves rerun changed cells" if controls["armed"] else "paused")
    if not controls["autorun_global"]:
        head += " · autorun off for all"
    if run:
        head += f" · run {run['run_id']}: {len(run['done'])}/{len(run['cells'])} done"
    typer.secho(head, bold=True)
    for error in errors:
        typer.secho(f"  compile error (line {error['line']}): {error['message']}", fg="red")
    for cell in snap["cells"]:
        why = cell["reason"] or (cell["error"] or "").strip().partition("\n")[0]
        auto = "" if cell["autorun"]["effective"] else "  [autorun off]"
        line = f"  {cell['status']:<7} {cell['cell']:<24} {why}{auto}".rstrip()
        typer.secho(line, fg=_STATUS_COLORS.get(cell["status"]))


@app.command()
def head(
    cell: str,
    file: Path = FILE_ARG,
    rows: int = typer.Option(10, "--rows", "-n", min=1, max=live_protocol.MAX_ROWS),
    json_out: bool = typer.Option(False, "--json", help="Print one JSON document instead"),
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """The first rows of a cell's landed output, without running anything.

    Through the open TUI's session when one has the notebook (it reads every
    output, including the DuckDB files the TUI holds), else on a fresh
    connection: Parquet outputs, and databases reachable without an ATTACH.
    Exit 1 when the output can't be read (never landed, or not readable here)."""
    got: dict[str, Any] | None = None
    if probe(file) is not None:
        try:
            with LiveClient.connect(file) as client:
                got = {**client.call("head", cell=cell, rows=rows), "source": "live"}
        except LiveError as err:
            if err.code != live_protocol.NOT_READABLE:
                _live_fail(err, json_out)
    if got is None:
        got = _offline_head(file, cell, rows, set_, plugins, json_out)
    if json_out:
        typer.echo(json.dumps(got, default=str))
        return
    _print_table(got["columns"], got["rows"])


def _offline_head(
    file: Path, cell: str, rows: int, sets: list[str] | None, plugins: list[str] | None,
    json_out: bool,
) -> dict[str, Any]:
    import duckdb

    from .conduit import landed_relation, read_landed

    project = _project(file, sets, plugins)
    if cell not in project.cells:
        _live_fail(LiveError(live_protocol.UNKNOWN_CELL, f"unknown cell: {cell}"), json_out)
    relation = landed_relation(project, cell)
    conn = duckdb.connect()
    try:
        got = read_landed(conn, relation, rows)
    finally:
        conn.close()
    if got is None:
        why = "it lands nothing (effect-only)" if relation is None else (
            "it never landed, or it lands in a database only a run attaches "
            "(run it, or open it in the TUI and read it there)"
        )
        _live_fail(LiveError(live_protocol.NOT_READABLE, f"can't read {cell}: {why}"), json_out)
    return {"cell": cell, **got.to_dict(rows), "source": "offline"}


def _print_table(columns: list[str], rows: list[list[Any]]) -> None:
    """Rows as aligned text, each value cut at 40 characters."""
    cells = [[_cut(v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in cells]) for i, c in enumerate(columns)]
    typer.echo("  ".join(c.ljust(w) for c, w in zip(columns, widths)).rstrip())
    for row in cells:
        typer.echo("  ".join(v.ljust(w) for v, w in zip(row, widths)).rstrip())


def _cut(value: Any) -> str:
    text = "NULL" if value is None else str(value)
    return text if len(text) <= 40 else text[:39] + "…"


@app.command()
def skill(
    install: bool = typer.Option(
        False, "--install", help="Write it to DIR/.claude/skills/quicksql/SKILL.md"
    ),
    directory: Path = typer.Option(Path("."), "--dir", help="With --install: the project"),
    force: bool = typer.Option(False, "--force", help="With --install: overwrite an edited copy"),
) -> None:
    """The agent skill for quicksql notebooks (a Claude Code SKILL.md): the
    cell format, the offline loop, and working alongside an open TUI through
    `quicksql live`. Printed, or installed into a project."""
    from importlib.resources import files

    text = files("quicksql").joinpath("skill.md").read_text()
    if not install:
        typer.echo(text, nl=False)
        return
    target = directory / ".claude" / "skills" / "quicksql" / "SKILL.md"
    if target.exists() and target.read_text() != text and not force:
        typer.secho(f"{target} has edits of its own: pass --force to replace it", fg="red")
        raise typer.Exit(EXIT_FAILED)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)
    typer.echo(f"installed {target}")


# ---------- quicksql live: an agent alongside the open TUI ----------

live_app = typer.Typer(
    no_args_is_help=True,
    help="Talk to the TUI that has a notebook open: watch its runs, wait for a save's "
    "outcome, read outputs, and drive it as its keys do. Exit codes: 0 ok, 1 a cell or "
    "query failed, 2 usage, 3 compile error or unknown cell, 4 no TUI has the notebook "
    "(or a timeout), 5 refused (busy, or the file says otherwise).",
)
app.add_typer(live_app, name="live")

_EXIT_FOR = {
    live_protocol.NO_SESSION: EXIT_NO_SESSION,
    live_protocol.PROTOCOL_MISMATCH: EXIT_NO_SESSION,
    live_protocol.TIMEOUT: EXIT_NO_SESSION,
    live_protocol.EVENTS_LOST: EXIT_NO_SESSION,
    live_protocol.BUSY: EXIT_REFUSED,
    live_protocol.REFUSED: EXIT_REFUSED,
    live_protocol.UNKNOWN_CELL: EXIT_COMPILE,
    live_protocol.NO_PROJECT: EXIT_COMPILE,
    live_protocol.INVALID: 2,
    live_protocol.UNKNOWN_METHOD: 2,
    live_protocol.UNSUPPORTED: 2,
    live_protocol.NOT_READABLE: EXIT_FAILED,
    live_protocol.QUERY_FAILED: EXIT_FAILED,
    live_protocol.INTERNAL: EXIT_FAILED,
}

JSON_OPT = typer.Option(False, "--json", help="Print one JSON document on stdout instead")


def _live_fail(err: LiveError, json_out: bool) -> None:
    if json_out:
        typer.echo(json.dumps({"error": err.to_dict()}))
    else:
        typer.secho(f"{err.code}: {err.message}", fg="red", err=True)
    raise typer.Exit(_EXIT_FOR.get(err.code, EXIT_FAILED))


def _live_call(file: Path, json_out: bool, fn: Any) -> Any:
    """``fn(client)`` against the notebook's live session; a LiveError
    becomes its message (or JSON) and exit code."""
    try:
        with LiveClient.connect(file) as client:
            return fn(client)
    except LiveError as err:
        _live_fail(err, json_out)


def _say(doc: Any, json_out: bool, text: str) -> None:
    typer.echo(json.dumps(doc, default=str) if json_out else text)


def _controls_text(controls: dict[str, Any]) -> str:
    text = "armed: saves rerun changed cells" if controls["armed"] else "paused: saves don't rerun"
    if not controls["autorun_global"]:
        text += "; autorun off for all"
    if controls["autorun_off"]:
        text += f"; autorun off: {', '.join(controls['autorun_off'])}"
    return text


@live_app.command("arm")
def live_arm(file: Path = FILE_ARG, json_out: bool = JSON_OPT) -> None:
    """Arm watch reruns, as p does: a save reruns the cells it changed."""
    controls = _live_call(file, json_out, lambda c: c.call("arm"))
    _say(controls, json_out, _controls_text(controls))


@live_app.command("disarm")
def live_disarm(file: Path = FILE_ARG, json_out: bool = JSON_OPT) -> None:
    """Pause watch reruns: a save only recompiles."""
    controls = _live_call(file, json_out, lambda c: c.call("disarm"))
    _say(controls, json_out, _controls_text(controls))


@live_app.command("autorun")
def live_autorun(
    state: str = typer.Argument(..., help="on or off"),
    file: Path = FILE_ARG,
    select: Optional[list[str]] = typer.Option(None, "--select", "-s", help="These cells"),
    all_: bool = typer.Option(False, "--all", help="Every cell (the A key)"),
    json_out: bool = JSON_OPT,
) -> None:
    """Switch the session's autorun for cells (the a key) or for all (A).

    Only the file switches a cell on for good: `on` for a cell whose file says
    `autorun: false` is refused (exit 5) with the line to edit."""
    if state not in ("on", "off"):
        raise typer.BadParameter("on or off", param_hint="'STATE'")
    if not all_ and not select:
        raise typer.BadParameter("name cells with -s, or pass --all", param_hint="'--select'")
    params = {"on": state == "on", **({"all": True} if all_ else {"cells": list(select)})}
    controls = _live_call(file, json_out, lambda c: c.call("autorun", **params))
    _say(controls, json_out, _controls_text(controls))


@live_app.command("run")
def live_run(
    file: Path = FILE_ARG,
    select: Optional[list[str]] = typer.Option(None, "--select", "-s", help="Run these (+ upstreams)"),
    all_: bool = typer.Option(False, "--all", help="Run every cell, and arm (the R key)"),
    cancel: bool = typer.Option(False, "--cancel", help="Replace a run under way instead of refusing"),
    wait: bool = typer.Option(False, "--wait", help="Wait for the run and print its results"),
    timeout: float = typer.Option(600, "--timeout", help="With --wait: seconds to wait"),
    json_out: bool = JSON_OPT,
) -> None:
    """Run cells in the TUI, as r and R do.

    Refused (exit 5) while a run is under way, unless --cancel. With --wait, the same document as
    `quicksql run --json`, and exit 1 if a cell failed."""
    if not all_ and not select:
        raise typer.BadParameter("name cells with -s, or pass --all", param_hint="'--select'")
    started = time.perf_counter()

    def go(client: LiveClient) -> Any:
        params = {"all": True} if all_ else {"cells": list(select)}
        run_id = client.call("run", cancel=cancel, **params)["run_id"]
        if not wait:
            return {"run_id": run_id}
        return client.call("run.wait", run_id=run_id, timeout=timeout)

    got = _live_call(file, json_out, go)
    if not wait:
        _say(got, json_out, f"run {got['run_id']} started in the TUI")
        return
    counts = got["summary"]
    status = got["state"] if got["state"] != "finished" else ("failed" if counts["failed"] else "ok")
    if json_out:
        doc = {
            "file": str(file), "run_id": got["run_id"], "status": status, "summary": counts,
            "elapsed": round(time.perf_counter() - started, 4), "errors": [],
            "cells": got["cells"],
        }
        typer.echo(json.dumps(doc, default=str))
    else:
        _print_cells(got["cells"])
        typer.echo(f"  run {got['run_id']}: {status}")
    if status in ("replaced", "dropped"):
        raise typer.Exit(EXIT_REFUSED)
    if status == "failed":
        raise typer.Exit(EXIT_FAILED)


@live_app.command("cancel")
def live_cancel(file: Path = FILE_ARG, json_out: bool = JSON_OPT) -> None:
    """Stop the run under way after its current cell."""
    got = _live_call(file, json_out, lambda c: c.call("cancel"))
    if not got["cancelled"]:
        text = "no run under way"
    else:
        text = "cancelled" + (f"; won't run: {', '.join(got['not_run'])}" if got["not_run"] else "")
    _say(got, json_out, text)


@live_app.command("wait")
def live_wait(
    file: Path = FILE_ARG,
    timeout: float = typer.Option(600, "--timeout", help="Seconds to wait"),
    json_out: bool = JSON_OPT,
) -> None:
    """Wait for the outcome of the file as saved now (run it after a save).

    Returns once the TUI has finished reacting to the file as it is now
    (compiled it, and rerun what changed if armed), and prints the outcome:
    ran, paused, nothing_to_rerun or compile_failed. Exit 1 if a cell
    failed, 3 on a compile error."""
    try:
        h = file_hash(file)
    except OSError as exc:
        raise typer.BadParameter(str(exc), param_hint="'FILE'")
    got = _live_call(file, json_out, lambda c: c.call("wait", hash=h, timeout=timeout))
    if json_out:
        typer.echo(json.dumps(got, default=str))
    else:
        _print_outcome(got)
    if got["state"] == "compile_failed":
        raise typer.Exit(EXIT_COMPILE)
    if got["summary"]["failed"]:
        raise typer.Exit(EXIT_FAILED)


def _print_outcome(got: dict[str, Any]) -> None:
    state = got["state"]
    later = " (a later save superseded yours)" if got.get("superseded") else ""
    if state == "paused":
        changed = ", ".join(got["changed"]) or "nothing"
        typer.echo(f"paused{later}: changed {changed}; saves don't rerun "
                   "(`quicksql live arm`, or press p in the TUI)")
    elif state == "nothing_to_rerun":
        typer.echo(f"nothing to rerun{later}")
    elif state == "compile_failed":
        typer.secho(f"compile error{later}:", fg="red")
        for error in got["errors"]:
            where = f"line {error['line']}" if error.get("line") else "notebook"
            typer.secho(f"  {where}: {error['message']}", fg="red")
    else:
        counts = got["summary"]
        typer.echo(f"ran{later}: {counts['ok']} ok, {counts['failed']} failed, "
                   f"{counts['skipped']} skipped")
        _print_cells(got["results"])


@live_app.command("events")
def live_events(
    file: Path = FILE_ARG,
    follow: bool = typer.Option(False, "--follow", "-f", help="Keep streaming new events"),
    since: Optional[int] = typer.Option(None, "--since", help="Only events after this seq"),
    kinds: Optional[str] = typer.Option(None, "--kinds", help="Comma-separated kinds to keep"),
) -> None:
    """The session's events as NDJSON, one per line.

    Kinds: compiled,
    compile_failed, run_started, cell_started, cell_finished, run_finished,
    run_ended, settled, controls_changed, notebook_closed. Without --follow,
    the recent ones (after --since) and exit."""
    wanted = [k for k in (kinds or "").split(",") if k]

    def show(event: dict[str, Any]) -> None:
        if not wanted or event["kind"] in wanted:
            typer.echo(json.dumps(event, default=str))
            sys.stdout.flush()

    def go(client: LiveClient) -> None:
        if not follow:
            params = {} if since is None else {"seq": since}
            for event in client.call("events.since", **params)["events"]:
                show(event)
            return
        for event in client.events(since=since, kinds=wanted or None):
            show(event)

    try:
        _live_call(file, True, go)
    except KeyboardInterrupt:
        pass


@live_app.command("query")
def live_query(
    sql: str,
    file: Path = FILE_ARG,
    rows: int = typer.Option(20, "--rows", "-n", min=1, max=live_protocol.MAX_ROWS),
    json_out: bool = JSON_OPT,
) -> None:
    """One read-only SELECT through the TUI's session.

    Each landed cell is a view named after it: `SELECT status, count(*) FROM orders GROUP BY 1`.
    Anything but one SELECT (FROM, SUMMARIZE, DESCRIBE) is refused (exit 5)."""
    got = _live_call(file, json_out, lambda c: c.call("query", sql=sql, rows=rows))
    if json_out:
        typer.echo(json.dumps(got, default=str))
    else:
        _print_table(got["columns"], got["rows"])


@live_app.command("focus")
def live_focus(
    cell: str,
    file: Path = FILE_ARG,
    tab: Optional[str] = typer.Option(None, "--tab", help="sql, data, config, log or catalog"),
    json_out: bool = JSON_OPT,
) -> None:
    """Select a cell in the TUI (and show a tab) to point the person at it."""
    if tab is not None and tab not in ("sql", "data", "config", "log", "catalog"):
        raise typer.BadParameter("sql, data, config, log or catalog", param_hint="'--tab'")
    _live_call(file, json_out, lambda c: c.call("focus", cell=cell, tab=tab))
    _say({}, json_out, f"focused {cell}")


@live_app.command("notify")
def live_notify(
    message: str,
    file: Path = FILE_ARG,
    severity: str = typer.Option("information", "--severity", help="information, warning or error"),
    json_out: bool = JSON_OPT,
) -> None:
    """Show the person a toast in the TUI (and log it)."""
    _live_call(file, json_out, lambda c: c.call("notify", message=message, severity=severity))
    _say({}, json_out, "notified")


@app.command()
def tui(
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Open the interactive TUI (VisiData-style keys, reflect-only)."""
    try:
        from .tui import QsqlApp
    except ModuleNotFoundError as exc:
        if (exc.name or "").partition(".")[0] != "textual":
            raise  # a bug of ours, not the missing extra
        typer.secho("the TUI needs the 'tui' extra: pip install 'quicksql[tui]'", fg="red")
        raise typer.Exit(1)
    if file.exists():
        _project(file, set_, plugins)  # fail fast on compile errors
    QsqlApp(
        path=file, overrides=gather_overrides(list(set_ or [])), plugins=plugins or (),
        live=True,
    ).run()


@app.command()
def lsp() -> None:
    """Run the quicksql language server over stdio (linting + column completion).

    Point your editor's LSP client at ``quicksql lsp`` for a ``.qsql`` file — see
    editors/nvim/qsql.lua for a Neovim setup."""
    try:
        from .lsp.server import start_io
    except ImportError:
        typer.secho(
            "the language server needs the 'lsp' extra: pip install 'quicksql[lsp]'",
            fg="red",
        )
        raise typer.Exit(1)
    start_io()
