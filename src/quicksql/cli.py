"""The quicksql CLI: init / run / watch / tui / list / show / compile."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import json
import sys
import textwrap
import time
from collections import Counter
from pathlib import Path
from typing import Any, Optional

import typer

from .bootstrap import load_builtins
from .compiler import Project, compile_file
from .errors import ConfigErrorGroup, QsqlError
from .overrides import gather_overrides
from .runner import _message, run_project
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


def _load_plugin_module(spec: str) -> None:
    if spec.endswith(".py"):
        path = Path(spec).resolve()
        name = f"qsql_user_{path.stem}_{hashlib.sha1(str(path).encode()).hexdigest()[:8]}"
        if name in sys.modules:
            return
        mod_spec = importlib.util.spec_from_file_location(name, path)
        if mod_spec is None or mod_spec.loader is None:
            raise QsqlError(f"cannot load plugin module {spec!r}")
        module = importlib.util.module_from_spec(mod_spec)
        sys.modules[name] = module
        mod_spec.loader.exec_module(module)
    else:
        importlib.import_module(spec)


def _compile(file: Path, sets: list[str] | None, plugins: list[str] | None) -> Project:
    """Load plugins and compile, warnings to stderr; a QsqlError propagates."""
    load_builtins()
    rc = file.parent / "quicksqlrc.py"
    if rc.exists():
        _load_plugin_module(str(rc))
    for spec in plugins or []:
        _load_plugin_module(spec)
    project = compile_file(file, overrides=gather_overrides(list(sets or [])))
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
    failed = False
    for r in results:
        if r.ok:
            typer.secho(
                f"  ok {r.cell:<24} {r.rows if r.rows is not None else '?':>8} rows"
                f"  {r.elapsed * 1000:8.1f} ms  -> {r.target}",
                fg="green",
            )
        elif r.status == "skipped":
            typer.secho(f"  skip {r.cell}: {r.error}", fg="yellow")
        else:
            failed = True
            first, *rest = (r.error or "unknown error").strip().splitlines()
            typer.secho(f"  FAIL {r.cell}: {first}", fg="red")
            if rest:
                # e.g. duckdb's LINE/caret context; one indent for the whole
                # block keeps the caret under what it points at
                typer.secho(textwrap.indent("\n".join(rest), "      "), fg="red")
    return failed


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
        status, errors = "compile_error", _error_dicts(exc)
    except Exception as exc:  # before any cell ran: a plugin that won't import, a locked db
        status, errors = "error", [{"cell": None, "line": None, "message": _message(exc)}]
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


def _error_dicts(exc: QsqlError) -> list[dict[str, Any]]:
    if isinstance(exc, ConfigErrorGroup):
        return [{"cell": e.cell, "line": e.line, "message": e.message} for e in exc.errors]
    return [{"cell": None, "line": None, "message": str(exc)}]


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
    from .registry import PLUGINS

    project = _project(file, set_, plugins)

    def names(plugs: list) -> str:
        return " → ".join(f"{p.name}({p.priority})" for p in plugs) or "(none)"

    typer.echo(f"run chain:      {names(PLUGINS.chain())} → [execute on engine, land via sink]")
    for hook in (
        "render_context", "after_render", "resolve_engine", "resolve_sink",
        "collect_edges", "sink_config", "should_rerun",
        "after_compile", "before_run", "after_run",
    ):
        typer.echo(f"{hook + ':':<16}{names(PLUGINS.overriding(hook))}")
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

    project = _project(file, set_, plugins)  # load plugins + fail fast on compile errors
    typer.echo(f"watching {file} — Ctrl+C to stop")
    try:
        for project, event in watch_events(
            file, overrides=gather_overrides(list(set_ or [])), on_event=_live_event,
            project=project,
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
    fingerprinted, so a changed CSV or warehouse table leaves a cell ok."""
    if json_out:  # like run --json: stdout gets one document, whatever goes wrong
        try:
            doc = {"file": str(file), "errors": [], "cells": [
                vars(row) for row in statuses(_compile(file, set_, plugins))
            ]}
        except QsqlError as exc:
            doc = {"file": str(file), "errors": _error_dicts(exc), "cells": []}
        typer.echo(json.dumps(doc, indent=2 if sys.stdout.isatty() else None))
        if doc["errors"]:
            raise typer.Exit(EXIT_COMPILE)
        return
    rows = statuses(_project(file, set_, plugins))
    for row in rows:
        line = f"  {row.status:<7} {row.cell:<24} {row.reason}".rstrip()
        typer.secho(line, fg=_STATUS_COLORS.get(row.status))


@app.command()
def tui(
    file: Path = FILE_ARG,
    set_: Optional[list[str]] = SET_OPT,
    plugins: Optional[list[str]] = PLUGINS_OPT,
) -> None:
    """Open the interactive TUI (VisiData-style keys, reflect-only)."""
    if file.exists():
        _project(file, set_, plugins)  # load plugins + fail fast on compile errors
    else:  # the TUI opens its notebook picker; plugins still need loading
        load_builtins()
        rc = file.parent / "quicksqlrc.py"
        if rc.exists():
            _load_plugin_module(str(rc))
        for spec in plugins or []:
            _load_plugin_module(spec)
    from .tui import QsqlApp

    QsqlApp(path=file, overrides=gather_overrides(list(set_ or []))).run()


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
