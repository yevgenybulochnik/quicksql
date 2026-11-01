"""Watch a quicksql file: recompile on save, rerun changed cells + their dependents.

A cell changed when its fingerprint (state.py: SQL, engine, context, the config
fields that affect results, its upstreams' landed outputs) differs from the one
the session last ran it with, so a config edit reruns it and toggling autorun or
tags doesn't. Cells with autorun:false are skipped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterator, Sequence

from .bootstrap import compile_file
from .compiler import Project
from .errors import QsqlError
from .graph import downstream
from .models import RunEvent, RunResult
from .registry import PluginRegistry
from .runner import RunSession, run_project
from .state import current_fingerprints


def touches(changes: set[tuple[Any, str]], path: Path) -> bool:
    """True when a watchfiles change-set includes our file.

    We watch the *parent directory*, not the file: editors that save atomically
    (nvim, vim, ...) replace the file's inode on every write, and a watch on
    the file path itself goes deaf after the first save.
    """
    target = str(path.resolve())
    return any(str(Path(changed).resolve()) == target for _, changed in changes)


def should_rerun(cell: Any, plugins: PluginRegistry) -> bool:
    """First-result decision (the Autorun plugin answers); default: rerun."""
    answer = plugins.first_result("should_rerun", cell)
    return True if answer is None else bool(answer)


def plan_rerun(project: Project, session: RunSession) -> list[str]:
    """Cells whose fingerprint differs from the one the session last ran them
    with or first saw them at (a new cell counts as changed), plus downstream,
    propagating only through cells that rerun: one that won't (autorun: false)
    keeps its old output, so a cell reading only it has nothing new to read."""
    session.load_state(project)
    current = current_fingerprints(project, session.landed)
    changed = {name for name, fp in current.items() if session.seen.get(name) != fp}
    return downstream(
        list(project.cells), project.edges, changed,
        through=lambda n: should_rerun(project.cells[n], project.registries.plugins),
    )


def run_changed(
    path: Path | str,
    session: RunSession,
    overrides: dict[str, Any] | None = None,
    on_event: Callable[[RunEvent], None] | None = None,
    plugins: Sequence[str] = (),
) -> tuple[Project, list[RunResult]]:
    """Recompile with the notebook's own registries (its rc reloads if edited)
    and rerun what changed."""
    project = compile_file(path, overrides, plugins)
    to_run = plan_rerun(project, session)
    results = (
        run_project(project, select=to_run, closure=False, session=session, on_event=on_event)
        if to_run
        else []
    )
    return project, results


def watch_events(
    path: Path | str,
    overrides: dict[str, Any] | None = None,
    stop_event: Any = None,
    on_event: Callable[[RunEvent], None] | None = None,
    project: Project | None = None,
    plugins: Sequence[str] = (),
) -> Iterator[tuple[Project | None, list[RunResult] | QsqlError]]:
    """Initial full autorun pass, then one event per file save.

    Yields (project, results); on compile failure yields (None, error) and
    keeps watching. Pass the already-compiled ``project`` to skip compiling
    again at startup; a threading.Event as stop_event to end the loop;
    on_event to stream RunEvents live.
    """
    import watchfiles

    path = Path(path)
    session = RunSession()  # one conduit for the whole watch, not per rerun
    try:
        project = project or compile_file(path, overrides, plugins)
        autorun = [
            n for n in project.order if should_rerun(project.cells[n], project.registries.plugins)
        ]
        results = (
            run_project(project, select=autorun, closure=False, session=session, on_event=on_event)
            if autorun
            else []
        )
        session.see(project)  # the baseline for cells the pass didn't run
        yield project, results
        for changes in watchfiles.watch(path.parent, stop_event=stop_event):
            if not touches(changes, path):
                continue
            try:
                project, results = run_changed(
                    path, session, overrides, on_event=on_event, plugins=plugins
                )
            except QsqlError as exc:
                yield None, exc
                continue
            yield project, results
    finally:
        session.close()
