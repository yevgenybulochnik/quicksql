"""Watch a quicksql file: recompile on save, rerun changed cells + their dependents.

Change detection hashes SQL bodies only, so config-only edits don't trigger
reruns (known limitation). Cells with autorun:false are skipped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterator

from .compiler import Project, compile_file
from .errors import QsqlError
from .graph import downstream
from .models import RunEvent, RunResult
from .runner import RunSession, run_project


def hashes_of(project: Project) -> dict[str, str]:
    return {name: cell.hash for name, cell in project.cells.items()}


def touches(changes: set[tuple[Any, str]], path: Path) -> bool:
    """True when a watchfiles change-set includes our file.

    We watch the *parent directory*, not the file: editors that save atomically
    (nvim, vim, ...) replace the file's inode on every write, and a watch on
    the file path itself goes deaf after the first save.
    """
    target = str(path.resolve())
    return any(str(Path(changed).resolve()) == target for _, changed in changes)


def should_rerun(cell: Any) -> bool:
    """First-result decision (the Autorun plugin answers); default: rerun."""
    from .registry import PLUGINS

    answer = PLUGINS.first_result("should_rerun", cell)
    return True if answer is None else bool(answer)


def plan_rerun(old_hashes: dict[str, str], project: Project) -> list[str]:
    """Changed cells (new ones count) plus downstream, propagating only through
    cells that rerun: one that won't (autorun: false) keeps its old output, so
    a cell reading only it has nothing new to read."""
    changed = {n for n, c in project.cells.items() if old_hashes.get(n) != c.hash}
    return downstream(
        list(project.cells), project.edges, changed,
        through=lambda n: should_rerun(project.cells[n]),
    )


def config_only_changes(old: Project, new: Project) -> list[str]:
    """Cells whose SQL body is unchanged but whose config differs — the body
    hash is blind to these, so no rerun fires; surface them in logs instead.
    Compared via model_dump: each compile composes a fresh CellConfig class,
    so pydantic instance equality is always False across compiles."""
    return [
        n
        for n, c in new.cells.items()
        if n in old.cells
        and old.cells[n].hash == c.hash
        and old.cells[n].config.model_dump() != c.config.model_dump()
    ]


def run_changed(
    path: Path | str,
    old_hashes: dict[str, str],
    overrides: dict[str, Any] | None = None,
    session: RunSession | None = None,
    on_event: Callable[[RunEvent], None] | None = None,
) -> tuple[Project, list[RunResult]]:
    project = compile_file(path, overrides)
    to_run = plan_rerun(old_hashes, project)
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
) -> Iterator[tuple[Project | None, list[RunResult] | QsqlError]]:
    """Initial full autorun pass, then one event per file save.

    Yields (project, results); on compile failure yields (None, error) and
    keeps watching. Pass a threading.Event as stop_event to end the loop;
    pass on_event to stream RunEvents (plus config-only-change notes) live.
    """
    import watchfiles

    path = Path(path)
    session = RunSession()  # one conduit for the whole watch, not per rerun
    try:
        project = compile_file(path, overrides)
        autorun = [n for n in project.order if should_rerun(project.cells[n])]
        results = (
            run_project(project, select=autorun, closure=False, session=session, on_event=on_event)
            if autorun
            else []
        )
        yield project, results
        hashes = hashes_of(project)
        for changes in watchfiles.watch(path.parent, stop_event=stop_event):
            if not touches(changes, path):
                continue
            previous = project
            try:
                project, results = run_changed(
                    path, hashes, overrides, session=session, on_event=on_event
                )
            except QsqlError as exc:
                yield None, exc
                continue
            if on_event is not None:
                cfg_only = config_only_changes(previous, project)
                if cfg_only:
                    on_event(
                        RunEvent("note", detail=f"config changed (no rerun): {', '.join(cfg_only)}")
                    )
            hashes = hashes_of(project)
            yield project, results
    finally:
        session.close()
