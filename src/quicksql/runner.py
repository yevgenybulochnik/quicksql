"""Run cells in topo order: per-cell executor + sink, wrapped by the plugin chain.

The base cell-runner does the real work (prepare sinks, execute, write, preview);
plugins overriding ``run`` wrap it as decorators, outermost = lowest priority.
Cell failures become ``RunResult(status="failed")`` inside the base runner so chain
plugins (retries, ...) see error results; a catch-all *outside* the chain turns
a buggy plugin into an error result too, and the run continues — except that
every cell downstream of a failure is skipped, never run on stale data.
"""

from __future__ import annotations

import shutil
import time
import traceback
from collections import Counter
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

import duckdb
import polars as pl

from .conduit import ConduitSession, conduit_target, ensure_extension, preview
from .config import make_sink
from .errors import ExecutorError, QsqlError, describe
from .executors.base import EngineSession, SessionLost, result_view
from .models import RenderedCell, RunContext, RunEvent, RunResult, Status
from .registry import Registries
from .state import (
    CellState,
    Landed,
    cell_inputs,
    current_fingerprints,
    fingerprint,
    read_state,
    state_file,
    write_state,
)

if TYPE_CHECKING:
    from .compiler import Project

Inner = Callable[[RenderedCell, RunContext], RunResult]


class RunSession:
    """Reusable conduit state across runs: connection, temp dir, extension cache,
    each cell's last status here (exact reruns rerun an upstream that wasn't ok),
    what each landed output was built from, and the fingerprints watch
    compares saves against.

    watch/TUI pass one session so reruns skip reconnect/re-LOAD; the conduit
    reconnects automatically when the project's target database changes.
    Without a session, run_project creates and closes a private one per run.
    """

    def __init__(self) -> None:
        self.conn: Any = None
        self.tmpdir: str | None = None
        self.loaded_extensions: set[str] = set()
        self.engine_sessions: dict[str, Any] = {}  # context key -> engine session
        self.materialized: set[str] = set()  # cells with a live temp table
        self.last_status: dict[str, Status] = {}  # cell -> its latest result's status
        self.landed: dict[str, Landed] = {}  # cell -> what its landed output was built from
        # cell -> the fingerprint this session last ran it with, or first saw it at
        self.seen: dict[str, str] = {}
        self._target: str | None = None
        self._state_for: Path | None = None  # the notebook whose state file seeded landed

    def conduit(self, project: Project) -> Any:
        target = conduit_target(project.config, project.root)
        if self.conn is None or target != self._target:
            if self.conn is not None:
                self.conn.close()
            self.conn = duckdb.connect(target)
            self._target = target
            self.loaded_extensions = set()
            self.materialized = set()  # conduit temps died with the connection
            self.engine_sessions = {  # and the sessions on it
                key: live for key, live in self.engine_sessions.items()
                if not isinstance(live, ConduitSession)
            }
        return self.conn

    def load_state(self, project: Project) -> None:
        """Seed ``landed`` from the notebook's state file, once: outputs an
        earlier session landed are what this one's cells read."""
        if project.path is None or self._state_for == project.path:
            return
        self._state_for = project.path
        for name, cell in read_state(project).items():
            if cell.landed is not None:
                self.landed.setdefault(name, cell.landed)

    def see(self, project: Project) -> None:
        """Take each cell's current fingerprint as the baseline watch compares
        saves against, where this session hasn't run or seen the cell yet."""
        self.load_state(project)
        for name, fp in current_fingerprints(project, self.landed).items():
            self.seen.setdefault(name, fp)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None
            self._target = None
        for session in self.engine_sessions.values():
            close = getattr(session, "close", None)
            if callable(close):
                close()
        self.engine_sessions = {}
        self.materialized = set()
        self.last_status = {}
        self.landed = {}
        self.seen = {}
        self._state_for = None
        if self.tmpdir:
            shutil.rmtree(self.tmpdir, ignore_errors=True)
            self.tmpdir = None
        self.loaded_extensions = set()


def _selection(project: Project, select: list[str] | None, closure: bool) -> list[str]:
    if select is None:
        return list(project.order)
    unknown = [s for s in select if s not in project.cells]
    if unknown:
        from .errors import ConfigError

        raise ConfigError(f"unknown cell(s) in --select: {', '.join(unknown)}")
    wanted: set[str] = set(select)
    if closure:  # selected cells plus their upstream closure
        stack = list(select)
        while stack:
            name = stack.pop()
            wanted.add(name)
            stack.extend(u for u in project.cells[name].depends_on if u not in wanted)
    return [n for n in project.order if n in wanted]


def _expand_stale_upstreams(project: Project, names: list[str], session: RunSession) -> list[str]:
    """Exact-set runs (watch/TUI) still need, transitively, the upstreams whose
    output this session can't vouch for: a same-context upstream whose temp
    table isn't live (landed artifacts can't stand in for an in-engine temp
    reference), and any upstream whose last result here wasn't ok (its landed
    output is stale or missing). Autorun doesn't apply: rerunning is the honest
    way to fresh data. An upstream never run in this session is trusted, as before."""
    wanted = set(names)
    stack = list(names)
    while stack:
        cell = project.cells[stack.pop()]
        for upstream in cell.depends_on:  # ref() and @depends_on edges
            missing_temp = upstream in cell.context_refs and upstream not in session.materialized
            not_ok = session.last_status.get(upstream, "ok") != "ok"
            if upstream not in wanted and (missing_temp or not_ok):
                wanted.add(upstream)
                stack.append(upstream)
    return [n for n in project.order if n in wanted]


class StatementError(ExecutorError):
    """One of a cell's statements failed: ``index`` of ``count``, from 1. The
    message numbers it only when there's more than one to tell apart."""

    def __init__(self, index: int, count: int, cause: Exception) -> None:
        self.index = index
        detail = error_message(cause)
        super().__init__(f"statement {index} of {count}: {detail}" if count > 1 else detail)


def _context_key(cell: RenderedCell, registries: Registries) -> str:
    return registries.executors.get(cell.engine).context_key(cell.config)


def _context_session(cell: RenderedCell, ctx: RunContext) -> tuple[EngineSession, bool]:
    """The cell's context session, opened on the context's first cell and kept
    by the run session; without one, a private session the caller closes."""
    executor = ctx.registries.executors.get(cell.engine)
    if ctx.session is None:
        return executor.open_session(cell.config, ctx), True
    key = _context_key(cell, ctx.registries)
    if key not in ctx.session.engine_sessions:
        ctx.session.engine_sessions[key] = executor.open_session(cell.config, ctx)
    return ctx.session.engine_sessions[key], False


def execute_cell(cell: RenderedCell, ctx: RunContext, land: bool = True) -> str:
    """The cell protocol, once for every engine; returns the conduit view the
    sink lands from, or '' when the cell lands nothing.

    Setup statements run for effect; the terminal statement is the result. A
    cell some same-context cell refs materializes it as a session temp, once,
    and lands by reading the temp back; any other cell extracts it directly.
    A cell whose sink lands nothing runs every statement for effect.
    ``land=False`` stops after the temp: its landed output already stands.
    """
    session, private = _context_session(cell, ctx)
    lands_output = ctx.registries.sinks.get(cell.sink_type).lands_output
    scoped = cell.reffed_in_context or bool(cell.context_refs)
    statements = ctx.registries.executors.get(cell.engine).split_statements(cell.sql)
    *setup, final = statements

    def numbered(index: int, call: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return call(*args, **kwargs)
        except SessionLost:
            raise  # nothing ran: the runner reopens the session and retries
        except Exception as exc:
            raise StatementError(index, len(statements), exc) from exc

    try:
        for index, stmt in enumerate(setup if lands_output else statements, start=1):
            numbered(index, session.execute, stmt, scoped=scoped)
        if not lands_output:
            return ""
        last = len(statements)
        if cell.reffed_in_context:
            final = numbered(last, session.materialize, cell.name, final)
            if ctx.session is not None:
                ctx.session.materialized.add(cell.name)
            ctx.emit("cell_step", cell.name, "materialized temp for downstream refs")
        if not land:
            return ""
        return numbered(last, session.extract, final, result_view(cell.name), ctx, scoped=scoped)
    finally:
        if private:
            session.close()


def _forget_context(project: Project, session: RunSession, key: str) -> None:
    """Close and drop a context's session. Its temps went with it, so they
    leave session.materialized, and an exact rerun re-materializes them."""
    gone = session.engine_sessions.pop(key, None)
    if gone is not None:
        with suppress(Exception):  # it may be dead already
            gone.close()
    session.materialized -= {
        n for n in session.materialized
        if n in project.cells and _context_key(project.cells[n], project.registries) == key
    }


def _context_upstreams(project: Project, cell: RenderedCell) -> list[str]:
    """The same-context cells whose temps ``cell`` reads, transitively."""
    needed: set[str] = set()
    stack = list(cell.context_refs)
    while stack:
        name = stack.pop()
        if name not in needed:
            needed.add(name)
            stack.extend(project.cells[name].context_refs)
    return [n for n in project.order if n in needed]


def _restore_temps(project: Project, cell: RenderedCell, ctx: RunContext) -> None:
    """Re-create the same-context temps ``cell`` reads that the run session no
    longer holds: a lost session took every temp in it, also those of cells
    that ran before the loss. A no-op otherwise, since upstreams run first."""
    if ctx.session is None:
        return
    missing = [n for n in _context_upstreams(project, cell) if n not in ctx.session.materialized]
    if missing:
        ctx.emit("cell_step", cell.name, f"re-materializing {', '.join(map(repr, missing))}")
    for name in missing:
        execute_cell(project.cells[name], ctx, land=False)


def _retry_in_a_new_session(project: Project, cell: RenderedCell, ctx: RunContext) -> str:
    """The engine rejected a statement because the context's session is gone.
    Nothing ran, so reopen the session, re-create the temps the cell reads,
    and run it once more; losing the new session too fails the cell."""
    key = _context_key(cell, project.registries)
    _forget_context(project, ctx.session, key)
    ctx.emit("cell_step", cell.name, f"{key} session lost: reopening")
    _restore_temps(project, cell, ctx)
    return execute_cell(cell, ctx)


def _blame_the_query(cell: RenderedCell, ctx: RunContext) -> None:
    """Landing failed. A lazily extracted result (the conduit's view) runs the
    cell's terminal query inside the sink's statement, so a runtime error in it
    (a bad cast) reads as the sink's and quotes the sink's SQL. Run the query
    alone: an error of its own is the cell's, numbered and quoting its SQL;
    none, and the sink's error stands."""
    if cell.reffed_in_context or not ctx.registries.sinks.get(cell.sink_type).lands_output:
        return  # landed from a temp that already ran it, or nothing to land
    session, private = _context_session(cell, ctx)
    statements = ctx.registries.executors.get(cell.engine).split_statements(cell.sql)
    try:
        session.check(statements[-1])
    except Exception as exc:
        raise StatementError(len(statements), len(statements), exc) from exc
    finally:
        if private:
            session.close()


def _drop_stale_temp(cell: RenderedCell, session: RunSession, registries: Registries) -> None:
    """A cell that didn't come out ok may have a temp left from an earlier
    run; drop it so nothing reads it stale."""
    live = (
        session.engine_sessions.get(_context_key(cell, registries))
        if cell.reffed_in_context
        else None
    )
    if live is not None:
        with suppress(Exception):  # a dead session took its temps along
            live.drop(cell.name)


def _drop_dead_sessions(project: Project, session: RunSession) -> list[str]:
    """Before the run plans its cells: a session known dead is dropped, so its
    context's next cell opens a fresh one, and its temps leave
    session.materialized, so an exact rerun re-materializes them. Returns the
    dropped contexts' keys."""
    dead = [key for key, live in session.engine_sessions.items() if not live.alive()]
    for key in dead:
        _forget_context(project, session, key)
    return dead


def base_runner(project: Project) -> Inner:
    """The innermost cell-runner; `project` rides in via closure so the chain
    signature stays (cell, ctx)."""

    def run_cell(cell: RenderedCell, ctx: RunContext) -> RunResult:
        started = time.perf_counter()
        try:
            # cell-declared extensions load via the Extensions plugin's
            # before_execute; sinks are core mechanism, so theirs load here
            ctx.emit("cell_step", cell.name, "preparing sinks")
            sink = make_sink(cell.config, ctx.root, project.registries)
            for ext in sink.requires:
                ensure_extension(ctx, ext, cell.name)
            for upstream in cell.depends_on:
                up_sink = make_sink(project.cells[upstream].config, ctx.root, project.registries)
                for ext in up_sink.requires:
                    ensure_extension(ctx, ext, cell.name)
                up_sink.prepare(ctx.conn)
            sink.prepare(ctx.conn)
            ctx.emit("cell_step", cell.name, f"executing on {cell.engine}")
            try:
                _restore_temps(project, cell, ctx)
                view = execute_cell(cell, ctx)
            except SessionLost:
                if ctx.session is None:
                    raise
                view = _retry_in_a_new_session(project, cell, ctx)
            ctx.emit("cell_step", cell.name, f"landing via {cell.sink_type}")
            try:
                rows, target = sink.write(cell, view, ctx.conn)
            except duckdb.Error:
                _blame_the_query(cell, ctx)
                raise
            # effect-only sinks (none) land nothing, so there's nothing to read back
            head = preview(ctx.conn, sink.ref_expr(cell.name)) if sink.lands_output else pl.DataFrame()
            ctx.emit("cell_step", cell.name, f"preview cached ({head.height} rows)")
            return RunResult(
                cell=cell.name,
                status="ok",
                rows=rows,
                target=target,
                elapsed=time.perf_counter() - started,
                preview=head,
            )
        except Exception as exc:
            return _failed(cell.name, exc, elapsed=time.perf_counter() - started)

    return run_cell


def build_chain(project: Project) -> Inner:
    runner: Inner = base_runner(project)
    for plug in reversed(project.registries.plugins.chain()):
        runner = _wrap(plug, runner)
    return runner


def _wrap(plug: Any, inner: Inner) -> Inner:
    def wrapped(cell: RenderedCell, ctx: RunContext) -> RunResult:
        return plug.run(cell, ctx, inner)

    return wrapped


def run_project(
    project: Project,
    select: list[str] | None = None,
    closure: bool = True,
    session: RunSession | None = None,
    on_event: Callable[[RunEvent], None] | None = None,
) -> list[RunResult]:
    owns_session = session is None
    session = session or RunSession()
    ctx = RunContext(
        conn=session.conduit(project),
        root=project.root,
        config=project.config,
        tmpdir=session.tmpdir,
        ext_cache=session.loaded_extensions,
        session=session,
        on_event=on_event,
        registries=project.registries,
    )
    chain = build_chain(project)
    session.load_state(project)
    reopened = _drop_dead_sessions(project, session)
    names = _selection(project, select, closure)
    if select is not None and not closure:
        names = _expand_stale_upstreams(project, names, session)
    results: list[RunResult] = []
    run_started = time.perf_counter()
    try:
        ctx.emit(
            "run_started",
            detail=f"{len(names)} cell(s): {', '.join(names)}",
            cells=tuple(names),
        )
        for key in reopened:
            ctx.emit("note", detail=f"{key}: session lost; reopening it")
        _notify(ctx, "before_run", lambda p: p.before_run(project, ctx))
        # cell that didn't come out ok -> the failed cells behind it (itself if
        # it failed); depends_on carries both ref() and @depends_on edges
        failed_roots: dict[str, list[str]] = {}
        for name in names:
            cell = project.cells[name]
            roots = list(
                dict.fromkeys(r for up in cell.depends_on for r in failed_roots.get(up, ()))
            )
            if roots:  # its inputs are stale or missing: don't run it at all
                result = _skipped(name, roots)
            else:
                ctx.emit("cell_started", name, f"{cell.engine} → {cell.sink_type}")
                # what it's built from: the upstream outputs as they've landed now
                own, upstreams = cell_inputs(cell, project.config, session.landed)
                built_from = fingerprint(own, upstreams)
                try:
                    result = chain(cell, ctx)
                except Exception as exc:  # a buggy plugin must not kill the run
                    result = _failed(name, exc)
                session.seen[name] = built_from
                if result.ok:
                    session.landed[name] = Landed(built_from, own, upstreams)
            if not result.ok:
                failed_roots[name] = roots or [name]
                # any temp table it left is stale; forgetting it makes a later
                # exact rerun (watch/TUI) re-include the cell
                session.materialized.discard(name)
                _drop_stale_temp(cell, session, project.registries)
            # a later exact rerun pulls the cell back in unless this says ok
            session.last_status[name] = result.status
            results.append(result)
            ctx.emit("cell_finished", name, result=result)
        _notify(ctx, "after_run", lambda p: p.after_run(project, results))
        _save_state(project, session, ctx)
        # snapshot: a failing on_event appends to ctx.log, so iterating the
        # live list would grow it forever
        for line in list(ctx.log):  # plugin lifecycle failures were invisible before
            ctx.emit("note", detail=line)
        counts = Counter(r.status for r in results)
        ctx.emit(  # terminal: consumers may stop listening after this
            "run_finished",
            detail=f"{counts['ok']} ok, {counts['failed']} failed, {counts['skipped']} skipped"
            f" in {time.perf_counter() - run_started:.1f}s",
        )
    finally:
        session.tmpdir = ctx.tmpdir  # adopt a lazily-created temp dir
        if owns_session:
            session.close()
    return results


def _save_state(project: Project, session: RunSession, ctx: RunContext) -> None:
    """Record each cell's last status and what its landed output was built
    from; a cell this session hasn't run keeps what the file had."""
    if project.path is None:
        return
    before = read_state(project)
    cells = {}
    for name in project.order:
        status = session.last_status.get(name) or (before[name].status if name in before else None)
        landed = session.landed.get(name)
        if status is not None or landed is not None:
            cells[name] = CellState(status, landed)
    try:
        write_state(project, cells)
    except OSError as exc:
        ctx.log.append(f"couldn't write {state_file(project.path)}: {exc}")


def _failed(name: str, exc: Exception, elapsed: float = 0.0) -> RunResult:
    """Call from inside the ``except``: the traceback is the one being handled."""
    return RunResult(
        cell=name,
        status="failed",
        elapsed=elapsed,
        error=error_message(exc),
        traceback=traceback.format_exc(),
        statement=exc.index if isinstance(exc, StatementError) else None,
    )


def error_message(exc: Exception) -> str:
    """How a failure reads: our errors and duckdb's ("Catalog Error: ...")
    describe themselves; anything else, e.g. a plugin bug, needs its type to
    read: "KeyError: 'x'"."""
    return describe(exc, (QsqlError, duckdb.Error))


def _skipped(name: str, roots: list[str]) -> RunResult:
    noun = "upstream" if len(roots) == 1 else "upstreams"
    quoted = ", ".join(f"'{root}'" for root in roots)
    return RunResult(cell=name, status="skipped", error=f"{noun} {quoted} failed")


def _notify(ctx: RunContext, hook: str, call: Callable[[Any], None]) -> None:
    """Fire-and-forget lifecycle notifications: failures are logged, never fatal."""
    for plug in ctx.registries.plugins.overriding(hook):
        try:
            call(plug)
        except Exception:
            ctx.log.append(f"plugin {plug.name!r} {hook} failed:\n{traceback.format_exc()}")
