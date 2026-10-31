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
from typing import TYPE_CHECKING, Any, Callable

import duckdb
import polars as pl

from .config import input_spec
from .errors import QsqlError, describe
from .models import RenderedCell, RunContext, RunEvent, RunResult, Status
from .registry import EXECUTORS, PLUGINS
from .sinks import make_sink

if TYPE_CHECKING:
    from .compiler import Project

Inner = Callable[[RenderedCell, RunContext], RunResult]


def _preview(conn: Any, ref_expr: str, limit: int = 100) -> pl.DataFrame:
    """Read the landed output back through ref_expr into a small Polars frame.
    Built row-wise: duckdb's .pl()/.arrow() need pyarrow, an optional extra."""
    rel = conn.sql(f"SELECT * FROM {ref_expr} LIMIT {limit}")
    return pl.DataFrame(rel.fetchall(), schema=rel.columns, orient="row")


def _load_extension(conn: Any, ext: str) -> None:
    try:
        conn.execute(f"LOAD {ext}")
    except duckdb.Error:
        conn.execute(f"INSTALL {ext}")
        conn.execute(f"LOAD {ext}")


def _load_ext_cached(ctx: RunContext, ext: str, cell: str | None = None) -> None:
    if ext in ctx.ext_cache:
        return
    # first load per session — may INSTALL (the slow, network path)
    ctx.emit("cell_step", cell, f"loading duckdb extension {ext}")
    _load_extension(ctx.conn, ext)
    ctx.ext_cache.add(ext)


class RunSession:
    """Reusable conduit state across runs: connection, temp dir, extension cache,
    and each cell's last status here (exact reruns rerun an upstream that wasn't ok).

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
        self._target: str | None = None

    def conduit(self, project: Project) -> Any:
        path = input_spec(project.config, "duckdb").get("path")
        target = str(project.root / path) if path else ":memory:"
        if self.conn is None or target != self._target:
            if self.conn is not None:
                self.conn.close()
            self.conn = duckdb.connect(target)
            self._target = target
            self.loaded_extensions = set()
            self.materialized = set()  # conduit temps died with the connection
        return self.conn

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


def base_runner(project: Project) -> Inner:
    """The innermost cell-runner; `project` rides in via closure so the chain
    signature stays (cell, ctx)."""

    def run_cell(cell: RenderedCell, ctx: RunContext) -> RunResult:
        started = time.perf_counter()
        try:
            # cell-declared extensions load via the Extensions plugin's
            # before_execute; sinks are core mechanism, so theirs load here
            ctx.emit("cell_step", cell.name, "preparing sinks")
            sink = make_sink(cell.config, ctx.root)
            for ext in sink.requires:
                _load_ext_cached(ctx, ext, cell.name)
            for upstream in cell.depends_on:
                up_sink = make_sink(project.cells[upstream].config, ctx.root)
                for ext in up_sink.requires:
                    _load_ext_cached(ctx, ext, cell.name)
                up_sink.prepare(ctx.conn)
            sink.prepare(ctx.conn)
            ctx.emit("cell_step", cell.name, f"executing on {cell.engine}")
            view = EXECUTORS.get(cell.engine).execute(cell, ctx)
            if cell.reffed_in_context and ctx.session is not None:
                ctx.session.materialized.add(cell.name)
                ctx.emit("cell_step", cell.name, "materialized temp for downstream refs")
            ctx.emit("cell_step", cell.name, f"landing via {cell.sink_type}")
            rows, target = sink.write(cell, view, ctx.conn)
            # effect-only sinks (none) land nothing, so there's nothing to read back
            preview = _preview(ctx.conn, sink.ref_expr(cell.name)) if sink.lands_output else pl.DataFrame()
            ctx.emit("cell_step", cell.name, f"preview cached ({preview.height} rows)")
            return RunResult(
                cell=cell.name,
                status="ok",
                rows=rows,
                target=target,
                elapsed=time.perf_counter() - started,
                preview=preview,
            )
        except Exception as exc:
            return _failed(cell.name, exc, elapsed=time.perf_counter() - started)

    return run_cell


def build_chain(project: Project) -> Inner:
    runner: Inner = base_runner(project)
    for plug in reversed(PLUGINS.chain()):
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
    )
    chain = build_chain(project)
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
                try:
                    result = chain(cell, ctx)
                except Exception as exc:  # a buggy plugin must not kill the run
                    result = _failed(name, exc)
            if not result.ok:
                failed_roots[name] = roots or [name]
                # any temp table it left is stale; dropping it makes a later
                # exact rerun (watch/TUI) re-include the cell
                session.materialized.discard(name)
            # a later exact rerun pulls the cell back in unless this says ok
            session.last_status[name] = result.status
            results.append(result)
            ctx.emit("cell_finished", name, result=result)
        _notify(ctx, "after_run", lambda p: p.after_run(project, results))
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


def _failed(name: str, exc: Exception, elapsed: float = 0.0) -> RunResult:
    """Call from inside the ``except``: the traceback is the one being handled."""
    return RunResult(
        cell=name,
        status="failed",
        elapsed=elapsed,
        error=_message(exc),
        traceback=traceback.format_exc(),
    )


def _message(exc: Exception) -> str:
    """Our errors and duckdb's ("Catalog Error: ...") describe themselves;
    anything else, e.g. a plugin bug, needs its type to read: "KeyError: 'x'"."""
    return describe(exc, (QsqlError, duckdb.Error))


def _skipped(name: str, roots: list[str]) -> RunResult:
    noun = "upstream" if len(roots) == 1 else "upstreams"
    quoted = ", ".join(f"'{root}'" for root in roots)
    return RunResult(cell=name, status="skipped", error=f"{noun} {quoted} failed")


def _notify(ctx: RunContext, hook: str, call: Callable[[Any], None]) -> None:
    """Fire-and-forget lifecycle notifications: failures are logged, never fatal."""
    for plug in PLUGINS.overriding(hook):
        try:
            call(plug)
        except Exception:
            ctx.log.append(f"plugin {plug.name!r} {hook} failed:\n{traceback.format_exc()}")
