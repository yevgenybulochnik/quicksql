"""The TUI's runs: a notebook's cells run off the UI thread on its RunSession.

`RunController` owns the session, what the last runs left (``results``) and
what's still pending (``running``). One run at a time uses a session: a new
run (or a notebook switch) cancels the one under way, which stops after its
current cell, and the new run's thread waits for it. Each run is tagged with a
generation, bumped on the UI thread when it starts: events and results from an
older one come from a replaced run, and are logged but never shown.
Everything that touches the screen goes through the app's ``run_*`` hooks, on
the UI thread. Each run's life is also reported to the app's live host
(``live.host.LiveHost``), which an agent reads over the socket: requested,
adopted, every event (of replaced runs too), ended or dropped.
"""

from __future__ import annotations

import threading
import traceback
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator, Optional

from ..errors import QsqlError
from ..models import RunEvent, RunResult
from ..runner import RunSession, run_project

if TYPE_CHECKING:
    from ..compiler import Project
    from .app import QsqlApp


class RunController:
    def __init__(self, app: QsqlApp) -> None:
        self.app = app
        self.session = RunSession()  # per notebook, reused across its reruns
        self.results: dict[str, RunResult] = {}
        self.running: set[str] = set()
        self.gen = 0
        # runs in flight per session: a replaced session is closed by whoever
        # leaves it last — the switch if it's idle, else that run's finally.
        # Closing a DuckDB connection under another thread's query risks a crash.
        self._session_lock = threading.Lock()
        self._session_runs: dict[RunSession, int] = {}
        self._run_lock = threading.Lock()  # held by the one run using a session
        self._cancel = threading.Event()  # the latest run's; set when it's replaced

    def reset(self) -> None:
        """A notebook switch: a fresh session, no results, and an in-flight run
        of the old notebook stops after its current cell and paints nothing."""
        self._cancel.set()
        self._replace_session()
        self.gen += 1
        self.results = {}
        self.running = set()

    def connect(self) -> Any:
        """A DuckDB handle that reads what this session landed: a cursor of
        the live conduit (it may already hold sink ATTACHes, and cursors are
        per-thread-safe), or a throwaway connection before the first run."""
        if self.session.conn is not None:
            return self.session.conn.cursor()
        import duckdb

        return duckdb.connect()

    def close(self) -> None:
        self._cancel.set()
        self.session.close()

    def cancel(self) -> None:
        """Stop the run under way after its current cell (an agent's cancel)."""
        self._cancel.set()

    @contextmanager
    def reading(self) -> Iterator[Any]:
        """``connect``, for a read off the UI thread (an agent's head or
        query): the session counts as in use until the read is done, as for a
        run, so a notebook switch can't close it under the read."""
        session = self._acquire(None)
        try:
            if session.conn is not None:
                conn = session.conn.cursor()
            else:
                import duckdb

                conn = duckdb.connect()
            try:
                yield conn
            finally:
                conn.close()
        finally:
            self._release(session)

    def start(
        self,
        select: Optional[list[str]],
        closure: bool,
        path: Path,
        project: Project | None = None,
    ) -> int:
        """Bump the run generation (on the UI thread), cancel the run under way
        and hand off to a thread worker, which starts once that run has
        stopped. A save passes the project it just compiled, so the run
        doesn't compile it again. Returns the run's generation, its id."""
        self.gen += 1
        self._cancel.set()
        self._cancel = threading.Event()
        self.app.host.run_requested(self.gen, project.source_hash if project else None)
        # bound now: a notebook switch before the thread starts must not
        # redirect this run to the new notebook or its session
        work = partial(
            self._run, select, closure, self.gen, path, self.session, project, self._cancel
        )
        # not `exclusive`: Textual would only flag the old thread cancelled,
        # and the run lock already keeps the runs apart
        self.app.run_worker(work, thread=True, group="run")
        return self.gen

    def _run(
        self,
        select: Optional[list[str]],
        closure: bool,
        gen: int,
        path: Path,
        session: RunSession,
        project: Project | None,
        cancel: threading.Event,
    ) -> None:
        call = self.app.call_from_thread
        with self._session_in_use(session), self._run_lock:
            if cancel.is_set():
                # replaced while it waited: the newer run does the work
                call(self.app.host.run_dropped, gen, "replaced before it started")
                return
            if project is None:
                try:
                    project = self.app.compile_notebook(path)
                except QsqlError as exc:
                    call(self.app.log_line, f"compile error: {exc}")
                    call(self.app.run_compile_failed, gen, path, exc)
                    return
            call(self._adopt, gen, project)
            results: list[RunResult] = []
            try:
                results = run_project(
                    project,
                    select=select,
                    closure=closure,
                    session=session,
                    on_event=lambda e: call(self.on_event, gen, e),
                    cancel=cancel.is_set,
                )
            except Exception:  # never leave cells stuck at "…" on a runner crash
                call(self.app.log_line, f"run failed:\n{traceback.format_exc()}")
            finally:
                call(self.apply, gen, results)

    def _adopt(self, gen: int, project: Project) -> None:
        self.app.host.run_adopted(gen, project)
        if gen != self.gen:
            return  # a replaced run's compile may be older, or another notebook's
        self.app.run_adopted(project)

    def on_event(self, gen: int, event: RunEvent) -> None:
        """Route the runner's live events: everything is logged; run_started
        seeds the pending set from the *post-expansion* cell list (closure,
        missing-temp and not-ok upstreams included — the select list undercounts those),
        and each cell_finished retires its cell so status flips one at a time."""
        self.app.host.run_event(gen, event)
        self.app.log_line(event.line())
        result = event.result if event.kind == "cell_finished" else None
        error = (result.error or "").strip() if result is not None else ""
        if result is not None and result.status == "failed" and "\n" in error:
            # the full message, once per result: the line above keeps only its
            # first line (a one-line message, or a skipped cell's, is all there)
            self.app.log_line(f"{event.cell} failed:\n{result.error}")
        if gen != self.gen:
            return  # a replaced run may still be draining: log it, never show it
        if event.kind == "run_started":
            self.running = set(event.cells or ())
            self.app.run_progressed()
        elif event.kind == "cell_finished" and event.cell is not None:
            self.running.discard(event.cell)
            if event.result is not None:
                self.results[event.cell] = event.result
            self.app.run_progressed(event.cell)

    def apply(self, gen: int, results: list[RunResult]) -> None:
        self.app.host.run_ended(gen, results)
        self.app.run_landed()  # outputs and warehouses just changed
        if gen != self.gen:
            return  # a newer run owns the table; its own events paint it
        for result in results:  # normally a no-op: cell_finished stored each
            self.results[result.cell] = result
        self.running.clear()
        self.app.run_finished()

    def _replace_session(self) -> None:
        with self._session_lock:
            old, self.session = self.session, RunSession()
            idle = old not in self._session_runs
        if idle:
            old.close()

    @contextmanager
    def _session_in_use(self, session: RunSession) -> Iterator[None]:
        self._acquire(session)
        try:
            yield
        finally:
            self._release(session)

    def _acquire(self, session: RunSession | None) -> RunSession:
        """Count a use of ``session`` (None: the current one, taken under the
        lock, so a switch can't close it between the read and the count)."""
        with self._session_lock:
            session = session or self.session
            self._session_runs[session] = self._session_runs.get(session, 0) + 1
            return session

    def _release(self, session: RunSession) -> None:
        with self._session_lock:
            left = self._session_runs.pop(session) - 1
            if left:
                self._session_runs[session] = left
            retired = not left and session is not self.session
        if retired:  # its notebook was switched away mid-use
            session.close()
