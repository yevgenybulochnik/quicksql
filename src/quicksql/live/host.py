"""The live host: one open notebook's session state, shared by the TUI's keys
and the agent's socket.

It owns the controls the TUI used to keep on the app (watch reruns armed,
autorun switched off per cell or for all), a mirror of the run state (which
run is current, its queued and running cells, each cell's last result), the
outcome of every save it compiled (by the text's hash), and the event log
with its subscribers. Everything that touches the screen or the run session
goes through its driver (the TUI's ``tui.agent.AppDriver``; a fake in
tests), so the host itself knows no Textual and runs no SQL but the reads it
is asked for.

Every method runs on one thread, the TUI's event loop, which is where key
handlers, run events (``call_from_thread``) and socket requests all arrive;
only ``read``, ``query`` and ``browse`` run on a worker thread, through the
driver's ``reading`` handle, and touch no host state. The catalog's
refreshes behind an answer, and what its warming changed, reach the host
through the driver's ``post``.
"""

from __future__ import annotations

import os
import time
from collections import Counter, deque
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, ContextManager, Iterable, Optional, Protocol

from ..conduit import landed_relation, read_landed
from ..models import RunEvent, RunResult
from ..state import statuses
from ..watcher import should_rerun
from .protocol import (
    BUSY,
    INVALID,
    LOAD_FAILED,
    MAX_LINE,
    MAX_ROWS,
    NO_PROJECT,
    NOT_READABLE,
    QUERY_FAILED,
    REFUSED,
    UNKNOWN_CELL,
    UNKNOWN_PATH,
    LiveError,
    encode,
)

if TYPE_CHECKING:
    from ..catalog import CatalogCache, CatalogNode, Resolved
    from ..compiler import Project


class Driver(Protocol):
    """What the host asks of whoever shows the notebook (the TUI)."""

    def start_run(self, select: Optional[list[str]], closure: bool) -> int:
        """Start a run, replacing any under way; returns its run id. The
        driver reports it back through ``run_requested`` before returning."""

    def cancel_run(self) -> None:
        """Stop the run under way after its current cell."""

    def reading(self) -> ContextManager[Any]:
        """A DuckDB handle that reads what the session landed, held in use
        (a notebook switch waits for it) until the block ends."""

    def connect(self) -> Any:
        """A DuckDB handle that reads what the session landed (the catalog
        reads cell outputs through it); the caller closes it."""

    def controls_changed(self) -> None:
        """Repaint whatever shows the controls."""

    def log_line(self, text: str) -> None: ...

    def focus_cell(self, cell: str, tab: Optional[str]) -> None: ...

    def toast(self, message: str, severity: str) -> None: ...

    def post(self, fn: Callable[[], None]) -> None:
        """Run ``fn`` on the host's thread (the TUI's loop) soon: how a
        worker thread reaches the host, e.g. the catalog's refreshes behind."""


class Subscriber(Protocol):
    def offer(self, event: dict[str, Any]) -> bool:
        """Take the event, or say False when full: it's then dropped."""

    def lost(self) -> None:
        """It fell behind and was dropped."""


RUN_RECORDS = 64  # runs remembered for `run --wait`
OUTCOMES = 32  # saves remembered for `wait`
ACTIVE = ("pending", "running")


class LiveHost:
    def __init__(self, driver: Driver, path: Path, ring: int = 1000) -> None:
        self.driver = driver
        self.path = Path(path)
        self.seq = 0
        self.ring: deque[dict[str, Any]] = deque(maxlen=ring)
        self._subscribers: list[Subscriber] = []
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self.clients = 0
        # controls: the keys and the agent share these. A notebook switch
        # disarms (as the TUI always has) and keeps the autorun switches.
        self.armed = False
        self.autorun_global = True
        self.autorun_off: set[str] = set()
        self._reset_notebook()

    def _reset_notebook(self) -> None:
        self.project: Project | None = None
        self.compile_error: dict[str, Any] | None = None
        self.results: dict[str, RunResult] = {}
        self.latest_run = 0  # the newest run requested
        self.runs: dict[int, dict[str, Any]] = {}  # run id -> state, hash, results
        self.run_cells: tuple[str, ...] = ()  # the latest run's cells, once started
        self.finished_in_run: set[str] = set()
        self.current: str | None = None  # its cell under way
        self.outcomes: dict[str, dict[str, Any]] = {}  # source hash -> how it settled
        self.latest_outcome: dict[str, Any] | None = None
        self.seen_at: dict[str, float] = {}  # source hash -> when it last compiled
        self._settles: dict[int, tuple[str, list[str]]] = {}  # run -> (hash, changed)
        self._awaiting: tuple[str, list[str]] | None = None  # a save's rerun, not yet requested
        # the schema catalog: the Catalog tab's and an agent's, one per
        # notebook, read through the disk (remote engines' levels outlive it)
        from ..catalog import default_cache

        cache = default_cache(on_refresh=lambda node: self._refreshed_behind(cache, node))
        self.catalog_cache = cache
        self._catalog_root: CatalogNode | None = None
        self._catalog_project: Project | None = None

    def switch(self, path: Path) -> None:
        """A notebook opened (first, or another with ``o``): forget the last
        one's runs and saves, and disarm (a notebook always opens paused)."""
        if self.project is not None or self.compile_error is not None:
            self._emit("notebook_closed", notebook=str(self.path))
        self.path = Path(path)
        self.ring.clear()
        self.armed = False
        self._reset_notebook()

    # ---------- events ----------

    def subscribe(self, subscriber: Subscriber) -> None:
        self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: Subscriber) -> None:
        if subscriber in self._subscribers:
            self._subscribers.remove(subscriber)

    def listen(self, listener: Callable[[dict[str, Any]], None]) -> Callable[[], None]:
        """Call ``listener`` with every event; returns the unlisten."""
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener) if listener in self._listeners else None

    def client_connected(self, delta: int) -> None:
        """A socket client came (+1) or went (-1): the TUI shows it."""
        self.clients = max(0, self.clients + delta)
        self.driver.controls_changed()

    def since(self, seq: int) -> list[dict[str, Any]] | None:
        """The events after ``seq``, or None when some are gone from the ring."""
        if seq >= self.seq:
            return []
        oldest = self.ring[0]["seq"] if self.ring else self.seq + 1
        if seq + 1 < oldest:
            return None
        return [e for e in self.ring if e["seq"] > seq]

    def _emit(self, kind: str, **payload: Any) -> dict[str, Any]:
        self.seq += 1
        event = {"seq": self.seq, "kind": kind, **payload}
        self.ring.append(event)
        for sub in list(self._subscribers):
            if not sub.offer(event):  # full: a bounded queue never grows
                self._subscribers.remove(sub)
                sub.lost()
        for listener in list(self._listeners):
            listener(event)
        return event

    # ---------- controls ----------

    def controls(self) -> dict[str, Any]:
        return {
            "armed": self.armed,
            "autorun_global": self.autorun_global,
            "autorun_off": sorted(self.autorun_off),
        }

    def file_autorun(self, name: str) -> bool:
        """What the notebook says (``-- @autorun``, or a plugin's should_rerun)."""
        project = self._need_project()
        return should_rerun(project.cells[name], project.registries.plugins)

    def cell_autorun(self, name: str) -> bool:
        """Whether a save reruns ``name``: the file, the session and the global switch."""
        if self.project is None or name not in self.project.cells:
            return False
        return self.autorun_global and self.file_autorun(name) and name not in self.autorun_off

    def autorun_of(self, name: str) -> dict[str, bool]:
        return {
            "effective": self.cell_autorun(name),
            "file": self.file_autorun(name),
            "session": name not in self.autorun_off,
            "global": self.autorun_global,
        }

    def _changed(self, by: str, what: str) -> None:
        if by == "agent":
            self.driver.log_line(f"agent: {what}")
        self._emit("controls_changed", **self.controls(), by=by)
        self.driver.controls_changed()

    def set_armed(self, on: bool, by: str) -> dict[str, Any]:
        if self.armed != on:
            self.armed = on
            self._changed(by, "arm" if on else "disarm")
        return self.controls()

    def toggle_armed(self, by: str = "keys") -> None:
        self.set_armed(not self.armed, by)

    def set_autorun(self, cells: Iterable[str], on: bool, by: str) -> dict[str, Any]:
        """Switch the session's autorun for ``cells``. Only the file switches
        a cell on for good: ``on`` for a cell it switches off is refused."""
        cells = list(cells)
        self._known(cells)
        if on:
            off_in_file = [c for c in cells if not self.file_autorun(c)]
            if off_in_file:
                lines = ", ".join(f"{c} (line {self.project.cells[c].line})" for c in off_in_file)
                raise LiveError(
                    REFUSED,
                    f"the file switches autorun off for {lines}: remove its "
                    "`-- @autorun: false` to switch it on",
                )
            changed = [c for c in cells if c in self.autorun_off]
            self.autorun_off.difference_update(cells)
        else:
            changed = [c for c in cells if c not in self.autorun_off]
            self.autorun_off.update(cells)
        if changed:
            self._changed(by, f"autorun {', '.join(changed)} {'on' if on else 'off'}")
        return self.controls()

    def toggle_autorun(self, name: str, by: str = "keys") -> None:
        """The ``a`` key: flip the session's switch for ``name``, whatever the
        file says (switching on a file-off cell changes nothing it runs)."""
        on = name in self.autorun_off
        self.autorun_off.symmetric_difference_update({name})
        self._changed(by, f"autorun {name} {'on' if on else 'off'}")

    def set_autorun_all(self, on: bool, by: str) -> dict[str, Any]:
        if self.autorun_global != on:
            self.autorun_global = on
            self._changed(by, f"autorun all {'on' if on else 'off'}")
        return self.controls()

    def toggle_autorun_global(self, by: str = "keys") -> None:
        self.set_autorun_all(not self.autorun_global, by)

    # ---------- compiles ----------

    def loaded(self, project: Project) -> None:
        """The notebook opened (or reopened with ``o``) and compiled."""
        self._adopt(project)
        self._warm(project)
        self._emit("compiled", hash=project.source_hash, cells=list(project.order), changed=[])
        self._settle(project.source_hash, "paused" if not self.armed else "nothing_to_rerun")

    def recompiled(self, project: Project, planned: list[str]) -> list[str]:
        """A save compiled; ``planned`` is what changed (``plan_rerun``).
        Returns the cells to rerun now. The save settles here, unless a rerun
        follows: then that run's end settles it."""
        outputs = set(self.project.order) if self.project is not None else set()
        self._adopt(project)
        h = project.source_hash
        self._emit("compiled", hash=h, cells=list(project.order), changed=planned)
        # a save changes the top level and where outputs land, never a warehouse
        self._catalog_changed("save", top_level=True, scopes=outputs | set(project.order))
        self._warm(project)  # it may name a table the last one didn't
        if not self.armed:
            self._settle(h, "paused", changed=planned)
            return []
        to_run = [n for n in planned if self.cell_autorun(n)]
        if to_run:
            self._awaiting = (h, planned)
        else:
            self._settle(h, "nothing_to_rerun", changed=planned)
        return to_run

    def compile_failed(self, h: str, errors: list[dict[str, Any]]) -> None:
        """A save didn't compile; the TUI keeps showing the last good project."""
        self.seen_at[h] = time.time()
        self.compile_error = {"hash": h, "errors": errors}
        self._emit("compile_failed", hash=h, errors=errors)
        self._settle(h, "compile_failed", errors=errors)

    def _adopt(self, project: Project) -> None:
        self.project = project
        self.compile_error = None
        self.seen_at[project.source_hash] = time.time()

    # ---------- runs (reported by the TUI's run controller) ----------

    def run_requested(self, run: int, h: str | None) -> None:
        """A run started (bumped its generation) on the UI thread. ``h`` is
        its project's hash when it was handed one (a save's rerun)."""
        for older in self.runs.values():
            if older["state"] in ACTIVE:
                older["replaced"] = True
        self.latest_run = run
        self.runs[run] = {"state": "pending", "hash": h, "results": [], "replaced": False}
        while len(self.runs) > RUN_RECORDS:
            self.runs.pop(min(self.runs))
        if self._awaiting is not None and self._awaiting[0] == h:
            self._settles[run] = self._awaiting
            self._awaiting = None

    def run_adopted(self, run: int, project: Project) -> None:
        """The run compiled (or was handed) ``project``."""
        if run in self.runs:
            self.runs[run]["hash"] = project.source_hash
        if run == self.latest_run:
            self._adopt(project)

    def run_event(self, run: int, event: RunEvent) -> None:
        record = self.runs.get(run)
        current = run == self.latest_run
        if record is not None:
            if event.kind == "run_started":
                record["state"] = "running"
            elif event.kind == "cell_finished" and event.result is not None:
                record["results"].append(event.result)
        if current:
            if event.kind == "run_started":
                self.run_cells = tuple(event.cells or ())
                self.finished_in_run = set()
                self.current = None
            elif event.kind == "cell_started":
                self.current = event.cell
            elif event.kind == "cell_finished" and event.cell is not None:
                self.current = None
                self.finished_in_run.add(event.cell)
                if event.result is not None:
                    self.results[event.cell] = event.result
        payload = event.to_dict()
        payload.pop("kind")
        if event.result is not None:
            payload["result"] = self.result_dict(event.result)
        if event.kind == "run_started" and record is not None:
            payload["hash"] = record["hash"]
        self._emit(event.kind, run_id=run, current=current, **payload)

    def run_ended(self, run: int, results: list[RunResult]) -> None:
        """The run's thread finished (the controller's ``apply``)."""
        self._catalog_changed("run", run=run, scopes=self._run_scopes(results))
        record = self.runs.get(run)
        if record is None:
            return
        replaced = record["replaced"] or run != self.latest_run
        record["state"] = "replaced" if replaced else "finished"
        if results:
            record["results"] = list(results)
        if run == self.latest_run:
            self.run_cells, self.finished_in_run, self.current = (), set(), None
        self._emit("run_ended", run_id=run, state=record["state"], summary=_summary(results))
        h = record["hash"]
        if not replaced and h is not None:
            # a save's rerun settles that save; any other run of the same
            # text (r, R, an agent's) updates its outcome to what it ran
            changed = self._settles.pop(run, (h, []))[1]
            self._settle(h, "ran", changed=changed, results=results)

    def run_dropped(self, run: int, reason: str) -> None:
        """The run never ran: replaced while it waited, or its compile failed."""
        record = self.runs.get(run)
        if record is None:
            return
        record["state"] = "dropped"
        self._settles.pop(run, None)
        self._emit("run_ended", run_id=run, state="dropped", reason=reason,
                   summary=_summary([]))

    def busy(self) -> bool:
        return any(r["state"] in ACTIVE for r in self.runs.values())

    # ---------- the schema catalog ----------

    def catalog_root(self, project: Project | None = None) -> CatalogNode:
        """The catalog's top level for ``project`` (the host's by default),
        built once per project: its engines' clients live across requests."""
        project = project or self._need_project()
        if self._catalog_project is not project:
            from ..catalog import project_root_node

            self._catalog_root = project_root_node(project, connect=self.driver.connect)
            self._catalog_project = project
        return self._catalog_root

    def catalog_scope(self) -> tuple[CatalogNode, CatalogCache]:
        """The top level and the cache a ``browse`` reads, taken on the loop
        thread before a worker reads them."""
        return self.catalog_root(self._need_project()), self.catalog_cache

    def browse(
        self,
        root: CatalogNode,
        cache: CatalogCache,
        path: list[str],
        *,
        offset: int,
        rows: int,
        match: str | None = None,
        columns: list[str] | None = None,
        top_level: bool = False,
        refresh: bool = False,
    ) -> dict[str, Any]:
        """One page of the catalog level ``path`` names (a worker thread):
        walked and loaded through ``cache``, filtered, each row named for the
        next segment, everything shown redacted, and cut to fit one message."""
        from ..catalog import CatalogPathError, narrow, resolve

        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise LiveError(INVALID, "offset is a non-negative integer")
        rows = _rows(rows)
        registries = self._registries()
        with self.driver.reading():  # held in use: a notebook switch waits for the load
            try:
                got = resolve(root, path, cache, refresh=refresh)
            except CatalogPathError as err:
                raise LiveError(UNKNOWN_PATH, _redact(str(err), registries)) from None
            except Exception as exc:
                message = _redact(f"{type(exc).__name__}: {exc}", registries)
                raise LiveError(LOAD_FAILED, f"loading {' / '.join(path) or 'the catalog'}: {message}") from None
        try:
            frame = narrow(got.node, got.frame, match=match, columns=columns, top_level=top_level)
        except ValueError as exc:
            raise LiveError(INVALID, str(exc)) from None
        budget = MAX_LINE - 4096  # the envelope's room
        while True:
            doc = _catalog_page(got, frame, offset, rows, path, registries)
            if rows == 1 or len(encode({"id": 0, "result": doc})) <= budget:
                return doc
            rows = max(1, rows // 2)

    def _registries(self) -> Any:
        if self.project is not None:
            return self.project.registries
        from ..bootstrap import builtin_registries

        return builtin_registries()

    def _run_scopes(self, results: Iterable[RunResult]) -> set[str]:
        """The top-level rows a run's results may have changed: the contexts
        on every engine its cells ran on or landed in, and their outputs. A
        skipped cell never ran."""
        project = self.project
        if project is None:
            return set()
        ran = [project.cells[r.cell] for r in results
               if r.cell in project.cells and r.status != "skipped"]
        if not ran:
            return set()
        engines = {c.engine for c in ran} | {c.sink_type for c in ran}
        top = self.catalog_root(project).load()
        scopes = {row["name"] for row in top.iter_rows(named=True)
                  if row["kind"] == "context" and row["engine"] in engines}
        return scopes | {c.name for c in ran}

    def _warm(self, project: Project) -> None:
        """Load ahead, behind the loop, the tables the notebook's cells read
        that aren't fresh (``catalog.named_tables``): the first read of each,
        by the agent, the Catalog tab or the LSP, costs no round trip."""
        from ..catalog import named_tables

        self.catalog_cache.warm(named_tables(project))

    def _refreshed_behind(self, cache: CatalogCache, node: CatalogNode) -> None:
        """A refresh behind an answer changed a level (a worker thread): say
        so on the loop, unless that cache is an earlier notebook's."""

        def emit() -> None:
            if cache is self.catalog_cache:
                scopes = [node.scope] if node.scope is not None else []
                self._emit("catalog_invalidated", reason="refreshed", run_id=None,
                           top_level=False, scopes=scopes)

        self.driver.post(emit)

    def _catalog_changed(
        self, reason: str, *, run: int | None = None, top_level: bool = False,
        scopes: Iterable[str] = (),
    ) -> None:
        scopes = sorted(scopes)
        if not top_level and not scopes:
            return
        if top_level and self.project is not None:
            self.catalog_cache.invalidate(self.catalog_root())
        if scopes:
            self.catalog_cache.invalidate(scopes=scopes)
        self._emit("catalog_invalidated", reason=reason, run_id=run, top_level=top_level, scopes=scopes)

    # ---------- outcomes ----------

    def _settle(
        self,
        h: str,
        state: str,
        *,
        changed: Iterable[str] = (),
        errors: Iterable[dict[str, Any]] = (),
        results: Iterable[RunResult] = (),
    ) -> None:
        results = list(results)
        kept = self.outcomes.get(h)
        if state == "nothing_to_rerun" and kept is not None and kept["state"] == "ran":
            # the same text saved again: what its run found still stands
            event = self._emit("settled", hash=h, state=kept["state"])
            kept["seq"] = event["seq"]
            self.latest_outcome = kept
            return
        outcome = {
            "hash": h,
            "state": state,
            "changed": list(changed),
            "errors": list(errors),
            "summary": _summary(results),
            "results": [self.result_dict(r) for r in results],
            "controls": self.controls(),
        }
        event = self._emit("settled", hash=h, state=state)
        outcome["seq"] = event["seq"]
        self.outcomes.pop(h, None)
        self.outcomes[h] = outcome
        while len(self.outcomes) > OUTCOMES:
            self.outcomes.pop(next(iter(self.outcomes)))
        self.latest_outcome = outcome

    def wait_outcome(
        self, h: str, since: int, disk_hash: str, disk_mtime: float
    ) -> dict[str, Any] | None:
        """How the save of text ``h`` settled, or None while that's still
        to come. ``since`` is the sequence the wait started at; ``disk_hash``
        and ``disk_mtime`` describe the file now. A save the file no longer
        holds will never compile: once a later one settles, that one comes
        back marked ``superseded``."""
        if any(r["state"] in ACTIVE and r["hash"] in (None, h) for r in self.runs.values()):
            return None  # a run of this text (or of one not compiled yet) is under way
        outcome = self.outcomes.get(h)
        if outcome is not None:
            if disk_hash != h:
                return {**outcome, "superseded": True}
            if disk_mtime <= self.seen_at.get(h, 0):
                return {**outcome, "superseded": False}
            return None  # saved again since it compiled: wait for that save
        if disk_hash != h:
            latest = self.latest_outcome
            if latest is not None and latest["seq"] > since:
                return {**latest, "superseded": True}
        return None

    # ---------- what an agent reads ----------

    def result_dict(self, result: RunResult, rows: int = 0) -> dict[str, Any]:
        """A result as JSON, with where the cell is in the file and, for a
        failed statement, its rendered SQL (split as the runner split it)."""
        out = result.to_dict(rows)
        cell = self.project.cells.get(result.cell) if self.project else None
        out["line"] = cell.line if cell else None
        out["line_end"] = cell.line_end if cell else None
        out["statement_sql"] = None
        if cell is not None and result.statement:
            executor = self.project.registries.executors.get(cell.engine)
            statements = executor.split_statements(cell.sql)
            if 0 < result.statement <= len(statements):
                out["statement_sql"] = statements[result.statement - 1].strip()
        return out

    def run_info(self) -> dict[str, Any] | None:
        active = {run: r for run, r in self.runs.items() if r["state"] in ACTIVE}
        if not active:
            return None
        run = max(active)
        record = active[run]
        return {
            "run_id": run,
            "state": record["state"],
            "hash": record["hash"],
            "cells": list(self.run_cells),
            "current": self.current,
            "done": [c for c in self.run_cells if c in self.finished_in_run],
        }

    def _pending(self) -> set[str]:
        if self.run_info() is None:
            return set()
        return set(self.run_cells) - self.finished_in_run

    def snapshot(self) -> dict[str, Any]:
        project = self.project
        cells = []
        if project is not None:
            rows = {s.cell: s for s in statuses(project)}
            pending = self._pending()
            for name in project.order:
                cell = project.cells[name]
                result = self.results.get(name)
                status, reason = rows[name].status, rows[name].reason
                if name == self.current:
                    status, reason = "running", ""
                elif name in pending:
                    status, reason = "queued", ""
                elif name in self.finished_in_run and result is not None:
                    status, reason = result.status, ""  # the state file lags the run
                entry = {
                    "cell": name,
                    "status": status,
                    "reason": reason,
                    "line": cell.line,
                    "line_end": cell.line_end,
                    "engine": cell.engine,
                    "sink": cell.sink_type,
                    "context": cell.context,
                    "depends_on": list(cell.depends_on),
                    "autorun": self.autorun_of(name),
                    "rows": None, "elapsed": None, "target": None,
                    "error": None, "statement": None, "statement_sql": None,
                }
                if result is not None and status not in ("running", "queued"):
                    last = self.result_dict(result)
                    for key in ("rows", "elapsed", "target", "error", "statement", "statement_sql"):
                        entry[key] = last[key]
                cells.append(entry)
        return {
            "notebook": str(self.path),
            "pid": os.getpid(),
            "seq": self.seq,
            "hash": project.source_hash if project else None,
            "compile_error": self.compile_error,
            "controls": self.controls(),
            "run": self.run_info(),
            "cells": cells,
        }

    def landed(self, cell: str) -> str:
        """The relation to read cell ``cell``'s landed output from, checked on
        the loop thread before a worker reads it (``read``)."""
        self._known([cell])
        if cell in self._pending():
            raise LiveError(BUSY, f"{cell} is running: read it after its cell_finished")
        relation = landed_relation(self.project, cell)
        if relation is None:
            raise LiveError(NOT_READABLE, f"{cell} is effect-only: it lands nothing to read")
        return relation

    def read(self, cell: str, relation: str, rows: int) -> dict[str, Any]:
        """The head of a landed output, through the session (a worker thread)."""
        rows = _rows(rows)
        with self.driver.reading() as conn:
            head = read_landed(conn, relation, rows)
        if head is None:
            raise LiveError(
                NOT_READABLE,
                f"{cell} can't be read in this session: it never landed, or it lands in a "
                "database this session hasn't attached yet (run it here once)",
            )
        return {"cell": cell, **head.to_dict(rows)}

    def views(self) -> dict[str, str]:
        """Each landed cell's relation, to name it in a query (loop thread)."""
        project = self._need_project()
        out = {}
        for name in project.order:
            relation = landed_relation(project, name)
            if relation is not None:
                out[name] = relation
        return out

    def query(self, sql: str, views: dict[str, str], rows: int) -> dict[str, Any]:
        """One read-only SELECT through the session, each landed cell a view
        of its output (a worker thread). The views are TEMP on this handle's
        own connection, so the run's connection never sees them."""
        import duckdb

        from ..models import Preview

        rows = _rows(rows)
        with self.driver.reading() as conn:
            try:
                statements = conn.extract_statements(sql)
            except duckdb.Error as exc:
                raise LiveError(INVALID, f"query doesn't parse: {exc}") from None
            if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
                raise LiveError(REFUSED, "query takes one SELECT (or FROM, SUMMARIZE, DESCRIBE)")
            for name, relation in views.items():
                quoted = name.replace('"', '""')
                try:
                    conn.execute(f'CREATE OR REPLACE TEMP VIEW "{quoted}" AS SELECT * FROM {relation}')
                except duckdb.Error:
                    continue  # never landed, or its database isn't attached here
            try:
                rel = conn.sql(sql)
                head = Preview(tuple(rel.columns), tuple(rel.limit(rows).fetchall()))
            except duckdb.Error as exc:
                raise LiveError(QUERY_FAILED, str(exc)) from None
        return head.to_dict(rows)

    # ---------- what an agent asks for ----------

    def request_run(
        self, cells: Optional[list[str]], all_: bool, cancel: bool
    ) -> dict[str, Any]:
        self._need_project()
        if not all_:
            if not cells:
                raise LiveError(INVALID, "name the cells to run, or all")
            self._known(cells)
        if self.busy() and not cancel:
            info = self.run_info()
            what = ", ".join(info["cells"]) if info and info["cells"] else "starting"
            raise LiveError(
                BUSY, f"a run is under way ({what}): wait for it, or replace it with --cancel"
            )
        if all_:
            self.set_armed(True, by="agent")  # like R
        self.driver.log_line(f"agent: run {'all' if all_ else ', '.join(cells)}")
        run = self.driver.start_run(None if all_ else list(cells), True)
        return {"run_id": run}

    def cancel(self) -> dict[str, Any]:
        if not self.busy():
            return {"cancelled": False, "not_run": []}
        not_run = [c for c in self.run_cells if c not in self.finished_in_run and c != self.current]
        self.driver.log_line("agent: cancel")
        self.driver.cancel_run()
        return {"cancelled": True, "not_run": not_run}

    def run_record(self, run: int) -> dict[str, Any] | None:
        record = self.runs.get(run)
        if record is None:
            return None
        return {
            "run_id": run,
            "state": record["state"],
            "hash": record["hash"],
            "summary": _summary(record["results"]),
            "cells": [self.result_dict(r) for r in record["results"]],
        }

    def focus(self, cell: str, tab: Optional[str]) -> dict[str, Any]:
        self._known([cell])
        self.driver.focus_cell(cell, tab)
        return {}

    def notify(self, message: str, severity: str) -> dict[str, Any]:
        if severity not in ("information", "warning", "error"):
            raise LiveError(INVALID, "severity is information, warning or error")
        self.driver.toast(message, severity)
        self.driver.log_line(f"agent: {message}")
        return {}

    # ---------- helpers ----------

    def _need_project(self) -> Project:
        if self.project is None:
            raise LiveError(NO_PROJECT, "the notebook hasn't compiled yet")
        return self.project

    def _known(self, cells: Iterable[str]) -> None:
        project = self._need_project()
        unknown = [c for c in cells if c not in project.cells]
        if unknown:
            raise LiveError(UNKNOWN_CELL, f"unknown cell(s): {', '.join(unknown)}")


def _summary(results: Iterable[RunResult]) -> dict[str, int]:
    counts = Counter(r.status for r in results)
    return {s: counts[s] for s in ("ok", "failed", "skipped")}


def _redact(text: str, registries: Any) -> str:
    from ..catalog import redact

    return redact(text, registries)


def _catalog_page(
    got: Resolved, frame: Any, offset: int, rows: int, path: list[str], registries: Any
) -> dict[str, Any]:
    """A page of a resolved level, as the socket sends it: every string
    shown through the engines' redaction."""
    from ..catalog import page

    doc = page(got.node, frame, offset, rows)

    def shown(value: Any) -> Any:
        return _redact(value, registries) if isinstance(value, str) else value

    return {
        "path": [shown(p) for p in path],
        "title": shown(doc["title"]),
        "leaf": doc["leaf"],
        "columns": [shown(c) for c in doc["columns"]],
        "rows": [[shown(v) for v in row] for row in doc["rows"]],
        "names": None if doc["names"] is None else [shown(n) for n in doc["names"]],
        "total": doc["total"],
        "offset": doc["offset"],
        "next_offset": doc["next_offset"],
        "cached": got.cached,
        "age": round(got.age),  # seconds since it was loaded from its engine
        "stale": got.stale,  # past revalidation: a refresh is under way behind it
    }


def _rows(rows: int) -> int:
    if not isinstance(rows, int) or rows < 1:
        raise LiveError(INVALID, "rows is a positive integer")
    return min(rows, MAX_ROWS)
