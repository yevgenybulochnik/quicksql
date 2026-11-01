"""The live session's socket: a host served on the TUI's own event loop.

Requests are handled on that loop, the same thread as key handlers and run
events, so the host needs no locks; DuckDB reads (``head``, ``query``) go to a
worker thread. A subscriber gets a bounded queue: one that falls behind is
dropped with ``events_lost`` rather than buffered without end.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

from .host import LiveHost
from .protocol import (
    EVENTS_LOST,
    INTERNAL,
    INVALID,
    MAX_LINE,
    PROTOCOL,
    PROTOCOL_MISMATCH,
    TIMEOUT,
    UNKNOWN_METHOD,
    UNSUPPORTED,
    LiveError,
    decode,
    encode,
    file_hash,
    socket_path,
)

QUEUE = 1000  # events a subscriber may lag behind before it's dropped
MAX_WAIT = 3600.0


class _Subscription:
    """A subscriber's bounded queue (``host.Subscriber``)."""

    def __init__(self, size: int) -> None:
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(size)
        self.dropped = asyncio.Event()

    def offer(self, event: dict[str, Any]) -> bool:
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            return False
        return True

    def lost(self) -> None:
        self.dropped.set()


class LiveServer:
    """Serve ``host`` for ``notebook`` until ``close``. ``screen`` says the
    host has a screen for ``focus`` and ``notify`` (the TUI does)."""

    def __init__(
        self, host: LiveHost, notebook: Path, *, screen: bool = True, queue: int = QUEUE
    ) -> None:
        self.host = host
        self.notebook = Path(notebook)
        self.screen = screen
        self.queue_size = queue
        self.path: Path | None = None
        self._server: asyncio.AbstractServer | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tick = asyncio.Event()
        self._unlisten = host.listen(self._wake)

    # ---------- lifecycle ----------

    async def start(self) -> str | None:
        """Bind the notebook's socket. Returns None when serving, else why not
        (another session answers on it)."""
        path = socket_path(self.notebook)
        path.parent.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            os.chmod(path.parent, 0o700)
        if path.exists() or path.is_symlink():
            if await _answers(path):
                return f"another quicksql session has {self.notebook.name} open"
            path.unlink(missing_ok=True)  # stale: its TUI died without closing it
        umask = os.umask(0o177)  # the socket is born 0600
        try:
            self._server = await asyncio.start_unix_server(
                self._handle, path=str(path), limit=MAX_LINE
            )
        finally:
            os.umask(umask)
        self.path = path
        return None

    def close(self) -> None:
        """Stop serving: close every connection and remove the socket."""
        self._unlisten()
        if self._server is not None:
            self._server.close()
            self._server = None
        for writer in list(self._writers):
            writer.close()
        self._writers.clear()
        if self.path is not None:
            self.path.unlink(missing_ok=True)
            self.path = None

    # ---------- connections ----------

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        self.host.client_connected(+1)
        try:
            await self._converse(reader, writer)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        except ValueError:  # a line past MAX_LINE
            pass
        finally:
            self.host.client_connected(-1)
            self._writers.discard(writer)
            writer.close()

    async def _converse(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        greeted = False
        while True:
            line = await reader.readline()
            if not line:
                return
            request_id: Any = ""
            try:
                message = decode(line)
                request_id = message.get("id", "")
                method = message.get("method")
                params = message.get("params") or {}
                if not isinstance(method, str) or not isinstance(params, dict):
                    raise LiveError(INVALID, "a request has a method and params")
                if not greeted:
                    if method != "hello":
                        raise LiveError(PROTOCOL_MISMATCH, "say hello first")
                    result = self._hello(params)
                    greeted = True
                elif method == "events.subscribe":
                    await self._stream(request_id, params, reader, writer)
                    return
                else:
                    result = await self._dispatch(method, params)
                reply = {"id": request_id, "result": result}
            except LiveError as err:
                reply = {"id": request_id, "error": err.to_dict()}
                writer.write(encode(reply))
                await writer.drain()
                if err.code == PROTOCOL_MISMATCH:
                    return
                continue
            except Exception as exc:  # a bug of ours must not take the TUI down
                error = LiveError(INTERNAL, f"{type(exc).__name__}: {exc}")
                writer.write(encode({"id": request_id, "error": error.to_dict()}))
                await writer.drain()
                continue
            writer.write(encode(reply))
            await writer.drain()

    def _hello(self, params: dict[str, Any]) -> dict[str, Any]:
        if params.get("protocol") != PROTOCOL:
            raise LiveError(
                PROTOCOL_MISMATCH,
                f"this TUI speaks protocol {PROTOCOL}, the client {params.get('protocol')}: "
                "restart the TUI (or update quicksql) so they match",
            )
        return {
            "protocol": PROTOCOL,
            "notebook": str(self.host.path),
            "pid": os.getpid(),
            "methods": sorted(self._methods()),
        }

    def _methods(self) -> dict[str, Callable[[dict[str, Any]], Awaitable[Any] | Any]]:
        host = self.host
        methods: dict[str, Callable[[dict[str, Any]], Any]] = {
            "snapshot": lambda p: host.snapshot(),
            "events.since": self._since,
            "events.subscribe": lambda p: None,  # handled by _converse
            "wait": self._wait,
            "head": self._head,
            "query": self._query,
            "arm": lambda p: host.set_armed(True, by="agent"),
            "disarm": lambda p: host.set_armed(False, by="agent"),
            "autorun": self._autorun,
            "run": lambda p: host.request_run(
                _cells(p), bool(p.get("all")), bool(p.get("cancel"))
            ),
            "run.wait": self._run_wait,
            "cancel": lambda p: host.cancel(),
        }
        if self.screen:
            methods["focus"] = lambda p: host.focus(_str(p, "cell"), p.get("tab"))
            methods["notify"] = lambda p: host.notify(
                _str(p, "message"), p.get("severity") or "information"
            )
        return methods

    async def _dispatch(self, method: str, params: dict[str, Any]) -> Any:
        handler = self._methods().get(method)
        if handler is None:
            if method in ("focus", "notify"):
                raise LiveError(UNSUPPORTED, f"{method} needs a TUI; this session has no screen")
            raise LiveError(UNKNOWN_METHOD, f"unknown method: {method}")
        result = handler(params)
        if asyncio.iscoroutine(result):
            result = await result
        return result

    # ---------- methods ----------

    def _since(self, params: dict[str, Any]) -> dict[str, Any]:
        """The events after ``seq``; without one, every event the ring holds."""
        if params.get("seq") is None:
            return {"seq": self.host.seq, "events": list(self.host.ring)}
        events = self.host.since(_int(params, "seq", 0))
        if events is None:
            raise LiveError(EVENTS_LOST, "those events are gone; read a snapshot instead")
        return {"seq": self.host.seq, "events": events}

    def _autorun(self, params: dict[str, Any]) -> dict[str, Any]:
        on = params.get("on")
        if not isinstance(on, bool):
            raise LiveError(INVALID, "autorun takes on: true or false")
        if params.get("all"):
            return self.host.set_autorun_all(on, by="agent")
        cells = _cells(params)
        if not cells:
            raise LiveError(INVALID, "name the cells, or all")
        return self.host.set_autorun(cells, on, by="agent")

    async def _head(self, params: dict[str, Any]) -> dict[str, Any]:
        cell = _str(params, "cell")
        relation = self.host.landed(cell)  # checked here, on the loop
        return await asyncio.to_thread(self.host.read, cell, relation, _int(params, "rows", 10))

    async def _query(self, params: dict[str, Any]) -> dict[str, Any]:
        sql = _str(params, "sql")
        views = self.host.views()
        return await asyncio.to_thread(self.host.query, sql, views, _int(params, "rows", 20))

    async def _wait(self, params: dict[str, Any]) -> dict[str, Any]:
        """How the save of the text with ``hash`` settled: waits for the
        watcher to see it and for its rerun to end."""
        h = _str(params, "hash")
        since = self.host.seq
        deadline = asyncio.get_running_loop().time() + _timeout(params)
        while True:
            try:
                disk_hash = file_hash(self.host.path)
                disk_mtime = self.host.path.stat().st_mtime
            except OSError:
                disk_hash, disk_mtime = "", 0.0
            outcome = self.host.wait_outcome(h, since, disk_hash, disk_mtime)
            if outcome is not None:
                return outcome
            await self._next_event(deadline, "the TUI hasn't settled that save yet")

    async def _run_wait(self, params: dict[str, Any]) -> dict[str, Any]:
        run = _int(params, "run_id", 0)
        deadline = asyncio.get_running_loop().time() + _timeout(params)
        while True:
            record = self.host.run_record(run)
            if record is None:
                raise LiveError(INVALID, f"no run {run} in this session")
            if record["state"] not in ("pending", "running"):
                return record
            await self._next_event(deadline, f"run {run} is still going")

    async def _stream(
        self,
        request_id: Any,
        params: dict[str, Any],
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """``events.subscribe``: the ack, the backlog after ``since``, then each
        event as it happens, until the client goes or falls behind."""
        kinds = set(params.get("kinds") or ())
        cells = set(params.get("cells") or ())
        sub = _Subscription(self.queue_size)
        self.host.subscribe(sub)  # before reading the backlog: no gap between them
        try:
            backlog: list[dict[str, Any]] = []
            if params.get("since") is not None:
                found = self.host.since(_int(params, "since", 0))
                if found is None:
                    raise LiveError(EVENTS_LOST, "those events are gone; read a snapshot instead")
                backlog = found
            writer.write(encode({"id": request_id, "result": {"seq": self.host.seq}}))
            for event in backlog:
                if _wanted(event, kinds, cells):
                    writer.write(encode({"event": event}))
            await writer.drain()
            gone = asyncio.ensure_future(reader.read(1))  # EOF: the client left
            try:
                while True:
                    get = asyncio.ensure_future(sub.queue.get())
                    lost = asyncio.ensure_future(sub.dropped.wait())
                    done, _ = await asyncio.wait(
                        {get, lost, gone}, return_when=asyncio.FIRST_COMPLETED
                    )
                    for task in (get, lost):
                        if task not in done:
                            task.cancel()
                    if gone in done:
                        return
                    if get in done:
                        event = get.result()
                        if _wanted(event, kinds, cells):
                            writer.write(encode({"event": event}))
                            await writer.drain()
                        if event["kind"] == "notebook_closed":
                            return
                        continue
                    lost_error = LiveError(EVENTS_LOST, "fell behind: resubscribe after a snapshot")
                    writer.write(encode({"id": request_id, "error": lost_error.to_dict()}))
                    await writer.drain()
                    return
            finally:
                gone.cancel()
        except LiveError as err:
            writer.write(encode({"id": request_id, "error": err.to_dict()}))
            await writer.drain()
        finally:
            self.host.unsubscribe(sub)

    # ---------- waking waiters ----------

    def _wake(self, event: dict[str, Any]) -> None:
        tick, self._tick = self._tick, asyncio.Event()
        tick.set()

    async def _next_event(self, deadline: float, why: str) -> None:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise LiveError(TIMEOUT, f"timed out: {why}")
        tick = self._tick
        try:
            # a file the watcher hasn't seen yet produces no event: look again
            # now and then, as the wait also reads the file
            await asyncio.wait_for(tick.wait(), min(remaining, 0.5))
        except asyncio.TimeoutError:
            pass


async def _answers(path: Path) -> bool:
    """Whether a live session answers ``hello`` on ``path``."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_unix_connection(str(path)), 2)
    except (OSError, asyncio.TimeoutError):
        return False
    try:
        writer.write(encode({"id": 0, "method": "hello", "params": {"protocol": PROTOCOL}}))
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 2)
        return bool(line)
    except (OSError, asyncio.TimeoutError):
        return False
    finally:
        writer.close()


def _wanted(event: dict[str, Any], kinds: set[str], cells: set[str]) -> bool:
    if kinds and event["kind"] not in kinds:
        return False
    if cells and event.get("cell") is not None and event["cell"] not in cells:
        return False
    return True


def _str(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise LiveError(INVALID, f"{key} is required")
    return value


def _int(params: dict[str, Any], key: str, default: int) -> int:
    value = params.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise LiveError(INVALID, f"{key} is an integer")
    return value


def _cells(params: dict[str, Any]) -> list[str] | None:
    cells = params.get("cells")
    if cells is None:
        return None
    if not isinstance(cells, list) or not all(isinstance(c, str) for c in cells):
        raise LiveError(INVALID, "cells is a list of cell names")
    return cells


def _timeout(params: dict[str, Any]) -> float:
    value = params.get("timeout", 600)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        raise LiveError(INVALID, "timeout is a positive number of seconds")
    return min(float(value), MAX_WAIT)

