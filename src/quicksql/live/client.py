"""The CLI's side of the live session: a blocking client for the open TUI's
socket. Stdlib only (with ``protocol``), so ``quicksql live`` loads neither
Textual, Polars nor DuckDB."""

from __future__ import annotations

import socket
from pathlib import Path
from typing import Any, Iterator

from .protocol import (
    EVENTS_LOST,
    MAX_LINE,
    NO_SESSION,
    PROTOCOL,
    PROTOCOL_MISMATCH,
    TIMEOUT,
    LiveError,
    decode,
    encode,
    socket_path,
)

CONNECT_TIMEOUT = 5.0
READ_TIMEOUT = 120.0  # head and query read through the session: give them time


class LiveClient:
    """One connection to the live session of a notebook."""

    def __init__(self, sock: socket.socket, notebook: Path) -> None:
        self.sock = sock
        self.notebook = notebook
        self._file = sock.makefile("rb")
        self._next_id = 0
        self.hello: dict[str, Any] = {}

    @classmethod
    def connect(cls, notebook: Path, timeout: float = CONNECT_TIMEOUT) -> LiveClient:
        """Connect and say hello; ``LiveError(no_session)`` when no TUI has
        the notebook open."""
        notebook = Path(notebook)
        path = socket_path(notebook)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            sock.connect(str(path))
        except (FileNotFoundError, ConnectionRefusedError, socket.timeout) as exc:
            sock.close()
            raise LiveError(
                NO_SESSION,
                f"no live session for {notebook}: open it with `quicksql tui {notebook}` "
                f"({type(exc).__name__})",
            ) from None
        client = cls(sock, notebook)
        try:
            client.hello = client.call("hello", protocol=PROTOCOL, client="quicksql")
        except BaseException:
            client.close()
            raise
        return client

    def close(self) -> None:
        self._file.close()
        self.sock.close()

    def __enter__(self) -> LiveClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def call(self, method: str, **params: Any) -> Any:
        """One request, its result; a session's error raises ``LiveError``.
        A request with a ``timeout`` param (``wait``, ``run.wait``) gets that
        long, and a little more, for its answer; any other gets READ_TIMEOUT."""
        request_id = self._send(method, params)
        limit = params.get("timeout")
        self.sock.settimeout(
            float(limit) + 10 if isinstance(limit, (int, float)) else READ_TIMEOUT
        )
        while True:
            message = self._read()
            if "event" in message:
                continue  # not for a plain request
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                raise LiveError(error.get("code", "error"), error.get("message", ""))
            return message.get("result")

    def events(
        self,
        since: int | None = None,
        kinds: list[str] | None = None,
        cells: list[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Subscribe, and return the events as they come (the backlog after
        ``since`` first) until the session closes the stream. Subscribed on
        return, not on the first ``next``: nothing happens in between unseen."""
        params: dict[str, Any] = {}
        if since is not None:
            params["since"] = since
        if kinds:
            params["kinds"] = kinds
        if cells:
            params["cells"] = cells
        request_id = self._send("events.subscribe", params)
        while True:
            message = self._read()
            if message.get("id") != request_id or "event" in message:
                continue
            if "error" in message:
                error = message["error"]
                raise LiveError(error.get("code", EVENTS_LOST), error.get("message", ""))
            break  # the ack
        self.sock.settimeout(None)  # events come when they come
        return self._stream(request_id)

    def _stream(self, request_id: int) -> Iterator[dict[str, Any]]:
        while True:
            try:
                message = self._read()
            except LiveError as err:
                if err.code == NO_SESSION:
                    return  # the TUI closed the notebook, or quit
                raise
            if "event" in message:
                yield message["event"]
                continue
            if message.get("id") == request_id and "error" in message:
                error = message["error"]
                raise LiveError(error.get("code", EVENTS_LOST), error.get("message", ""))

    def _send(self, method: str, params: dict[str, Any]) -> int:
        self._next_id += 1
        try:
            self.sock.sendall(encode({"id": self._next_id, "method": method, "params": params}))
        except OSError as exc:
            raise LiveError(NO_SESSION, f"the live session went away: {exc}") from None
        return self._next_id

    def _read(self) -> dict[str, Any]:
        try:
            line = self._file.readline(MAX_LINE)
        except socket.timeout:
            raise LiveError(TIMEOUT, "the live session didn't answer in time") from None
        except OSError as exc:
            raise LiveError(NO_SESSION, f"the live session went away: {exc}") from None
        if not line:
            raise LiveError(NO_SESSION, "the live session closed the connection")
        return decode(line)


def probe(notebook: Path) -> dict[str, Any] | None:
    """The hello of the live session hosting ``notebook``, or None when
    nothing answers (a protocol mismatch still counts as hosted)."""
    if not socket_path(Path(notebook)).exists():
        return None
    try:
        with LiveClient.connect(notebook, timeout=2.0) as client:
            return client.hello
    except LiveError as err:
        if err.code == PROTOCOL_MISMATCH:
            return {"protocol": None}
        return None
