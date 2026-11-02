"""The live session's wire protocol: what the open TUI and ``quicksql live``
say to each other over a per-notebook Unix socket.

Newline-delimited JSON, one document per line (herdr's shape):

* request  ``{"id": 1, "method": "arm", "params": {}}``
* response ``{"id": 1, "result": {...}}`` or ``{"id": 1, "error": {"code", "message"}}``
* event    ``{"event": {"seq": 7, "kind": "cell_finished", ...}}`` (subscribers only)

The first request on a connection is ``hello``, carrying ``PROTOCOL``; a
mismatch is an error the client reports as "restart the TUI". Both sides
ignore fields they don't know. Stdlib only: the CLI imports this without
Textual, Polars or DuckDB.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

PROTOCOL = 1
MAX_LINE = 4 * 1024 * 1024  # one message; a query's rows are capped well below
MAX_ROWS = 1000  # head/query/catalog rows per request
# Unix socket paths are capped at 108 bytes on Linux, 104 on macOS
MAX_SOCKET_PATH = 100

# error codes; the CLI maps them to exit codes
NO_SESSION = "no_session"  # nothing answers on the notebook's socket
PROTOCOL_MISMATCH = "protocol"
TIMEOUT = "timeout"
INVALID = "invalid"  # malformed request or params
UNKNOWN_METHOD = "unknown_method"
UNKNOWN_CELL = "unknown_cell"
NO_PROJECT = "no_project"  # the notebook hasn't compiled since the TUI opened it
BUSY = "busy"  # a run is under way, or the cell asked for is running
REFUSED = "refused"  # the file says otherwise, or the request isn't allowed
NOT_READABLE = "not_readable"  # an output this session can't read (yet)
QUERY_FAILED = "query_failed"  # the query ran and DuckDB raised
UNKNOWN_PATH = "unknown_path"  # a catalog path segment that leads nowhere
LOAD_FAILED = "load_failed"  # a catalog level's load raised (its engine, unreachable?)
EVENTS_LOST = "events_lost"
UNSUPPORTED = "unsupported"  # a TUI verb on a host without a screen
INTERNAL = "internal"  # a bug in the session; the connection carries on


class LiveError(Exception):
    """A request the live session answered with an error, or couldn't reach."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message}


def encode(message: dict[str, Any]) -> bytes:
    """One message as a line. ``default=str`` covers a plugin's odd value."""
    return (json.dumps(message, default=str, separators=(",", ":")) + "\n").encode()


def decode(line: bytes) -> dict[str, Any]:
    try:
        message = json.loads(line)
    except ValueError as exc:
        raise LiveError(INVALID, f"not JSON: {exc}") from None
    if not isinstance(message, dict):
        raise LiveError(INVALID, "a message is a JSON object")
    return message


def file_hash(notebook: Path) -> str:
    """``models.source_hash`` of the notebook as it is on disk now: what a
    project compiled from this text reports as its ``source_hash``."""
    from ..models import source_hash

    return source_hash(notebook.read_text())


def socket_path(notebook: Path) -> Path:
    """Where the live session for ``notebook`` listens: next to its state file
    under the quicksql home, or, when that path is too long for a Unix socket,
    in the runtime directory, keyed on the home and the notebook both."""
    from ..state import quicksql_home, state_file

    resolved = notebook.resolve()
    stem = state_file(resolved).stem  # <name>-<key of the resolved path>
    home = quicksql_home()
    path = home / "run" / f"{stem}.sock"
    if len(os.fsencode(path)) <= MAX_SOCKET_PATH:
        return path
    key = hashlib.sha256(f"{home.resolve()}\0{resolved}".encode()).hexdigest()[:16]
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    base = Path(runtime) / "quicksql" if runtime else Path(f"/tmp/quicksql-{os.getuid()}")
    return base / f"{key}.sock"
