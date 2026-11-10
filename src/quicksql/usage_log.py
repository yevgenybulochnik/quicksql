"""The usage log on disk: what cells read, one JSON line per logged cell.

``$QSQL_HOME/usage/`` (0700). A process appends to ``YYYY-MM-DD.<pid>.ndjson``
(0600), so a TUI and a CLI run never share a file and nothing needs a lock; a
run's lines go in one ``write`` on an ``O_APPEND`` descriptor. Once a day is
over (older than yesterday) its files fold into one, ``YYYY-MM-DD.ndjson``,
without its duplicate lines.

Only the recorder (``plugins.usage``) writes here and only the report
(``usage``) reads it back; this module knows a line's envelope (``v``, ``day``,
``notebook``, ``cell``, ``key``) and nothing of its content.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator

from .home import quicksql_home

VERSION = 1
RETENTION_DAYS = 365
_DAY_FILE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:\.(\d+))?\.ndjson$")

Key = tuple[str, str, str]  # (notebook, cell, key): one logged read of a cell


def usage_dir() -> Path:
    return quicksql_home() / "usage"


def _key(entry: dict[str, Any]) -> Key:
    return (str(entry.get("notebook")), str(entry.get("cell")), str(entry.get("key")))


def _entries(data: bytes) -> Iterator[dict[str, Any]]:
    """The lines of a day file this version can read: a torn line (a process
    died mid-write) and a line of another version are skipped."""
    for raw in data.splitlines():
        try:
            entry = json.loads(raw)
        except ValueError:
            continue
        if isinstance(entry, dict) and entry.get("v") == VERSION:
            yield entry


class UsageLog:
    def __init__(self, root: Path | None = None, pid: int | None = None) -> None:
        self.root = root if root is not None else usage_dir()
        self.pid = pid if pid is not None else os.getpid()

    # ---- writing

    def append(self, entries: Iterable[dict[str, Any]], day: str) -> None:
        """Add lines to this process's file for ``day``. Raises OSError when
        the log can't be written."""
        data = b"".join(
            json.dumps(e, separators=(",", ":"), sort_keys=True).encode() + b"\n" for e in entries
        )
        if not data:
            return
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)  # mkdir's mode goes through the umask
        path = self.root / f"{day}.{self.pid}.ndjson"
        fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            view = memoryview(data)
            while view:  # one write, repeated only when it came up short
                view = view[os.write(fd, view) :]
        finally:
            os.close(fd)

    # ---- reading

    def _files(self) -> list[tuple[str, Path]]:
        """(day, path) of every day file, oldest day first; a day's compacted
        file before its per-process ones."""
        try:
            found = [
                (m.group(1), int(m.group(2) or -1), p)
                for p in self.root.iterdir()
                if (m := _DAY_FILE.match(p.name))
            ]
        except OSError:
            return []
        return [(day, path) for day, _, path in sorted(found)]

    def day_files(self, day: str) -> list[Path]:
        return [path for d, path in self._files() if d == day]

    def read(self, since: str | None = None) -> Iterator[dict[str, Any]]:
        """Every readable line, oldest day first; from day ``since`` on."""
        for day, path in self._files():
            if since is not None and day < since:
                continue
            try:
                data = path.read_bytes()
            except OSError:  # compacted or pruned by another process meanwhile
                continue
            yield from _entries(data)

    def keys(self, day: str) -> set[Key]:
        """What every process logged on ``day``."""
        return {_key(entry) for entry in self.read(since=day) if entry.get("day") == day}

    # ---- upkeep

    def prune(self, today: date, keep_days: int = RETENTION_DAYS) -> None:
        """Delete the day files older than ``keep_days``."""
        oldest = (today - timedelta(days=keep_days)).isoformat()
        for day, path in self._files():
            if day < oldest:
                try:
                    path.unlink()
                except OSError:
                    pass

    def compact(self, today: date) -> None:
        """Fold each day older than yesterday that still has per-process
        files into one file."""
        yesterday = (today - timedelta(days=1)).isoformat()
        days: dict[str, list[Path]] = {}
        for day, path in self._files():
            if day < yesterday:
                days.setdefault(day, []).append(path)
        for day, files in days.items():
            if any(path.name != f"{day}.ndjson" for path in files):
                self.compact_day(day, files)

    def compact_day(self, day: str, files: list[Path]) -> bool:
        """Fold ``files``, a listing of ``day``'s files, into the day's one
        file. Every file is opened before any is read: if one is gone by then,
        another process is compacting the day, and this one leaves it alone
        (it could otherwise rename a result missing that file's lines over the
        other's). Returns whether it compacted."""
        opened = []
        try:
            for path in files:
                opened.append(open(path, "rb"))
        except OSError:
            for f in opened:
                f.close()
            return False
        try:
            seen: set[Key] = set()
            kept: list[dict[str, Any]] = []
            for f in opened:
                for entry in _entries(f.read()):
                    if _key(entry) not in seen:
                        seen.add(_key(entry))
                        kept.append(entry)
        finally:
            for f in opened:
                f.close()
        target = self.root / f"{day}.ndjson"
        try:
            fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".tmp-")  # 0600
            try:
                with os.fdopen(fd, "wb") as out:
                    for entry in kept:
                        line = json.dumps(entry, separators=(",", ":"), sort_keys=True)
                        out.write(line.encode() + b"\n")
                os.replace(tmp, target)
            except BaseException:
                _unlink(tmp)
                raise
        except OSError:
            return False
        for path in files:
            if path != target:
                _unlink(path)
        return True


def _unlink(path: Path | str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass
