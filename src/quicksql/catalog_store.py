"""The catalog on disk: the levels remote engines' catalogs loaded, kept
across sessions, so a reopened TUI or a restarted LSP answers without a round
trip.

``$QSQL_HOME/catalog/`` (0700) holds one file per level (0600), named by the
sha256 of its cache key, since a key can carry a DSN. Each file is a JSON
header line (format version, when the level was loaded, its scope, its
title) followed by the frame as Arrow IPC, so one rename publishes both. The
TUI and the LSP both write, so every write goes to a temp file that is
renamed into place. Past ``max_bytes`` the least recently read levels go.

Each scope a level was stored under has a marker, ``scopes/<sha256(scope)>``,
holding when it was last cleared (0: never). Clearing a scope (a run ended
on its engine) writes the time there, so a level saved under it before then
is a miss, for this session and the next; a scope nothing was stored under
(a cell's output) writes nothing.

The disk is a cache: a failure to read or write it is a miss, never an
error. Policy (how old is too old) is ``catalog.CatalogCache``'s.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

import polars as pl

from .state import quicksql_home

__all__ = ["CatalogStore", "Stored", "default_store", "PERSIST_ENV"]

PERSIST_ENV = "QSQL_CATALOG_PERSIST"  # not QUICKSQL_*: overrides.py reads those as config
DEFAULT_MAX_BYTES = 256 * 2**20
_VERSION = 1
_SUFFIX = ".level"


@dataclass(frozen=True)
class Stored:
    """A level read back: its frame, and when it was loaded from its engine."""

    frame: pl.DataFrame
    saved_at: float


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class CatalogStore:
    def __init__(
        self,
        root: Path,
        max_bytes: int = DEFAULT_MAX_BYTES,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.root = Path(root)
        self.max_bytes = max_bytes
        self._clock = clock  # recency, for eviction

    # ---------- levels ----------

    def get(self, key: str, scope: str | None) -> Stored | None:
        """The level stored under ``key``, unless it's missing, unreadable,
        or older than the last clear of ``scope`` (the reader's: a level is
        cleared by where it's read from, whoever wrote it)."""
        path = self._level(key)
        try:
            data = path.read_bytes()
        except OSError:
            return None
        try:
            head, _, body = data.partition(b"\n")
            header = json.loads(head)
            if header.get("v") != _VERSION:
                raise ValueError("another version's level")
            saved_at = float(header["saved_at"])
            frame = pl.read_ipc(io.BytesIO(body), memory_map=False)
        except Exception:
            self._unlink(path)
            return None
        cleared = self._cleared_at(scope)
        if cleared is not None and saved_at < cleared:
            return None
        self._touch(path)  # read: the most recently used
        return Stored(frame, saved_at)

    def put(self, key: str, frame: pl.DataFrame, saved_at: float, scope: str | None, title: str = "") -> None:
        """Store ``frame`` as ``key``'s level, loaded at ``saved_at`` (when
        its load began: a clear after that misses it)."""
        header = json.dumps({"v": _VERSION, "saved_at": saved_at, "scope": scope, "title": title})
        buf = io.BytesIO()
        buf.write(header.encode() + b"\n")
        try:
            frame.write_ipc(buf, compression="zstd")
        except Exception:
            return
        if scope is not None:
            self._mark(scope)  # before the level: a clear from now on finds it
        path = self._level(key)
        if self._write(path, buf.getvalue()):
            self._touch(path)
            self._evict(keep=path)

    def drop(self, key: str) -> None:
        self._unlink(self._level(key))

    def clear(self, scopes: Iterable[str], at: float) -> None:
        """Levels saved under ``scopes`` before ``at`` are misses from now
        on, here and in any later session."""
        for scope in scopes:
            cleared = self._cleared_at(scope)
            if cleared is None or cleared >= at:
                continue  # nothing stored under it, or a newer clear stands
            self._write(self._marker(scope), repr(at).encode())

    def size_bytes(self) -> int:
        return sum(size for _, _, size in self._levels())

    # ---------- the files ----------

    def _level(self, key: str) -> Path:
        return self.root / f"{_digest(key)}{_SUFFIX}"

    def _marker(self, scope: str) -> Path:
        return self.root / "scopes" / _digest(scope)

    def _mark(self, scope: str) -> None:
        """Give ``scope`` its marker (never cleared) unless it has one: made
        whole beside it and linked into place, so a clear another process
        writes meanwhile is never overwritten."""
        marker = self._marker(scope)
        if marker.exists():
            return
        tmp = self._temp(b"0", marker.parent)
        if tmp is None:
            return
        try:
            os.link(tmp, marker)
        except OSError:
            pass  # FileExistsError: another writer made it first
        finally:
            self._unlink(tmp)

    def _cleared_at(self, scope: str | None) -> float | None:
        if scope is None:
            return None
        try:
            return float(self._marker(scope).read_text())
        except (OSError, ValueError):
            return None

    def _write(self, path: Path, data: bytes) -> bool:
        """``data`` to ``path`` through a temp file renamed into place;
        False when the disk said no."""
        tmp = self._temp(data, path.parent)
        if tmp is None:
            return False
        try:
            os.replace(tmp, path)
            return True
        except OSError:
            self._unlink(tmp)
            return False

    def _temp(self, data: bytes, directory: Path) -> Path | None:
        """A 0600 temp file holding ``data`` in the store (``directory``, and
        the store, made 0700), or None when the disk said no."""
        try:
            for d in (self.root, directory):
                d.mkdir(mode=0o700, parents=True, exist_ok=True)
                os.chmod(d, 0o700)  # mkdir's mode goes through the umask
            fd, name = tempfile.mkstemp(dir=self.root, prefix=".tmp-")  # 0600
        except OSError:
            return None
        tmp = Path(name)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
        except OSError:
            self._unlink(tmp)
            return None
        return tmp

    def _levels(self) -> list[tuple[float, Path, int]]:
        """(last read, path, bytes) for every level."""
        found = []
        try:
            entries = list(os.scandir(self.root))
        except OSError:
            return []
        for entry in entries:
            if not entry.name.endswith(_SUFFIX):
                continue
            try:
                st = entry.stat()
            except OSError:
                continue
            found.append((st.st_mtime, Path(entry.path), st.st_size))
        return found

    def _evict(self, keep: Path) -> None:
        """Past the cap, the least recently read go first (``keep``, just
        written, stays even when it's bigger than the cap alone)."""
        levels = self._levels()
        total = sum(size for _, _, size in levels)
        for _, path, size in sorted(levels):
            if total <= self.max_bytes:
                return
            if path == keep:
                continue
            self._unlink(path)
            total -= size

    def _touch(self, path: Path) -> None:
        """Mark ``path`` used now: its mtime is the recency eviction reads."""
        now = self._clock()
        try:
            os.utime(path, (now, now))
        except OSError:
            pass

    @staticmethod
    def _unlink(path: Path) -> None:
        try:
            path.unlink()
        except OSError:
            pass


def default_store() -> CatalogStore | None:
    """The store under ``$QSQL_HOME/catalog/``, or None when
    ``QSQL_CATALOG_PERSIST`` turns it off (0, false, no, off)."""
    if os.environ.get(PERSIST_ENV, "").strip().lower() in ("0", "false", "no", "off"):
        return None
    return CatalogStore(quicksql_home() / "catalog")
