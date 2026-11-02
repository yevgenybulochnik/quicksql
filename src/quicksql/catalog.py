"""The schema catalog the TUI browses and an agent reads: a project's
contexts, its @catalog targets and its cells' landed outputs, as drillable
``CatalogNode``s.

Pure data + blocking loaders. The TUI runs ``load()`` in a thread worker and
pushes the frame as a Sheet whose Enter drills via ``child``; ``resolve``
walks the same tree by name, a path segment per level. Each engine's executor
builds its own contexts' trees (``Executor.catalog``); outputs are read back
through DuckDB. Leaves load field-path frames
(``executors.base.field_path_frame``).
"""

from __future__ import annotations

import difflib
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

import duckdb
import polars as pl

from .conduit import duckdb_field_paths, load_extension
from .config import make_sink
from .models import CatalogNode

if TYPE_CHECKING:
    from .catalog_store import CatalogStore
    from .compiler import Project
    from .registry import Registries

__all__ = [
    "CatalogCache",
    "CatalogNode",
    "CatalogPathError",
    "Hit",
    "Resolved",
    "narrow",
    "page",
    "project_root_node",
    "redact",
    "resolve",
    "sink_output_node",
    "stamp",
]

DEFAULT_MAX_BYTES = 64 * 2**20  # a cache holds this much; the least recently used goes first
DEFAULT_REVALIDATE_AFTER = 15 * 60  # seconds: a persisted level older than this refreshes behind
DEFAULT_MAX_AGE = 7 * 24 * 3600  # seconds: one older than this is a miss
REFRESH_WORKERS = 4  # refreshes behind running at once, per cache


@dataclass(frozen=True)
class Hit:
    """A level as the cache answered it: its frame, whether this call didn't
    load it (in memory, on disk, or another asker's load), how many seconds
    ago it was loaded from its engine, and whether that's past revalidation,
    so a refresh is under way behind the answer."""

    frame: pl.DataFrame
    cached: bool
    age: float = 0.0
    stale: bool = False


@dataclass
class _Entry:
    frame: pl.DataFrame
    scope: str | None
    size: int
    saved_at: float  # when its load began
    persist: bool


class _Load:
    """A load in flight: its askers wait on ``done``."""

    def __init__(self, scope: str | None) -> None:
        self.scope = scope
        self.done = threading.Event()
        self.hit: Hit | None = None
        self.error: BaseException | None = None
        self.stale = False  # a clear came while it loaded: answer, but don't keep


class _Behind:
    """Runs refreshes behind answers, at most ``workers`` at once, on daemon
    threads (quitting never waits for one) that end when there's no work."""

    def __init__(self, workers: int) -> None:
        self.workers = workers
        self._jobs: deque[Callable[[], None]] = deque()
        self._running = 0
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)

    def submit(self, job: Callable[[], None]) -> None:
        with self._lock:
            self._jobs.append(job)
            if self._running < self.workers:
                self._running += 1
                threading.Thread(target=self._work, name="catalog-refresh", daemon=True).start()

    def _work(self) -> None:
        while True:
            with self._lock:
                if not self._jobs:
                    self._running -= 1
                    self._idle.notify_all()
                    return
                job = self._jobs.popleft()
            try:
                job()
            except Exception:
                pass  # a refresh that fails leaves the level it had

    def wait(self, timeout: float | None = None) -> bool:
        with self._lock:
            return self._idle.wait_for(lambda: not self._jobs and not self._running, timeout)


class CatalogCache:
    """Loaded frames by node cache_key: browsing revisits levels, and loads
    are remote. Safe to share between threads (the TUI's workers, an agent's
    requests):

    * a second asker for a level already loading waits for that load
      instead of starting another;
    * a load that a clear overtakes answers its askers but isn't kept;
    * past ``max_bytes`` the least recently used levels go (one level bigger
      than the cap is kept alone).

    With a ``store`` (``catalog_store``), a level that persists
    (``CatalogNode.persist``: a remote engine's) is read through the disk:
    memory, then disk, then a load, which is stored on both. Such a level
    ages from its load: past ``revalidate_after`` seconds it still answers
    at once, marked stale, and is refreshed behind the answer (at most
    ``REFRESH_WORKERS`` at once; ``on_refresh(node)`` hears of a refresh that
    changed it); past ``max_age`` it's a miss. A level that doesn't persist
    stays until it's cleared, as before.

    Invalidate one node (a refetch), the levels under some top-level rows
    (``scopes``: what a run touched), or everything (a recompile, a notebook
    switch). Clears reach the disk too: the node's level goes, a scope's
    levels saved before the clear are misses in later sessions as well, and
    a clear of everything makes this cache pass over what the disk had.
    """

    def __init__(
        self,
        max_bytes: int = DEFAULT_MAX_BYTES,
        *,
        store: CatalogStore | None = None,
        revalidate_after: float = DEFAULT_REVALIDATE_AFTER,
        max_age: float = DEFAULT_MAX_AGE,
        on_refresh: Callable[[CatalogNode], None] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.max_bytes = max_bytes
        self.store = store
        self.revalidate_after = revalidate_after
        self.max_age = max_age
        self.on_refresh = on_refresh
        self.clock = clock
        self._frames: OrderedDict[str, _Entry] = OrderedDict()
        self._loading: dict[str, _Load] = {}
        self._size = 0
        self._floor = float("-inf")  # disk levels saved before a clear of everything are misses
        self._lock = threading.Lock()
        self._behind = _Behind(REFRESH_WORKERS)

    def __len__(self) -> int:
        return len(self._frames)

    @property
    def size_bytes(self) -> int:
        """What the cached frames take, by Polars' estimate."""
        return self._size

    def peek(self, node: CatalogNode) -> pl.DataFrame | None:
        """The node's frame in memory, or None: never loads."""
        with self._lock:
            entry = self._frames.get(node.cache_key)
            if entry is None:
                return None
            self._frames.move_to_end(node.cache_key)
            return entry.frame

    def load(self, node: CatalogNode) -> tuple[pl.DataFrame, bool]:
        """The node's frame and whether this call didn't load it (cached, or
        another asker's load)."""
        hit = self.read(node)
        return hit.frame, hit.cached

    def read(self, node: CatalogNode) -> Hit:
        """The node's level: from memory, the disk, another asker's load or
        its own, with how old it is."""
        key = node.cache_key
        with self._lock:
            hit = self._remembered(node)
            if hit is not None:
                return hit
            pending = self._loading.get(key)
            owner = pending is None
            if owner:
                pending = self._loading[key] = _Load(node.scope)
        if not owner:
            pending.done.wait()
            if pending.error is not None:
                raise pending.error
            return replace(pending.hit, cached=True)
        try:
            hit = self._fill(node, pending)
        except BaseException as exc:
            pending.error = exc
            raise
        finally:
            with self._lock:
                if self._loading.get(key) is pending:
                    del self._loading[key]
            pending.done.set()
        if hit.stale:
            with self._lock:
                self._refresh_behind(node)
        return hit

    def wait_refreshes(self, timeout: float | None = None) -> bool:
        """Wait until no refresh is under way; False if ``timeout`` came first."""
        return self._behind.wait(timeout)

    def invalidate(
        self, node: CatalogNode | None = None, *, scopes: Iterable[str] | None = None
    ) -> None:
        """Drop ``node``'s frame, the frames under ``scopes``, or (neither
        given) every frame. A load in flight for what's dropped answers its
        askers but isn't kept, and the next asker loads afresh."""
        now = self.clock()
        if scopes is not None:
            scopes = set(scopes)
        with self._lock:
            if node is not None:
                keys = [node.cache_key]
            elif scopes is not None:
                keys = [k for k, e in self._frames.items() if e.scope in scopes]
                keys += [k for k, p in self._loading.items() if p.scope in scopes]
            else:
                keys = [*self._frames, *self._loading]
                self._floor = now
            for key in keys:
                entry = self._frames.pop(key, None)
                if entry is not None:
                    self._size -= entry.size
                pending = self._loading.pop(key, None)
                if pending is not None:
                    pending.stale = True
        if self.store is None:
            return
        if node is not None:
            if node.persist:
                self.store.drop(node.cache_key)
        elif scopes is not None:
            self.store.clear(scopes, at=now)

    # ---------- inside ----------

    def _remembered(self, node: CatalogNode) -> Hit | None:
        """The node's level in memory (the lock held), aged; None when
        there's none or it's past ``max_age``."""
        key = node.cache_key
        entry = self._frames.get(key)
        if entry is None:
            return None
        age = self.clock() - entry.saved_at
        if entry.persist and age > self.max_age:
            del self._frames[key]
            self._size -= entry.size
            return None
        self._frames.move_to_end(key)
        stale = entry.persist and age > self.revalidate_after
        if stale:
            self._refresh_behind(node)
        return Hit(entry.frame, True, age, stale)

    def _fill(self, node: CatalogNode, pending: _Load) -> Hit:
        """The owner's read: the disk when the level persists, else a load,
        kept unless a clear overtook it."""
        started = self.clock()
        store = self.store if node.persist else None
        if store is not None:
            stored = store.get(node.cache_key, node.scope)
            if stored is not None and stored.saved_at >= self._floor:
                age = started - stored.saved_at
                if age <= self.max_age:
                    with self._lock:
                        if not pending.stale:
                            self._keep(node, stored.frame, stored.saved_at)
                    pending.hit = Hit(stored.frame, True, age, age > self.revalidate_after)
                    return pending.hit
        frame = node.load()
        with self._lock:
            kept = not pending.stale
            if kept:
                self._keep(node, frame, started)
        if kept and store is not None:
            self._persist(node, frame, started, pending)
        pending.hit = Hit(frame, False)
        return pending.hit

    def _persist(self, node: CatalogNode, frame: pl.DataFrame, started: float, pending: _Load) -> None:
        """Write a kept load to disk. A clear that comes while it's written
        (``pending`` is still registered, so the clear marks it) may have
        found no marker for its scope yet: the level goes again."""
        assert self.store is not None
        self.store.put(node.cache_key, frame, saved_at=started, scope=node.scope, title=node.title)
        if pending.stale:
            self.store.drop(node.cache_key)

    def _refresh_behind(self, node: CatalogNode) -> None:
        """Queue a refresh of ``node`` (the lock held), unless one, or any
        load of it, is under way. It loads like an owner: a clear that
        overtakes it means it isn't kept."""
        key = node.cache_key
        if key in self._loading:
            return
        pending = self._loading[key] = _Load(node.scope)
        self._behind.submit(lambda: self._refresh(node, pending))

    def _refresh(self, node: CatalogNode, pending: _Load) -> None:
        """A refresh behind an answer (a worker thread). One that fails
        leaves the level it had; one that changed it is told, once its
        waiters have it."""
        key = node.cache_key
        started = self.clock()
        kept = changed = False
        try:
            frame = node.load()
            with self._lock:
                old = self._frames.get(key)
                kept = not pending.stale
                changed = old is None or not old.frame.equals(frame)
                if kept:
                    self._keep(node, frame, started)
            pending.hit = Hit(frame, False)
            if kept and self.store is not None and node.persist:
                self._persist(node, frame, started, pending)
        except Exception as exc:
            pending.error = exc
        finally:
            with self._lock:
                if self._loading.get(key) is pending:
                    del self._loading[key]
            pending.done.set()
        if kept and changed and self.on_refresh is not None:
            self.on_refresh(node)

    def _keep(self, node: CatalogNode, frame: pl.DataFrame, saved_at: float) -> None:
        """Hold ``frame`` as ``node``'s level (the lock held), within the cap."""
        key = node.cache_key
        size = frame.estimated_size()
        old = self._frames.pop(key, None)
        if old is not None:
            self._size -= old.size
        self._frames[key] = _Entry(frame, node.scope, size, saved_at, node.persist)
        self._size += size
        while self._size > self.max_bytes and len(self._frames) > 1:
            _, dropped = self._frames.popitem(last=False)
            self._size -= dropped.size


# ---------- walking by name ----------


class CatalogPathError(LookupError):
    """A path that doesn't lead anywhere: segment ``depth`` names no row of
    its level (``unknown``; ``candidates`` are the names closest to it), or
    asks for a level beneath a leaf (``leaf``)."""

    def __init__(
        self, path: Sequence[str], depth: int, reason: str, message: str,
        candidates: Sequence[str] = (), total: int = 0,
    ) -> None:
        super().__init__(message)
        self.path = tuple(path)
        self.depth = depth
        self.segment = path[depth]
        self.reason = reason
        self.candidates = list(candidates)
        self.total = total  # how many names that level has


@dataclass(frozen=True)
class Resolved:
    """Where a path led: the node, its loaded frame, whether the frame came
    without a load of this call's own, how many seconds ago it was loaded,
    and whether it's being refreshed behind this answer (``Hit``)."""

    node: CatalogNode
    frame: pl.DataFrame
    cached: bool
    path: tuple[str, ...]
    age: float = 0.0
    stale: bool = False


def resolve(
    root: CatalogNode, path: Sequence[str], cache: CatalogCache, refresh: bool = False
) -> Resolved:
    """Walk ``path`` (a row name per level, see ``CatalogNode.row_name``) down
    from ``root`` and load where it ends, through ``cache``.

    A level that declares its ``name_columns`` and isn't cached is never
    listed on the way: the next level is built from the segment, so a cold
    exact path costs the one load at its end. Such a guess is checked when
    the end fails to load or comes back empty (Postgres answers an unknown
    table with no columns): the levels guessed through are listed then, so an
    unknown segment is named rather than a load error passed on. ``refresh``
    drops where the path ends from the cache first, so it loads afresh.
    """
    node = root
    guessed: list[tuple[CatalogNode, int]] = []  # (level, depth) built from a name
    for depth, segment in enumerate(path):
        if node.child is None:
            raise _past_leaf(path, depth, node)
        frame = cache.peek(node)
        row = node.row_for(segment) if frame is None else None
        if row is None:
            if frame is None:
                frame, _ = cache.load(node)
            row = _find(node, frame, path, depth)
        else:
            guessed.append((node, depth))
        child = node.child(row)
        if child is None:  # e.g. a context whose engine has no catalog
            raise CatalogPathError(path, depth, "leaf", f"{segment} can't be browsed")
        node = child
    if refresh:
        cache.invalidate(node)
    try:
        hit = cache.read(node)
    except Exception:
        _check(guessed, path, cache)
        raise
    if hit.frame.height == 0 and guessed:
        _check(guessed, path, cache)
    return Resolved(node, hit.frame, hit.cached, tuple(path), hit.age, hit.stale)


def _find(node: CatalogNode, frame: pl.DataFrame, path: Sequence[str], depth: int) -> dict[str, Any]:
    segment = path[depth]
    names = []
    for row in frame.iter_rows(named=True):
        name = node.row_name(row)
        if name == segment:
            return row
        names.append(name)
    close = difflib.get_close_matches(segment, names, n=10, cutoff=0.6)
    where = " / ".join(path[:depth]) or "the catalog"
    hint = f"; close: {', '.join(close)}" if close else ""
    raise CatalogPathError(
        path, depth, "unknown", f"{segment!r} isn't in {where} ({len(names)} names{hint})",
        candidates=close, total=len(names),
    )


def _check(guessed: list[tuple[CatalogNode, int]], path: Sequence[str], cache: CatalogCache) -> None:
    """List each level a walk guessed through; raise for the first segment
    it doesn't have. A level that won't list says nothing either way."""
    for level, depth in guessed:
        try:
            frame, _ = cache.load(level)
        except Exception:
            return
        _find(level, frame, path, depth)


def _past_leaf(path: Sequence[str], depth: int, node: CatalogNode) -> CatalogPathError:
    return CatalogPathError(path, depth, "leaf", f"{node.title} has nothing beneath it")


# ---------- pages and filters ----------


def narrow(
    node: CatalogNode,
    frame: pl.DataFrame,
    *,
    match: str | None = None,
    columns: Sequence[str] | None = None,
    top_level: bool = False,
) -> pl.DataFrame:
    """``frame`` cut down before paging: rows whose name (a leaf's field
    path) contains ``match``, ignoring case; at a leaf, only top-level
    columns; only ``columns``."""
    leaf = "field_path" in frame.columns
    if match:
        needle = match.lower()
        if leaf:
            frame = frame.filter(pl.col("field_path").str.to_lowercase().str.contains(needle, literal=True))
        else:
            keep = [needle in node.row_name(row).lower() for row in frame.iter_rows(named=True)]
            frame = frame.filter(pl.Series(keep, dtype=pl.Boolean))
    if top_level and leaf:
        frame = frame.filter(~pl.col("field_path").str.contains(".", literal=True))
    if columns:
        missing = [c for c in columns if c not in frame.columns]
        if missing:
            raise ValueError(
                f"no column {', '.join(missing)}: this level has {', '.join(frame.columns)}"
            )
        frame = frame.select(list(columns))
    return frame


def page(node: CatalogNode, frame: pl.DataFrame, offset: int, rows: int) -> dict[str, Any]:
    """One page of a level as plain data: its rows, each row's name (what a
    path segment says to go beneath it; None at a leaf) and where the next
    page starts (None at the last)."""
    part = frame.slice(offset, rows)
    leaf = node.child is None
    end = offset + part.height
    return {
        "title": node.title,
        "leaf": leaf,
        "columns": list(frame.columns),
        "rows": [list(r) for r in part.rows()],
        "names": None if leaf else [node.row_name(r) for r in part.iter_rows(named=True)],
        "total": frame.height,
        "offset": offset,
        "next_offset": end if end < frame.height else None,
    }


def redact(text: str, registries: Registries) -> str:
    """``text`` with every secret an engine knows the shape of hidden: what
    the catalog shows (names, titles, error messages) goes through here."""
    for name in registries.executors.names():
        text = registries.executors.get(name).redact(text)
    return text


# ---------- node builders ----------


def sink_output_node(
    cell_name: str,
    config: Any,
    root: Path,
    registries: Registries,
    connect: Callable[[], Any] | None = None,
) -> CatalogNode:
    """Leaf: field paths of a cell's landed sink, read back via its ref_expr.

    ``connect`` supplies the DuckDB connection; pass a cursor factory of the
    live conduit when it may already hold the sink's ATTACH (a throwaway
    connection cannot re-ATTACH a file another connection holds).
    """

    def load() -> pl.DataFrame:
        sink = make_sink(config, root, registries)
        con = connect() if connect is not None else duckdb.connect()
        try:
            for ext in sink.requires:
                load_extension(con, ext)
            sink.prepare(con)
            rel = con.sql(f"SELECT * FROM {sink.ref_expr(cell_name)} LIMIT 0")
            return duckdb_field_paths(rel.columns, rel.types)
        finally:
            con.close()

    return CatalogNode(title=f"schema({cell_name})", load=load)


def stamp(node: CatalogNode | None, *, scope: str | None, persist: bool) -> CatalogNode | None:
    """``node`` and every level beneath it, stamped with the top-level row
    it sits under (``scope``) and whether it's kept on disk (``persist``:
    its engine's ``catalog_persists``). The root stamps its subtrees; the
    LSP stamps the leaves it builds itself."""
    if node is None:
        return None
    child = node.child
    if child is None:
        return replace(node, scope=scope, persist=persist)
    return replace(
        node, scope=scope, persist=persist,
        child=lambda row: stamp(child(row), scope=scope, persist=persist),
    )


def project_root_node(project: Project, connect: Callable[[], Any] | None = None) -> CatalogNode:
    """Top level: the project's engine contexts (deduped), @catalog-declared
    browse targets, and cell outputs. Each subtree is scoped by its row's
    name (``CatalogNode.scope``) and persists as its engine declares
    (``stamp``)."""
    registries = project.registries
    contexts: list[tuple[str, str, Any]] = []  # (context_key, engine, config)
    seen: set[str] = set()
    for name in project.order:
        cell = project.cells[name]
        key = registries.executors.get(cell.engine).context_key(cell.config)
        if key not in seen:
            seen.add(key)
            contexts.append((key, cell.engine, cell.config))

    declared: dict[str, CatalogNode] = {}  # row name -> ready node
    rows = [("context", key, engine, "") for key, engine, _ in contexts]
    for engine, targets in (getattr(project.config, "catalog", None) or {}).items():
        executor = registries.executors.get(engine)
        for target in targets:
            made = executor.catalog_target(target, project.root)
            if made is None:
                break  # the engine takes no @catalog targets
            name, node = made
            if name in seen:
                continue  # a cell's context already covers it
            seen.add(name)
            declared[name] = node
            rows.append(("context", name, engine, "@catalog"))

    for name in project.order:
        cell = project.cells[name]
        rows.append(("output", name, cell.engine, f"{cell.engine} → {cell.sink_type}"))
    frame = pl.DataFrame(
        rows,
        schema={"kind": pl.Utf8, "name": pl.Utf8, "engine": pl.Utf8, "detail": pl.Utf8},
        orient="row",
    )
    by_key = {key: (engine, config) for key, engine, config in contexts}

    def child(row: dict[str, Any]) -> CatalogNode | None:
        name = row["name"]
        if row["kind"] == "output":  # local, and a run rewrites it: never kept
            cell = project.cells[name]
            node = sink_output_node(cell.name, cell.config, project.root, registries, connect=connect)
            return stamp(node, scope=name, persist=False)
        executor = registries.executors.get(row["engine"])
        if name in declared:
            node = declared[name]
        else:
            node = executor.catalog(by_key[name][1], project.root)
        return stamp(node, scope=name, persist=executor.catalog_persists)

    return CatalogNode(title="catalog", load=lambda: frame, child=child)
