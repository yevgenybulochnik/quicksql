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
from collections import OrderedDict
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Iterable, Sequence

import duckdb
import polars as pl

from .conduit import duckdb_field_paths, load_extension
from .config import make_sink
from .models import CatalogNode

if TYPE_CHECKING:
    from .compiler import Project
    from .registry import Registries

__all__ = [
    "CatalogCache",
    "CatalogNode",
    "CatalogPathError",
    "Resolved",
    "narrow",
    "page",
    "project_root_node",
    "redact",
    "resolve",
    "sink_output_node",
]

DEFAULT_MAX_BYTES = 64 * 2**20  # a cache holds this much; the least recently used goes first


class _Load:
    """A load in flight: its askers wait on ``done``."""

    def __init__(self, scope: str | None) -> None:
        self.scope = scope
        self.done = threading.Event()
        self.frame: pl.DataFrame | None = None
        self.error: BaseException | None = None
        self.stale = False  # a clear came while it loaded: answer, but don't keep


class CatalogCache:
    """Loaded frames by node cache_key: browsing revisits levels, and loads
    are remote. Safe to share between threads (the TUI's workers, an agent's
    requests):

    * a second asker for a level already loading waits for that load
      instead of starting another;
    * a load that a clear overtakes answers its askers but isn't kept;
    * past ``max_bytes`` the least recently used levels go (one level bigger
      than the cap is kept alone).

    Invalidate one node (a refetch), the levels under some top-level rows
    (``scopes``: what a run touched), or everything (a recompile, a notebook
    switch).
    """

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES) -> None:
        self.max_bytes = max_bytes
        self._frames: OrderedDict[str, tuple[pl.DataFrame, str | None, int]] = OrderedDict()
        self._loading: dict[str, _Load] = {}
        self._size = 0
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self._frames)

    @property
    def size_bytes(self) -> int:
        """What the cached frames take, by Polars' estimate."""
        return self._size

    def peek(self, node: CatalogNode) -> pl.DataFrame | None:
        """The node's cached frame, or None: never loads."""
        with self._lock:
            entry = self._frames.get(node.cache_key)
            if entry is None:
                return None
            self._frames.move_to_end(node.cache_key)
            return entry[0]

    def load(self, node: CatalogNode) -> tuple[pl.DataFrame, bool]:
        """The node's frame and whether this call didn't load it (cached, or
        another asker's load)."""
        key = node.cache_key
        with self._lock:
            entry = self._frames.get(key)
            if entry is not None:
                self._frames.move_to_end(key)
                return entry[0], True
            pending = self._loading.get(key)
            owner = pending is None
            if owner:
                pending = self._loading[key] = _Load(node.scope)
        if not owner:
            pending.done.wait()
            if pending.error is not None:
                raise pending.error
            return pending.frame, True
        try:
            frame = node.load()
        except BaseException as exc:
            pending.error = exc
            raise
        else:
            pending.frame = frame
            with self._lock:
                if not pending.stale:
                    self._store(key, frame, node.scope)
            return frame, False
        finally:
            with self._lock:
                if self._loading.get(key) is pending:
                    del self._loading[key]
            pending.done.set()

    def invalidate(
        self, node: CatalogNode | None = None, *, scopes: Iterable[str] | None = None
    ) -> None:
        """Drop ``node``'s frame, the frames under ``scopes``, or (neither
        given) every frame. A load in flight for what's dropped answers its
        askers but isn't kept, and the next asker loads afresh."""
        with self._lock:
            if node is not None:
                keys = [node.cache_key]
            elif scopes is not None:
                wanted = set(scopes)
                keys = [k for k, (_, s, _) in self._frames.items() if s in wanted]
                keys += [k for k, p in self._loading.items() if p.scope in wanted]
            else:
                keys = [*self._frames, *self._loading]
            for key in keys:
                entry = self._frames.pop(key, None)
                if entry is not None:
                    self._size -= entry[2]
                pending = self._loading.pop(key, None)
                if pending is not None:
                    pending.stale = True

    def _store(self, key: str, frame: pl.DataFrame, scope: str | None) -> None:
        size = frame.estimated_size()
        self._frames[key] = (frame, scope, size)
        self._size += size
        while self._size > self.max_bytes and len(self._frames) > 1:
            _, (_, _, dropped) = self._frames.popitem(last=False)
            self._size -= dropped


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
    """Where a path led: the node, its loaded frame, and whether the frame
    came without a load of this call's own."""

    node: CatalogNode
    frame: pl.DataFrame
    cached: bool
    path: tuple[str, ...]


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
        frame, cached = cache.load(node)
    except Exception:
        _check(guessed, path, cache)
        raise
    if frame.height == 0 and guessed:
        _check(guessed, path, cache)
    return Resolved(node, frame, cached, tuple(path))


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


def _scoped(node: CatalogNode | None, scope: str) -> CatalogNode | None:
    """``node`` and every level beneath it, stamped with ``scope``."""
    if node is None:
        return None
    child = node.child
    if child is None:
        return replace(node, scope=scope)
    return replace(node, scope=scope, child=lambda row: _scoped(child(row), scope))


def project_root_node(project: Project, connect: Callable[[], Any] | None = None) -> CatalogNode:
    """Top level: the project's engine contexts (deduped), @catalog-declared
    browse targets, and cell outputs. Each subtree is scoped by its row's
    name (``CatalogNode.scope``)."""
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
        if row["kind"] == "output":
            cell = project.cells[name]
            node = sink_output_node(cell.name, cell.config, project.root, registries, connect=connect)
        elif name in declared:
            node = declared[name]
        else:
            engine, config = by_key[name]
            node = registries.executors.get(engine).catalog(config, project.root)
        return _scoped(node, name)

    return CatalogNode(title="catalog", load=lambda: frame, child=child)
