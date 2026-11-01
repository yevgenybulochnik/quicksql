"""Core data shapes shared across the pipeline: parse -> config -> render -> run."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass, field
from datetime import date, time
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal, Optional


def source_hash(text: str) -> str:
    """The hash a notebook's text compiles under (``Project.source_hash``).
    A live session and ``quicksql live wait`` both compute it, one from what it
    compiled and the other from the file on disk, so it stays this simple."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


class Scope(str, Enum):
    """Where a plugin's config fields may appear."""

    GLOBAL = "global"
    CELL = "cell"
    BOTH = "both"


class Merge(str, Enum):
    """How a config field combines across layers (global -> cell -> overrides)."""

    OVERRIDE = "override"
    DEEP = "deep"
    EXTEND = "extend"


@dataclass
class RawBlock:
    """A parsed chunk of the file: the global header (name=None) or one cell.

    ``sql`` is the directive-stripped body; ``source`` is the untouched slice
    of the file (directives included) spanning lines ``line``..``line_end``.
    """

    name: str | None
    directives: dict[str, Any]
    sql: str
    line: int
    line_end: int = 0
    source: str = ""
    # repeated directive keys in this block: (key, earlier_line, later_line);
    # the later value silently won — recorded so consumers can warn
    duplicates: list[tuple[str, int, int]] = field(default_factory=list)


@dataclass
class RenderedCell:
    """A cell after Jinja rendering: final SQL, resolved engine/sink, edges."""

    name: str
    config: Any
    sql_raw: str
    sql: str
    hash: str
    engine: str
    sink_type: str
    depends_on: list[str] = field(default_factory=list)
    extensions: list[str] = field(default_factory=list)
    line: int = 0
    line_end: int = 0
    source: str = ""
    context: str = ""
    context_refs: list[str] = field(default_factory=list)
    external_refs: list[str] = field(default_factory=list)
    reffed_in_context: bool = False


Status = Literal["ok", "failed", "skipped"]


@dataclass(frozen=True)
class Preview:
    """The head of a cell's landed output as plain Python values: what a run
    caches for front-ends, which read it without a dataframe library (the TUI
    builds its own Polars frame from it)."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    @property
    def height(self) -> int:
        return len(self.rows)

    def column(self, name: str) -> list[Any]:
        i = self.columns.index(name)
        return [row[i] for row in self.rows]

    def to_dict(self, rows: int) -> dict[str, Any]:
        """Up to ``rows`` rows as JSON-safe values (see ``_jsonable``)."""
        return {
            "columns": list(self.columns),
            "rows": [[_jsonable(v) for v in row] for row in self.rows[:rows]],
        }


@dataclass
class RunResult:
    """Outcome of running one cell: where it landed, how many rows, or why not.

    ``status`` is the outcome; ``ok`` is read-only, derived from it.
    ``error`` is the human message: why it failed, or for a skipped cell
    (which never ran) the upstream that did. ``statement`` numbers, from 1, the
    cell's statement that failed, None when the failure was outside them.
    ``traceback`` is the failure's full trace, for debugging.
    """

    cell: str
    status: Status
    rows: int | None = None
    target: str | None = None
    elapsed: float = 0.0
    error: str | None = None
    traceback: str | None = None
    statement: int | None = None
    preview: Preview | None = None  # an ok cell's; empty when its sink lands nothing

    @property
    def ok(self) -> bool:
        """True when the cell ran and landed."""
        return self.status == "ok"

    def to_dict(self, rows: int = 0) -> dict[str, Any]:
        """The outcome as JSON-safe values. ``rows`` > 0 adds ``preview``:
        up to that many rows of the cached preview (itself at most 100), or
        None when there is none (a failed or skipped cell)."""
        out: dict[str, Any] = {
            "cell": self.cell,
            "status": self.status,
            "rows": self.rows,
            "target": self.target,
            "elapsed": round(self.elapsed, 4),  # seconds, to 0.1 ms
            "error": self.error,
            "statement": self.statement,
            "traceback": self.traceback,
        }
        if rows > 0:
            head = self.preview
            out["preview"] = None if head is None else head.to_dict(rows)
        return out


def _jsonable(value: Any) -> Any:
    """One preview value as JSON: dates as ISO text, non-finite floats as
    text (JSON has no NaN), lists/structs recursively, anything else —
    Decimal (kept exact), bytes, timedelta, UUID — as its str()."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (date, time)):  # datetime is a date
        return value.isoformat()
    return str(value)


@dataclass(frozen=True)
class RunEvent:
    """One moment of a run, streamed live to whoever passed ``on_event``.

    Kinds: ``run_started`` | ``cell_started`` | ``cell_step`` |
    ``cell_finished`` (carries the RunResult) | ``run_finished`` | ``note``.
    """

    kind: str
    cell: str | None = None
    detail: str = ""
    result: RunResult | None = None
    # run_started only: the post-expansion list of cells this run will execute
    # (closure / missing-temp upstreams included) — consumers can't derive it
    # from their own select list
    cells: tuple[str, ...] | None = None

    def to_dict(self, rows: int = 0) -> dict[str, Any]:
        """The event as JSON-safe values; ``rows`` adds the result's preview
        rows, as ``RunResult.to_dict`` does."""
        return {
            "kind": self.kind,
            "cell": self.cell,
            "detail": self.detail,
            "cells": list(self.cells) if self.cells is not None else None,
            "result": self.result.to_dict(rows) if self.result is not None else None,
        }

    def line(self) -> str:
        """One human-readable log line; the TUI log tab and CLI both use this."""
        if self.kind == "cell_finished" and self.result is not None:
            r = self.result
            if r.ok:
                rows = r.rows if r.rows is not None else "?"
                return f"{self.cell}: ok — {rows} rows in {r.elapsed * 1000:.0f} ms -> {r.target}"
            if r.status == "skipped":
                return f"{self.cell}: skipped — {r.error}"
            reason = (r.error or "unknown error").strip().splitlines()[0]
            return f"{self.cell}: FAILED — {reason}"
        if self.kind == "cell_started":
            return f"{self.cell}: started ({self.detail})"
        if self.kind == "run_started":
            return f"run started — {self.detail}"
        if self.kind == "run_finished":
            return f"run finished — {self.detail}"
        if self.kind == "note":
            return f"note: {self.detail}"
        return f"{self.cell}: {self.detail}" if self.cell else self.detail


@dataclass
class RunContext:
    """Per-run state handed to executors, sinks, and plugin `run` hooks."""

    conn: Any
    root: Path
    log: list[str] = field(default_factory=list)
    config: Any = None
    tmpdir: str | None = None
    ext_cache: set[str] = field(default_factory=set)
    session: Any = None
    on_event: Any = None  # Callable[[RunEvent], None] | None — set by run_project
    registries: Any = None  # the project's Registries — set by run_project

    def emit(
        self,
        kind: str,
        cell: str | None = None,
        detail: str = "",
        result: Any = None,
        cells: tuple[str, ...] | None = None,
    ) -> None:
        """Stream a RunEvent to the consumer; a broken callback lands in the
        log instead of killing the run (events are observability, not control)."""
        if self.on_event is None:
            return
        try:
            self.on_event(RunEvent(kind=kind, cell=cell, detail=detail, result=result, cells=cells))
        except Exception as exc:
            self.log.append(f"on_event callback failed for {kind}: {exc}")


@dataclass(frozen=True)
class CatalogNode:
    """One drillable level of the schema catalog: a frame to show, and how to
    go deeper. Executors build their contexts' trees from these (``Executor.catalog``);
    the TUI browses them and the LSP reads table leaves for column completions.

    ``load`` blocks (network/disk) and returns a Polars frame — call it off the
    UI thread. ``child`` maps a named row of the loaded frame to the next level;
    None marks a leaf. A leaf describing a table's columns loads a field-path
    frame (``executors.base.field_path_frame``).
    """

    title: str
    load: Callable[[], Any]
    child: Optional[Callable[[dict[str, Any]], Optional["CatalogNode"]]] = None
    # stable cache identity across node re-creation (drilling rebuilds nodes);
    # set it wherever the display title isn't globally unique
    key: str | None = None

    @property
    def cache_key(self) -> str:
        return self.key if self.key is not None else self.title
