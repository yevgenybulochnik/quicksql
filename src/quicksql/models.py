"""Core data shapes shared across the pipeline: parse -> config -> render -> run."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, time
from enum import Enum
from pathlib import Path
from typing import Any, Literal


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
    uses_sources: bool = False
    line: int = 0
    line_end: int = 0
    source: str = ""
    context: str = ""
    context_refs: list[str] = field(default_factory=list)
    external_refs: list[str] = field(default_factory=list)
    reffed_in_context: bool = False


Status = Literal["ok", "failed", "skipped"]


@dataclass
class RunResult:
    """Outcome of running one cell: where it landed, how many rows, or why not.

    ``status`` is the outcome; ``ok`` is read-only, derived from it.
    ``error`` is the human message: why it failed, or for a skipped cell
    (which never ran) the upstream that did. ``traceback`` is the failure's
    full trace, for debugging.
    """

    cell: str
    status: Status
    rows: int | None = None
    target: str | None = None
    elapsed: float = 0.0
    error: str | None = None
    traceback: str | None = None
    preview: Any | None = None

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
            "traceback": self.traceback,
        }
        if rows > 0:
            frame = self.preview
            out["preview"] = None if frame is None else {
                "columns": list(frame.columns),
                "rows": [[_jsonable(v) for v in row] for row in frame.head(rows).rows()],
            }
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
    overrides: dict[str, Any] = field(default_factory=dict)
    log: list[str] = field(default_factory=list)
    config: Any = None
    tmpdir: str | None = None
    ext_cache: set[str] = field(default_factory=set)
    session: Any = None
    on_event: Any = None  # Callable[[RunEvent], None] | None — set by run_project

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
