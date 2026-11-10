"""The usage report: the source tables notebooks read and the columns they
read from them, ranked, for ``quicksql usage``.

It reads the log the recorder wrote (``usage_log``; ``plugins.usage`` writes it
when ``QSQL_USAGE=1``). A *use* is a distinct (notebook, local day): reruns,
several cells of one notebook, and several lines for one cell on one day count
once. Score is the sum over uses of ``0.5 ** (age in days / half-life)``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Iterable

# (engine, context, kind, parts); a file: (None, None, "file", (path,))
TableId = tuple[str | None, str | None, str, tuple[str, ...]]
ColumnId = tuple[TableId, str]
Use = tuple[str, str]  # (notebook, day)
OpenId = tuple[str, tuple[TableId | None, ...], bool]  # (column, candidates, nested)


@dataclass
class Usage:
    """Every use in the log's window, by what was used."""

    tables: dict[TableId, set[Use]] = field(default_factory=dict)
    stars: dict[TableId, set[Use]] = field(default_factory=dict)
    columns: dict[ColumnId, set[Use]] = field(default_factory=dict)
    roles: dict[ColumnId, set[str]] = field(default_factory=dict)
    joins: dict[tuple[ColumnId, ColumnId], set[Use]] = field(default_factory=dict)
    unresolved: dict[OpenId, set[Use]] = field(default_factory=dict)
    unresolved_roles: dict[OpenId, set[str]] = field(default_factory=dict)


def _table_id(table: dict[str, Any]) -> TableId:
    parts = tuple(str(p) for p in table["parts"])
    if table["kind"] == "file":
        return (None, None, "file", parts)
    return (table.get("engine"), table.get("context"), str(table["kind"]), parts)


def _read_entry(entry: dict[str, Any]) -> tuple[Use, list[Any]]:
    """One line as (its use, what to add): raises on a line that doesn't fit,
    so nothing of it is counted."""
    use = (str(entry["notebook"]), date.fromisoformat(entry["day"]).isoformat())
    ids = [_table_id(t) for t in entry.get("tables", ())]
    found: list[Any] = [("table", i, bool(t.get("star"))) for i, t in zip(ids, entry["tables"])]
    for column in entry.get("columns", ()):
        key = (ids[column["table"]], str(column["column"]))
        found.append(("column", key, {str(r) for r in column["roles"]}))
    for left, right in entry.get("joins", ()):
        found.append(("join", ((ids[left[0]], str(left[1])), (ids[right[0]], str(right[1])))))
    for open_ in entry.get("unresolved", ()):
        candidates = tuple(None if c is None else ids[c] for c in open_["candidates"])
        key = (str(open_["column"]), candidates, bool(open_.get("nested")))
        found.append(("open", key, {str(r) for r in open_["roles"]}))
    return use, found


def collect(entries: Iterable[dict[str, Any]], today: date, days: int = 90) -> Usage:
    """The uses logged in the last ``days`` days. What a cell read on a day is
    the union of its lines for that day, and a use is a (notebook, day), so
    every line just adds its (notebook, day) to what it names."""
    since = (today - timedelta(days=days)).isoformat()
    usage = Usage()
    for entry in entries:
        try:
            use, found = _read_entry(entry)
        except (KeyError, IndexError, TypeError, ValueError):
            continue  # another writer's idea of a line
        if use[1] < since:
            continue
        for kind, key, *rest in found:
            if kind == "table":
                usage.tables.setdefault(key, set()).add(use)
                if rest[0]:
                    usage.stars.setdefault(key, set()).add(use)
            elif kind == "column":
                usage.columns.setdefault(key, set()).add(use)
                usage.roles.setdefault(key, set()).update(rest[0])
            elif kind == "join":
                usage.joins.setdefault(key, set()).add(use)
            else:
                usage.unresolved.setdefault(key, set()).add(use)
                usage.unresolved_roles.setdefault(key, set()).update(rest[0])
    return usage


@dataclass
class ColumnUsage:
    name: str
    score: float
    uses: int
    roles: list[str]
    type: str = ""
    description: str = ""
    note: str = ""
    pinned: bool = False
    missing: bool = False  # recorded, but the table no longer has it


@dataclass
class JoinUsage:
    left: ColumnId
    right: ColumnId
    score: float
    uses: int


@dataclass
class TableUsage:
    engine: str | None
    context: str | None
    kind: str
    parts: tuple[str, ...]
    score: float
    uses: int
    notebooks: list[tuple[str, int]]  # most uses first
    columns: list[ColumnUsage]
    joins: list[JoinUsage]
    stars: int  # uses that read it through a `select *`
    unresolved: list[ColumnUsage]  # columns this table or another candidate has
    label: str = ""  # what its context is, to a reader

    @property
    def id(self) -> TableId:
        return (self.engine, self.context, self.kind, self.parts)

    @property
    def name(self) -> str:
        return ".".join(self.parts)


def _score(uses: set[Use], today: date, half_life: float) -> float:
    return sum(
        0.5 ** (max((today - date.fromisoformat(day)).days, 0) / half_life) for _, day in uses
    )


def rank(
    usage: Usage, today: date, half_life: float = 30.0, engine: str | None = None
) -> list[TableUsage]:
    """Tables by score, each with its columns by score. ``engine`` keeps the
    tables on that engine (``file``: the files)."""

    def scored(uses: set[Use]) -> float:
        return _score(uses, today, half_life)

    tables = []
    for table, uses in usage.tables.items():
        if engine is not None and engine != (table[0] if table[2] != "file" else "file"):
            continue
        per_notebook: dict[str, int] = {}
        for notebook, _ in uses:
            per_notebook[notebook] = per_notebook.get(notebook, 0) + 1
        columns = [
            ColumnUsage(name, scored(found), len(found), sorted(usage.roles[(owner, name)]))
            for (owner, name), found in usage.columns.items()
            if owner == table
        ]
        joins = [
            JoinUsage(left, right, scored(found), len(found))
            for (left, right), found in usage.joins.items()
            if table in (left[0], right[0])
        ]
        unresolved = [
            ColumnUsage(key[0], scored(found), len(found), sorted(usage.unresolved_roles[key]))
            for key, found in usage.unresolved.items()
            if table in key[1]
        ]
        tables.append(
            TableUsage(
                *table,
                score=scored(uses),
                uses=len(uses),
                notebooks=sorted(per_notebook.items(), key=lambda item: (-item[1], item[0])),
                columns=sorted(columns, key=lambda c: (-c.score, c.name)),
                joins=sorted(joins, key=lambda j: (-j.score, j.left, j.right)),
                stars=len(usage.stars.get(table, ())),
                unresolved=sorted(unresolved, key=lambda c: (-c.score, c.name)),
            )
        )
    return sorted(tables, key=lambda t: (-t.score, t.kind, t.parts, t.engine or "", t.context or ""))
