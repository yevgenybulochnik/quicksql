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
from pathlib import Path
from typing import Any, Iterable, Sequence

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
    return sorted(
        tables,
        key=lambda t: (-t.score, t.kind == "file", t.parts, t.engine or "", t.context or ""),
    )


# ---- the report


@dataclass
class Report:
    tables: list[TableUsage]  # ranked, uncut
    today: date
    days: int
    half_life: float
    notebooks: list[str] = field(default_factory=list)  # the ones it was asked about


def keep(
    usage: Usage,
    contexts: set[tuple[str | None, str | None]],
    notebooks: set[str],
    directories: list[str],
) -> None:
    """Narrow ``usage`` to a notebook's world: the tables on ``contexts``,
    and the files one of ``notebooks`` read or that sit under one of
    ``directories``."""

    def kept(table: TableId) -> bool:
        if table[2] != "file":
            return (table[0], table[1]) in contexts
        if any(notebook in notebooks for notebook, _ in usage.tables[table]):
            return True
        return any(table[3][0].startswith(d.rstrip("/") + "/") for d in directories)

    for table in [t for t in usage.tables if not kept(t)]:
        del usage.tables[table]


def _label(cells: list[str]) -> str:
    shown = cells[:3] + ([f"+{len(cells) - 3}"] if len(cells) > 3 else [])
    return "context of " + ", ".join(shown)


def _renamed(usage: Usage, names: dict[TableId, TableId]) -> None:
    """Fold each table of ``names`` into the one it maps to: two spellings
    of one table are one table, used once per (notebook, day)."""

    def to(table: TableId) -> TableId:
        return names.get(table, table)

    def fold(found: dict[Any, set[Any]], rename: Any) -> dict[Any, set[Any]]:
        out: dict[Any, set[Any]] = {}
        for key, value in found.items():
            out.setdefault(rename(key), set()).update(value)
        return out

    def column(key: ColumnId) -> ColumnId:
        return (to(key[0]), key[1])

    def open_(key: OpenId) -> OpenId:
        return (key[0], tuple(dict.fromkeys(c if c is None else to(c) for c in key[1])), key[2])

    usage.tables = fold(usage.tables, to)
    usage.stars = fold(usage.stars, to)
    usage.columns = fold(usage.columns, column)
    usage.roles = fold(usage.roles, column)
    usage.joins = fold(usage.joins, lambda j: (column(j[0]), column(j[1])))
    usage.unresolved = fold(usage.unresolved, open_)
    usage.unresolved_roles = fold(usage.unresolved_roles, open_)


def _file_leaf(path: str, registries: Any) -> Any:
    """A file's columns, read through DuckDB with the notebook's reader for
    its extension: a catalog leaf, so the cache holds it like a table's."""
    from .conduit import duckdb_field_paths, load_extension
    from .models import CatalogNode
    from .sources import reader_for

    def load() -> Any:
        import duckdb

        reader = reader_for(path, None, registries.readers)
        con = duckdb.connect()
        try:
            for extension in reader.requires:
                load_extension(con, extension)
            relation = con.sql(f"SELECT * FROM {reader.expr(path)} LIMIT 0")
            return duckdb_field_paths(relation.columns, relation.types)
        finally:
            con.close()

    return CatalogNode(title=f"file:{path}", load=load)


# a table's columns from its catalog leaf: lower-cased name -> (type, description)
Columns = dict[str, tuple[str, str]]


def enrich(
    usage: Usage,
    contexts: dict[tuple[str | None, str | None], tuple[Any, Any]],
    cache: Any,
    load: bool,
) -> dict[TableId, Columns]:
    """Bring the catalog to ``usage``, for the tables on ``contexts`` (each
    with the named notebook's (project, cell) that reaches it) and the files:

    * two spellings whose leaves share a ``cache_key`` (``orders`` and
      ``public.orders``) become one table: building a leaf loads nothing;
    * a table's columns come from its leaf's frame when the cache has it at
      hand (memory, or the disk for an engine that persists); ``load`` loads
      the ones that are missing;
    * an unresolved column resolves when every candidate's columns are
      known: to the first candidate that has it when they come from nested
      scopes (innermost first), else to the only one that has it.

    Returns the columns of each table whose frame was at hand."""
    from .catalog import stamp

    leaves: dict[TableId, Any] = {}
    for table in usage.tables:
        engine, context, kind, parts = table
        try:
            if kind == "file":
                if load and contexts:
                    project = next(iter(contexts.values()))[0]
                    leaves[table] = _file_leaf(parts[0], project.registries)
                continue
            project, cell = contexts[(engine, context)]
            executor = project.registries.executors.get(cell.engine)
            leaf = executor.catalog_table(cell.config, project.root, list(parts))
            if leaf is not None:
                leaves[table] = stamp(
                    leaf, scope=executor.context_key(cell.config), persist=executor.catalog_persists
                )
        except Exception:  # an engine that can't say: the table stays as logged
            continue

    # one table per leaf, under its most qualified spelling
    spellings: dict[tuple[Any, ...], list[TableId]] = {}
    for table, leaf in leaves.items():
        spellings.setdefault((table[0], table[1], leaf.cache_key), []).append(table)
    names = {}
    for same in spellings.values():
        canonical = max(same, key=lambda t: (len(t[3]), t[3]))
        names.update({table: canonical for table in same if table != canonical})
        leaves[canonical] = leaves[same[0]]
    _renamed(usage, names)

    columns: dict[TableId, Columns] = {}
    for table in usage.tables:
        leaf = leaves.get(table)
        if leaf is None:
            continue
        frame = cache.at_hand(leaf)
        if frame is None and load:
            try:
                frame = cache.load(leaf)[0]
            except Exception:  # unreachable, or gone: no columns for it
                continue
        if frame is None or not {"column", "field_path", "type"} <= set(frame.columns):
            continue
        columns[table] = {
            str(row["column"]).lower(): (str(row["type"] or ""), str(row.get("description") or ""))
            for row in frame.iter_rows(named=True)
            if row["column"] == row["field_path"]  # top-level columns
        }

    for key in list(usage.unresolved):
        name, candidates, nested = key
        if not all(c is not None and c in columns for c in candidates):
            continue  # a candidate nobody can check keeps it unresolved
        having = [c for c in candidates if name.lower() in columns[c]]
        if not having or (not nested and len(having) > 1):
            continue
        column = (having[0], name)
        usage.columns.setdefault(column, set()).update(usage.unresolved.pop(key))
        usage.roles.setdefault(column, set()).update(usage.unresolved_roles.pop(key))
    return columns


def report(
    projects: Sequence[Any] = (),
    *,
    today: date | None = None,
    days: int = 90,
    half_life: float = 30.0,
    engine: str | None = None,
    load: bool = False,
    cache: Any = None,
) -> Report:
    """What the log holds for the last ``days`` days. With ``projects``
    (compiled notebooks): what every logged notebook read on their contexts,
    each context named by their cells on it, enriched from their catalogs
    (``enrich``; ``cache`` is a ``CatalogCache``, the default one when None).
    Without: every context, by its hash. Logged notebooks are only read
    about, never compiled."""
    from .plugins.usage import context_of
    from .usage_log import UsageLog

    today = today or date.today()
    since = (today - timedelta(days=days)).isoformat()
    usage = collect(UsageLog().read(since=since), today, days)
    cells: dict[tuple[str | None, str | None], list[str]] = {}
    reach: dict[tuple[str | None, str | None], tuple[Any, Any]] = {}
    for project in projects:
        for name in project.order:
            cell = project.cells[name]
            context = (cell.engine, context_of(project, cell))
            cells.setdefault(context, []).append(name)
            reach.setdefault(context, (project, cell))
    paths = [str(project.path.resolve()) for project in projects]
    columns: dict[TableId, Columns] = {}
    if projects:
        keep(usage, set(cells), set(paths), [str(Path(p).parent) for p in paths])
        if usage.tables:
            if cache is None:
                from .catalog import default_cache

                cache = default_cache()
            columns = enrich(usage, reach, cache, load)
    tables = rank(usage, today, half_life, engine)
    for table in tables:
        if (table.engine, table.context) in cells:
            table.label = _label(cells[(table.engine, table.context)])
        known = columns.get(table.id)
        for column in table.columns if known is not None else ():
            found = known.get(column.name.lower())
            column.missing = found is None
            column.type, column.description = found or ("", "")
    return Report(tables, today, days, half_life, paths)


ROLE_ORDER = ("select", "from", "join", "where", "group", "having", "window", "order")


def _roles(roles: Iterable[str]) -> list[str]:
    """Roles in the order a query's clauses are written."""
    return sorted(roles, key=lambda r: (ROLE_ORDER.index(r) if r in ROLE_ORDER else 99, r))


def _short(notebook: str) -> str:
    """A notebook's path as its directory and name: enough to tell two apart."""
    return "/".join(notebook.rsplit("/", 2)[-2:])


def _plural(count: int, noun: str) -> str:
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def _heading(table: TableUsage) -> str:
    where = "" if table.kind == "file" else f" — {table.label or f'context {table.context}'}"
    return (
        f"{table.engine or table.kind} · {table.name}{where}"
        f" · {_plural(len(table.notebooks), 'notebook')}, score {table.score:.1f}"
    )


def _used_in(table: TableUsage) -> str:
    shown = [f"{_short(path)} ({uses})" for path, uses in table.notebooks[:3]]
    more = len(table.notebooks) - len(shown)
    return ", ".join(shown + ([f"+{more}"] if more > 0 else []))


def _side(column: ColumnId) -> str:
    """``claims.member_id``: a join's side by its table's own name."""
    (_, _, kind, parts), name = column
    owner = parts[-1].rsplit("/", 1)[-1] if kind == "file" else parts[-1]
    return f"{owner}.{name}"


def _joins(table: TableUsage) -> str:
    return " · ".join(f"{_side(j.left)} = {_side(j.right)} ({j.uses})" for j in table.joins)


def _note(column: ColumnUsage) -> str:
    said = [
        "pinned" if column.pinned else "",
        column.note,
        column.description,
        "no longer in the table" if column.missing else "",
    ]
    return "; ".join(part for part in said if part)


def _unresolved(table: TableUsage) -> str:
    return ", ".join(f"{c.name} ({c.uses})" for c in table.unresolved)


def hint(found: Report) -> str:
    """What an empty report says: nothing was recorded, and how recording
    is turned on."""
    return (
        f"no usage recorded in the last {found.days} days: recording is opt-in, set"
        " QSQL_USAGE=1 where notebooks run (quicksql run, watch, tui)"
    )


def to_json(found: Report, top: int = 20, columns: int = 15) -> dict[str, Any]:
    def column(c: ColumnUsage) -> dict[str, Any]:
        return {
            "name": c.name,
            "score": round(c.score, 3),
            "uses": c.uses,
            "roles": _roles(c.roles),
            "type": c.type,
            "description": c.description,
            "note": c.note,
            "pinned": c.pinned,
            "missing": c.missing,
        }

    def side(end: ColumnId) -> dict[str, str]:
        return {"table": ".".join(end[0][3]), "column": end[1]}

    doc: dict[str, Any] = {
        "today": found.today.isoformat(),
        "days": found.days,
        "half_life": found.half_life,
        "notebooks": found.notebooks,
        "tables": [
            {
                "engine": t.engine,
                "context": t.context,
                "context_label": t.label,
                "kind": t.kind,
                "name": t.name,
                "parts": list(t.parts),
                "score": round(t.score, 3),
                "uses": t.uses,
                "stars": t.stars,
                "notebooks": [{"path": path, "uses": uses} for path, uses in t.notebooks],
                "columns": [column(c) for c in t.columns[:columns]],
                "joins": [
                    {
                        "left": side(j.left),
                        "right": side(j.right),
                        "uses": j.uses,
                        "score": round(j.score, 3),
                    }
                    for j in t.joins
                ],
                "unresolved": [
                    {"name": c.name, "uses": c.uses, "roles": _roles(c.roles)}
                    for c in t.unresolved
                ],
            }
            for t in found.tables[:top]
        ],
    }
    if not found.tables:
        doc["hint"] = hint(found)
    return doc


def _row(cells: Iterable[Any]) -> str:
    return "|" + "".join(f" {cell} |" if str(cell) else " |" for cell in cells)


def to_markdown(found: Report, top: int = 20, columns: int = 15) -> str:
    """The agent's format: a section per table, its columns as a table."""
    if not found.tables:
        return hint(found) + "\n"
    out = [
        f"# quicksql usage: last {found.days} days",
        "",
        "A use is a (notebook, day) that read it; score weighs recent uses"
        f" (half-life {found.half_life:g} days).",
    ]
    for table in found.tables[:top]:
        out += ["", f"## {_heading(table)}", f"Used in: {_used_in(table)}"]
        out += [_row(("column", "type", "uses", "read as", "note")), "|---|---|---|---|---|"]
        out += [
            _row((c.name, c.type, c.uses, ", ".join(_roles(c.roles)), _note(c)))
            for c in table.columns[:columns]
        ]
        more = len(table.columns) - columns
        if more > 0:
            out.append(f"+{_plural(more, 'more column')}")
        if table.joins:
            out.append(f"Joins: {_joins(table)}")
        if table.stars:
            out.append(f"Read with `select *`: {table.stars}")
        if table.unresolved:
            out.append(
                f"Unresolved (this table or another in the query has it): {_unresolved(table)}"
            )
    more = len(found.tables) - top
    if more > 0:
        out += ["", f"+{_plural(more, 'more table')}"]
    return "\n".join(out) + "\n"


def to_text(found: Report, top: int = 20, columns: int = 15) -> str:
    if not found.tables:
        return hint(found) + "\n"
    out = [
        f"usage: last {found.days} days; a use is a (notebook, day), score weighs recent"
        f" uses (half-life {found.half_life:g} days)"
    ]
    for table in found.tables[:top]:
        out += ["", _heading(table), f"  used in: {_used_in(table)}"]
        rows = [("column", "type", "uses", "read as", "note")] + [
            (c.name, c.type, str(c.uses), ", ".join(_roles(c.roles)), _note(c))
            for c in table.columns[:columns]
        ]
        if len(rows) > 1:
            widths = [max(len(row[i]) for row in rows) for i in range(4)]
            for row in rows:
                cells = [
                    row[0].ljust(widths[0]),
                    row[1].ljust(widths[1]),
                    row[2].rjust(widths[2]),
                    row[3].ljust(widths[3]),
                    row[4],
                ]
                out.append(("  " + "  ".join(cells)).rstrip())
        more = len(table.columns) - columns
        if more > 0:
            out.append(f"  +{_plural(more, 'more column')}")
        if table.joins:
            out.append(f"  joins: {_joins(table)}")
        if table.stars:
            out.append(f"  select *: {table.stars}")
        if table.unresolved:
            out.append(f"  unresolved: {_unresolved(table)}")
    more = len(found.tables) - top
    if more > 0:
        out += ["", f"+{_plural(more, 'more table')}"]
    return "\n".join(out) + "\n"
