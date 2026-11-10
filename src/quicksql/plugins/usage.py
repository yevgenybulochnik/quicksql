"""Usage tracking: which source tables and columns a notebook's cells read.

Off unless ``QSQL_USAGE=1`` is set in the process that runs the notebook; then
each run's ``ok`` cells are scanned (``sqlscan``), their reads resolved across
``ref()`` (``lineage``) and logged under ``$QSQL_HOME/usage/`` (``usage_log``),
for ``quicksql usage`` to report. ``-- @usage: false`` opts a notebook or a cell
out; nothing opts one in.

Every compile and the LSP load this module with the other builtins, so it
imports the scanner (and sqlglot's optimizer with it) only once a run records.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence


@dataclass(frozen=True)
class Source:
    """A source table as the log names it. A table is on the context of the
    cell that reads it from its engine (``context`` hashes that context's
    identity); a file has neither engine nor context: its path identifies it."""

    engine: str | None
    context: str | None
    kind: str  # table | file
    parts: tuple[str, ...]


@dataclass(frozen=True)
class CellScan:
    """A cell as lineage needs it: its scan, the context its tables are on,
    whether it records, and the relation its sink lands (None: effect-only)."""

    scan: Any  # sqlscan.Scan
    engine: str
    context: str
    usage: bool = True
    landed: Any = None  # sqlscan.Rel


Column = tuple[Source, str]


@dataclass
class Reads:
    """What a cell reads from sources, its ``ref()``s resolved."""

    tables: dict[Source, bool] = field(default_factory=dict)  # -> a `*` reaches its result
    columns: dict[Column, set[str]] = field(default_factory=dict)  # -> roles
    joins: set[tuple[Column, Column]] = field(default_factory=set)
    # (column, candidates, nested) -> roles; a None candidate: a relation that
    # is no source and whose columns aren't known
    unresolved: dict[tuple[str, tuple[Source | None, ...], bool], set[str]] = field(
        default_factory=dict
    )
    errors: tuple[tuple[int, str], ...] = ()


def _join_key(column: Column) -> tuple[Any, ...]:
    source, name = column
    return (source.parts, name, source.kind, source.engine or "", source.context or "")


class _Lineage:
    """Reads resolved across a notebook's cells. A column read from a ``ref()``
    is a read of the source column the producer passes through unchanged: by
    name, renamed, or through its ``*``."""

    def __init__(
        self, cells: Mapping[str, CellScan], resolve_file: Callable[[str], str]
    ) -> None:
        from ..sqlscan import same_table

        self.cells = cells
        self.resolve_file = resolve_file
        self.same_table = same_table
        # what the notebook itself makes is derived, to every cell: the tables
        # cells CREATE, and the relation each sink lands
        self.made: set[tuple[str, ...]] = set()
        self.made_files: set[str] = set()
        for cell in cells.values():
            self.made |= cell.scan.created
            landed = cell.landed
            if landed is not None and landed.kind == "table":
                # an ATTACH alias is the conduit's own: match on (schema, table)
                self.made.add(tuple(p.lower() for p in landed.parts[-2:]))
            elif landed is not None and landed.kind == "file":
                self.made_files.add(resolve_file(landed.parts[0]))

    def source(self, cell: CellScan, rel: Any) -> Source | None:
        """A table or file ``cell`` reads as a source; None when the notebook
        makes it."""
        if rel.kind == "file":
            path = self.resolve_file(rel.parts[0])
            return None if path in self.made_files else Source(None, None, "file", (path,))
        folded = tuple(p.lower() for p in rel.parts)
        if any(self.same_table(folded, made) for made in self.made):
            return None
        return Source(cell.engine, cell.context, "table", rel.parts)

    # a read resolves to (the source columns it reads, or the candidates one of
    # which it reads): ([...], None) | ([], (...)); ([], None) reads nothing

    def expand(self, cell: CellScan, rel: Any, column: str) -> tuple[list[Column], Any]:
        """Column ``column`` of ``rel``, read in ``cell``."""
        if rel.kind == "ref":
            return self.through(rel.parts[0], column)
        source = self.source(cell, rel)
        return ([(source, column)] if source is not None else []), None

    def through(self, name: str, column: str) -> tuple[list[Column], Any]:
        """Column ``column`` of the cell ``name``'s output."""
        producer = self.cells.get(name)
        if producer is None or not producer.usage:
            return [], None
        for output, origin in producer.scan.outputs:
            if output.lower() == column.lower():
                # None: computed there, and its inputs counted there
                return self.expand(producer, *origin) if origin is not None else ([], None)
        passing = [rel for rel, out in producer.scan.star if column.lower() not in out]
        if len(passing) == 1:
            return self.expand(producer, passing[0], column)
        return self.settle(producer, passing, column)

    def settle(
        self, cell: CellScan, candidates: Sequence[Any], column: str
    ) -> tuple[list[Column], Any]:
        """A column one of ``candidates`` (relations of ``cell``) has. A ref
        that names its columns is ruled in or out by them: the run was ok, so
        when it has the column no other candidate does."""
        found: list[Source | None] = []
        for rel in candidates:
            if rel is None or rel.kind != "ref":
                found.append(self.source(cell, rel) if rel is not None else None)
                continue
            producer = self.cells.get(rel.parts[0])
            if producer is None or not producer.usage:
                found.append(None)
                continue
            if any(output.lower() == column.lower() for output, _ in producer.scan.outputs):
                return self.through(rel.parts[0], column)  # ruled in
            if not any(column.lower() not in out for _, out in producer.scan.star):
                continue  # ruled out
            ends, more = self.through(rel.parts[0], column)
            found += [source for source, _ in ends] or list(more or (None,))
        found = list(dict.fromkeys(found))
        if len(found) == 1 and found[0] is not None:
            return [(found[0], column)], None
        if all(source is None for source in found):
            return [], None
        return [], tuple(found)

    def star(self, cell: CellScan, rel: Any, into: dict[Source, bool]) -> None:
        """The sources a ``*`` over ``rel`` reads all the columns of."""
        if rel.kind != "ref":
            source = self.source(cell, rel)
            if source is not None:
                into[source] = True
            return
        producer = self.cells.get(rel.parts[0])
        if producer is not None and producer.usage:
            for passed, _ in producer.scan.star:
                self.star(producer, passed, into)

    def reads(self, cell: CellScan) -> Reads:
        scan, out = cell.scan, Reads(errors=tuple(cell.scan.errors))
        for rel, starred in scan.tables:
            if rel.kind == "ref":
                if starred:
                    self.star(cell, rel, out.tables)
                continue
            source = self.source(cell, rel)
            if source is not None:
                out.tables[source] = out.tables.get(source, False) or starred
        for use in scan.columns:
            self._record(out, self.expand(cell, use.rel, use.column), use.column, use.roles)
        for open_ in scan.unresolved:
            if open_.nested:
                # an inner and an outer relation may both have it: the report's
                # to resolve, with their columns at hand
                found = tuple(
                    self.source(cell, rel) if rel is not None and rel.kind != "ref" else None
                    for rel in open_.candidates
                )
                if any(source is not None for source in found):
                    self._record(out, ([], found), open_.column, open_.roles, nested=True)
            else:
                resolved = self.settle(cell, open_.candidates, open_.column)
                self._record(out, resolved, open_.column, open_.roles)
        for left, right in scan.joins:
            sides = [self.expand(cell, rel, column) for rel, column in (left, right)]
            if all(len(ends) == 1 and more is None for ends, more in sides):
                a, b = sorted((ends[0] for ends, _ in sides), key=_join_key)
                if a != b:
                    out.joins.add((a, b))
                    for source, _ in (a, b):
                        out.tables.setdefault(source, False)
        return out

    def _record(
        self,
        out: Reads,
        resolved: tuple[list[Column], Any],
        column: str,
        roles: Any,
        nested: bool = False,
    ) -> None:
        ends, candidates = resolved
        for source, name in ends:
            out.columns.setdefault((source, name), set()).update(roles)
            out.tables.setdefault(source, False)
        if candidates:
            out.unresolved.setdefault((column, tuple(candidates), nested), set()).update(roles)
            for source in candidates:
                if source is not None:
                    out.tables.setdefault(source, False)


def lineage(
    cells: Mapping[str, CellScan],
    order: Sequence[str],
    resolve_file: Callable[[str], str] = lambda path: path,
) -> dict[str, Reads]:
    """Each cell's reads of source tables and files, in ``order``. ``resolve_file``
    makes a file's path as written the path that identifies it."""
    resolver = _Lineage(cells, resolve_file)
    return {name: resolver.reads(cells[name]) for name in order}
