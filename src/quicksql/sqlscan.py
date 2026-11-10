"""What one cell's rendered SQL reads: source tables, files and ``ref()``'d
cells, statement by statement.

``scan`` splits the body into statements, parses each strictly in the
executor's dialect, qualifies it (sqlglot's optimizer) and walks its scopes. It
reads no file system, no catalog and no cwd, and keeps no SQL text: an
unreadable statement is an index and a kind, never the parser's message, which
quotes the statement.
"""

from __future__ import annotations

import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Literal, Mapping

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, traverse_scope

from .executors.base import statement_spans

MAX_BYTES = 100_000  # a larger body isn't scanned


@dataclass(frozen=True)
class Rel:
    """A relation a statement reads. ``parts`` is a table's name, qualifiers
    first, as qualify normalized them; a file's path as written (a URL without
    its userinfo and query); a ref's cell."""

    kind: Literal["table", "file", "ref"]
    parts: tuple[str, ...]


@dataclass(frozen=True)
class ColumnUse:
    """A column read straight from a relation, and the clauses that read it:
    select | where | join | group | having | order | window | from."""

    rel: Rel
    column: str
    roles: frozenset[str]


@dataclass(frozen=True)
class Unresolved:
    """A column the statement alone can't place: unqualified with several
    relations in scope, or in a subquery that may be correlated. One of
    ``candidates`` has it; None stands for a relation whose columns aren't
    known and that is no source. ``nested``: the candidates come from nested
    scopes, innermost first, so the first that has the column is the one read;
    otherwise exactly one has it."""

    column: str
    roles: frozenset[str]
    candidates: tuple[Rel | None, ...]
    nested: bool = False


@dataclass(frozen=True)
class Scan:
    # each relation read; whether a `*` over it reaches a statement's result
    tables: tuple[tuple[Rel, bool], ...] = ()
    # columns read from a relation; a ref's are the recorder's to resolve
    columns: tuple[ColumnUse, ...] = ()
    unresolved: tuple[Unresolved, ...] = ()
    # what the cell CREATEs (tables and views), lower-cased
    created: frozenset[tuple[str, ...]] = frozenset()
    # (statement number from 1, kind): "parse", "qualify", "unsupported:<Kind>";
    # (0, "too large") for a body that wasn't scanned
    errors: tuple[tuple[int, str], ...] = ()


# statements that read no relation unless they hold a query
_NO_READ = tuple(
    kind
    for name in (
        "Create", "Drop", "Insert", "Set", "Attach", "Detach", "Use", "Alter", "Comment",
        "TruncateTable", "Transaction", "Commit", "Rollback", "Pragma", "Analyze", "Grant",
        "Install", "Declare", "Describe", "Show", "Values", "Copy",
    )  # fmt: skip
    if (kind := getattr(exp, name, None)) is not None
)
# a Command (sqlglot's fallback for syntax it doesn't model) that reads nothing
_NO_READ_COMMANDS = frozenset({"LOAD"})
_QUERY = (exp.Select, exp.SetOperation)
_URL = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]*://)(?:[^/@]*@)?([^?#]*)")


class _Unknown:
    """A relation whose columns aren't known and that is no source: a table
    the cell creates, a table function that reads no file."""


_UNKNOWN = _Unknown()


@dataclass(frozen=True)
class _End:
    """Where a read ends: a column of a source relation."""

    rel: Rel
    column: str


@dataclass(frozen=True)
class _Open:
    """A read that ends on one of several relations."""

    column: str
    candidates: tuple[Rel | None, ...]
    nested: bool


def _role(column: exp.Column) -> str:
    """The clause a column is read in: its nearest clause ancestor."""
    key, node = column.arg_key, column.parent
    while node is not None:
        if isinstance(node, exp.Where):
            return "where"
        if isinstance(node, exp.Join):
            return "join" if key in ("on", "using") else "from"
        if isinstance(node, exp.Group):
            return "group"
        if isinstance(node, exp.Having):
            return "having"
        if isinstance(node, exp.Order):
            return "order"
        if isinstance(node, _QUERY):
            return "select"
        key, node = node.arg_key, node.parent
    return "select"


def _correlatable(scope: Scope) -> bool:
    """Whether the scope's query can read its enclosing query's relations."""
    return scope.is_subquery or scope.is_udtf


@contextmanager
def _quiet() -> Iterator[None]:
    """sqlglot warns on stderr when it falls back to a Command, and when the
    optimizer meets what it can't place; a scan says so in its errors."""
    logger = logging.getLogger("sqlglot")
    level = logger.level
    logger.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        logger.setLevel(level)


def _file_path(text: str) -> str:
    """A path as written; a URL without its userinfo, query and fragment."""
    match = _URL.match(text)
    return match.group(1) + match.group(2) if match else text


def _ends_with(parts: tuple[str, ...], other: tuple[str, ...]) -> bool:
    """Whether two dotted names agree on as many trailing parts as the
    shorter has: ``t`` and ``main.t`` name one table."""
    n = min(len(parts), len(other))
    return n > 0 and parts[-n:] == other[-n:]


def _function_name(func: exp.Func) -> str:
    return (func.name if isinstance(func, exp.Anonymous) else func.sql_name()).lower()


def _path_literals(func: exp.Func) -> list[str]:
    """A table function's first argument as paths: a string literal, or a
    list of them; [] for anything else."""
    if isinstance(func, exp.Anonymous):
        args = list(func.expressions)
    else:
        args = []
        for key in func.arg_types:
            value = func.args.get(key)
            args += value if isinstance(value, list) else [value] if value is not None else []
    first = args[0] if args else None
    if isinstance(first, exp.Literal) and first.is_string:
        return [first.name]
    if isinstance(first, exp.Array) and first.expressions:
        items = first.expressions
        if all(isinstance(i, exp.Literal) and i.is_string for i in items):
            return [i.name for i in items]
    return []


def _table_key(table: exp.Table, dialect: str | None) -> tuple[Any, ...] | None:
    """What a relation in FROM is matched to a ``ref()`` by: its name parts,
    or a table function's call."""
    this = table.this
    if isinstance(this, exp.Identifier):
        return ("table", *(part.name for part in table.parts))
    if isinstance(this, exp.Func):
        return ("call", this.sql(dialect=dialect))
    return None


def _created(tree: exp.Expression) -> tuple[str, ...] | None:
    """The table or view a CREATE statement makes, lower-cased."""
    if not isinstance(tree, exp.Create) or (tree.args.get("kind") or "").upper() not in (
        "TABLE",
        "VIEW",
    ):
        return None
    target = tree.this
    if isinstance(target, exp.Schema):
        target = target.this
    if not isinstance(target, exp.Table) or not isinstance(target.this, exp.Identifier):
        return None
    return tuple(part.name.lower() for part in target.parts)


def _as_select(stmt: exp.Expression) -> exp.Select:
    """The SELECT an UPDATE, DELETE or MERGE reads (sqlglot builds no scope
    for them): its tables in FROM, its condition, and the values it assigns as
    the select list. The columns it assigns are writes, and are left out."""
    values: list[exp.Expression] = []
    joins: list[exp.Join] = []
    where = stmt.args.get("where")
    if isinstance(stmt, exp.Update):
        values = [eq.expression for eq in stmt.expressions if isinstance(eq, exp.EQ)]
        from_ = stmt.args.get("from_")
        if from_ is not None:
            joins = [exp.Join(this=from_.this.copy())]
    elif isinstance(stmt, exp.Delete):
        joins = [exp.Join(this=t.copy()) for t in stmt.args.get("using") or []]
    else:  # MERGE
        joins = [exp.Join(this=stmt.args["using"].copy(), on=stmt.args["on"].copy())]
        whens = stmt.args.get("whens")
        for when in whens.expressions if whens is not None else []:
            condition, then = when.args.get("condition"), when.args.get("then")
            if isinstance(condition, exp.Expression):
                values.append(condition)
            if isinstance(then, exp.Update):
                values += [eq.expression for eq in then.expressions if isinstance(eq, exp.EQ)]
            elif isinstance(then, exp.Insert) and isinstance(then.expression, exp.Expression):
                row = then.expression
                values += row.expressions if isinstance(row, exp.Tuple) else [row]
    select = exp.Select()
    select.set("expressions", [v.copy() for v in values] or [exp.Literal.number(1)])
    select.set("from_", exp.From(this=stmt.this.copy()))
    if joins:
        select.set("joins", joins)
    if where is not None:
        select.set("where", where.copy())
    with_ = stmt.args.get("with_")
    if with_ is not None:
        select.set("with_", with_.copy())
    return select


class _Scanner:
    """One cell's scan: what its statements add up to."""

    def __init__(
        self,
        dialect: str | None,
        refs: Mapping[str, str],
        file_functions: frozenset[str],
        file_suffixes: frozenset[str],
    ) -> None:
        self.dialect = dialect
        self.file_functions = file_functions
        self.file_suffixes = tuple(file_suffixes)
        self.refs: dict[tuple[Any, ...], str] = {}
        for text, cell in refs.items():
            key = self._ref_key(text)
            if key is not None:
                self.refs[key] = cell
        self.created: frozenset[tuple[str, ...]] = frozenset()
        self.tables: dict[Rel, bool] = {}
        self.columns: dict[tuple[Rel, str], set[str]] = {}
        self.unresolved: dict[tuple[str, tuple[Rel | None, ...], bool], set[str]] = {}
        self.errors: list[tuple[int, str]] = []

    def _ref_key(self, text: str) -> tuple[Any, ...] | None:
        """The key of the relation a ``ref()`` rendered to, read the way a
        statement's own tables are."""
        try:
            tree = sqlglot.parse_one(f"SELECT * FROM {text}", dialect=self.dialect)
            tree = qualify(tree, dialect=self.dialect, validate_qualify_columns=False)
        except Exception:
            return None
        table = tree.find(exp.Table)
        return _table_key(table, self.dialect) if table is not None else None

    # ---- statements

    def run(self, sql: str) -> None:
        spans = statement_spans(sql, self.dialect or "")
        if not spans:
            try:  # no statement, or text the tokenizer can't read?
                sqlglot.tokenize(sql, dialect=self.dialect)
            except Exception:
                self.errors.append((1, "parse"))
            return
        trees: list[tuple[int, exp.Expression]] = []
        for index, (start, end) in enumerate(spans, 1):
            try:
                tree = sqlglot.parse_one(sql[start:end], dialect=self.dialect)
            except Exception:  # the message quotes the statement: keep none of it
                self.errors.append((index, "parse"))
                continue
            trees.append((index, tree))
        self.created = frozenset(c for _, tree in trees if (c := _created(tree)) is not None)
        for index, tree in trees:
            self._statement(index, tree, terminal=index == len(spans))
        self.errors.sort()

    def _statement(self, index: int, tree: exp.Expression, terminal: bool) -> None:
        kind = type(tree).__name__
        if isinstance(tree, exp.Command):
            if str(tree.this).upper() not in _NO_READ_COMMANDS:
                self.errors.append((index, f"unsupported:{kind}"))
            return
        if isinstance(tree, _NO_READ) and tree.find(*_QUERY) is None:
            return
        if isinstance(tree, (exp.Update, exp.Delete, exp.Merge)):
            tree = _as_select(tree)
        for table in tree.find_all(exp.Table):
            table.meta["written"] = table.name  # a path, before the dialect folds its case
        for column in tree.find_all(exp.Column):
            # as written: qualify places a bare column on a subquery's own table,
            # though it may be the enclosing query's
            column.meta["written"] = (column.table, column.name)
        try:
            tree = qualify(tree, dialect=self.dialect, validate_qualify_columns=False)
            scopes = traverse_scope(tree)
            found = _Statement(self, scopes)
            found.read()
        except Exception:
            self.errors.append((index, "qualify"))
            return
        if not scopes:
            self.errors.append((index, f"unsupported:{kind}"))
            return
        for rel in found.tables:
            self.tables.setdefault(rel, False)
        for key, roles in found.columns.items():
            self.columns.setdefault(key, set()).update(roles)
        for key, roles in found.unresolved.items():
            self.unresolved.setdefault(key, set()).update(roles)

    # ---- relations

    def classify(self, table: exp.Table) -> tuple[Rel, ...] | _Unknown:
        """A table in FROM as the source relations it stands for."""
        key = _table_key(table, self.dialect)
        if key is None:
            return _UNKNOWN
        if key in self.refs:
            return (Rel("ref", (self.refs[key],)),)
        this = table.this
        if isinstance(this, exp.Func):
            if _function_name(this) in self.file_functions:
                paths = _path_literals(this)
                if paths:
                    return tuple(Rel("file", (_file_path(p),)) for p in paths)
            return _UNKNOWN  # no source, and its arguments stay out of the scan
        parts = tuple(part.name for part in table.parts)
        folded = tuple(p.lower() for p in parts)
        if any(_ends_with(folded, made) for made in self.created):
            return _UNKNOWN
        if len(parts) == 1 and parts[0].lower().endswith(self.file_suffixes or ("\0",)):
            # a replacement scan: the path as written
            return (Rel("file", (_file_path(table.meta.get("written") or parts[0]),)),)
        return (Rel("table", parts),)


class _Statement:
    """One qualified statement's scopes, and what they read."""

    def __init__(self, scanner: _Scanner, scopes: list[Scope]) -> None:
        self.scanner = scanner
        self.scopes = scopes
        self.tables: dict[Rel, None] = {}  # in the order met
        self.columns: dict[tuple[Rel, str], set[str]] = {}
        self.unresolved: dict[tuple[str, tuple[Rel | None, ...], bool], set[str]] = {}
        self._classified: dict[int, tuple[Rel, ...] | _Unknown] = {}

    def read(self) -> None:
        for scope in self.scopes:
            for _, source in scope.selected_sources.values():
                self.source(source)
        seen: set[int] = set()
        for scope in self.scopes:  # inner scopes first: a correlated column is
            for column in scope.columns:  # listed again by each enclosing scope
                if id(column) in seen or isinstance(column.this, exp.Star):
                    continue
                seen.add(id(column))
                self.record(self.resolve(scope, column), _role(column))

    def record(self, reads: list[_End | _Open], role: str) -> None:
        for read in reads:
            if isinstance(read, _End):
                self.columns.setdefault((read.rel, read.column), set()).add(role)
            else:
                key = (read.column, read.candidates, read.nested)
                self.unresolved.setdefault(key, set()).add(role)

    # ---- columns

    def resolve(self, scope: Scope, column: exp.Column) -> list[_End | _Open]:
        """What a column of ``scope`` reads."""
        name, qualifier = column.name, column.table
        written = column.meta.get("written")
        if _correlatable(scope) and self.enclosing(scope):
            if written is None and isinstance(column.parent, exp.Dot):
                return []  # qualify's guess at a struct, where it can't know the relation
            if written is not None and not written[0]:
                # bare as written: the subquery's relation or an enclosing one
                return self.place(scope, written[1])
        if qualifier:
            source = self.lookup(scope, qualifier)
            if source is not None:
                return self.read_from(source, name)
            name = qualifier  # it names no relation: a struct's root column
        return self.place(scope, name)

    def enclosing(self, scope: Scope) -> list[Scope]:
        """The scopes whose relations a correlated ``scope`` can read,
        nearest first."""
        found = []
        while _correlatable(scope) and scope.parent is not None:
            scope = scope.parent
            if scope.selected_sources:
                found.append(scope)
        return found

    def lookup(self, scope: Scope, alias: str) -> Any:
        """The source ``alias`` names in ``scope``, or in a scope around it."""
        at: Scope | None = scope
        while at is not None:
            if alias in at.selected_sources:
                return self.source(at.selected_sources[alias][1])
            at = at.parent
        return None

    def read_from(self, source: Any, name: str) -> list[_End | _Open]:
        """A column read from a relation the statement names."""
        if isinstance(source, tuple):
            return [_End(rel, name) for rel in source]
        return []

    def options(self, scope: Scope, name: str) -> list[tuple[Rel, ...] | None]:
        """The relations of ``scope`` that may have a column ``name``."""
        found: list[tuple[Rel, ...] | None] = []
        for _, source in scope.selected_sources.values():
            source = self.source(source)
            if isinstance(source, tuple):
                found.append(source)
            elif isinstance(source, _Unknown):
                found.append(None)
        return found

    def place(self, scope: Scope, name: str) -> list[_End | _Open]:
        """An unqualified column: on the one relation that can have it, else
        unresolved, with the scope's relations and then the enclosing ones as
        candidates."""
        levels = [self.options(at, name) for at in (scope, *self.enclosing(scope))]
        found = [option for level in levels for option in level]
        if len(found) == 1:
            return [_End(rel, name) for rel in found[0] or ()]
        candidates = tuple(
            dict.fromkeys(rel for option in found for rel in (option or (None,)))
        )
        if all(rel is None for rel in candidates):
            return []
        nested = sum(1 for level in levels if level) > 1
        return [_Open(name, candidates, nested)]

    def source(self, source: Any) -> Any:
        """A scope's source as what a column read from it reads: source
        relations, a derived query's scope, or an unknown."""
        if isinstance(source, Scope):
            return source if isinstance(source.expression, _QUERY) else _UNKNOWN
        if not isinstance(source, exp.Table):
            return _UNKNOWN
        found = self._classified.get(id(source))
        if found is None:
            found = self._classified[id(source)] = self.scanner.classify(source)
            if not isinstance(found, _Unknown):
                for rel in found:
                    self.tables[rel] = None
        return found


def scan(
    sql: str,
    dialect: str | None,
    refs: Mapping[str, str],
    file_functions: frozenset[str],
    file_suffixes: frozenset[str],
) -> Scan:
    """What a cell's rendered ``sql`` reads. ``refs`` maps the text each
    ``ref()`` rendered to onto the cell it names; ``file_functions`` (the table
    functions that read a file) and ``file_suffixes`` (the extensions a
    replacement scan reads) are empty for a cell off the conduit's engine."""
    if len(sql.encode()) > MAX_BYTES:
        return Scan(errors=((0, "too large"),))
    scanner = _Scanner(dialect, refs, file_functions, file_suffixes)
    with _quiet():
        scanner.run(sql)
    return Scan(
        tables=tuple(scanner.tables.items()),
        columns=tuple(
            ColumnUse(rel, column, frozenset(roles))
            for (rel, column), roles in scanner.columns.items()
        ),
        unresolved=tuple(
            Unresolved(column, frozenset(roles), candidates, nested)
            for (column, candidates, nested), roles in scanner.unresolved.items()
        ),
        created=scanner.created,
        errors=tuple(scanner.errors),
    )
