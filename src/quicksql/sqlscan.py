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
    # equalities between columns of two relations, each side (relation, column),
    # the pair in a fixed order
    joins: tuple[tuple[tuple[Rel, str], tuple[Rel, str]], ...] = ()
    # the terminal statement's named columns: each one's pass-through origin,
    # None when it's computed
    outputs: tuple[tuple[str, tuple[Rel, str] | None], ...] = ()
    # the relations a terminal `*` passes through, each with the columns the
    # `*` leaves out or replaces (lower-cased)
    star: tuple[tuple[Rel, frozenset[str]], ...] = ()
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
        if isinstance(node, exp.Window) and key in ("partition_by", "order"):
            return "window"
        if isinstance(node, (exp.Where, exp.Qualify)):
            return "where"
        if isinstance(node, (exp.From, exp.Lateral)):
            return "from"
        if isinstance(node, exp.Join):
            return "join" if key in ("on", "using") else "from"
        if isinstance(node, exp.Group):
            return "group"
        if isinstance(node, exp.Having):
            return "having"
        if isinstance(node, exp.Order):
            return "window" if isinstance(node.parent, exp.Window) else "order"
        if isinstance(node, _QUERY):
            return "select"
        key, node = node.arg_key, node.parent
    return "select"


def _star(select: exp.Expression) -> exp.Star | None:
    """A select-list item as the star it is: ``*`` or ``t.*``."""
    if isinstance(select, exp.Column):
        select = select.this
    return select if isinstance(select, exp.Star) else None


def _left_out(star: exp.Star) -> tuple[set[str], list[str]]:
    """The columns a star excludes (lower-cased), and the ones it replaces
    with a computed value."""
    excluded = {e.name.lower() for e in star.args.get("except_") or []}
    replaced = [e.alias_or_name for e in star.args.get("replace") or []]
    return excluded, replaced


def _conjuncts(condition: exp.Expression) -> Iterator[exp.Expression]:
    condition = condition.unnest()
    if isinstance(condition, exp.And):
        yield from _conjuncts(condition.this)
        yield from _conjuncts(condition.expression)
    else:
        yield condition


def _join_key(end: tuple[Rel, str]) -> tuple[Any, ...]:
    return (end[0].parts, end[1], end[0].kind)


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


def same_table(parts: tuple[str, ...], other: tuple[str, ...]) -> bool:
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
        self.joins: dict[tuple[tuple[Rel, str], tuple[Rel, str]], None] = {}
        self.outputs: tuple[tuple[str, tuple[Rel, str] | None], ...] = ()
        self.star: tuple[tuple[Rel, frozenset[str]], ...] = ()
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
            outputs, star = found.shape(found.root()) if scopes else ([], [])
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
        self.joins.update(found.joins)
        for rel, _ in star:
            self.tables[rel] = True
        if terminal:
            self.outputs = tuple((name, (o.rel, o.column) if o else None) for name, o in outputs)
            self.star = tuple(star)

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
        if any(same_table(folded, made) for made in self.created):
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
        self.joins: dict[tuple[tuple[Rel, str], tuple[Rel, str]], None] = {}
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
        for scope in self.scopes:
            self.read_joins(scope)

    def record(self, reads: list[_End | _Open], role: str) -> None:
        for read in reads:
            if isinstance(read, _End):
                self.columns.setdefault((read.rel, read.column), set()).add(role)
            else:
                key = (read.column, read.candidates, read.nested)
                self.unresolved.setdefault(key, set()).add(role)

    # ---- columns

    def resolve(
        self, scope: Scope, column: exp.Column, trail: frozenset[Any] = frozenset()
    ) -> list[_End | _Open]:
        """What a column of ``scope`` reads: followed through the CTEs and
        subqueries that pass it on unchanged, to the source relation."""
        name, qualifier = column.name, column.table
        written = column.meta.get("written")
        if _correlatable(scope) and self.enclosing(scope):
            if written is None and isinstance(column.parent, exp.Dot):
                # qualify's guess at a struct, where it can't know the relation.
                # `u.b` on the subquery's own `u` it rewrites to a field of an
                # enclosing table's column `u`: that was a column `b`
                field = column.parent.expression
                if name in scope.selected_sources and isinstance(field, exp.Identifier):
                    return self.place(scope, field.name, trail)
                return []
            if written is not None and not written[0]:
                # bare as written: the subquery's relation or an enclosing one
                return self.place(scope, written[1], trail)
        if qualifier:
            source = self.lookup(scope, qualifier)
            if source is not None:
                return self.read_from(source, name, trail)
            name = qualifier  # it names no relation: a struct's root column
        return self.place(scope, name, trail)

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

    def read_from(self, source: Any, name: str, trail: frozenset[Any]) -> list[_End | _Open]:
        """A column read from a relation the statement names."""
        sure, options = self.source_options(source, name, trail)
        return sure if sure is not None else self.settle(name, [options])

    def source_options(
        self, source: Any, name: str, trail: frozenset[Any]
    ) -> tuple[list[_End | _Open] | None, list[tuple[Rel, ...] | None]]:
        """A column ``name`` of one source, as (what it reads when the source
        is known to have it, the relations that may have it otherwise). None
        among them: a relation whose columns aren't known."""
        if isinstance(source, tuple):
            return None, [source]
        if isinstance(source, _Unknown):
            return None, [None]
        return self.derived(source, name, trail)

    def options(
        self, scope: Scope, name: str, trail: frozenset[Any], only: str | None = None
    ) -> tuple[list[_End | _Open] | None, list[tuple[Rel, ...] | None]]:
        """``source_options`` over the scope's sources, or just ``only``."""
        sure = None
        found: list[tuple[Rel, ...] | None] = []
        for alias, (_, source) in scope.selected_sources.items():
            if only is not None and alias != only:
                continue
            has, options = self.source_options(self.source(source), name, trail)
            if has is not None and sure is None:
                sure = has
            found += options
        return sure, found

    def leaves(self, scope: Scope) -> list[Scope]:
        """A set operation's SELECTs, left to right."""
        if not isinstance(scope.expression, exp.SetOperation):
            return [scope]
        return [leaf for branch in scope.union_scopes for leaf in self.leaves(branch)]

    def follow(self, scope: Scope, select: exp.Expression, trail: frozenset[Any]) -> list[Any]:
        """What reading one of the scope's select-list items reads: its column
        when the item is a bare one (aliased or not), nothing when computed."""
        value = select.unalias().unnest()
        if isinstance(value, exp.Column) and _star(value) is None:
            return self.resolve(scope, value, trail)
        return []

    def derived(
        self, scope: Scope, name: str, trail: frozenset[Any]
    ) -> tuple[list[_End | _Open] | None, list[tuple[Rel, ...] | None]]:
        """``source_options`` of a CTE or subquery: followed through its
        select of that name, else through a ``*`` that doesn't leave it out."""
        key = (id(scope), name)
        if key in trail:  # a recursive CTE reading itself
            return None, []
        trail = trail | {key}
        query = scope.expression
        if isinstance(query, exp.SetOperation):
            leaves = self.leaves(scope)
            names = [s.alias_or_name for s in leaves[0].expression.selects] if leaves else []
            starred = any(_star(s) for leaf in leaves for s in leaf.expression.selects)
            if starred or name not in names:
                return None, [None] if starred else []
            position, reads = names.index(name), []
            for leaf in leaves:  # each branch's column at that position is read
                selects = leaf.expression.selects
                if position < len(selects):
                    reads += self.follow(leaf, selects[position], trail)
            return reads, []
        for select in query.selects:
            if _star(select) is None and select.alias_or_name == name:
                return self.follow(scope, select, trail), []
        found: list[tuple[Rel, ...] | None] = []
        for select in query.selects:
            star = _star(select)
            if star is None:
                continue
            excluded, replaced = _left_out(star)
            if name.lower() in (r.lower() for r in replaced):
                return [], []  # computed here
            if name.lower() in excluded:
                continue
            only = select.table if isinstance(select, exp.Column) else None
            sure, options = self.options(scope, name, trail, only or None)
            if sure is not None:
                return sure, []
            found += options
        return None, found

    def settle(self, name: str, levels: list[list[tuple[Rel, ...] | None]]) -> list[_End | _Open]:
        """A column over the relations that may have it, by scope (innermost
        first): read from the only one, else unresolved."""
        found = [option for level in levels for option in level]
        if len(found) == 1:
            return [_End(rel, name) for rel in found[0] or ()]
        candidates = tuple(dict.fromkeys(rel for option in found for rel in (option or (None,))))
        if all(rel is None for rel in candidates):
            return []
        return [_Open(name, candidates, nested=sum(1 for level in levels if level) > 1)]

    def place(self, scope: Scope, name: str, trail: frozenset[Any]) -> list[_End | _Open]:
        """An unqualified column: read from the relation known to have it or
        the only one that can, else unresolved, with the scope's relations and
        then the enclosing ones as candidates."""
        levels: list[list[tuple[Rel, ...] | None]] = []
        for at in (scope, *self.enclosing(scope)):
            sure, options = self.options(at, name, trail)
            if sure is not None:
                if not any(levels):
                    return sure  # no nearer relation could have shadowed it
                options = [*options, None]
            levels.append(options)
        return self.settle(name, levels)

    # ---- joins

    def read_joins(self, scope: Scope) -> None:
        """Equalities between columns of two relations: in a join's ON (USING
        is one by now), and among a WHERE's top-level conjuncts (a comma join)."""
        query = scope.expression
        if not isinstance(query, exp.Select):
            return
        conditions = [j.args["on"] for j in query.args.get("joins") or [] if j.args.get("on")]
        where = query.args.get("where")
        if where is not None:
            conditions.append(where.this)
        for condition in conditions:
            for equality in _conjuncts(condition):
                left, right = equality.this, equality.args.get("expression")
                if not (
                    isinstance(equality, exp.EQ)
                    and isinstance(left, exp.Column)
                    and isinstance(right, exp.Column)
                    and left.table != right.table
                    and {left.table, right.table} <= set(scope.selected_sources)
                ):
                    continue
                ends = [self.resolve(scope, side) for side in (left, right)]
                if all(len(e) == 1 and isinstance(e[0], _End) for e in ends):
                    a, b = sorted(((e[0].rel, e[0].column) for e in ends), key=_join_key)
                    if a != b:
                        self.joins[(a, b)] = None

    # ---- the statement's result

    def root(self) -> Scope:
        """The scope of the statement's own query."""
        root = self.scopes[-1]
        query = root.expression
        while isinstance(query, exp.Subquery):  # COPY (SELECT ...)
            query = query.this
        return {id(s.expression): s for s in self.scopes}.get(id(query), root)

    def shape(
        self, scope: Scope, trail: frozenset[int] = frozenset()
    ) -> tuple[list[tuple[str, _End | None]], list[tuple[Rel, frozenset[str]]]]:
        """A query's result as (its named columns, each with its pass-through
        origin; the source relations a ``*`` passes through, each with the
        columns the ``*`` leaves out)."""
        query = scope.expression
        if id(scope) in trail or not isinstance(query, _QUERY):
            return [], []
        trail = trail | {id(scope)}
        outputs: list[tuple[str, _End | None]] = []
        stars: list[tuple[Rel, frozenset[str]]] = []
        if isinstance(query, exp.SetOperation):
            for position, leaf in enumerate(self.leaves(scope)):
                named, passed = self.shape(leaf, trail)
                if position == 0:  # the first branch names them; each is several columns
                    outputs = [(name, None) for name, _ in named]
                stars += passed
            return outputs, stars
        for select in query.selects:
            star = _star(select)
            if star is None:
                reads = self.follow(scope, select, frozenset())
                origin = reads[0] if len(reads) == 1 and isinstance(reads[0], _End) else None
                outputs.append((select.alias_or_name, origin))
                continue
            excluded, replaced = _left_out(star)
            left_out = frozenset(excluded | {r.lower() for r in replaced})
            only = select.table if isinstance(select, exp.Column) else None
            for alias, (_, source) in scope.selected_sources.items():
                if only and alias != only:
                    continue
                source = self.source(source)
                if isinstance(source, tuple):
                    stars += [(rel, left_out) for rel in source]
                elif isinstance(source, Scope):
                    named, passed = self.shape(source, trail)
                    outputs += [(n, o) for n, o in named if n.lower() not in left_out]
                    stars += [(rel, out | left_out) for rel, out in passed]
            outputs += [(name, None) for name in replaced]
        return outputs, stars

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
        joins=tuple(scanner.joins),
        outputs=scanner.outputs,
        star=scanner.star,
        created=scanner.created,
        errors=tuple(scanner.errors),
    )
