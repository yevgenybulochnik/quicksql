"""Editor-agnostic analysis: diagnostics (linting) and completions.

Both reuse the compiler and catalog directly, so no LSP transport is needed to
test them. Positions are 0-indexed (line, character) per the LSP convention.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, ParseError

from ..compiler import compile_text
from ..errors import ConfigErrorGroup, CycleError, ParseError as QsqlParseError, QsqlError
from ..executors.base import statement_spans
from .googlesql import find_execute_query, parse_errors
from . import vocab
from .mask import Opaque, mask_jinja
from .schema import SchemaCache, columns_for, relation_names

# quicksql engine names line up with sqlglot dialect names
_DIALECT = {"duckdb": "duckdb", "postgres": "postgres", "sqlite": "sqlite", "bigquery": "bigquery"}

# cursor in table-reference position: FROM/JOIN + partial dotted path
_TABLE_POS = re.compile(r"\b(?:from|join)\s+`?([\w.\-]*)$", re.IGNORECASE)

@dataclass
class Diagnostic:
    line: int
    character: int
    end_line: int
    end_character: int
    message: str
    severity: str = "error"  # "error" | "warning"
    source: str = "quicksql"


@dataclass
class Completion:
    label: str
    kind: str  # "field" | "reference" | "table" | "keyword" | "function"
    detail: str = ""


@dataclass
class Analyzer:
    """Holds the schema cache across requests (remote introspection is slow)."""

    cache: SchemaCache = field(default_factory=SchemaCache)

    @cached_property
    def _gsql_bin(self) -> str | None:
        """googlesql `execute_query`, resolved once per Analyzer (or None)."""
        return find_execute_query()

    # ---------- diagnostics ----------

    def diagnostics(self, text: str, root: Path | str = ".") -> list[Diagnostic]:
        try:
            project = compile_text(text, Path(root))
        except ConfigErrorGroup as group:
            return [_line_diag(text, e.line, e.message) for e in group.errors]
        except QsqlParseError as exc:
            return [_line_diag(text, _line_in(str(exc)), str(exc))]
        except (CycleError, QsqlError) as exc:
            return [_line_diag(text, 1, str(exc))]
        diags: list[Diagnostic] = [
            _line_diag(text, line, message, "warning")
            for line, message in project.warnings
        ]
        for cell in project.cells.values():
            diags.extend(_syntax_diagnostics(cell, self._gsql_bin))
        return diags

    # ---------- completions ----------

    def completions(
        self, text: str, root: Path | str, line: int, character: int
    ) -> list[Completion]:
        try:
            project = compile_text(text, Path(root))
        except QsqlError:
            project = None  # can't scope-resolve, but keywords still help
        lines = text.splitlines()
        prefix = lines[line][:character] if 0 <= line < len(lines) else ""

        if project is not None:
            in_ref = re.search(r"\b(ref|source)\(\s*['\"]?\w*$", prefix)
            if in_ref:
                return [
                    Completion(name, "reference", f"cell · {project.cells[name].engine}")
                    for name in project.order
                ]

        cell = _cell_at(project, line + 1) if project is not None else None

        if cell is not None:
            # table-reference position: FROM / JOIN followed by a partial
            # (possibly backticked, dotted) relation path
            tbl = _TABLE_POS.search(prefix)
            if tbl is not None:
                segments = tbl.group(1).split(".")[:-1]  # done segments only
                names = relation_names(project, cell, segments, self.cache)
                if names:
                    return [Completion(n, "table", detail) for n, detail in names]

        dialect = _DIALECT.get(cell.engine) if cell is not None else None
        items = [Completion(k, "keyword") for k in vocab.keywords(dialect)]
        items += [Completion(f, "function") for f in vocab.functions(dialect)]
        if cell is None:
            return items

        alias_m = re.search(r"([A-Za-z_]\w*)\.\w*$", prefix)
        alias = alias_m.group(1).lower() if alias_m else None
        # the very token being completed (`e.`) is a dangling dot that makes
        # sqlglot drop the whole FROM clause — patch it to a harmless literal
        # of the same length so the join scope survives mid-keystroke
        source = cell.source
        if alias_m is not None:
            source = _patch_dangling(cell, line, character, alias_m.start(1))
        scope = _resolve_scope(project, cell, source, line, character)
        relations = [scope[alias]] if alias and alias in scope else list(scope.values())

        seen: set[str] = set()
        cols: list[Completion] = []
        for rel in relations:
            for name, type_ in columns_for(project, cell, rel, self.cache):
                if name not in seen:
                    seen.add(name)
                    cols.append(Completion(name, "field", type_))
        return cols + items

    # ---------- navigation (go-to-def / outline) ----------

    def definition(
        self, text: str, root: Path | str, line: int, character: int
    ) -> tuple[int, int] | None:
        """`ref('x')` under the cursor -> the (line, 0) where cell x is defined."""
        try:
            project = compile_text(text, Path(root))
        except QsqlError:
            return None
        lines = text.splitlines()
        if not (0 <= line < len(lines)):
            return None
        for m in re.finditer(r"\bref\(\s*['\"]([^'\"]+)['\"]", lines[line]):
            if m.start() <= character <= m.end():
                cell = project.cells.get(m.group(1))
                if cell is not None:
                    return (cell.line - 1, 0)
        return None

    def document_symbols(self, text: str, root: Path | str) -> list[tuple[str, int, int, str]]:
        """(cell name, start_line, end_line, engine) for the outline, 0-indexed."""
        try:
            project = compile_text(text, Path(root))
        except QsqlError:
            return []
        return [
            (c.name, c.line - 1, max(c.line_end - 1, c.line - 1), c.engine)
            for c in project.cells.values()
        ]


# ---------- helpers ----------


def _line_in(message: str) -> int:
    m = re.search(r"line (\d+)", message)
    return int(m.group(1)) if m else 1


def _line_diag(text: str, line1: int, message: str, severity: str = "error") -> Diagnostic:
    """A diagnostic spanning a whole 1-indexed source line."""
    lines = text.splitlines()
    row = max(line1 - 1, 0)
    width = len(lines[row]) if row < len(lines) else 0
    return Diagnostic(row, 0, row, width, message, severity)


def _syntax_diagnostics(cell: Any, gsql_bin: str | None = None) -> list[Diagnostic]:
    dialect = _DIALECT.get(cell.engine)
    if dialect is None or "{%" in cell.source:
        return []  # control-flow Jinja can't be masked into valid SQL (v1 limit)
    masked, _ = mask_jinja(cell.source, _cell_vars(cell))
    if dialect == "bigquery" and gsql_bin is not None:
        hits = parse_errors(gsql_bin, masked)
        if hits is not None:  # None: tool failed, fall through to sqlglot
            out: list[Diagnostic] = []
            for line, col0, msg in hits:
                # googlesql line/col are 1-indexed within masked (== cell.source
                # positions), whose line 1 is file line cell.line
                row = cell.line + line - 2
                col = max(col0 - 1, 0)
                out.append(Diagnostic(row, col, row, col + 1, msg, "error", "googlesql"))
            return out
    try:
        sqlglot.parse(masked, dialect=dialect)
    except ParseError as exc:
        out = []
        for err in exc.errors:
            # sqlglot line/col are 1-indexed within cell.source, whose line 1
            # is file line cell.line
            row = cell.line + int(err.get("line", 1)) - 2
            col = max(int(err.get("col", 1)) - 1, 0)
            desc = err["description"]
            if "Unsupported pipe syntax" in desc:
                # valid BigQuery sqlglot can't parse — warn, don't block
                msg = (
                    f"{desc} Cell not fully validated; install googlesql "
                    "execute_query for complete pipe-syntax checks."
                )
                out.append(Diagnostic(row, col, row, col + 1, msg, "warning", "sqlglot"))
            else:
                out.append(Diagnostic(row, col, row, col + 1, desc, "error", "sqlglot"))
        return out
    return []


def _cell_vars(cell: Any) -> dict[str, Any]:
    """The cell's merged ``vars:`` for in-place mask substitution."""
    return getattr(cell.config, "vars", None) or {}


def _cell_at(project: Any, line1: int) -> Any | None:
    for cell in project.cells.values():
        if cell.line <= line1 <= cell.line_end:
            return cell
    return None


def _row_index(cell: Any, line: int, n: int) -> int | None:
    """File line (0-indexed) -> cell-source row, or None if out of range.
    Shared so `_patch_dangling` and statement-offset math guard identically."""
    idx = line - (cell.line - 1)
    return idx if 0 <= idx < n else None


def _patch_dangling(cell: Any, line: int, character: int, token_start: int) -> str:
    """Replace the in-progress ``alias.partial`` token at the cursor with a
    same-length ``1``-literal so the statement parses (length-preserving)."""
    src_lines = cell.source.splitlines()
    idx = _row_index(cell, line, len(src_lines))
    if idx is None:
        return cell.source
    row = src_lines[idx]
    end = character
    while end < len(row) and (row[end].isalnum() or row[end] == "_"):
        end += 1  # swallow the token's remainder past the cursor
    src_lines[idx] = row[:token_start] + "1" + " " * (end - token_start - 1) + row[end:]
    return "\n".join(src_lines)


def _statement_slice(
    cell: Any, src: str, masked: str, dialect: str | None, line: int | None, character: int | None
) -> str:
    """Narrow ``masked`` to the statement the cursor is in, so scope resolves per
    statement rather than merging every statement's tables into one flat scope.

    Statement offsets are found on the raw ``src`` (not ``masked``) so a
    ``{{ var }}`` value substitution can't inject a split point; ``mask_jinja`` is
    length-preserving, so those offsets index ``masked`` directly. Falls back to
    the whole ``masked`` when the cursor can't be placed (out-of-range row, no
    spans, or a leading comment before the first statement)."""
    if line is None or character is None:
        return masked
    src_lines = src.split("\n")
    idx = _row_index(cell, line, len(src_lines))
    if idx is None:
        return masked
    offset = sum(len(seg) + 1 for seg in src_lines[:idx]) + character
    here = [(a, b) for a, b in statement_spans(src, dialect or "") if a <= offset]
    if not here:
        return masked
    start, end = here[-1]  # the statement with the greatest start <= cursor offset
    return masked[start:end]


def _resolve_scope(
    project: Any,
    cell: Any,
    source: str | None = None,
    line: int | None = None,
    character: int | None = None,
) -> dict[str, Any]:
    """alias/name (lowercased) -> relation: a mask Ref/Source tag for a
    ``{{ ref/source }}`` placeholder, a Projection for a CTE alias, else the
    (qualified) table-name string. Scoped to the cursor's statement when
    ``line``/``character`` are given."""
    dialect = _DIALECT.get(cell.engine)
    src = source if source is not None else cell.source
    masked, spans = mask_jinja(src, _cell_vars(cell))
    by_placeholder = {s.name: s.tag for s in spans if s.name}
    masked = _statement_slice(cell, src, masked, dialect, line, character)
    try:
        tree = sqlglot.parse_one(masked, dialect=dialect, error_level=ErrorLevel.IGNORE)
    except Exception:
        return {}
    if tree is None:
        return {}
    scope: dict[str, Any] = {}
    for table in tree.find_all(exp.Table):
        key = (table.alias_or_name or "").lower()
        if not key:
            continue
        if table.name in by_placeholder:  # a {{ }} placeholder
            tag = by_placeholder[table.name]
            if isinstance(tag, Opaque):
                continue  # unknown value — no columns to offer for the alias
            scope[key] = tag
        else:  # engine-native table, keep catalog (bigquery project) and
            # schema qualifiers
            qualified = [table.catalog, table.db, table.name]
            scope[key] = ".".join(part for part in qualified if part)
    # a CTE's columns are its projection — statically known, overriding the
    # bare table-name entry its FROM reference produced
    from .schema import Projection

    for cte in tree.find_all(exp.CTE):
        key = (cte.alias or "").lower()
        cols = tuple(
            p.alias_or_name
            for p in getattr(cte.this, "expressions", [])
            if p.alias_or_name and p.alias_or_name != "*"
        )
        if key and cols:
            scope[key] = Projection(cols)
    return scope
