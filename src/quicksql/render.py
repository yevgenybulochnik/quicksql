"""Jinja rendering of SQL bodies only — config values stay literal.

Template globals are plugin contributions: every plugin overriding
``render_context(rctx)`` returns a dict of globals, merged under a collision
guard (two providers for one key is a compile error). The builtin globals —
``ref``/``source``/``var``/``env`` — come from the Refs/Sources/Vars/Env
plugins. ``rctx`` is the capability object: sanctioned verbs for recording
dependency edges and required extensions instead of access to private state.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from jinja2 import StrictUndefined, TemplateError
from jinja2.sandbox import SandboxedEnvironment
from pydantic import BaseModel

from .errors import ConfigError, QsqlError, describe
from .registry import Registries
from .sinks.base import Sink


@dataclass
class Producer:
    """What a downstream cell needs to know about an upstream: how its output
    is read back (sink) and which execution context it runs in."""

    sink: Sink
    context: str = ""


@dataclass
class RenderContext:
    """Per-cell render capabilities handed to render hooks.

    Verbs mutate render-collected state (edges, extensions) through a small
    contract; the fields themselves feed the compiler afterwards.
    """

    name: str
    config: Any
    root: Path
    producers: Mapping[str, Producer]
    registries: Registries  # the notebook's: e.g. source() picks its reader here
    context: str = ""
    supports_context_refs: bool = False
    edges: list[str] = field(default_factory=list)
    extensions: list[str] = field(default_factory=list)
    used_source: bool = False
    context_refs: list[str] = field(default_factory=list)
    external_refs: list[str] = field(default_factory=list)

    def add_edge(self, cell_name: str) -> None:
        """Record that this cell depends on another cell's landed output."""
        if cell_name not in self.producers:
            raise ConfigError(f"cell {self.name!r}: unknown cell {cell_name!r} in ref()")
        if cell_name not in self.edges:
            self.edges.append(cell_name)

    def producer_expr(self, cell_name: str) -> str:
        """How this cell reads the named upstream: a bare temp-table name when
        both share an execution context (in-engine, dialect-neutral), else the
        producer sink's read-back expression on the DuckDB conduit."""
        if cell_name not in self.producers:
            raise ConfigError(f"cell {self.name!r}: unknown cell {cell_name!r} in ref()")
        producer = self.producers[cell_name]
        if (
            self.supports_context_refs
            and producer.context
            and producer.context == self.context
        ):
            if cell_name not in self.context_refs:
                self.context_refs.append(cell_name)
            return cell_name
        if cell_name not in self.external_refs:
            self.external_refs.append(cell_name)
        return producer.sink.ref_expr(cell_name)

    def require_extensions(self, extensions: list[str]) -> None:
        for ext in extensions:
            if ext not in self.extensions:
                self.extensions.append(ext)

    def mark_source_used(self) -> None:
        self.used_source = True

    def resolve_path(self, path: str) -> str:
        return path if os.path.isabs(path) else str(self.root / path)


def _call_hook(plug: Any, hook: str, rctx: RenderContext, *args: Any) -> Any:
    """A plugin bug in a render hook is the cell's template error, not a crash
    of compile, or of the LSP, which compiles on every edit."""
    try:
        return getattr(plug, hook)(rctx, *args)
    except QsqlError:
        raise  # already says what's wrong
    except Exception as exc:
        raise ConfigError(
            f"cell {rctx.name!r}: template error: plugin {plug.name!r} {hook}: "
            f"{describe(exc, ())}"
        ) from exc


def build_context(rctx: RenderContext) -> dict[str, Any]:
    """Merge plugin-contributed globals under the collision guard."""
    context: dict[str, Any] = {"config": rctx.config}
    owners = {key: "core" for key in context}
    for plug in rctx.registries.plugins.overriding("render_context"):
        for key, value in (_call_hook(plug, "render_context", rctx) or {}).items():
            if key in owners:
                raise ConfigError(
                    f"cell {rctx.name!r}: render context key {key!r} from plugin "
                    f"{plug.name!r} collides with {owners[key]}"
                )
            owners[key] = f"plugin {plug.name!r}"
            context[key] = value
    return context


class _SqlSandbox(SandboxedEnvironment):
    """Jinja's sandbox, plus: a pydantic model exposes only its fields.

    The base sandbox hides underscore attributes but lets a template call any
    public method, and every model has some that reach outside the template
    (``parse_file(path)`` reads from disk). Every global is checked, the
    builtin ``config`` included; nested models go through the same check on
    each attribute lookup.
    """

    def is_safe_attribute(self, obj: Any, attr: str, value: Any) -> bool:
        if isinstance(obj, BaseModel):
            return attr in type(obj).model_fields
        return super().is_safe_attribute(obj, attr, value)


def render_sql(
    name: str,
    sql_raw: str,
    config: Any,
    producers: Mapping[str, Producer],
    root: Path,
    registries: Registries,
    context: str = "",
    supports_context_refs: bool = False,
) -> tuple[str, RenderContext]:
    rctx = RenderContext(
        name=name,
        config=config,
        root=root,
        producers=producers,
        registries=registries,
        context=context,
        supports_context_refs=supports_context_refs,
    )
    context = build_context(rctx)

    # sandboxed: a template may call its globals but not walk from them to
    # Python internals (`__globals__`, `__mro__`), so it can't run arbitrary code
    jinja = _SqlSandbox(undefined=StrictUndefined, keep_trailing_newline=True)
    try:
        sql = jinja.from_string(sql_raw).render(**context)
    except QsqlError:
        raise  # already says what's wrong: an unknown ref(), a plugin global's own error
    except Exception as exc:  # Jinja's own, or Python's from inside it: {{ 1/0 }}
        detail = describe(exc, (TemplateError,))
        raise ConfigError(f"cell {name!r}: template error: {detail}") from exc

    for plug in registries.plugins.overriding("after_render"):
        out = _call_hook(plug, "after_render", rctx, sql)
        if out is not None:
            sql = out
    return sql, rctx
