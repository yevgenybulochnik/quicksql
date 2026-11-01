"""The builtin directive vocabulary — plugins bundling config with behavior.

Vars/Sources own their directive *and* the template global that consumes it;
Refs/Env are behavior-only contributors of the remaining core globals.
"""

from __future__ import annotations

import os
from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

from ..config import registries_of
from ..errors import ConfigError
from ..models import Merge, Scope
from ..registry import plugin
from .base import Plugin, qfield

_MISSING = object()


@plugin
class Engine(Plugin):
    """The engine: directive; answers engine resolution for explicit settings."""

    class Config(BaseModel):
        engine: str | None = qfield(None)

        @field_validator("engine")
        @classmethod
        def _registered(cls, v: str | None, info: ValidationInfo) -> str | None:
            registries = registries_of(info)
            if v is not None and registries is not None and v not in registries.executors:
                have = ", ".join(registries.executors.names())
                raise ValueError(f"engine {v!r} is not a registered executor (have: {have})")
            return v

    def resolve_engine(self, config: Any) -> str | None:
        return config.engine or None


@plugin
class Input(Plugin):
    """The input: directive; infers the engine from its sole top-level key."""

    class Config(BaseModel):
        input: dict[str, Any] = qfield({}, merge=Merge.DEEP)

    def resolve_engine(self, config: Any) -> str | None:
        inp = getattr(config, "input", None) or {}
        if len(inp) == 1:
            return next(iter(inp))
        if len(inp) > 1:
            raise ConfigError(
                f"cannot infer engine from input keys {list(inp)}; set @engine explicitly"
            )
        return None


@plugin
class Sources(Plugin):
    """The sources: directive plus the source() template global that reads it."""

    class Config(BaseModel):
        sources: dict[str, Any] = qfield({}, merge=Merge.DEEP)

        @field_validator("sources")
        @classmethod
        def _shape(cls, v: dict[str, Any]) -> dict[str, Any]:
            out: dict[str, Any] = {}
            for name, spec in v.items():
                if isinstance(spec, str):
                    spec = {"path": spec}
                if not isinstance(spec, dict) or "path" not in spec:
                    raise ValueError(f"source {name!r} needs a path (got {spec!r})")
                out[name] = spec
            return out

    def render_context(self, rctx: Any) -> dict[str, Any]:
        from ..sources import reader_for

        def source(name_or_path: str, **opts: Any) -> str:
            sources_cfg = rctx.config.sources or {}
            if name_or_path in sources_cfg:
                spec = dict(sources_cfg[name_or_path])
                path = spec.pop("path")
                type_ = spec.pop("type", None)
                opts = {**spec, **opts}
            else:
                path, type_ = name_or_path, opts.pop("type", None)
            reader = reader_for(path, type_, rctx.registries.readers)
            rctx.require_extensions(reader.requires)
            rctx.mark_source_used()
            return reader.expr(rctx.resolve_path(path), **opts)

        return {"source": source}


@plugin
class Extensions(Plugin):
    """The extensions: directive plus loading them on the conduit before a
    cell executes (config-declared ∪ render-collected)."""

    class Config(BaseModel):
        extensions: list[str] = qfield([], merge=Merge.EXTEND)

    def before_execute(self, cell: Any, ctx: Any) -> None:
        from ..runner import _load_ext_cached

        for ext in cell.extensions:
            _load_ext_cached(ctx, ext, cell.name)


@plugin
class Output(Plugin):
    """The output: directive; answers sink resolution from output.type."""

    def resolve_sink(self, config: Any) -> str | None:
        return (config.output or {}).get("type")

    class Config(BaseModel):
        output: dict[str, Any] = qfield({"type": "parquet", "dir": "data/"}, merge=Merge.DEEP)

        @field_validator("output")
        @classmethod
        def _sink_registered(cls, v: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
            registries = registries_of(info)
            sink_type = v.get("type", "parquet")
            if registries is not None and sink_type not in registries.sinks:
                have = ", ".join(registries.sinks.names())
                raise ValueError(f"output.type {sink_type!r} is not a registered sink (have: {have})")
            return v


@plugin
class Autorun(Plugin):
    """The autorun: directive plus the watch-mode rerun filter it drives."""

    class Config(BaseModel):
        autorun: bool = qfield(True, affects_results=False)

    def should_rerun(self, cell: Any) -> bool | None:
        return bool(cell.config.autorun)


@plugin
class Vars(Plugin):
    """The vars: directive plus the var() template global that reads it."""

    class Config(BaseModel):
        vars: dict[str, Any] = qfield({}, merge=Merge.DEEP)

    def render_context(self, rctx: Any) -> dict[str, Any]:
        values = rctx.config.vars or {}

        def var(key: str, default: Any = _MISSING) -> Any:
            if key in values:
                return values[key]
            if default is not _MISSING:
                return default
            raise ConfigError(f"cell {rctx.name!r}: undefined var {key!r}")

        return {"var": var, "vars": values}


@plugin
class Refs(Plugin):
    """Behavior-only: the ref() global — producer read-back + edge recording."""

    def render_context(self, rctx: Any) -> dict[str, Any]:
        def ref(cell_name: str) -> str:
            rctx.add_edge(cell_name)
            return rctx.producer_expr(cell_name)

        return {"ref": ref}


@plugin
class Env(Plugin):
    """Behavior-only: the env() global."""

    def render_context(self, rctx: Any) -> dict[str, Any]:
        def env(key: str, default: str = "") -> str:
            return os.environ.get(key, default)

        return {"env": env}


@plugin
class DependsOn(Plugin):
    """The depends_on: directive plus contributing its explicit edges."""

    scope = Scope.CELL

    class Config(BaseModel):
        depends_on: list[str] = qfield([], merge=Merge.EXTEND)

    def collect_edges(self, name: str, config: Any) -> list[str] | None:
        return list(config.depends_on)


@plugin
class Schema(Plugin):
    """The schema: directive plus injecting it into DB sink configs."""

    class Config(BaseModel):
        schema_: str | None = qfield(None, alias="schema")

    def sink_config(self, config: Any, cfg: dict[str, Any]) -> dict[str, Any] | None:
        if "schema" not in cfg and getattr(config, "schema_", None):
            return {**cfg, "schema": config.schema_}
        return None


@plugin
class Catalog(Plugin):
    """The catalog: directive — extra schema-browser targets (TUI ``S``)
    beyond the contexts cells' inputs imply. Mapping engine -> targets:
    bigquery takes ``project`` or ``project.dataset``; postgres takes DSNs;
    sqlite/duckdb take database paths."""

    scope = Scope.GLOBAL

    class Config(BaseModel):
        catalog: dict[str, list[str]] = qfield({}, merge=Merge.DEEP, affects_results=False)

        # nb: validator names must be unique across all composed plugin
        # Configs — a duplicate (e.g. Sources' _shape) is silently shadowed
        # in the merged model's MRO
        @field_validator("catalog", mode="before")
        @classmethod
        def _catalog_targets(cls, v: dict[str, Any], info: ValidationInfo) -> dict[str, Any]:
            registries = registries_of(info)
            out: dict[str, Any] = {}
            for engine, targets in (v or {}).items():
                if registries is not None and engine not in registries.executors:
                    raise ValueError(
                        f"catalog engine {engine!r} is not a registered executor"
                        f" (have: {', '.join(registries.executors.names())})"
                    )
                out[engine] = [targets] if isinstance(targets, str) else list(targets)
            return out


@plugin
class Tags(Plugin):
    class Config(BaseModel):
        tags: list[str] = qfield([], merge=Merge.EXTEND, affects_results=False)
