"""Config model composition and layer merging.

``build_models`` composes every registered plugin's ``Config`` into one
GlobalConfig / CellConfig via multi-inheritance ``create_model``, cached on the
tuple of Config classes composed. Raw directive dicts merge highest-priority
last (global -> cell -> run overrides), honoring each field's merge strategy,
then validate against the composed model.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from pydantic import BaseModel, ValidationError, create_model

from .errors import ConfigError
from .models import Merge, Scope
from .plugins.base import EmptyConfig
from .registry import PLUGINS, FieldEntry, PluginRegistry


def build_models(plugins: PluginRegistry = PLUGINS) -> tuple[type[BaseModel], type[BaseModel]]:
    return (
        _compose("GlobalConfig", plugins, (Scope.GLOBAL, Scope.BOTH)),
        _compose("CellConfig", plugins, (Scope.CELL, Scope.BOTH)),
    )


def _compose(name: str, plugins: PluginRegistry, scopes: tuple[Scope, ...]) -> type[BaseModel]:
    bases = tuple(
        type(p).Config for p in plugins.by_scope(*scopes) if type(p).Config is not EmptyConfig
    )
    return _compose_bases(name, bases)


# Keyed on the Config classes themselves, so any registry change that matters
# (a new plugin, a name re-registered with a new Config) misses without explicit
# invalidation; lru_cache never stores a raise, so a validator collision fails
# every compile. A registry state needs two entries (GlobalConfig, CellConfig):
# 32 holds 16 states, far more than one process uses, while bounding the dead
# entries tests leave behind by registering a throwaway plugin class each.
@lru_cache(maxsize=32)
def _compose_bases(name: str, bases: tuple[type[BaseModel], ...]) -> type[BaseModel]:
    _check_validator_names(bases)
    return create_model(name, __base__=bases or (EmptyConfig,))


def _check_validator_names(bases: tuple[type[BaseModel], ...]) -> None:
    """Composition is multi-inheritance, so two plugin Configs defining
    validators under the same attribute name would silently shadow one
    another in the MRO — fail loudly instead."""
    owners: dict[str, type[BaseModel]] = {}
    for base in bases:
        decorators = base.__pydantic_decorators__
        names = {
            attr
            for group in ("validators", "field_validators", "model_validators")
            for attr in getattr(decorators, group, {})
        }
        for attr in names:
            if attr in owners and owners[attr] is not base:
                raise ConfigError(
                    f"validator name collision: {attr!r} is defined by both"
                    f" {owners[attr].__qualname__} and {base.__qualname__};"
                    " rename one — composed configs shadow duplicates silently"
                )
            owners[attr] = base


def deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if isinstance(out.get(k), dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _field_merge(entry: FieldEntry) -> Merge:
    extra = entry.info.json_schema_extra or {}
    return Merge(extra.get("qsql_merge", Merge.OVERRIDE.value))


def _merge_value(merge: Merge, base: Any, new: Any) -> Any:
    if merge is Merge.DEEP and isinstance(base, dict) and isinstance(new, dict):
        return deep_merge(base, new)
    if merge is Merge.EXTEND and isinstance(base, list) and isinstance(new, list):
        return base + new
    return new


def combine(layers: list[dict[str, Any]], plugins: PluginRegistry = PLUGINS) -> dict[str, Any]:
    fmap = plugins.field_map()
    merged: dict[str, Any] = {}
    for layer in layers:
        for key, val in layer.items():
            if key in merged:
                merged[key] = _merge_value(_field_merge(fmap[key]), merged[key], val)
            else:
                merged[key] = val
    return merged


def check_scope(
    raw: dict[str, Any], scope: Scope, plugins: PluginRegistry = PLUGINS, where: str = ""
) -> None:
    fmap = plugins.field_map()
    label = "global" if scope is Scope.GLOBAL else "cell"
    for key in raw:
        entry = fmap.get(key)
        if entry is None:
            raise ConfigError(f"unknown directive: @{key}{where}")
        if entry.plugin.scope not in (scope, Scope.BOTH):
            raise ConfigError(f"@{key} is not allowed in {label} scope{where}")


def _filter_overrides(
    overrides: dict[str, Any], scope: Scope, plugins: PluginRegistry
) -> dict[str, Any]:
    """Overrides apply wherever their field lives: keys for the other scope are
    dropped, but keys no plugin owns are still an error."""
    fmap = plugins.field_map()
    out: dict[str, Any] = {}
    for key, val in overrides.items():
        entry = fmap.get(key)
        if entry is None:
            raise ConfigError(f"unknown directive: @{key} (from run overrides)")
        if entry.plugin.scope in (scope, Scope.BOTH):
            out[key] = val
    return out


def _validate(model: type[BaseModel], data: dict[str, Any], where: str) -> BaseModel:
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        msgs = "; ".join(
            f"@{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise ConfigError(f"invalid config for {where}: {msgs}") from exc


def resolve_global(
    raw: dict[str, Any],
    overrides: dict[str, Any] | None = None,
    plugins: PluginRegistry = PLUGINS,
) -> BaseModel:
    global_model, _ = build_models(plugins)
    check_scope(raw, Scope.GLOBAL, plugins)
    ov = _filter_overrides(overrides or {}, Scope.GLOBAL, plugins)
    return _validate(global_model, combine([raw, ov], plugins), "global header")


def resolve_cell(
    global_raw: dict[str, Any],
    cell_raw: dict[str, Any],
    overrides: dict[str, Any] | None = None,
    plugins: PluginRegistry = PLUGINS,
    name: str = "cell",
) -> BaseModel:
    _, cell_model = build_models(plugins)
    fmap = plugins.field_map()
    inherited = {
        k: v
        for k, v in global_raw.items()
        if k in fmap and fmap[k].plugin.scope in (Scope.CELL, Scope.BOTH)
    }
    check_scope(cell_raw, Scope.CELL, plugins, where=f" (cell {name!r})")
    ov = _filter_overrides(overrides or {}, Scope.CELL, plugins)
    return _validate(cell_model, combine([inherited, cell_raw, ov], plugins), f"cell {name!r}")


def resolve_engine(cfg: BaseModel) -> str:
    """First-result decision: Engine answers for explicit @engine, Input infers
    from its own key; core default is duckdb."""
    return PLUGINS.first_result("resolve_engine", cfg) or "duckdb"


def resolve_sink_type(cfg: BaseModel) -> str:
    return PLUGINS.first_result("resolve_sink", cfg) or "parquet"
