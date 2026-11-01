"""Registries for every plugin kind, plus the decorators that mark a class as one.

Kinds: config/behavior plugins (``@plugin``), executors (``@executor``),
sinks (``@sink``), and source readers (``@source_reader``). A decorator only
marks its class; nothing registers by being imported. Each notebook gets its
own :class:`Registries`, built by the composition root
(:mod:`quicksql.bootstrap`) from the builtins plus the classes its
quicksqlrc.py and ``--plugins`` modules define, and carried by its project. So
two notebooks in one process (the TUI, the LSP) never share a registration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import ModuleType
from typing import TYPE_CHECKING, Any, Self

from .errors import ConfigError
from .models import Scope

if TYPE_CHECKING:
    from .plugins.base import Plugin


class Registry:
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self._items: dict[str, Any] = {}

    def register(self, name: str, obj: Any) -> None:
        if name in self._items:
            raise ConfigError(f"duplicate {self.kind} registration: {name!r}")
        self._items[name] = obj

    def get(self, name: str) -> Any:
        try:
            return self._items[name]
        except KeyError:
            have = ", ".join(self._items) or "none"
            raise ConfigError(f"unknown {self.kind}: {name} (registered: {have})") from None

    def names(self) -> list[str]:
        return list(self._items)

    def values(self) -> list[Any]:
        return list(self._items.values())

    def __contains__(self, name: object) -> bool:
        return name in self._items

    def remove(self, name: str) -> None:
        """Drop a registration, e.g. to prove a builtin owns its behavior."""
        self.get(name)  # unknown: say so
        del self._items[name]

    def copy(self) -> Self:
        out = type(self)(self.kind)
        out._items = dict(self._items)
        return out


@dataclass
class FieldEntry:
    """One config field: which plugin owns it and its pydantic FieldInfo."""

    plugin: Plugin
    field_name: str
    info: Any

    @property
    def key(self) -> str:
        return self.info.alias or self.field_name


def _config_fields(p: Plugin) -> dict[str, FieldEntry]:
    out: dict[str, FieldEntry] = {}
    for fname, info in type(p).Config.model_fields.items():
        entry = FieldEntry(plugin=p, field_name=fname, info=info)
        out[entry.key] = entry
    return out


class PluginRegistry(Registry):
    """Also derives the directive-key -> plugin map; duplicate fields are an error
    (pydantic would silently MRO-shadow them during model composition). Hook
    participant lists are computed once per registry state, not per call."""

    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self._hooks: dict[str, list[Plugin]] = {}
        self._chain: list[Plugin] | None = None

    def register(self, name: str, obj: Any) -> None:
        existing = self.field_map()
        for key in _config_fields(obj):
            if key in existing:
                raise ConfigError(
                    f"plugin {name!r} redefines config field {key!r} "
                    f"(already provided by plugin {existing[key].plugin.name!r})"
                )
        super().register(name, obj)
        self._forget_hooks()

    def remove(self, name: str) -> None:
        super().remove(name)
        self._forget_hooks()

    def _forget_hooks(self) -> None:
        self._hooks = {}
        self._chain = None

    def field_map(self) -> dict[str, FieldEntry]:
        out: dict[str, FieldEntry] = {}
        for p in self.values():
            out.update(_config_fields(p))
        return out

    def by_scope(self, *scopes: Scope) -> list[Plugin]:
        return [p for p in self.values() if p.scope in scopes]

    def overriding(self, method: str) -> list[Plugin]:
        """Plugins overriding a base hook, in (priority, registration) order."""
        if method not in self._hooks:
            from .plugins.base import Plugin

            base = getattr(Plugin, method)
            hooked = [
                (p.priority, i, p)
                for i, p in enumerate(self.values())
                if getattr(type(p), method) is not base
            ]
            self._hooks[method] = [p for _, _, p in sorted(hooked, key=lambda t: (t[0], t[1]))]
        return list(self._hooks[method])

    def first_result(self, method: str, *args: Any) -> Any:
        """Ask plugins in (priority, registration) order; first non-None wins."""
        for p in self.overriding(method):
            result = getattr(p, method)(*args)
            if result is not None:
                return result
        return None

    def chain(self) -> list[Plugin]:
        """Plugins participating in the execution chain, outermost first."""
        if self._chain is None:
            from .plugins.base import RUN_HOOKS, Plugin

            hooked = [
                (p.priority, i, p)
                for i, p in enumerate(self.values())
                if any(getattr(type(p), m) is not getattr(Plugin, m) for m in RUN_HOOKS)
            ]
            self._chain = [p for _, _, p in sorted(hooked, key=lambda t: (t[0], t[1]))]
        return list(self._chain)


# the attribute a decorator marks its class with: the kind it registers as
KIND = "__qsql_kind__"


class Registries:
    """One notebook's plugins, executors, sinks and source readers: what its
    directives mean, what its cells run on and land in, and how it reads files."""

    def __init__(self) -> None:
        self.plugins = PluginRegistry("plugin")
        self.executors = Registry("executor")
        self.sinks = Registry("sink")
        self.readers = Registry("source reader")

    def _of(self, kind: str) -> Registry:
        return {
            "plugin": self.plugins,
            "executor": self.executors,
            "sink": self.sinks,
            "source_reader": self.readers,
        }[kind]

    def add(self, *classes: type) -> Registries:
        """Register decorated classes by their kind: a plugin, executor or
        reader as an instance, a sink as its class (it's built per cell).
        Adding the class a name already holds does nothing, so loading a
        module twice is harmless; another class under that name is a
        duplicate. Returns self, to chain."""
        for cls in classes:
            kind = cls.__dict__.get(KIND)
            if kind is None:
                raise ConfigError(
                    f"{cls.__qualname__} isn't marked as a plugin, executor, sink or "
                    "source reader: decorate it with @plugin, @executor(name), "
                    "@sink(name) or @source_reader(name)"
                )
            registry = self._of(kind)
            if cls.name in registry:
                held = registry.get(cls.name)
                if (held if isinstance(held, type) else type(held)) is cls:
                    continue
            registry.register(cls.name, cls if kind == "sink" else cls())
        return self

    def copy(self) -> Registries:
        """Same registrations, own registries: adding to the copy leaves this
        one untouched. Instances are shared; plugins keep no per-notebook state."""
        out = Registries()
        out.plugins = self.plugins.copy()
        out.executors = self.executors.copy()
        out.sinks = self.sinks.copy()
        out.readers = self.readers.copy()
        return out


def marked(module: ModuleType) -> list[type]:
    """The decorated classes ``module`` defines (not the ones it imports), in
    definition order: what loading it as a plugin module registers."""
    return [
        obj
        for obj in vars(module).values()
        if isinstance(obj, type) and KIND in obj.__dict__ and obj.__module__ == module.__name__
    ]


def _snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def _mark(cls: type, kind: str, name: str) -> type:
    cls.name = name
    setattr(cls, KIND, kind)
    return cls


def plugin(cls: type) -> type:
    return _mark(cls, "plugin", getattr(cls, "name", "") or _snake(cls.__name__))


def executor(name: str):
    return lambda cls: _mark(cls, "executor", name)


def sink(name: str):
    return lambda cls: _mark(cls, "sink", name)


def source_reader(name: str):
    return lambda cls: _mark(cls, "source_reader", name)
