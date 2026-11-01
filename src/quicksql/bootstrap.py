"""The composition root: which plugins, executors, sinks and source readers a
notebook gets.

The builtins come from one list here. A notebook's ``quicksqlrc.py`` and any
``--plugins`` modules add the classes they define to that notebook's
registries only, so notebooks in one process (the TUI, the LSP) never see each
other's directives. The core never imports this module: front-ends build a
notebook's registries here and hand them to the compiler, and the project
carries them from there.
"""

from __future__ import annotations

import hashlib
import importlib
import sys
from pathlib import Path
from types import ModuleType
from typing import Any, Sequence

from . import compiler, sources
from .compiler import Project
from .errors import QsqlError
from .executors import bigquery_exec, duckdb_exec, postgres_exec, sqlite_exec
from .plugins import builtin, emit_sql
from .registry import Registries, marked
from .sinks import duckdb_sink, none_sink, parquet_sink, postgres_sink

# The builtins, in registration order: hooks of equal priority run in it, and
# error messages list each kind's names in it.
BUILTIN_MODULES: tuple[ModuleType, ...] = (
    builtin,
    emit_sql,
    duckdb_exec,
    sqlite_exec,
    bigquery_exec,
    postgres_exec,
    parquet_sink,
    duckdb_sink,
    postgres_sink,
    none_sink,
    sources,
)

RC_NAME = "quicksqlrc.py"

_builtins: Registries | None = None


def builtin_registries() -> Registries:
    """A fresh copy of the builtin registries, the caller's to add to."""
    global _builtins
    if _builtins is None:  # built once; every caller gets its own copy
        _builtins = Registries().add(*(cls for m in BUILTIN_MODULES for cls in marked(m)))
    return _builtins.copy()


def load_plugin_module(spec: str) -> ModuleType:
    """Import a plugin module: a dotted name, or a path to a ``.py`` file.

    A file is keyed on its path and content, so an edited file loads anew in a
    long-lived process (the TUI, the LSP) while an unchanged one isn't run
    twice. It runs from the bytes that were hashed, never a ``__pycache__``
    entry: those are keyed on mtime and size, which an edit within the same
    second can leave unchanged. A file that fails to run isn't kept: fix it
    and it loads."""
    if not spec.endswith(".py"):
        return importlib.import_module(spec)
    path = Path(spec).resolve()
    try:
        source = path.read_bytes()
    except OSError as exc:
        raise QsqlError(f"cannot load plugin module {spec!r}: {exc}") from exc
    digest = hashlib.sha256(str(path).encode() + b"\0" + source).hexdigest()[:12]
    name = f"qsql_user_{path.stem}_{digest}"
    if name in sys.modules:
        return sys.modules[name]
    module = ModuleType(name)
    module.__file__ = str(path)
    sys.modules[name] = module  # pydantic resolves annotations through it
    try:
        exec(compile(source, str(path), "exec"), module.__dict__)
    except BaseException:
        del sys.modules[name]
        raise
    return module


def registries_for(notebook: Path | str | None, plugins: Sequence[str] = ()) -> Registries:
    """A notebook's registries: the builtins, then the classes the
    ``quicksqlrc.py`` next to it defines, then those of each ``--plugins``
    module, in order."""
    registries = builtin_registries()
    specs: list[str] = []
    if notebook is not None:
        rc = Path(notebook).parent / RC_NAME
        if rc.is_file():
            specs.append(str(rc))
    specs.extend(plugins)
    for spec in specs:
        registries.add(*marked(load_plugin_module(spec)))
    return registries


def compile_file(
    path: Path | str,
    overrides: dict[str, Any] | None = None,
    plugins: Sequence[str] = (),
    registries: Registries | None = None,
) -> Project:
    """Compile a notebook with its own registries (``registries_for``), or the
    ones given."""
    if registries is None:
        registries = registries_for(path, plugins)
    return compiler.compile_file(path, overrides, registries=registries)


def compile_text(
    text: str,
    root: Path | str = ".",
    path: Path | None = None,
    overrides: dict[str, Any] | None = None,
    registries: Registries | None = None,
) -> Project:
    """Compile notebook text with the builtins, or the registries given. Text
    has no notebook file, so no ``quicksqlrc.py`` loads (the LSP's case)."""
    return compiler.compile_text(
        text, root, path, overrides, registries=registries or builtin_registries()
    )
