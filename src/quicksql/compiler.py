"""Compile pipeline: parse -> resolve config -> render -> graph => Project."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import conduit
from .config import make_sink, resolve_cell, resolve_engine, resolve_global, resolve_sink_type
from .errors import CellError, ConfigError, ConfigErrorGroup
from .executors.base import split_statements
from .graph import topo_sort
from .models import RawBlock, RenderedCell, RunResult, source_hash
from .parser import body_hash, parse_text
from .registry import Registries
from .render import Producer, render_sql


def _cell_error(blk: RawBlock, exc: ConfigError) -> CellError:
    """Strip the cell-name prefix/suffix our own messages carry — the group
    line already names the cell and its line."""
    msg = str(exc)
    for prefix in (f"invalid config for cell {blk.name!r}: ", f"cell {blk.name!r}: "):
        if msg.startswith(prefix):
            msg = msg[len(prefix) :]
            break
    return CellError(cell=blk.name or "<header>", line=blk.line, message=msg.replace(f" (cell {blk.name!r})", ""))


@dataclass
class Project:
    root: Path
    path: Path | None
    config: Any
    global_raw: dict[str, Any]
    cells: dict[str, RenderedCell]
    order: list[str]
    # the notebook's plugins, executors, sinks and readers: everything that
    # runs, watches or browses it reads them from here
    registries: Registries
    # the run overrides (--set, QUICKSQL_*) the project was compiled with;
    # quicksql itself doesn't read them back, an after_compile hook may
    overrides: dict[str, Any] = field(default_factory=dict)
    # (1-indexed line, message) for non-fatal compile findings, e.g. a
    # directive key repeated within a block (later silently won)
    warnings: list[tuple[int, str]] = field(default_factory=list)
    # models.source_hash of the text compiled: a live session reports it, so an
    # agent can tell which save a run or a compile error belongs to
    source_hash: str = ""

    @property
    def edges(self) -> dict[str, list[str]]:
        return {name: cell.depends_on for name, cell in self.cells.items()}

    def run(self, select: list[str] | None = None) -> list[RunResult]:
        from .runner import run_project

        return run_project(self, select=select)


def compile_text(
    text: str,
    root: Path | str = ".",
    path: Path | None = None,
    overrides: dict[str, Any] | None = None,
    *,
    registries: Registries,
) -> Project:
    """Compile notebook text with the notebook's registries, which the project
    then carries. Front-ends get them from :mod:`quicksql.bootstrap`."""
    root = Path(root)
    plugins, executors = registries.plugins, registries.executors
    overrides = overrides or {}
    where = str(path) if path else "<text>"
    blocks = parse_text(text)
    header, cell_blocks = blocks[0], blocks[1:]
    global_cfg = resolve_global(header.directives, overrides, registries=registries)

    # stage 1: resolve every cell's config, reporting all failures together
    configs: dict[str, Any] = {}
    errors: list[CellError] = []
    for blk in cell_blocks:
        try:
            configs[blk.name] = resolve_cell(
                header.directives, blk.directives, overrides, registries=registries, name=blk.name
            )
        except ConfigError as exc:
            errors.append(_cell_error(blk, exc))
    if errors:
        raise ConfigErrorGroup(where, errors)
    engines = {name: resolve_engine(cfg, plugins) for name, cfg in configs.items()}
    contexts = {
        name: executors.get(engines[name]).context_key(cfg) for name, cfg in configs.items()
    }
    producers = {
        name: Producer(sink=make_sink(cfg, root, registries), context=contexts[name])
        for name, cfg in configs.items()
    }

    # stage 2: render + wire every cell, again reporting all failures together
    cells: dict[str, RenderedCell] = {}
    for blk in cell_blocks:
        cfg = configs[blk.name]
        engine = engines[blk.name]
        try:
            sql, rec = render_sql(
                blk.name,
                blk.sql,
                cfg,
                producers,
                root,
                registries,
                context=contexts[blk.name],
                supports_context_refs=executors.get(engine).supports_context_refs,
            )
            deps: list[str] = []
            for plug in plugins.overriding("collect_edges"):
                for dep in plug.collect_edges(blk.name, cfg) or []:
                    if dep not in deps:
                        deps.append(dep)
            for dep in deps:
                if dep not in configs:
                    raise ConfigError(f"unknown cell {dep!r} in depends_on")
            for edge in rec.edges:
                if edge not in deps:
                    deps.append(edge)
            extensions = list(dict.fromkeys([*cfg.extensions, *rec.extensions]))
            # cross-context refs and file sources read through the duckdb
            # conduit; same-context refs stay in-engine and are exempt
            if engine != conduit.ENGINE and (rec.external_refs or rec.used_source or extensions):
                raise ConfigError(
                    f"uses cross-context ref()/source()/@extensions and must run on "
                    f"duckdb, not {engine!r} (duckdb is the cross-cell conduit)"
                )
        except ConfigError as exc:
            errors.append(_cell_error(blk, exc))
            continue
        cells[blk.name] = RenderedCell(
            name=blk.name,
            config=cfg,
            sql_raw=blk.sql,
            sql=sql,
            hash=body_hash(blk.sql),
            engine=engine,
            sink_type=resolve_sink_type(cfg, plugins),
            depends_on=deps,
            extensions=extensions,
            line=blk.line,
            line_end=blk.line_end,
            source=blk.source,
            context=contexts[blk.name],
            context_refs=list(rec.context_refs),
            external_refs=list(rec.external_refs),
        )
    if errors:
        raise ConfigErrorGroup(where, errors)
    referenced_in_context = {name for c in cells.values() for name in c.context_refs}
    for cell in cells.values():
        cell.reffed_in_context = cell.name in referenced_in_context

    # stage 3: cross-cell semantic checks that need every cell's sink/engine known
    semantic: list[CellError] = []
    on_conduit = conduit.conduit_target(global_cfg, root)
    for cell in cells.values():
        # duckdb cells all run on the conduit; a database of the cell's own would
        # be ignored, so name it in the header instead
        if cell.engine == conduit.ENGINE and conduit.conduit_target(cell.config, root) != on_conduit:
            executor = executors.get(conduit.ENGINE)
            own = executor.input_spec(cell.config).get("path") or ":memory:"
            header = executor.input_spec(global_cfg).get("path") or ":memory:"
            semantic.append(
                CellError(cell.name, cell.line, f"input.duckdb {own!r} isn't the conduit's "
                          f"({header!r}): every duckdb cell runs on the database the "
                          "header's input.duckdb names")
            )
        # an effect-only cell (its sink lands nothing, e.g. 'none') can't be read
        # back; only ref()/source() edges read data — a plain @depends_on ordering
        # edge is fine.
        for name in [*cell.context_refs, *cell.external_refs]:
            if not registries.sinks.get(cells[name].sink_type).lands_output:
                semantic.append(
                    CellError(cell.name, cell.line, f"cannot ref({name!r}): it uses sink "
                              f"{cells[name].sink_type!r} and lands nothing")
                )
        # a reffed-in-context cell is materialized as a temp from its terminal
        # statement; engines with native scripting can't be split to isolate it.
        if (
            cell.reffed_in_context
            and not executors.get(cell.engine).supports_multistatement_materialization
            # counted in its dialect, though the executor won't split it
            and len(split_statements(cell.sql, executors.get(cell.engine).dialect or "")) > 1
        ):
            semantic.append(
                CellError(cell.name, cell.line, f"a {cell.engine} cell referenced "
                          "in-context must be a single statement")
            )
    if semantic:
        raise ConfigErrorGroup(where, semantic)

    order = topo_sort(list(cells), {n: c.depends_on for n, c in cells.items()})
    warnings = [
        (
            later,
            f"directive {key!r} at line {later} replaces the one at line {earlier} "
            "(repeated within a block: later wins; merge strategies apply between "
            "global/cell/override layers, not repeated keys)",
        )
        for block in blocks
        for key, earlier, later in block.duplicates
    ]
    project = Project(
        root=root,
        path=path,
        config=global_cfg,
        global_raw=header.directives,
        cells=cells,
        order=order,
        registries=registries,
        overrides=overrides,
        warnings=warnings,
        source_hash=source_hash(text),
    )
    for plug in plugins.overriding("after_compile"):
        plug.after_compile(project)  # a validation seam: raising rejects the compile
    return project


def compile_file(
    path: Path | str, overrides: dict[str, Any] | None = None, *, registries: Registries
) -> Project:
    p = Path(path)
    return compile_text(
        p.read_text(), root=p.parent, path=p, overrides=overrides, registries=registries
    )
