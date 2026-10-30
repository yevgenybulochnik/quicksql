"""Compile pipeline: parse -> resolve config -> render -> graph => Project."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .bootstrap import load_builtins
from .config import resolve_cell, resolve_engine, resolve_global, resolve_sink_type
from .errors import CellError, ConfigError, ConfigErrorGroup
from .executors.base import split_statements
from .graph import topo_sort
from .models import RawBlock, RenderedCell, RunResult
from .parser import body_hash, parse_text
from .registry import EXECUTORS, PLUGINS
from .render import Producer, render_sql
from .sinks import make_sink


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
    overrides: dict[str, Any] = field(default_factory=dict)
    # (1-indexed line, message) for non-fatal compile findings, e.g. a
    # directive key repeated within a block (later silently won)
    warnings: list[tuple[int, str]] = field(default_factory=list)

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
) -> Project:
    load_builtins()
    root = Path(root)
    overrides = overrides or {}
    where = str(path) if path else "<text>"
    blocks = parse_text(text)
    header, cell_blocks = blocks[0], blocks[1:]
    global_cfg = resolve_global(header.directives, overrides)

    # stage 1: resolve every cell's config, reporting all failures together
    configs: dict[str, Any] = {}
    errors: list[CellError] = []
    for blk in cell_blocks:
        try:
            configs[blk.name] = resolve_cell(
                header.directives, blk.directives, overrides, name=blk.name
            )
        except ConfigError as exc:
            errors.append(_cell_error(blk, exc))
    if errors:
        raise ConfigErrorGroup(where, errors)
    engines = {name: resolve_engine(cfg) for name, cfg in configs.items()}
    contexts = {
        name: EXECUTORS.get(engines[name]).context_key(cfg) for name, cfg in configs.items()
    }
    producers = {
        name: Producer(sink=make_sink(cfg, root), context=contexts[name])
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
                context=contexts[blk.name],
                supports_context_refs=EXECUTORS.get(engine).supports_context_refs,
            )
            deps: list[str] = []
            for plug in PLUGINS.overriding("collect_edges"):
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
            if engine != "duckdb" and (rec.external_refs or rec.used_source or extensions):
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
            sink_type=resolve_sink_type(cfg),
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
    for cell in cells.values():
        # an effect-only (sink 'none') cell lands nothing, so it can't be read back;
        # only ref()/source() edges read data — a plain @depends_on ordering edge is fine.
        for name in [*cell.context_refs, *cell.external_refs]:
            if cells[name].sink_type == "none":
                semantic.append(
                    CellError(cell.name, cell.line, f"cannot ref({name!r}): it uses sink "
                              "'none' and lands nothing")
                )
        # a reffed-in-context cell is materialized as a temp from its terminal
        # statement; engines with native scripting can't be split to isolate it.
        if (
            cell.reffed_in_context
            and not EXECUTORS.get(cell.engine).supports_multistatement_materialization
            and len(split_statements(cell.sql, cell.engine)) > 1
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
        overrides=overrides,
        warnings=warnings,
    )
    for plug in PLUGINS.overriding("after_compile"):
        plug.after_compile(project)  # a validation seam: raising rejects the compile
    return project


def compile_file(path: Path | str, overrides: dict[str, Any] | None = None) -> Project:
    p = Path(path)
    return compile_text(p.read_text(), root=p.parent, path=p, overrides=overrides)
