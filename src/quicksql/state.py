"""Cell state: fingerprints, and the state file that records what each cell's
landed output was built from.

A cell's fingerprint hashes what its result depends on: its rendered SQL,
engine and context, its config fields marked ``affects_results``, and the
*landed* fingerprints of its upstreams, those of the outputs it actually read.
Hashing the upstreams' current fingerprints instead would be wrong: an edited
``autorun: false`` cell hasn't landed anything new, so its downstream, which
read the old output, is still fresh.

Each notebook's state file, under ``~/.quicksql/state/`` (``$QSQL_HOME`` moves
the whole directory), keeps each cell's last run's status and what its landed
output was built from. It feeds ``quicksql status`` and the fingerprints of
later runs' downstream cells; nothing is ever skipped because of it (no result
caching). Losing it costs only ``status``'s memory, so keying it on the
notebook's path is safe: a moved notebook starts over as ``new``.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Collection, Mapping

from pydantic import BaseModel

from .models import RenderedCell

if TYPE_CHECKING:
    from .compiler import Project

HOME_ENV = "QSQL_HOME"  # not QUICKSQL_*: overrides.py reads those as config
_VERSION = 2  # 1 was one shared file next to the notebooks


def quicksql_home() -> Path:
    """quicksql's per-user directory: ``$QSQL_HOME``, else ``~/.quicksql``."""
    return Path(os.environ.get(HOME_ENV) or Path.home() / ".quicksql")


@dataclass(frozen=True)
class Landed:
    """What a landed output was built from: its fingerprint, and the parts
    that make it up, so status can say why it went stale."""

    fingerprint: str
    own: str
    upstreams: dict[str, str | None] = field(default_factory=dict)


@dataclass(frozen=True)
class CellState:
    """A cell's entry in the state file: its last run's status, and what its
    landed output (if any) was built from."""

    status: str | None
    landed: Landed | None


@dataclass(frozen=True)
class CellStatus:
    """One row of ``quicksql status``."""

    cell: str
    status: str  # ok | failed | skipped | stale | new
    reason: str
    fingerprint: str  # what the cell would be built from now
    landed: str | None  # what its landed output was built from


def _canonical(value: Any) -> Any:
    """JSON for what json can't encode. A set iterates in an order Python
    seeds per process, so it's sorted: status compares fingerprints across
    processes."""
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=repr)
    return str(value)


def _digest(payload: Any) -> str:
    text = json.dumps(payload, sort_keys=True, default=_canonical)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _affecting(config: BaseModel | None, skip: Collection[str] = ()) -> dict[str, Any]:
    """The config's fields that affect results (``qfield(affects_results=...)``,
    default true), less the names in ``skip``."""
    if config is None:
        return {}
    fields = type(config).model_fields
    out = {}
    for name, value in config.model_dump().items():
        extra = fields[name].json_schema_extra
        if name not in skip and (not isinstance(extra, dict) or extra.get("qsql_affects_results", True)):
            out[name] = value
    return out


def own_hash(cell: RenderedCell, global_config: BaseModel | None = None) -> str:
    """The cell's own inputs: its SQL, engine and context, and every config
    field that affects results. A header field that cells don't carry
    (global-only, like render_dir) can affect them all, so it counts too."""
    config = _affecting(cell.config)
    header = _affecting(global_config, skip=type(cell.config).model_fields if cell.config else {})
    payload = {"sql": cell.sql, "engine": cell.engine, "context": cell.context, "config": config}
    if header:
        payload["header"] = header
    return _digest(payload)


def fingerprint(own: str, upstreams: Mapping[str, str | None]) -> str:
    return _digest({"own": own, "upstreams": dict(upstreams)})


def cell_inputs(
    cell: RenderedCell, global_config: BaseModel | None, landed: Mapping[str, Landed | None]
) -> tuple[str, dict[str, str | None]]:
    """What the cell would be built from now: its own hash, and each
    upstream's landed fingerprint (None for one that never landed)."""
    upstreams = {}
    for name in sorted(cell.depends_on):
        record = landed.get(name)
        upstreams[name] = record.fingerprint if record else None
    return own_hash(cell, global_config), upstreams


def current_fingerprints(project: Project, landed: Mapping[str, Landed | None]) -> dict[str, str]:
    return {
        name: fingerprint(*cell_inputs(cell, project.config, landed))
        for name, cell in project.cells.items()
    }


# ---------- the state file ----------


def state_file(notebook: Path) -> Path:
    """The notebook's own file under the quicksql home, keyed on its resolved
    path so every spelling of it (``./x.qsql``, ``sub/../x.qsql``, a symlink)
    shares one state, and named after it so a person can tell which is which."""
    resolved = notebook.resolve()
    key = hashlib.sha256(str(resolved).encode()).hexdigest()[:12]
    return quicksql_home() / "state" / f"{resolved.name}-{key}.json"


def _read(path: Path) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(doc, dict) or doc.get("version") != _VERSION:
        return {}  # another format: start over rather than misread it
    return doc


def read_state(project: Project) -> dict[str, CellState]:
    """This notebook's cells as the state file has them; {} without one, or
    for a notebook compiled from text (it has no file to key on)."""
    if project.path is None:
        return {}
    entries = _read(state_file(project.path)).get("cells")
    if not isinstance(entries, dict):
        return {}
    out: dict[str, CellState] = {}
    for name, entry in entries.items():
        try:
            landed = Landed(**entry["landed"]) if entry.get("landed") else None
            out[name] = CellState(entry.get("status"), landed)
        except (TypeError, AttributeError, KeyError):
            continue  # a hand-edited entry: forget it
    return out


def write_state(project: Project, cells: Mapping[str, CellState]) -> None:
    """Replace the notebook's state file. The write is atomic, so a reader
    never sees half a file. The file records the notebook's path, so a later
    prune can find the files of notebooks that no longer exist."""
    if project.path is None:
        return
    path = state_file(project.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "version": _VERSION,
        "notebook": str(project.path.resolve()),
        "cells": {
            name: {"status": cell.status, "landed": asdict(cell.landed) if cell.landed else None}
            for name, cell in cells.items()
        },
    }
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


# ---------- status ----------


def _why_stale(built: Landed | None, own: str, upstreams: Mapping[str, str | None]) -> str:
    if built is None:
        return "nothing landed"
    if built.own != own:
        return "its SQL or config changed"
    changed = [name for name, fp in upstreams.items() if built.upstreams.get(name) != fp]
    if changed:
        noun = "upstream" if len(changed) == 1 else "upstreams"
        return f"{noun} {', '.join(map(repr, changed))} changed"
    return "its upstreams changed"  # one it read is no longer an upstream


def statuses(project: Project) -> list[CellStatus]:
    """Each cell's state against the state file: ok, failed or skipped (its
    last run), stale (its landed output was built from something else than
    the cell would be built from now), or new (never run)."""
    state = read_state(project)
    landed = {name: cell.landed for name, cell in state.items()}
    out = []
    for name in project.order:
        own, upstreams = cell_inputs(project.cells[name], project.config, landed)
        current = fingerprint(own, upstreams)
        entry = state.get(name)
        built = entry.landed if entry else None
        if entry is None or entry.status is None:
            status, reason = "new", "never run"
        elif entry.status != "ok":
            status, reason = entry.status, ""
        elif built is None or built.fingerprint != current:
            status, reason = "stale", _why_stale(built, own, upstreams)
        else:
            status, reason = "ok", ""
        out.append(CellStatus(name, status, reason, current, built.fingerprint if built else None))
    return out
