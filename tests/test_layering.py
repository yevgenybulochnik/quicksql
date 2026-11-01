"""The layering gate: pyproject.toml's import-linter contracts hold on every pytest run."""

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"


def _contracts() -> dict[str, dict]:
    config = tomllib.loads(PYPROJECT.read_text())["tool"]["importlinter"]
    return {c["id"]: c for c in config["contracts"]}


def test_import_contracts_hold() -> None:
    # a subprocess: in-process lint_imports edits sys.path and reconfigures logging
    lint = Path(sys.executable).with_name("lint-imports")
    proc = subprocess.run(
        [str(lint), "--no-cache", "--no-logo", "--config", str(PYPROJECT)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        report = proc.stdout + proc.stderr
        pytest.fail(f"lint-imports failed (contracts: pyproject.toml [tool.importlinter])\n{report}", pytrace=False)


def test_every_module_is_classified() -> None:
    """An unlisted module escapes both contracts, so a new module must join a layer."""
    contracts = _contracts()
    core, extensions = contracts["core"], contracts["extensions"]
    front_ends = set(extensions["forbidden_modules"]) - set(core["source_modules"])
    assert set(core["forbidden_modules"]) == set(extensions["source_modules"]) | front_ends, (
        "the core and extensions contracts list different extensions or front-ends"
    )

    listed = core["source_modules"] + core["forbidden_modules"]
    src = ROOT / "src"
    modules = {".".join(p.relative_to(src).with_suffix("").parts) for p in src.rglob("*.py")}
    modules = {m.removesuffix(".__init__") for m in modules}
    unlisted = sorted(
        m for m in modules if not any(m == e or (e.endswith(".**") and m.startswith(e[:-2])) for e in listed)
    )
    assert not unlisted, f"classify in pyproject.toml [tool.importlinter]: {unlisted}"


def _imported_privates(path: Path, module: str) -> list[str]:
    """``module.name`` for each underscored name ``path`` imports from another
    quicksql module."""
    import ast

    package = module if path.name == "__init__.py" else module.rpartition(".")[0]
    found = []
    for node in ast.walk(ast.parse(path.read_text())):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:
            parts = package.split(".")
            source = ".".join(parts[: len(parts) - node.level + 1] + [node.module or ""]).rstrip(".")
        else:
            source = node.module or ""
        if source.split(".")[0] != "quicksql" or source == module:
            continue
        found += [f"{source}.{alias.name}" for alias in node.names if alias.name.startswith("_")]
    return found


def test_no_module_imports_another_modules_privates() -> None:
    """import-linter sees modules, not names: a front-end reaching into the
    runner's privates is front-end -> core, which the contracts allow. A
    private name is its module's own; anything shared gets a public one."""
    src = ROOT / "src"
    leaks = [
        f"{path.relative_to(ROOT)}: {name}"
        for path in sorted(src.rglob("*.py"))
        for name in _imported_privates(
            path, ".".join(path.relative_to(src).with_suffix("").parts).removesuffix(".__init__")
        )
    ]
    assert not leaks, "\n".join(leaks)
