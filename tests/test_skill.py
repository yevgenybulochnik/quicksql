"""The packaged agent skill (``src/quicksql/skill/``): a folder whose
references stay true to the code it ships with."""

import re
import shutil
from pathlib import Path

import pytest

import quicksql
from quicksql.bootstrap import builtin_registries, compile_file
from quicksql.models import Preview, RunResult
from quicksql.runner import run_project
from quicksql.vocabulary import directives_markdown

SKILL = Path(quicksql.__file__).parent / "skill"
LINK = re.compile(r"\]\(((?:references|examples)/[^)#]+)\)")


def text_of_all() -> str:
    return "\n".join(p.read_text() for p in sorted(SKILL.rglob("*.md")))


def test_the_skill_is_a_folder_led_by_skill_md() -> None:
    head = (SKILL / "SKILL.md").read_text()
    assert head.startswith("---\nname: quicksql\ndescription: ")
    assert len(head.splitlines()) < 150  # lean: the depth lives in references/


def test_every_link_resolves_and_every_file_is_linked_from_skill_md() -> None:
    linked = set(LINK.findall((SKILL / "SKILL.md").read_text()))
    for path in linked:
        assert (SKILL / path).is_file(), path
    files = {str(p.relative_to(SKILL)) for p in SKILL.rglob("*") if p.is_file()}
    assert files - {"SKILL.md"} == linked, "an orphan file, or a link to nothing"


def test_the_directives_reference_is_generated_from_the_builtins() -> None:
    committed = (SKILL / "references" / "directives.md").read_text()
    assert committed == directives_markdown(builtin_registries()), (
        "stale: regenerate with `uv run quicksql directives --markdown "
        "> src/quicksql/skill/references/directives.md`"
    )


def test_the_skill_names_every_live_command() -> None:
    from quicksql.cli import live_app

    text = text_of_all()
    for command in live_app.registered_commands:
        assert f"quicksql live {command.name}" in text, command.name


def test_the_engines_reference_names_every_builtin_engine_sink_and_reader() -> None:
    text = (SKILL / "references" / "engines-and-sinks.md").read_text()
    registries = builtin_registries()
    for name in [*registries.executors.names(), *registries.sinks.names()]:
        assert f"`{name}`" in text, name
    for reader in ("csv", "parquet", "json", "excel"):
        assert reader in text


def test_the_json_reference_names_every_field_a_result_carries() -> None:
    text = (SKILL / "references" / "json.md").read_text()
    result = RunResult(cell="a", status="ok", preview=Preview(("x",), ((1,),))).to_dict(rows=1)
    for key in [*result, "line", "line_end", "statement_sql"]:
        assert f'"{key}"' in text, key


@pytest.mark.parametrize("example", sorted(p.name for p in (SKILL / "examples").glob("*.qsql")))
def test_every_example_compiles_and_runs(example, tmp_path) -> None:
    nb = tmp_path / example
    shutil.copy(SKILL / "examples" / example, nb)
    results = run_project(compile_file(nb))
    assert results and all(r.ok for r in results), [(r.cell, r.error) for r in results]
