"""A notebook's vocabulary, generated from its registries: the directives it
can write, and the engines and sinks they can name."""

from pydantic import BaseModel

from quicksql.plugins import Plugin, qfield
from quicksql.vocabulary import directives, directives_markdown, engines, sinks


def test_every_builtin_directive_says_what_it_does(registries) -> None:
    rows = directives(registries)
    assert rows and all(d.description for d in rows), [d.key for d in rows if not d.description]


def test_a_directive_carries_scope_type_default_merge_and_whether_it_reruns(registries) -> None:
    rows = {d.key: d for d in directives(registries)}
    assert rows["depends_on"].scope == "cell" and rows["depends_on"].merge == "extend"
    assert rows["catalog"].scope == "global" and rows["vars"].scope == "both"
    assert rows["autorun"].type == "boolean" and rows["autorun"].default is True
    assert rows["autorun"].reruns is False and rows["output"].reruns is True
    assert rows["output"].default == {"type": "parquet", "dir": "data/"}
    assert rows["output"].merge == "deep" and rows["output"].type == "mapping"
    assert rows["engine"].type == "string or null" and rows["tags"].type == "list of strings"
    assert "schema" in rows and "schema_" not in rows  # the directive's key, not the field's
    assert rows["engine"].plugin == "engine" and rows["engine"].source == "builtin"


def test_a_plugins_directive_joins_the_vocabulary(registries, add_plugin) -> None:
    @add_plugin
    class Owner(Plugin):
        """Who owns the cell."""

        class Config(BaseModel):
            owner: str | None = qfield(None, description="The team that owns the cell.")

    row = next(d for d in directives(registries) if d.key == "owner")
    assert row.description == "The team that owns the cell."
    assert row.plugin == "owner" and row.source != "builtin"


def test_a_directive_without_a_description_falls_back_to_its_plugins_doc(
    registries, add_plugin
) -> None:
    @add_plugin
    class Team(Plugin):
        """Which team reviews the cell."""

        class Config(BaseModel):
            team: str | None = qfield(None)

    row = next(d for d in directives(registries) if d.key == "team")
    assert row.description == "Which team reviews the cell."


def test_engines_and_sinks(registries) -> None:
    by_name = {e["name"]: e for e in engines(registries)}
    assert {"duckdb", "sqlite", "postgres", "bigquery"} <= set(by_name)
    assert by_name["duckdb"]["dialect"] == "duckdb"
    assert by_name["postgres"]["input_shorthand"] == "dsn"
    landing = {s["name"]: s["lands_output"] for s in sinks(registries)}
    assert landing["parquet"] is True and landing["none"] is False


def test_the_markdown_is_a_table_row_per_directive(registries) -> None:
    md = directives_markdown(registries)
    for d in directives(registries):
        assert f"| `{d.key}` |" in md
    assert "quicksql directives" in md  # how to see a notebook's own
