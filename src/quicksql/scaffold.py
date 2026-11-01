"""The starter file `quicksql init` writes: runs offline, demonstrates the basics."""

from __future__ import annotations

from pathlib import Path

from .state import quicksql_home

BASE_SQL = """\
-- quicksql starter — one SQL file, cells split by `-- @cell`, config in @-comments.
-- Run it:      quicksql run
-- Inspect it:  quicksql list / quicksql show <cell> / quicksql tui
-- Watch it:    quicksql watch   (re-runs changed cells + their dependents on save)

-- @engine: duckdb
-- @output: { type: parquet, dir: data/ }
-- @autorun: true

/*@
vars: { active_only: true }
*/

-- @cell users
SELECT * FROM (VALUES
    (1, 'ada', true),
    (2, 'bob', false),
    (3, 'cyd', true)
) AS t(user_id, name, active);

-- @cell events
SELECT (range % 3 + 1)::INT AS user_id,
       'event_' || range::VARCHAR AS event
FROM range(10);

-- @cell active_user_events
-- @depends_on: [users]
SELECT u.name, e.event
FROM {{ ref('users') }} u
JOIN {{ ref('events') }} e USING (user_id)
{% if var('active_only') %}WHERE u.active{% endif %};

-- Things to try:
--   * point a cell at another engine:   /*@ input: { sqlite: legacy.db } */
--   * land a cell somewhere else:       -- @output: { type: duckdb, path: warehouse.db }
--   * read files:                       SELECT * FROM {{ source('seeds/users.csv') }}
--   * override at run time:             quicksql run --set output.type=duckdb
"""


# builtin starting templates offered by init and the TUI's notebook picker
TEMPLATES: dict[str, str] = {"base": BASE_SQL}


def user_templates_dir() -> Path:
    return quicksql_home() / "templates"


def user_templates() -> dict[str, Path]:
    """User-defined starting templates: ~/.quicksql/templates/*.qsql[.sql]
    (``$QSQL_HOME`` moves the directory), named by file stem. Missing
    directory just means none."""
    directory = user_templates_dir()
    if not directory.is_dir():
        return {}
    out: dict[str, Path] = {}
    for f in sorted(directory.glob("*.qsql")) + sorted(directory.glob("*.qsql.sql")):
        name = f.name
        for suffix in (".qsql.sql", ".qsql"):
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        out.setdefault(name, f)
    return out


def template_names() -> list[str]:
    """Builtins first, then user templates alphabetically."""
    return [*TEMPLATES, *sorted(user_templates())]


def template_content(name: str) -> str:
    if name in TEMPLATES:
        return TEMPLATES[name]
    paths = user_templates()
    if name in paths:
        return paths[name].read_text()
    raise KeyError(f"unknown template: {name!r}")


def write_scaffold(path: Path | str, template: str = "base") -> Path:
    """Write a starting template; refuses to overwrite an existing file."""
    path = Path(path)
    if path.exists():
        raise FileExistsError(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(template_content(template))
    return path
