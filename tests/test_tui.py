import re

import polars as pl
import pytest
from textual._compositor import CompositorUpdate
from textual.geometry import Region
from textual.widgets import DataTable, TabbedContent

from quicksql.bootstrap import compile_file
from quicksql.scaffold import write_scaffold
from quicksql.tui import QsqlApp


@pytest.fixture
def notebook(tmp_path):
    return write_scaffold(tmp_path / "base.qsql")


async def test_missing_file_shows_picker_and_template_creates(tmp_path) -> None:
    from quicksql.tui import NotebookPicker

    target = tmp_path / "base.qsql"
    app = QsqlApp(path=target, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        await pilot.press("enter")  # sole option: create from the template
        await pilot.pause()
        await pilot.press("enter")  # accept the prefilled name (base.qsql)
        await pilot.pause()
        assert target.exists()
        assert app.project and "users" in app.project.cells
        assert app.results == {}  # picker never runs anything


async def test_missing_file_picker_lists_existing_notebooks(tmp_path) -> None:
    from quicksql.tui import NotebookPicker

    write_scaffold(tmp_path / "other.qsql")
    app = QsqlApp(path=tmp_path / "base.qsql", watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        await pilot.press("enter")  # first option: open other.qsql
        await pilot.pause()
        assert app.path.name == "other.qsql"
        assert app.project
        assert not (tmp_path / "base.qsql").exists()  # nothing was scaffolded


async def test_picker_lists_qsql_and_qsql_sql_but_ignores_plain_sql(tmp_path) -> None:
    from textual.widgets import OptionList

    from quicksql.tui import NotebookPicker

    (tmp_path / "pipeline.qsql").write_text("-- @cell a\nSELECT 1;\n")
    (tmp_path / "legacy.qsql.sql").write_text("-- @cell b\nSELECT 2;\n")
    (tmp_path / "schema_dump.sql").write_text("CREATE TABLE noise (id INT);\n")
    app = QsqlApp(path=tmp_path / "base.qsql", watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        ol = app.screen.query_one(OptionList)
        prompts = [str(ol.get_option_at_index(i).prompt) for i in range(ol.option_count)]
        assert any("pipeline.qsql" in p for p in prompts)
        assert any("legacy.qsql.sql" in p for p in prompts)
        assert not any("schema_dump.sql" in p for p in prompts)


async def test_user_templates_offered_and_seed_the_requested_file(tmp_path, qsql_home) -> None:
    from textual.widgets import OptionList

    (qsql_home / "templates").mkdir()
    (qsql_home / "templates" / "metrics.qsql").write_text(
        "-- @cell tpl\nSELECT 7 AS seven;\n"
    )
    workdir = tmp_path / "proj"
    workdir.mkdir()
    app = QsqlApp(path=workdir / "base.qsql", watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        ol = app.screen.query_one(OptionList)
        prompts = [str(ol.get_option_at_index(i).prompt) for i in range(ol.option_count)]
        assert any("template 'base'" in p for p in prompts)
        assert any("template 'metrics'" in p for p in prompts)
        await pilot.press("down", "enter")  # base first, metrics second
        await pilot.pause()
        await pilot.press("enter")  # accept the prefilled name (the requested file)
        await pilot.pause()
        # the user template seeded the *requested* filename
        assert (workdir / "base.qsql").read_text().startswith("-- @cell tpl")
        assert list(app.project.cells) == ["tpl"]


async def test_o_switches_between_notebooks_and_resets_state(tmp_path) -> None:
    from quicksql.tui import NotebookPicker

    alpha = write_scaffold(tmp_path / "alpha.qsql")
    beta = tmp_path / "beta.qsql"
    beta.write_text("-- @cell solo\nSELECT 1 AS x;\n")
    app = QsqlApp(path=alpha, watch=False)
    async with app.run_test() as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.results and app.armed
        await pilot.press("o")
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        await pilot.press("down", "enter")  # alpha, [beta], create base.qsql
        await pilot.pause()
        assert app.path == beta
        assert list(app.project.cells) == ["solo"]
        assert app.results == {}      # fresh notebook, fresh state
        assert app.armed is False     # arming is per-notebook


async def test_the_tui_loads_the_notebooks_rc_at_startup_and_on_o(tmp_path) -> None:
    from quicksql.tui import NotebookPicker

    (tmp_path / "quicksqlrc.py").write_text(
        "from pydantic import BaseModel\n"
        "from quicksql.plugins import Plugin, qfield\n"
        "from quicksql.registry import plugin\n\n\n"
        "@plugin\n"
        "class Sparkle(Plugin):\n"
        "    class Config(BaseModel):\n"
        "        sparkle: int = qfield(0)\n"
    )
    alpha = tmp_path / "alpha.qsql"
    alpha.write_text("-- @sparkle: 1\n-- @cell a\nSELECT 1 AS x;\n")
    beta = tmp_path / "beta.qsql"
    beta.write_text("-- @sparkle: 2\n-- @cell b\nSELECT 2 AS y;\n")
    app = QsqlApp(path=alpha, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.project is not None and list(app.project.cells) == ["a"]
        await pilot.press("o")
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        await pilot.press("down", "enter")  # alpha, [beta]
        await pilot.pause()
        assert app.path == beta
        assert app.project is not None and list(app.project.cells) == ["b"]


async def test_template_create_prompts_for_name_and_creates_named_file(tmp_path) -> None:
    from textual.widgets import Input

    from quicksql.tui import NotebookPicker

    alpha = write_scaffold(tmp_path / "alpha.qsql")
    app = QsqlApp(path=alpha, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("o")
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        await pilot.press("j", "enter")  # past `open alpha` onto the create option
        await pilot.pause()
        name_input = app.screen.query_one("#picker_name", Input)
        assert name_input.display  # the prompt appeared...
        assert name_input.value == "base.qsql"  # ...prefilled with the default
        name_input.value = "custom"  # rename; missing suffix gets appended
        await pilot.press("enter")
        await pilot.pause()
        assert (tmp_path / "custom.qsql").exists()
        assert app.path.name == "custom.qsql"
        assert app.project and "users" in app.project.cells


async def test_template_name_collision_warns_and_stays(tmp_path) -> None:
    from textual.widgets import Input, Static

    from quicksql.tui import NotebookPicker

    alpha = write_scaffold(tmp_path / "alpha.qsql")
    app = QsqlApp(path=alpha, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("o")
        await pilot.pause()
        await pilot.press("j", "enter")  # the create option
        await pilot.pause()
        name_input = app.screen.query_one("#picker_name", Input)
        name_input.value = "alpha"  # alpha.qsql already exists
        await pilot.press("enter")
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)  # still up
        status = str(app.screen.query_one("#picker_status", Static).render())
        assert "exists" in status
        name_input.value = "fresh.qsql"  # a free name goes through
        await pilot.press("enter")
        await pilot.pause()
        assert (tmp_path / "fresh.qsql").exists()
        assert app.path.name == "fresh.qsql"


async def test_template_prompt_escape_returns_to_list_not_cancel(tmp_path) -> None:
    from textual.widgets import Input

    from quicksql.tui import NotebookPicker

    alpha = write_scaffold(tmp_path / "alpha.qsql")
    app = QsqlApp(path=alpha, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("o")
        await pilot.pause()
        await pilot.press("j", "enter")  # the create option -> name prompt
        await pilot.pause()
        assert app.screen.query_one("#picker_name", Input).display
        await pilot.press("escape")  # back to the list, picker stays
        await pilot.pause()
        assert isinstance(app.screen, NotebookPicker)
        assert not app.screen.query_one("#picker_name", Input).display
        await pilot.press("escape")  # now it cancels
        await pilot.pause()
        assert not isinstance(app.screen, NotebookPicker)
        assert app.path == alpha  # nothing created, notebook unchanged


async def test_switch_picker_offers_templates_even_when_default_name_taken(tmp_path) -> None:
    from textual.widgets import OptionList

    base = write_scaffold(tmp_path / "base.qsql")  # the default target is taken
    app = QsqlApp(path=base, watch=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("o")
        await pilot.pause()
        ol = app.screen.query_one(OptionList)
        prompts = [str(ol.get_option_at_index(i).prompt) for i in range(ol.option_count)]
        # naming makes the collision solvable, so the template stays on offer
        assert any("template 'base'" in p for p in prompts)


async def test_boots_lists_cells_and_navigates(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        table = app.query_one("#cells", DataTable)
        assert table.row_count == 3
        assert app.current_cell == "users"
        await pilot.press("j")
        assert app.current_cell == "events"
        await pilot.press("k")
        assert app.current_cell == "users"


async def test_toggle_raw_rendered_and_autorun(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        assert app.show_rendered is True
        await pilot.press("t")
        assert app.show_rendered is False
        await pilot.press("a")
        assert "users" in app.autorun_off
        await pilot.press("a")
        assert "users" not in app.autorun_off
        await pilot.press("A")
        assert app.autorun_global is False


async def test_activating_data_tab_loads_current_cell_sheet(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        tabs = app.query_one(TabbedContent)
        tabs.active = "tab_data"  # click the tab instead of pressing Enter
        await pilot.pause()
        assert app.mode == "cells"  # browsing a tab is not a dive
        assert app.sheet_stack and app.sheet_stack[-1].title == "users"
        assert app.query_one("#data", DataTable).row_count > 0
        tabs.active = "tab_sql"
        await pilot.pause()
        assert app.mode == "cells"
        assert not app.sheet_stack


async def test_h_l_cycle_tabs_and_data_follows_selection(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        tabs = app.query_one(TabbedContent)
        assert tabs.active == "tab_sql"
        await pilot.press("l")
        assert tabs.active == "tab_data"
        assert app.sheet_stack[-1].title == "users"
        await pilot.press("j")  # j/k still move the cell selection
        assert app.current_cell == "events"
        assert app.sheet_stack[-1].title == "events"  # Data pane follows
        await pilot.press("l", "l")
        assert tabs.active == "tab_log"
        await pilot.press("l")  # onto the Catalog tab (still browsing)
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert tabs.active == "tab_catalog"
        await pilot.press("l")  # wraps around
        assert tabs.active == "tab_sql"
        await pilot.press("h")
        assert tabs.active == "tab_catalog"
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("h")
        assert tabs.active == "tab_log"


async def test_detail_pane_fills_remaining_height(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        detail = app.query_one("#detail")
        cells = app.query_one("#cells")
        assert cells.region.height <= 12
        assert detail.region.height >= 20  # fills the rest, never collapses


async def test_watch_recompile_with_new_cell_runs_it(notebook) -> None:
    # regression: filtering the rerun plan against the *old* project raised
    # KeyError for a freshly added cell and crashed the watch worker
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        await pilot.press("R")  # arm autorun
        await app.workers.wait_for_complete()
        await pilot.pause()
        notebook.write_text(notebook.read_text() + "\n-- @cell fresh\nSELECT 1 AS z;\n")
        to_run = app._on_recompiled(compile_file(notebook))
        assert "fresh" in to_run
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.results["fresh"].ok


async def test_no_initial_run_until_run_all(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False)  # defaults: nothing runs on start
    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.results == {}
        assert app.armed is False
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.armed is True
        assert app.results["users"].ok


async def test_watch_reruns_gated_until_armed(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False)
    async with app.run_test() as pilot:
        notebook.write_text(notebook.read_text().replace("range(10)", "range(5)"))
        assert app._on_recompiled(compile_file(notebook)) == []  # adopt, don't run
        assert app.results == {}
        assert app.project.cells["events"].sql_raw.count("range(5)")  # view refreshed
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        notebook.write_text(notebook.read_text().replace("range(5)", "range(7)"))
        to_run = app._on_recompiled(compile_file(notebook))
        assert "events" in to_run  # armed: changes rerun again
        await app.workers.wait_for_complete()


async def test_single_cell_run_does_not_arm_autorun(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False)
    async with app.run_test() as pilot:
        await pilot.press("r")  # run just the selected cell
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.results["users"].ok
        assert app.armed is False  # run-all is the explicit gate


async def test_p_arms_autorun_without_running(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False)
    async with app.run_test() as pilot:
        assert app.armed is False
        await pilot.press("p")  # arm watch reruns, but run nothing
        assert app.armed is True
        assert app.results == {}  # arming ran no cells (the whole point)
        # a save is reactive now, even though run-all never happened
        notebook.write_text(notebook.read_text().replace("range(10)", "range(5)"))
        assert "events" in app._on_recompiled(compile_file(notebook))
        await app.workers.wait_for_complete()
        await pilot.press("p")  # toggles back off
        assert app.armed is False


def test_preview_frame_survives_interval_parquet(tmp_path) -> None:
    # duckdb round-trips INTERVAL parquet, but polars' reader panics on it;
    # the cold-start preview must go through duckdb instead
    import duckdb

    f = write_scaffold(tmp_path / "base.sql")
    app = QsqlApp(path=f, watch=False, auto_run=False)
    app.project = compile_file(f)
    (tmp_path / "data").mkdir()
    con = duckdb.connect()
    con.execute(
        f"COPY (SELECT INTERVAL 3 DAY AS iv, 1 AS n) TO '{tmp_path}/data/users.parquet' (FORMAT PARQUET)"
    )
    con.close()
    frame = app._preview_frame("users")
    assert frame.height == 1
    assert frame["iv"].dtype == pl.Duration("us")


def test_preview_frame_reads_head_only(tmp_path) -> None:
    # landed files can be huge; the fallback must never load them fully
    f = write_scaffold(tmp_path / "base.sql")
    app = QsqlApp(path=f, watch=False, auto_run=False)
    app.project = compile_file(f)
    (tmp_path / "data").mkdir()
    pl.DataFrame({"n": range(50_000)}).write_parquet(tmp_path / "data" / "users.parquet")
    frame = app._preview_frame("users")
    assert frame.height <= app.PREVIEW_ROWS


def _cold_app(tmp_path, text: str) -> QsqlApp:
    """A TUI on ``text`` that has run nothing this session."""
    f = tmp_path / "base.qsql"
    f.write_text(text)
    app = QsqlApp(path=f, watch=False, auto_run=False)
    app.project = compile_file(f)
    return app


def test_preview_frame_falls_back_to_the_cells_own_sink(tmp_path) -> None:
    # a previous session's output, read where the cell's sink lands it — not a
    # data/<cell>.parquet left over from before output.dir changed
    app = _cold_app(
        tmp_path, "-- @output: { type: parquet, dir: out/ }\n-- @cell users\nSELECT 1 AS n;\n"
    )
    for folder, n in (("data", 0), ("out", 7)):
        (tmp_path / folder).mkdir()
        pl.DataFrame({"n": [n]}).write_parquet(tmp_path / folder / "users.parquet")
    assert app._preview_frame("users")["n"].to_list() == [7]


def test_preview_frame_reads_a_database_sink_the_live_conduit_attached(tmp_path) -> None:
    import duckdb

    from quicksql.runner import run_project

    con = duckdb.connect(str(tmp_path / "wh.db"))  # b landed in a previous session
    con.execute("CREATE TABLE b AS SELECT 7 AS n")
    con.close()
    app = _cold_app(
        tmp_path,
        "-- @output: { type: duckdb, path: wh.db }\n"
        "-- @cell a\nSELECT 1 AS n;\n-- @cell b\n-- @autorun: false\nSELECT 2 AS n;\n",
    )
    run_project(app.project, select=["a"], session=app.session)  # attaches wh.db
    try:
        assert app._preview_frame("b")["n"].to_list() == [7]
    finally:
        app.session.close()


def test_preview_frame_never_attaches_a_database_on_its_own(tmp_path) -> None:
    # previews follow the cursor: ATTACH would create the missing database file,
    # and a Postgres sink would connect to its server on every cursor move
    app = _cold_app(
        tmp_path, "-- @cell users\n-- @output: { type: duckdb, path: wh.db }\nSELECT 1 AS n;\n"
    )
    frame = app._preview_frame("users")
    assert frame["info"].to_list() == ["no output for 'users' yet — press r to run"]
    assert not (tmp_path / "wh.db").exists()


def test_preview_frame_says_an_effect_only_cell_lands_nothing(tmp_path) -> None:
    app = _cold_app(
        tmp_path, "-- @cell setup\n-- @output: { type: none }\nCREATE TABLE t AS SELECT 1;\n"
    )
    assert app._preview_frame("setup")["info"].to_list() == [
        "'setup' is effect-only (output: none): it lands nothing to preview"
    ]


async def test_data_table_not_rebuilt_on_cursor_moves(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive
        table = app.query_one("#data", DataTable)
        rebuilds: list[int] = []
        original = table.clear
        table.clear = lambda *a, **kw: (rebuilds.append(1), original(*a, **kw))[1]
        await pilot.press("j", "l", "j", "k", "h")
        assert not rebuilds  # cursor-only moves reuse the rendered table
        await pilot.press("]")  # sort changes content
        assert rebuilds


async def test_selection_toggle_updates_marker_without_rebuild(tmp_path) -> None:
    # toggling a row's selection must not clear+rebuild the table. The rebuild
    # sets Textual's _require_update_dimensions, which defers the cursor
    # re-scroll by a frame and makes a scrolled viewport visibly jump. A
    # selection change only flips one row's marker cell, so update it in place.
    from textual.coordinate import Coordinate

    nb = tmp_path / "tall.qsql"
    nb.write_text("-- @engine: duckdb\n-- @cell big\nSELECT range AS n FROM range(80);\n")
    app = QsqlApp(path=nb, watch=False, auto_run=False)
    async with app.run_test(size=(80, 16)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive into the data sheet
        await pilot.pause()
        table = app.query_one("#data", DataTable)

        # scroll deep, then move the cursor into the visible window: a spurious
        # re-scroll would be plainly visible this far down the sheet
        table.scroll_to(y=table.max_scroll_y, animate=False)
        await pilot.pause()
        await pilot.press("j")
        row = app.sheet_stack[-1].cursor[0]
        assert row > 1

        rebuilds: list[int] = []
        original = table.clear
        table.clear = lambda *a, **kw: (rebuilds.append(1), original(*a, **kw))[1]

        await pilot.press("s")  # toggle-select the current row
        await pilot.pause()
        assert not rebuilds  # selection reuses the rendered table (no clear)
        assert row in app.sheet_stack[-1].selected
        assert str(table.get_cell_at(Coordinate(row, 0))).startswith("▸")

        await pilot.press("s")  # toggle back off — still no rebuild, marker gone
        await pilot.pause()
        assert not rebuilds
        assert row not in app.sheet_stack[-1].selected
        assert not str(table.get_cell_at(Coordinate(row, 0))).startswith("▸")


async def test_keyboard_move_resumes_from_viewport_after_mouse_scroll(tmp_path) -> None:
    # the mouse wheel scrolls the data table's viewport but not the Sheet
    # cursor; a following j/k must resume from what's on screen, not snap the
    # view back to the stale cursor near the top of the sheet
    nb = tmp_path / "tall.qsql"
    nb.write_text("-- @engine: duckdb\n-- @cell big\nSELECT range AS n FROM range(80);\n")
    app = QsqlApp(path=nb, watch=False, auto_run=False)
    async with app.run_test(size=(80, 16)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive into the data sheet
        await pilot.pause()
        table = app.query_one("#data", DataTable)
        assert app.sheet_stack[-1].cursor[0] == 0

        # simulate a mouse-wheel scroll to the bottom: viewport moves, cursor doesn't
        table.scroll_to(y=table.max_scroll_y, animate=False)
        await pilot.pause()
        first = int(table.scroll_y)
        assert first > 1  # really scrolled away from the row-0 cursor
        assert app.sheet_stack[-1].cursor[0] == 0

        await pilot.press("k")  # up from the visible window, not from row 0
        assert app.sheet_stack[-1].cursor[0] == first - 1

        # j likewise resumes from the visible window
        table.scroll_to(y=table.max_scroll_y, animate=False)
        await pilot.pause()
        first = int(table.scroll_y)
        await pilot.press("j")
        assert app.sheet_stack[-1].cursor[0] == first + 1


async def test_wide_frames_render_a_column_window(tmp_path) -> None:
    cols = ", ".join(f"{i} AS c{i}" for i in range(60))
    f = tmp_path / "wide.sql"
    f.write_text(f"-- @cell wide\nSELECT {cols};\n")
    app = QsqlApp(path=f, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")
        table = app.query_one("#data", DataTable)
        assert len(table.columns) == app.MAX_DATA_COLS
        assert f"cols 1-{app.MAX_DATA_COLS}/60" in app.sub_title
        app.sheet_stack[-1] = app.sheet_stack[-1].move(0, 59)  # jump to last column
        app.data.refresh()
        assert f"cols {60 - app.MAX_DATA_COLS + 1}-60/60" in app.sub_title


async def test_catalog_browser_opens_drills_and_pops(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.mode == "catalog"
        top = app.sheet_stack[-1]
        assert top.title == "catalog"
        assert top.drill is not None
        kinds = top.frame["kind"].to_list()
        assert kinds.count("output") == 3  # one per scaffold cell
        await pilot.press("j")  # onto the first output row (one duckdb context)
        assert app.sheet_stack[-1].frame.row(1, named=True)["kind"] == "output"
        await pilot.press("enter")  # drill into the cell's field paths
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2
        leaf = app.sheet_stack[-1]
        assert leaf.frame.columns == ["column", "field_path", "type", "mode", "description"]
        assert leaf.frame.height > 0
        await pilot.press("enter")  # leaves don't drill further
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2
        await pilot.press("q")  # pop back to the catalog
        assert len(app.sheet_stack) == 1
        assert app.sheet_stack[-1].title == "catalog"
        await pilot.press("q")
        assert app.mode == "cells"


async def test_enter_drills_even_when_the_cells_table_lost_focus(notebook) -> None:
    """A Tab press (or a mouse click on the tab bar) moves focus off the
    #cells DataTable; every other key kept working because it's handled at
    app level, but Enter rode on the table's RowSelected event and silently
    died — no drill, no error. Enter must behave the same regardless of
    which widget has focus."""
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.sheet_stack[-1].title == "catalog"
        await pilot.press("j")  # onto the first output row
        await pilot.press("tab")  # focus wanders off the cells table
        assert not app.query_one("#cells", DataTable).has_focus
        await pilot.press("enter")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2  # the drill still happened
        assert app.sheet_stack[-1].frame.columns == [
            "column", "field_path", "type", "mode", "description",
        ]


async def test_catalog_cache_serves_redrills_and_ctrl_r_refetches(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("j", "enter")  # field paths of users, now cached
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2
        (notebook.parent / "data" / "users.parquet").unlink()
        await pilot.press("q", "enter")  # re-drill: cache serves despite the missing file
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2
        assert app.sheet_stack[-1].frame.columns == ["column", "field_path", "type", "mode", "description"]
        await pilot.press("ctrl+r")  # invalidate + refetch: the reload now fails
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 2  # the stale sheet stays visible
        await pilot.press("q", "enter")  # entry really gone: fresh drill fails too
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 1


async def test_run_and_notebook_switch_clear_the_catalog_cache(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app._catalog_cache) > 0
        await pilot.press("q", "R")  # back to cells, rerun: outputs changed
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app._catalog_cache) == 0
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app._catalog_cache) > 0
        app._load_notebook(notebook)  # switching notebooks resets everything
        assert len(app._catalog_cache) == 0


async def test_catalog_load_error_clears_loading_subtitle(notebook) -> None:
    # drilling into an output that never ran fails (no parquet yet); the
    # "loading …" subtitle must not stick around after the error toast
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("j", "enter")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.sheet_stack) == 1  # nothing was pushed
        assert not app.sub_title.startswith("loading")


async def test_the_data_sheet_follows_a_rerun_of_its_cell(notebook) -> None:
    """A watch rerun of the cell the Data tab shows refreshes its preview;
    sheets derived from it (freq, a committed filter) stay as they were."""
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")  # runs all and arms watch reruns
        await _settle(app, pilot)
        await pilot.press("j", "enter")  # dive into events: 10 rows
        assert app.sheet_stack[-1].frame.height == 10
        await pilot.press("F")
        freq = app.sheet_stack[-1]
        notebook.write_text(notebook.read_text().replace("range(10)", "range(4)"))
        assert "events" in app._on_recompiled(compile_file(notebook))
        await _settle(app, pilot)
        assert app.sheet_stack[-1] is freq  # a derived sheet is a snapshot
        assert app.sheet_stack[0].frame.height == 4
        await pilot.press("q")
        assert app.sheet_stack[-1].frame.height == 4
        assert app.query_one("#data", DataTable).row_count == 4


async def test_the_browsed_data_sheet_follows_a_rerun(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        await pilot.press("j", "2")  # browse events on the Data tab
        assert app.mode == "cells" and app.sheet_stack[-1].frame.height == 10
        notebook.write_text(notebook.read_text().replace("range(10)", "range(4)"))
        app._on_recompiled(compile_file(notebook))
        await _settle(app, pilot)
        assert app.sheet_stack[-1].frame.height == 4


async def test_a_run_leaves_a_dived_sheets_subtitle_alone(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        await pilot.press("enter")  # dive into users' Data sheet
        shown = app.sub_title
        assert shown.startswith("users · 3x")
        await pilot.press("r")  # rerun the cell under the dive
        await _settle(app, pilot)
        assert app.sub_title == shown


async def test_a_recompile_leaves_a_catalog_dives_subtitle_alone(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("S")
        await _settle(app, pilot)
        shown = app.sub_title
        assert shown.startswith("catalog · ")
        app._on_recompiled(compile_file(notebook))  # a save with nothing to rerun
        await pilot.pause()
        assert app.sub_title == shown


async def test_catalog_tab_browses_then_enter_dives(notebook) -> None:
    # cycling onto the Catalog tab loads the root listing but stays in cells
    # mode (browse pass-through, like the Data tab); Enter is what dives.
    # Leaving the tab keeps the listing; re-entry shows it without a reload.
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("h")  # cells mode: cycle backward, sql wraps to catalog
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.query_one(TabbedContent).active == "tab_catalog"
        assert app.mode == "cells"  # browsing, not dived
        assert app.catalog_stack and app.catalog_stack[-1].title == "catalog"
        assert app.query_one("#catalog", DataTable).row_count > 0
        root = app.catalog_stack[-1]
        await pilot.press("l")  # still browsing: h/l cycles on (catalog -> sql)
        await pilot.pause()
        assert app.query_one(TabbedContent).active == "tab_sql"
        assert app.catalog_stack == [root]  # leaving the tab keeps the dive state
        await pilot.press("h")  # back onto Catalog: the kept listing, no reload
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert len(app.catalog_stack) == 1 and app.catalog_stack[-1] is root
        await pilot.press("enter")  # dive: keys now live in the listing
        assert app.mode == "catalog"
        assert app.sheet_stack is app.catalog_stack  # active surface routes


@pytest.fixture
def deep_catalog(tmp_path):
    """A notebook whose catalog goes three levels deep: the root listing →
    the @catalog sqlite context's tables → a table's 40 columns (taller than
    the catalog viewport, so a lost scroll position would show)."""
    import sqlite3

    con = sqlite3.connect(tmp_path / "nums.db")
    con.execute("CREATE TABLE alpha (a INTEGER)")
    con.execute(f"CREATE TABLE wide ({', '.join(f'c{i:02d} INTEGER' for i in range(40))})")
    con.commit()
    con.close()
    path = tmp_path / "deep.qsql"
    path.write_text("/*@ catalog: { sqlite: [nums.db] } */\n-- @cell a\nSELECT 1 AS x;\n")
    return path


async def _settle(app, pilot) -> None:
    await app.workers.wait_for_complete()
    await pilot.pause()


async def _dive_three_levels(app, pilot) -> None:
    """S, drill root → sqlite context → `wide`, then cursor to row 37."""
    await pilot.press("S")
    await _settle(app, pilot)
    names = app.catalog_stack[-1].frame["name"].to_list()
    await pilot.press(*["j"] * names.index("sqlite:nums.db"), "enter")
    await _settle(app, pilot)
    await pilot.press("j", "enter")  # alpha, [wide]
    await _settle(app, pilot)
    await pilot.press("G", "k", "k")
    assert app.mode == "catalog"
    assert len(app.catalog_stack) == 3
    assert app.catalog_stack[-1].title == "sqlite:nums.db/wide"
    assert app.catalog_stack[-1].cursor[0] == 37


async def _click_tab(app, pilot, pane_id: str) -> None:
    await pilot.click(f"#--content-tab-{pane_id}")
    await _settle(app, pilot)


def _same_sheets(stack, kept) -> bool:
    return len(stack) == len(kept) and all(a is b for a, b in zip(stack, kept))


async def test_leaving_the_catalog_tab_keeps_the_dive(deep_catalog) -> None:
    # a click on another tab hands keys back to the cell list but keeps the
    # catalog stack (each Sheet holds its own cursor/sort/filter): a run
    # doesn't reload it, a click back shows the same sheet at the same row,
    # and Enter — or S — resumes the dive there instead of reloading the root
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        kept = list(app.catalog_stack)
        tabs = app.query_one(TabbedContent)
        await _click_tab(app, pilot, "tab_log")
        assert tabs.active == "tab_log"
        assert app.mode == "cells"  # keys back on the cell list
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("R")  # a run invalidates the cache, not the dive
        await _settle(app, pilot)
        assert _same_sheets(app.catalog_stack, kept)
        await _click_tab(app, pilot, "tab_catalog")
        assert tabs.active == "tab_catalog"
        assert app.mode == "cells"  # a click browses; Enter resumes
        assert _same_sheets(app.catalog_stack, kept)  # no root reload
        await pilot.press("enter")
        assert app.mode == "catalog"
        assert app.sheet_stack[-1] is kept[-1]
        assert app.query_one("#catalog", DataTable).cursor_row == 37
        await pilot.press("j")  # moves on from the kept row, not the viewport top
        assert app.catalog_stack[-1].cursor[0] == 38
        moved = list(app.catalog_stack)
        await _click_tab(app, pilot, "tab_sql")
        assert app.mode == "cells"
        await pilot.press("S")  # S resumes the kept dive the same way
        await _settle(app, pilot)
        assert tabs.active == "tab_catalog"
        assert app.mode == "catalog"
        assert _same_sheets(app.catalog_stack, moved)
        assert app.query_one("#catalog", DataTable).cursor_row == 38


async def test_digit_keys_jump_to_tabs_and_5_resumes_the_catalog_dive(deep_catalog) -> None:
    # 1-5 jump straight to SQL/Data/Config/Log/Catalog from any mode: 1-4
    # land with keys back on the cell list, 5 is S — resume the kept dive at
    # its top sheet and row, no root reload
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        kept = list(app.catalog_stack)
        tabs = app.query_one(TabbedContent)
        await pilot.press("4")
        await _settle(app, pilot)
        assert tabs.active == "tab_log"
        assert app.mode == "cells"
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("5")
        await _settle(app, pilot)
        assert tabs.active == "tab_catalog"
        assert app.mode == "catalog"
        assert _same_sheets(app.catalog_stack, kept)
        assert app.query_one("#catalog", DataTable).cursor_row == 37
        for key, tab in (("1", "tab_sql"), ("2", "tab_data"), ("3", "tab_config")):
            await pilot.press(key)
            await _settle(app, pilot)
            assert (tabs.active, app.mode) == (tab, "cells"), key
            await pilot.press("5")
            await _settle(app, pilot)
            assert (tabs.active, app.mode) == ("tab_catalog", "catalog"), key
            assert _same_sheets(app.catalog_stack, kept), key


async def test_5_without_a_dive_loads_and_dives_into_the_root(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("5")
        await _settle(app, pilot)
        assert app.query_one(TabbedContent).active == "tab_catalog"
        assert app.mode == "catalog"
        assert len(app.catalog_stack) == 1
        assert app.catalog_stack[-1].title == "catalog"


async def test_digit_keys_leave_a_data_dive(notebook) -> None:
    # 2 on the Data tab hands keys back to the cell list (the sheet then
    # follows the selection); 1/3/4 leave the tab, dropping the dive as
    # leaving the Data tab always has
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        tabs = app.query_one(TabbedContent)
        await pilot.press("enter", "F")  # dive, freq sheet on top
        assert app.mode == "data" and len(app.sheet_stack) == 2
        await pilot.press("2")
        await _settle(app, pilot)
        assert tabs.active == "tab_data"
        assert app.mode == "cells"
        await pilot.press("j")  # the cell list moves; the Data sheet follows
        assert app.current_cell == "events"
        assert [s.title for s in app.sheet_stack] == ["events"]
        await pilot.press("enter")
        assert app.mode == "data"
        await pilot.press("4")
        await _settle(app, pilot)
        assert tabs.active == "tab_log"
        assert app.mode == "cells"
        assert not app.panes[0]  # the data dive was dropped with the tab


async def test_digits_typed_into_search_or_filter_dont_switch_tabs(deep_catalog) -> None:
    from textual.widgets import Input

    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        tabs = app.query_one(TabbedContent)
        search = app.query_one("#search", Input)
        await pilot.press("slash", "4", "2")
        assert search.value == "42"
        assert tabs.active == "tab_sql"
        await pilot.press("escape", "S")
        await _settle(app, pilot)
        await pilot.press("f", "1")
        await _settle(app, pilot)
        assert search.value == "1"
        assert (tabs.active, app.mode) == ("tab_catalog", "catalog")


@pytest.mark.parametrize("key", ["escape", "ctrl+k"])
async def test_esc_and_ctrl_k_leave_the_catalog_dive_without_popping(deep_catalog, key) -> None:
    # Esc (or Ctrl+k, "up") hands keys back to the cell list but stays on the
    # Catalog tab with the dive kept, so h/l cycle tabs again; cycling back
    # and Enter resumes
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        kept = list(app.catalog_stack)
        tabs = app.query_one(TabbedContent)
        await pilot.press(key)
        await _settle(app, pilot)
        assert app.mode == "cells"
        assert tabs.active == "tab_catalog"
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press(key)  # already on the cell list: a no-op
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_catalog", "cells")
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("h")  # a tab cycle now, not a column move
        await _settle(app, pilot)
        assert tabs.active == "tab_log"
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("l")
        await _settle(app, pilot)
        assert tabs.active == "tab_catalog"
        assert app.mode == "cells"
        await pilot.press("enter")
        assert app.mode == "catalog"
        assert _same_sheets(app.catalog_stack, kept)
        assert app.query_one("#catalog", DataTable).cursor_row == 37
        await pilot.press("q")  # q still pops one level
        assert len(app.catalog_stack) == 2
        assert app.mode == "catalog"


async def test_escape_that_closes_an_input_or_overlay_stays_in_the_dive(deep_catalog) -> None:
    # Esc first closes what's on top (filter input, help); only the next Esc
    # leaves the dive
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        await pilot.press("f", "c", "escape")  # cancel a live filter
        await _settle(app, pilot)
        assert app.mode == "catalog"
        assert app.catalog_stack[-1].frame.height == 40
        await pilot.press("question_mark")
        await pilot.pause()
        await pilot.press("escape")  # close the help overlay
        await _settle(app, pilot)
        assert app.mode == "catalog"
        await pilot.press("escape")
        assert app.mode == "cells"


@pytest.mark.parametrize("key", ["escape", "ctrl+k"])
async def test_esc_and_ctrl_k_in_a_data_dive_hand_keys_back_to_the_cell_list(notebook, key) -> None:
    # same in data mode: keys back to the cell list on the Data tab (a split
    # collapses to the focused pane); the sheet then follows the selection
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        tabs = app.query_one(TabbedContent)
        await pilot.press("enter", "F", "|")  # dive, freq sheet, split
        assert app.mode == "data" and app.split
        await pilot.press(key)
        await _settle(app, pilot)
        assert app.mode == "cells"
        assert tabs.active == "tab_data"
        assert not app.split
        await pilot.press("j")  # j moves the cell list; the Data sheet follows
        assert app.current_cell == "events"
        assert [s.title for s in app.sheet_stack] == ["events"]
        await pilot.press("l")  # and l cycles tabs again
        await _settle(app, pilot)
        assert tabs.active == "tab_config"


async def test_ctrl_h_l_step_through_tabs_from_any_mode(notebook) -> None:
    # Ctrl+h/l are h/l's tab cycle (wrapping at both ends) from any mode, by
    # the digit keys' rules: landing on the Catalog dives (loading the root
    # first), any other tab hands keys back to the cell list
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        tabs = app.query_one(TabbedContent)
        for tab in ("tab_data", "tab_config", "tab_log"):
            await pilot.press("ctrl+l")
            await _settle(app, pilot)
            assert (tabs.active, app.mode) == (tab, "cells")
        await pilot.press("ctrl+l")
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_catalog", "catalog")
        assert [s.title for s in app.catalog_stack] == ["catalog"]
        kept = list(app.catalog_stack)
        await pilot.press("ctrl+l")  # out of the catalog dive, wrapping to SQL
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_sql", "cells")
        await pilot.press("ctrl+h")  # wrapping back resumes the kept dive
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_catalog", "catalog")
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("2", "enter", "F", "|")  # data dive, freq sheet, split
        assert app.mode == "data" and app.split
        await pilot.press("ctrl+l")
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_config", "cells")
        assert not app.split
        assert not app.panes[0]  # the data dive was dropped with the tab
        await pilot.press("ctrl+h")  # back on Data: browsing, the sheet follows
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_data", "cells")
        assert [s.title for s in app.sheet_stack] == ["users"]
        await pilot.press("enter", "F")
        assert app.mode == "data"
        await pilot.press("ctrl+h")
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_sql", "cells")
        assert not app.panes[0]


async def test_ctrl_h_l_leave_and_resume_a_deep_catalog_dive(deep_catalog) -> None:
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        kept = list(app.catalog_stack)
        tabs = app.query_one(TabbedContent)
        for away, back, tab in (("ctrl+h", "ctrl+l", "tab_log"), ("ctrl+l", "ctrl+h", "tab_sql")):
            await pilot.press(away)
            await _settle(app, pilot)
            assert (tabs.active, app.mode) == (tab, "cells"), away
            assert _same_sheets(app.catalog_stack, kept), away
            await pilot.press(back)
            await _settle(app, pilot)
            assert (tabs.active, app.mode) == ("tab_catalog", "catalog"), back
            assert _same_sheets(app.catalog_stack, kept), back
            assert app.query_one("#catalog", DataTable).cursor_row == 37, back


@pytest.fixture
def long_catalog(tmp_path):
    """A @catalog sqlite context whose one table has 60 columns: a column
    list tall enough to scroll to the middle of, clear of both ends."""
    import sqlite3

    con = sqlite3.connect(tmp_path / "wh.db")
    con.execute(f"CREATE TABLE wide ({', '.join(f'c{i:02d} INTEGER' for i in range(60))})")
    con.commit()
    con.close()
    path = tmp_path / "long.qsql"
    path.write_text("/*@ catalog: { sqlite: [wh.db] } */\n-- @cell a\nSELECT 1 AS x;\n")
    return path


async def _scroll_mid_column_list(app, pilot) -> DataTable:
    """S, drill root → sqlite context → `wide`, then j×45 and k×5: the
    cursor on row 40, mid-viewport, the viewport mid-list."""
    await pilot.press("S")
    await _settle(app, pilot)
    names = app.catalog_stack[-1].frame["name"].to_list()
    await pilot.press(*["j"] * names.index("sqlite:wh.db"), "enter")
    await _settle(app, pilot)
    await pilot.press("enter")  # the context's one table
    await _settle(app, pilot)
    await pilot.press(*["j"] * 45, *["k"] * 5)
    table = app.query_one("#catalog", DataTable)
    assert app.catalog_stack[-1].title == "sqlite:wh.db/wide"
    assert app.catalog_stack[-1].cursor[0] == table.cursor_row == 40
    assert 0 < table.scroll_y < table.max_scroll_y
    return table


class _TerminalMirror:
    """The screen as a real terminal holds it: every compositor update the
    app writes, replayed onto a character grid. A terminal keeps whatever
    isn't resent, so a stale picture shows here; the headless pilot writes
    nothing, and a screenshot re-renders whole lines from the widgets."""

    _CODES = re.compile(r"\x1b\[(\d+);(\d+)H|\x1b\[[\d;?]*[A-Za-z]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|(.)")

    def __init__(self, app: QsqlApp) -> None:
        self.cells: dict[tuple[int, int], str] = {}
        display = app._display

        def mirrored(screen, update) -> None:
            if isinstance(update, CompositorUpdate):
                self._feed(update.render_segments(app.console))
            display(screen, update)

        app._display = mirrored

    def _feed(self, data: str) -> None:
        y = x = 0
        for m in self._CODES.finditer(data):
            if m[1]:  # cursor move; every update line starts with one
                y, x = int(m[1]) - 1, int(m[2]) - 1
            elif m[3]:
                self.cells[y, x] = m[3]
                x += 1

    def lines(self, region: Region) -> list[str]:
        return [
            "".join(self.cells.get((y, x), " ") for x in range(region.x, region.right))
            for y in range(region.y, region.bottom)
        ]


# leave a kept dive scrolled mid-list, then come back: Ctrl+l/Ctrl+h and
# 4/5 resume it directly; Esc + h browses away, l browses back, Enter resumes
_CATALOG_ROUND_TRIPS = [
    pytest.param(("ctrl+l",), ("ctrl+h",), id="ctrl+l,ctrl+h"),
    pytest.param(("4",), ("5",), id="4,5"),
    pytest.param(("escape", "h"), ("l", "enter"), id="esc+h,l+enter"),
]


@pytest.mark.parametrize(("away", "back"), _CATALOG_ROUND_TRIPS)
async def test_returning_to_a_kept_catalog_dive_holds_the_viewport(long_catalog, away, back) -> None:
    # back on the dive, the same rows are on screen with the cursor on the
    # same one, and j moves on from there. The viewport used to jump: the
    # cursor was scrolled into view before the tab was laid out again, i.e.
    # into a 0x0 viewport
    app = QsqlApp(path=long_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        table = await _scroll_mid_column_list(app, pilot)
        scroll = table.scroll_y
        await pilot.press(*away)
        await _settle(app, pilot)
        assert app.query_one(TabbedContent).active != "tab_catalog"
        await pilot.press(*back)
        await _settle(app, pilot)
        assert app.mode == "catalog"
        assert (table.scroll_y, table.cursor_row) == (scroll, 40)
        await pilot.press("j")
        await pilot.pause()
        assert app.catalog_stack[-1].cursor[0] == table.cursor_row == 41
        assert table.scroll_y == scroll  # row 41 was already on screen


@pytest.mark.parametrize(("away", "back"), _CATALOG_ROUND_TRIPS)
async def test_returning_to_a_kept_catalog_dive_redraws_its_rows(long_catalog, away, back) -> None:
    # every row the terminal shows is the table's row at that height: a scroll
    # made while the pane had no size marked nothing to repaint, so the old
    # lines stayed up and each j redrew just the cursor cells over them,
    # leaving rows like "c40  c26  INTEGER"
    app = QsqlApp(path=long_catalog, watch=False, auto_run=False)
    screen = _TerminalMirror(app)
    async with app.run_test(size=(100, 40)) as pilot:
        table = await _scroll_mid_column_list(app, pilot)
        await pilot.press(*away)
        await _settle(app, pilot)
        await pilot.press(*back)
        await _settle(app, pilot)
        _assert_screen_shows_the_catalog_rows(app, screen, table, "returned")
        await pilot.press("j")
        await pilot.pause()
        _assert_screen_shows_the_catalog_rows(app, screen, table, "j")


def _assert_screen_shows_the_catalog_rows(app, screen, table, step: str) -> None:
    rows = screen.lines(table.content_region)[1:]  # under the header
    top = int(table.scroll_y)
    names = app.catalog_stack[-1].frame["column"][top : top + len(rows)]
    shown = [re.findall(r"\bc\d\d\b", row) for row in rows]
    assert shown == [[name, name] for name in names], step  # column, field_path


async def test_a_key_handled_before_the_returning_catalog_is_laid_out(long_catalog) -> None:
    # 5 and j typed in one burst are both handled before the next frame, so
    # j moves the cursor while the shown-again pane has no size yet: it
    # still lands one row down with the viewport and the screen in step
    app = QsqlApp(path=long_catalog, watch=False, auto_run=False)
    screen = _TerminalMirror(app)
    async with app.run_test(size=(100, 40)) as pilot:
        table = await _scroll_mid_column_list(app, pilot)
        scroll = table.scroll_y
        await pilot.press("4")
        await _settle(app, pilot)
        app._jump_to_tab("tab_catalog")  # what on_key does for 5...
        app._move(1, 0)  # ...and for j, with no frame between them
        await _settle(app, pilot)
        assert app.catalog_stack[-1].cursor[0] == table.cursor_row == 41
        assert table.scroll_y == scroll
        _assert_screen_shows_the_catalog_rows(app, screen, table, "5+j")


async def test_ctrl_j_dives_like_enter_but_never_drills(deep_catalog) -> None:
    # Ctrl+j ("down") is Enter on the cell list: dive into the Data sheet,
    # or resume a kept catalog dive. Inside a sheet it does nothing — Enter
    # would drill a catalog sheet or reset a data sheet to its preview
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await _settle(app, pilot)
        tabs = app.query_one(TabbedContent)
        await pilot.press("ctrl+j")
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_data", "data")
        assert [s.title for s in app.sheet_stack] == ["a"]
        await pilot.press("F", "ctrl+j")
        assert len(app.sheet_stack) == 2  # no reset to the preview
        await pilot.press("S")
        await _settle(app, pilot)
        names = app.catalog_stack[-1].frame["name"].to_list()
        await pilot.press(*["j"] * names.index("sqlite:nums.db"), "ctrl+j")
        await _settle(app, pilot)
        assert app.mode == "catalog"
        assert len(app.catalog_stack) == 1  # no drill into the context
        kept = list(app.catalog_stack)
        await pilot.press("escape", "ctrl+j")
        await _settle(app, pilot)
        assert (tabs.active, app.mode) == ("tab_catalog", "catalog")
        assert _same_sheets(app.catalog_stack, kept)
        await pilot.press("enter")  # whereas Enter drills
        await _settle(app, pilot)
        assert len(app.catalog_stack) == 2


async def test_ctrl_nav_keys_dont_act_in_the_search_input_or_an_overlay(deep_catalog) -> None:
    from textual.widgets import Input

    from quicksql.tui import HelpScreen

    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:

        async def press_each_staying_put(tab: str, mode: str) -> None:
            # one at a time: a pair like ctrl+h, ctrl+l would cancel out
            for key in ("ctrl+h", "ctrl+j", "ctrl+k", "ctrl+l"):
                await pilot.press(key)
                await _settle(app, pilot)
                assert (tabs.active, app.mode) == (tab, mode), key

        await pilot.pause()
        tabs = app.query_one(TabbedContent)
        search = app.query_one("#search", Input)
        await pilot.press("slash")
        await press_each_staying_put("tab_sql", "cells")
        assert search.has_focus
        await pilot.press("escape", "S")
        await _settle(app, pilot)
        await pilot.press("f")
        await press_each_staying_put("tab_catalog", "catalog")
        assert search.has_focus
        await pilot.press("escape", "question_mark")
        await _settle(app, pilot)
        await press_each_staying_put("tab_catalog", "catalog")
        assert isinstance(app.screen, HelpScreen)


async def test_notebook_switch_resets_a_kept_catalog_dive(deep_catalog) -> None:
    other = write_scaffold(deep_catalog.parent / "base.qsql")
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await _dive_three_levels(app, pilot)
        await _click_tab(app, pilot, "tab_log")
        assert len(app.catalog_stack) == 3  # kept across the tab switch
        app._load_notebook(other)
        assert app.catalog_stack == []  # ...but not across notebooks
        await pilot.press("S")
        await _settle(app, pilot)
        assert app.mode == "catalog"
        assert len(app.catalog_stack) == 1
        assert app.catalog_stack[-1].title == "catalog"
        assert "users" in app.catalog_stack[-1].frame["name"].to_list()


def _root_names(app) -> list[str]:
    return app.catalog_stack[-1].frame["name"].to_list()


@pytest.mark.parametrize("key", ["5", "S"])
async def test_notebook_switch_on_the_catalog_tab_then_dive_into_the_new_root(
    deep_catalog, key
) -> None:
    # the old listing must vanish with the switch (not linger under the new
    # notebook), and S/5 must dive into the new root — not wait forever on a
    # tab activation that never fires because Catalog is already active
    other = write_scaffold(deep_catalog.parent / "base.qsql")
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press(key)
        await _settle(app, pilot)
        assert "sqlite:nums.db" in _root_names(app)
        table = app.query_one("#catalog", DataTable)
        app._load_notebook(other)
        assert table.row_count == 0  # the old listing is gone right away
        await _settle(app, pilot)
        await pilot.press(key)
        await _settle(app, pilot)
        tabs = app.query_one(TabbedContent)
        assert (tabs.active, app.mode) == ("tab_catalog", "catalog")
        assert len(app.catalog_stack) == 1
        assert "users" in _root_names(app)
        assert table.row_count == app.catalog_stack[-1].frame.height


async def test_notebook_switch_on_the_catalog_tab_loads_the_new_root_to_browse(
    deep_catalog,
) -> None:
    other = write_scaffold(deep_catalog.parent / "base.qsql")
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("5")
        await _settle(app, pilot)
        app._load_notebook(other)
        await _settle(app, pilot)
        assert app.query_one(TabbedContent).active == "tab_catalog"
        assert app.mode == "cells"  # browse, as when cycling onto the tab
        assert len(app.catalog_stack) == 1
        assert "users" in _root_names(app)
        assert app.query_one("#catalog", DataTable).row_count > 0


async def test_5_on_an_empty_catalog_tab_loads_the_root_itself(deep_catalog) -> None:
    # Catalog tab active, no listing, nothing loading: a notebook that failed
    # to compile, then fixed by a watch recompile. No tab activation follows
    # 5, so 5 has to start the root load itself.
    broken = deep_catalog.parent / "broken.qsql"
    broken.write_text("-- @cell a\nSELECT * FROM {{ ref('missing') }};\n")
    app = QsqlApp(path=deep_catalog, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("5")
        await _settle(app, pilot)
        app._load_notebook(broken)
        await _settle(app, pilot)
        assert app.project is None and app.catalog_stack == []
        broken.write_text("-- @cell fixed\nSELECT 1 AS x;\n")
        app._on_recompiled(compile_file(broken))
        await pilot.press("5")
        await _settle(app, pilot)
        assert app.mode == "catalog"
        assert len(app.catalog_stack) == 1
        assert "fixed" in _root_names(app)


async def test_s_from_another_tab_starts_exactly_one_root_load(notebook, monkeypatch) -> None:
    # S starts the root load itself *and* activates the tab, whose handler
    # also wants the root: only one load may start, or two roots get pushed
    from quicksql.tui import app as tui

    calls = []
    real = tui.project_root_node

    def counting(*args, **kwargs):
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(tui, "project_root_node", counting)
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("S")
        await _settle(app, pilot)
        assert len(calls) == 1
        assert len(app.catalog_stack) == 1
        assert app.mode == "catalog"


async def test_s_mid_data_dive_switches_surfaces_cleanly(notebook) -> None:
    # S while dived in the Data sheet lands in a clean catalog dive: the
    # data-pane dive is dropped and keys route to the catalog listing, never
    # a stale invisible stack
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive into the cell's data sheet
        assert app.mode == "data"
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.query_one(TabbedContent).active == "tab_catalog"
        assert app.mode == "catalog"
        assert not app.panes[0]  # the data dive was dropped with the tab
        assert app.catalog_stack[-1].title == "catalog"
        await pilot.press("q")  # pops the catalog root, not a data sheet
        assert app.mode == "cells"
        assert app.catalog_stack == []


async def test_frequency_on_catalog_listing_stays_on_catalog_surface(notebook) -> None:
    # F on a catalog sheet pushes the derived sheet onto the catalog stack —
    # no flip to the Data tab — and q pops back to the listing
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.mode == "catalog"
        await pilot.press("F")  # frequency of the cursor column
        await pilot.pause()
        assert app.query_one(TabbedContent).active == "tab_catalog"  # no tab flip
        assert app.mode == "catalog"
        assert len(app.catalog_stack) == 2
        await pilot.press("q")
        assert len(app.catalog_stack) == 1
        assert app.catalog_stack[-1].title == "catalog"


async def test_enter_on_plain_data_sheet_still_resets_preview(notebook) -> None:
    # regression guard for the drill interception: sheets without a drill
    # payload keep the old Enter behavior (reset to the cell's preview)
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive
        await pilot.press("F")  # freq sheet on top (drill-less)
        assert len(app.sheet_stack) == 2
        await pilot.press("enter")
        assert len(app.sheet_stack) == 1
        assert app.sheet_stack[-1].title == "users"


async def test_f_live_filters_commits_on_enter_cancels_on_escape(notebook) -> None:
    from textual.widgets import Input

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        full_height = app.sheet_stack[-1].frame.height
        await pilot.press("f")
        assert app.query_one("#search", Input).has_focus
        await pilot.press("u", "s", "e", "r", "s")
        top = app.sheet_stack[-1]  # live: already narrowed while typing
        assert top.title == "filter(users)"
        assert top.frame.height == 1
        assert top.drill is not None  # a filtered catalog still drills
        await pilot.press("escape")  # cancel: unfiltered sheet restored
        assert app.sheet_stack[-1].frame.height == full_height
        assert len(app.sheet_stack) == 1
        await pilot.press("f")
        await pilot.press("e", "v", "e", "n", "t")
        await pilot.press("enter")  # commit: filtered pushed above the base
        assert len(app.sheet_stack) == 2
        assert app.sheet_stack[-1].title == "filter(event)"
        assert app.sheet_stack[-1].frame.height == 2  # events, active_user_events
        await pilot.press("q")
        assert app.sheet_stack[-1].frame.height == full_height


async def test_live_filter_keeps_column_widths_stable(notebook) -> None:
    # regression: every filter keystroke rebuilt the table and auto-width
    # refit columns to the narrowed rows — on zero matches they collapsed
    # to bare header widths, jumping back on backspace
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("j", "enter")  # field-path sheet
        await app.workers.wait_for_complete()
        await pilot.pause()
        table = app.query_one("#catalog", DataTable)
        widths_before = [c.get_render_width(table) for c in table.columns.values()]
        await pilot.press("f")
        await pilot.press("u", "s", "e", "r")  # narrows to a subset
        assert [c.get_render_width(table) for c in table.columns.values()] == widths_before
        await pilot.press("z", "z", "z")  # zero matches: still no collapse
        assert app.sheet_stack[-1].frame.height == 0
        assert [c.get_render_width(table) for c in table.columns.values()] == widths_before
        await pilot.press("escape")
        assert [c.get_render_width(table) for c in table.columns.values()] == widths_before


async def test_y_yanks_current_cell_or_selected_column_values(notebook, monkeypatch) -> None:
    copied: list[str] = []
    monkeypatch.setattr(QsqlApp, "copy_to_clipboard", lambda self, text: copied.append(text))
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("S")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("j", "enter")  # drill into the users field paths
        await app.workers.wait_for_complete()
        await pilot.pause()
        sheet = app.sheet_stack[-1]
        assert sheet.frame.columns == ["column", "field_path", "type", "mode", "description"]
        await pilot.press("l")  # cursor onto field_path
        await pilot.press("y")  # no selection: the current cell
        assert copied == [sheet.frame["field_path"][0]]
        await pilot.press("s", "j", "s")  # select rows 0 and 1
        await pilot.press("y")
        expected = ",\n".join(sheet.frame["field_path"][:2].to_list())
        assert copied[-1] == expected  # SQL-ready select-list snippet


@pytest.mark.terminal
def test_yank_reaches_the_terminal_clipboard_and_pastes_into_nvim(tmp_path) -> None:
    """End-to-end OSC 52: yank in the real TUI under tmux, confirm the escape
    sequence lands in tmux's clipboard buffer, then paste it into a live
    neovim session editing the notebook. Needs tmux + nvim on PATH; run with
    `pytest -m terminal`. If this passes but the system clipboard stays empty
    in your terminal, the break is between the terminal and the OS (tmux
    set-clipboard, kitty clipboard_control, WSLg bridge, ...)."""
    import shutil as sh
    import subprocess
    import time

    if not (sh.which("tmux") and sh.which("nvim")):
        pytest.skip("needs tmux and nvim")
    (tmp_path / "base.qsql").write_text(
        "-- @cell events\n"
        "SELECT 1 AS id, {'name': 'x', 'params': [{'key': 'k', 'value': 1}]} AS event;\n"
    )
    sock = f"qsqlyank{time.time_ns() % 100000}"

    def t(*args: str) -> str:
        return subprocess.run(
            ["tmux", "-L", sock, *args], capture_output=True, text=True
        ).stdout

    def keys(*args: str, wait: float = 1.0) -> None:
        t("send-keys", *args)
        time.sleep(wait)

    try:
        t("new-session", "-d", "-x", "110", "-y", "30", "-c", str(tmp_path), "quicksql tui base.qsql")
        t("set-option", "-g", "set-clipboard", "on")
        time.sleep(5)
        keys("R", wait=3.0)  # run all
        keys("S", wait=2.0)  # catalog
        keys("j", "Enter", wait=2.0)  # drill into the output's field paths
        keys("l", "s", "j", "s", "y")  # select two field paths, yank
        assert t("show-buffer") == "id,\nevent"  # OSC 52 captured by the terminal
        t("new-window", "-c", str(tmp_path), "nvim -u NONE base.qsql")
        time.sleep(2)
        keys("G", "o", "-- yanked:", "Enter")  # open a line in insert mode
        t("paste-buffer")
        time.sleep(1)
        keys("Escape")
        keys(":wq", "Enter", wait=2.0)
        assert (tmp_path / "base.qsql").read_text().endswith("-- yanked:\nid,\nevent\n")
    finally:
        subprocess.run(["tmux", "-L", sock, "kill-server"], capture_output=True)


async def test_run_all_populates_results_and_dive(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.results["users"].ok
        assert app.results["active_user_events"].rows == 7
        await pilot.press("enter")  # dive into Data sheet
        assert app.mode == "data"
        assert app.sheet_stack
        await pilot.press("F")  # frequency pushes a sheet
        assert len(app.sheet_stack) == 2
        await pilot.press("q")  # pop back
        assert len(app.sheet_stack) == 1
        await pilot.press("q")  # leave data mode
        assert app.mode == "cells"


async def test_question_mark_opens_help_overlay_covering_all_key_layers(notebook) -> None:
    # the help overlay must document keys from every layer, including the
    # vim/sheet keys that live only in on_key and appear in no footer binding
    from quicksql.tui import HelpScreen

    documented = {
        key.strip()
        for _, rows in HelpScreen.SECTIONS
        for keys, _ in rows
        for key in keys.split("/")
    }
    # footer bindings and on_key-only keys alike are covered
    keys = ("r", "R", "S", "y", "f", "gg", "[", "]", "-", "n", "?", "|", "w", "esc", *"12345",
            "ctrl+h", "ctrl+j", "ctrl+k", "ctrl+l")
    for key in keys:
        assert key in documented, f"{key!r} missing from the help overlay"

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("question_mark")
        await pilot.pause()
        assert isinstance(app.screen, HelpScreen)
        await pilot.press("question_mark")  # toggles closed
        await pilot.pause()
        assert not isinstance(app.screen, HelpScreen)
        await pilot.press("question_mark")
        await pilot.pause()
        assert isinstance(app.screen, HelpScreen)
        await pilot.press("escape")  # escape also closes
        await pilot.pause()
        assert not isinstance(app.screen, HelpScreen)


async def test_short_terminal_collapses_to_cells_only(notebook) -> None:
    # when the terminal gets too short, hide the detail tabs so only the top
    # cell-list section shows; restore it when there's room again
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        detail = app.query_one("#detail")
        cells = app.query_one("#cells")
        assert detail.display  # roomy: both sections visible
        await pilot.resize_terminal(100, 8)  # cramped
        await pilot.pause()
        assert not detail.display  # only the cell list remains
        assert cells.display
        await pilot.resize_terminal(100, 40)  # room again
        await pilot.pause()
        assert detail.display  # restored


@pytest.mark.postgres
async def test_catalog_shows_every_column_of_a_wide_postgres_table(pg_dsn, tmp_path) -> None:
    # a table with >1000 columns must show ALL of them in the catalog sheet —
    # each column is one field-path row, so the data-preview row cap must not
    # truncate a schema listing
    import psycopg

    ncols = 1100
    cols = ", ".join(f"c{i} int" for i in range(ncols))
    with psycopg.connect(pg_dsn, autocommit=True) as con:
        con.execute("DROP TABLE IF EXISTS wide_catalog")
        con.execute(f"CREATE TABLE wide_catalog ({cols})")
    try:
        nb = tmp_path / "wide.qsql"
        nb.write_text(
            "-- @engine: postgres\n"
            f"/*@ input: {{ postgres: {{ dsn: '{pg_dsn}' }} }} */\n\n"
            "-- @cell probe\nSELECT 1 AS x;\n"
        )
        app = QsqlApp(path=nb, watch=False, auto_run=False)
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()

            async def drill_to(pred) -> None:
                sheet = app.sheet_stack[-1]
                idx = next(
                    i for i, r in enumerate(sheet.frame.iter_rows(named=True)) if pred(r)
                )
                app.sheet_stack[-1] = sheet.top().move(idx, 0)
                await pilot.press("enter")
                await app.workers.wait_for_complete()
                await pilot.pause()

            await pilot.press("S")  # catalog root
            await app.workers.wait_for_complete()
            await pilot.pause()
            await drill_to(lambda r: r["kind"] == "context" and r["engine"] == "postgres")
            await drill_to(lambda r: r["schema"] == "public")
            await drill_to(lambda r: r["table"] == "wide_catalog")

            leaf = app.sheet_stack[-1]
            assert leaf.frame.columns == ["column", "field_path", "type", "mode", "description"]
            assert leaf.frame.height == ncols  # the fetch has every column
            table = app.query_one("#catalog", DataTable)
            assert table.row_count == ncols  # ...and every column is rendered
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as con:
            con.execute("DROP TABLE IF EXISTS wide_catalog")


async def test_vertical_split_copies_sheet_and_navigates_independently(notebook) -> None:
    # | splits the data pane into two side-by-side sheets; the new (right) pane
    # starts as a copy, then each pane navigates on its own drilling stack
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")  # dive into the first cell's data sheet
        await pilot.pause()
        assert not app.split
        assert not app.query_one("#data2", DataTable).display

        await pilot.press("|")  # split -> focus lands on the new right pane
        await pilot.pause()
        assert app.split
        assert app.active_pane == 1
        assert app.query_one("#data2", DataTable).display
        assert app.panes[0][-1].frame is app.panes[1][-1].frame  # a copy

        await pilot.press("w")  # switch focus back to the left pane
        await pilot.pause()
        assert app.active_pane == 0

        left_before = app.panes[0][-1].frame
        right_before = app.panes[1][-1].frame
        await pilot.press("]")  # sort mutates only the focused (left) pane
        await pilot.pause()
        assert app.panes[0][-1].frame is not left_before
        assert app.panes[1][-1].frame is right_before

        await pilot.press("|")  # collapse: keep the focused pane
        await pilot.pause()
        assert not app.split
        assert not app.query_one("#data2", DataTable).display


async def test_split_pane_q_collapses_to_the_other_pane(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        await pilot.press("enter")
        await pilot.pause()
        await pilot.press("|")  # split; right pane holds a single sheet
        await pilot.pause()
        assert app.split and app.active_pane == 1

        await pilot.press("q")  # empties the focused pane -> collapse, keep the other
        await pilot.pause()
        assert not app.split
        assert app.mode == "data"  # still viewing the surviving pane
        assert len(app.panes) == 1 and app.panes[0]
        assert not app.query_one("#data2", DataTable).display


async def test_run_streams_events_into_log_tab(notebook) -> None:
    from textual.widgets import RichLog

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        # a hidden tab's RichLog has no width and defers rendering — show it first
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        text = "\n".join(s.text for s in app.query_one("#log", RichLog).lines)
        assert "run started" in text
        assert "users: executing on duckdb" in text
        assert "users: ok" in text and "rows" in text
        assert "run finished" in text
        assert text.count("users: ok") == 1  # event line replaces the old summary line


async def test_a_config_edit_reruns_the_cell(notebook) -> None:
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("p")  # armed
        notebook.write_text(
            notebook.read_text().replace(
                "-- @cell users\n", "-- @cell users\n-- @output: { type: duckdb }\n"
            )
        )
        assert "users" in app._on_recompiled(compile_file(notebook))
        await app.workers.wait_for_complete()


async def test_a_save_with_nothing_to_rerun_says_so(notebook) -> None:
    from textual.widgets import RichLog

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        assert app._on_recompiled(compile_file(notebook)) == []
        await pilot.pause()
        lines = [s.text for s in app.query_one("#log", RichLog).lines]
        assert lines[-1] == "nothing to rerun"


async def test_a_save_compiles_the_notebook_once(notebook, monkeypatch) -> None:
    from quicksql.tui import app as tui

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.press("p")  # armed
        real = tui.compile_file
        compiles: list = []
        monkeypatch.setattr(tui, "compile_file", lambda *a, **k: compiles.append(a) or real(*a, **k))
        notebook.write_text(notebook.read_text().replace("range(10)", "range(5)"))
        assert app._on_recompiled(real(notebook))  # the watch worker's one compile
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert compiles == []  # the run reuses the project the save compiled
        assert app.results["events"].ok


# ---------- per-cell run progress: events drive the cell table ----------


def _row(app, name: str) -> tuple[str, ...]:
    table = app.query_one("#cells", DataTable)
    idx = app._row_names.index(name)
    return tuple(str(c) for c in table.get_row_at(idx))


async def test_run_events_drive_per_cell_status(notebook) -> None:
    """run_started seeds the pending set from the event's post-expansion cell
    list; each cell_finished retires its cell while the rest stay pending —
    the table must not flip wholesale at the end of the run."""
    from quicksql.models import RunEvent, RunResult

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        names = tuple(app.project.order)
        gen = app.runs.gen
        app.runs.on_event(gen, RunEvent("run_started", cells=names))
        await pilot.pause()
        assert app.running == set(names)
        assert all(_row(app, n)[3] == "…" for n in names)
        done = RunResult(cell=names[0], status="ok", rows=3, elapsed=0.25)
        app.runs.on_event(gen, RunEvent("cell_finished", names[0], result=done))
        await pilot.pause()
        assert names[0] not in app.running and names[1] in app.running
        assert _row(app, names[0])[3] == "✔" and _row(app, names[0])[4] == "3"
        assert _row(app, names[1])[3] == "…"


async def test_stale_generation_events_do_not_touch_state(notebook) -> None:
    """A replaced (still-draining) run keeps logging but must not fight the
    live run's pending set or results."""
    from quicksql.models import RunEvent, RunResult

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        stale = app.runs.gen - 1
        app.runs.on_event(stale, RunEvent("run_started", cells=("users",)))
        assert app.running == set()
        done = RunResult(cell="users", status="ok", rows=1, elapsed=0.1)
        app.runs.on_event(stale, RunEvent("cell_finished", "users", result=done))
        assert "users" not in app.results
        app.runs.apply(stale, [done])
        assert "users" not in app.results


async def test_running_cell_blanks_its_stale_rows_and_ms(notebook) -> None:
    """A rerunning cell must not display the previous run's rows/ms next to
    its spinner — blank them while the cell is pending."""
    from quicksql.models import RunEvent, RunResult

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        old = RunResult(cell="users", status="ok", rows=42, elapsed=1.234)
        app.runs.apply(app.runs.gen, [old])
        await pilot.pause()
        assert _row(app, "users")[4] == "42"
        app.runs.on_event(app.runs.gen, RunEvent("run_started", cells=("users",)))
        await pilot.pause()
        row = _row(app, "users")
        assert row[3] == "…" and row[4] == "" and row[5] == ""


async def test_run_worker_failure_never_leaves_stuck_spinners(notebook, monkeypatch) -> None:
    from quicksql.tui import runs as tui_mod

    def boom(*args, **kwargs):
        raise RuntimeError("executor exploded")

    monkeypatch.setattr(tui_mod, "run_project", boom)
    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.running == set()


async def test_mid_run_cells_retire_one_at_a_time(tmp_path, registries) -> None:
    """End-to-end through the real worker: while the second cell is executing,
    the first already shows done — not the old wholesale flip at run end."""
    import threading

    import polars as pl

    from quicksql.executors.base import EngineSession, Executor
    from quicksql.registry import executor

    gate = threading.Event()
    reached_second = threading.Event()

    class GatedSession(EngineSession):
        def execute(self, sql, *, scoped=True):
            pass

        def fetch(self, sql, *, scoped=True):
            if sql == "SELECT 2":  # the second cell
                reached_second.set()
                assert gate.wait(timeout=10)
            return pl.DataFrame({"x": [1]})

    @executor("gated")
    class GatedExecutor(Executor):
        def open_session(self, config, ctx):
            return GatedSession()

    registries.add(GatedExecutor)
    path = tmp_path / "base.qsql"
    path.write_text(
        "-- @engine: gated\n\n"
        "-- @cell first\nSELECT 1;\n\n"
        "-- @cell second\nSELECT 2;\n\n"
        "-- @cell third\nSELECT 3;\n"
    )
    app = QsqlApp(path=path, watch=False, registries=registries)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("R")
        while not reached_second.is_set():
            await pilot.pause(0.05)
        await pilot.pause()  # drain queued call_from_thread callbacks
        try:
            assert "first" not in app.running and app.results["first"].ok
            assert {"second", "third"} <= app.running
            assert _row(app, "first")[3] == "✔"
            assert _row(app, "second")[3] == "…" and _row(app, "third")[3] == "…"
        finally:
            gate.set()  # never leave the worker thread blocked on a failure
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.running == set()
        assert all(app.results[n].ok for n in ("first", "second", "third"))


async def test_skipped_downstream_shows_a_distinct_status(tmp_path) -> None:
    """A cell skipped because its upstream failed is neither done, failed,
    pending nor never-run: its cell_finished paints its own glyph."""
    path = tmp_path / "base.qsql"
    path.write_text(
        "-- @cell a\nSELECT * FROM nope;\n\n"
        "-- @cell b\nSELECT * FROM {{ ref('a') }};\n"
    )
    app = QsqlApp(path=path, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.running == set()
        assert _row(app, "a")[3] == "✗"
        assert _row(app, "b")[3] == "⊘"
        assert app.results["b"].status == "skipped"
        assert _row(app, "b")[4] == ""  # nothing landed, so no row count


async def test_failed_cell_detail_log_shows_the_message_not_a_traceback(tmp_path) -> None:
    from textual.widgets import RichLog

    path = tmp_path / "base.qsql"
    path.write_text("-- @cell a\nSELECT * FROM nope;\n")
    app = QsqlApp(path=path, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        # a hidden tab's RichLog has no width and defers rendering — show it first
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        text = "\n".join(s.text for s in app.query_one("#log", RichLog).lines)
        assert "a failed:\nCatalog Error: Table with name nope does not exist!" in text
        assert "Traceback" not in text


async def test_a_single_line_failure_is_logged_once(tmp_path) -> None:
    """The FAILED line already holds a one-line message; only a longer one
    gets the full block after it."""
    from textual.widgets import RichLog

    path = tmp_path / "base.qsql"
    path.write_text("-- @cell a\n-- @input: { sqlite: ':memory:' }\nSELECT * FROM nope;\n")
    app = QsqlApp(path=path, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        text = "\n".join(s.text for s in app.query_one("#log", RichLog).lines)
        assert "a: FAILED — sqlite: no such table: nope" in text
        assert text.count("no such table: nope") == 1


async def test_failed_cell_message_is_logged_once_not_per_cursor_move(tmp_path) -> None:
    from textual.widgets import RichLog

    path = tmp_path / "base.qsql"
    path.write_text("-- @cell a\nSELECT * FROM nope;\n\n-- @cell b\nSELECT 1 AS x;\n")
    app = QsqlApp(path=path, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        log = app.query_one("#log", RichLog)
        assert app.current_cell == "a"  # the failed cell is selected
        logged = len(log.lines)
        await pilot.press("j", "k", "j", "k")  # away and back, twice
        await pilot.pause()
        assert app.current_cell == "a"
        assert len(log.lines) == logged  # selecting it logs nothing new
        text = "\n".join(s.text for s in log.lines)
        assert text.count("a failed:\nCatalog Error") == 1


# ---------- one run session per notebook ----------


def _two_notebooks(tmp_path):
    """Same cell names, different SQL: a reused session would serve
    notebook one's temp table `a` to notebook two's `b`."""
    one = tmp_path / "one.qsql"
    one.write_text("-- @cell a\nSELECT 1 AS x;\n\n-- @cell b\nSELECT x FROM {{ ref('a') }};\n")
    two = tmp_path / "two.qsql"
    two.write_text(one.read_text().replace("SELECT 1 AS x", "SELECT 2 AS x"))
    return one, two


async def test_notebook_switch_gets_its_own_run_session(tmp_path) -> None:
    one, two = _two_notebooks(tmp_path)
    app = QsqlApp(path=one, watch=False)
    async with app.run_test() as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        first = app.session
        await pilot.press("o")
        await pilot.pause()
        await pilot.press("down", "enter")  # [one], two, create base.qsql
        await pilot.pause()
        assert app.path == two
        await pilot.press("p")  # arm watch reruns in notebook two
        two.write_text(two.read_text().replace("SELECT x FROM", "SELECT x * 10 AS x FROM"))
        assert app._on_recompiled(compile_file(two)) == ["b"]  # exact rerun of b
        await app.workers.wait_for_complete()
        await pilot.pause()
        # b read notebook two's a (2), not notebook one's live temp (1)
        assert app.results["b"].preview["x"].to_list() == [20]
        assert app.session is not first


async def test_idle_notebook_switch_closes_the_old_session(tmp_path) -> None:
    one, two = _two_notebooks(tmp_path)
    app = QsqlApp(path=one, watch=False)
    async with app.run_test() as pilot:
        await pilot.press("R")
        await app.workers.wait_for_complete()
        await pilot.pause()
        first = app.session
        assert first.conn is not None
        app._load_notebook(two)
        assert first.conn is None  # no run uses it: closed right away


def _gated_engine(registries):
    """Add a `gated` engine to ``registries`` whose cells block mid-execute
    until `gate` is set; `reached` fires once one is blocked there."""
    import threading

    from quicksql.executors.base import EngineSession, Executor
    from quicksql.registry import executor

    gate = threading.Event()
    reached = threading.Event()

    class GatedSession(EngineSession):
        def execute(self, sql, *, scoped=True):
            pass

        def fetch(self, sql, *, scoped=True):
            reached.set()
            assert gate.wait(timeout=10)
            return pl.DataFrame({"x": [1]})

    @executor("gated")
    class GatedExecutor(Executor):
        def open_session(self, config, ctx):
            return GatedSession()

    registries.add(GatedExecutor)
    return gate, reached


async def test_switch_mid_run_closes_the_old_session_only_after_the_run(
    tmp_path, registries
) -> None:
    """Closing a DuckDB connection under another thread's query risks a
    crash: the run that still uses the old session closes it on its way out."""
    gate, reached = _gated_engine(registries)
    one = tmp_path / "one.qsql"
    one.write_text("-- @engine: gated\n\n-- @cell a\nSELECT 1;\n")
    two = tmp_path / "two.qsql"
    two.write_text("-- @cell a\nSELECT 2 AS x;\n")
    app = QsqlApp(path=one, watch=False, registries=registries)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("R")
        while not reached.is_set():
            await pilot.pause(0.05)
        old = app.session
        try:
            app._load_notebook(two)
            assert app.session is not old
            assert old.conn is not None  # mid-query: not closed underneath the run
        finally:
            gate.set()  # never leave the worker thread blocked on a failure
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert old.conn is None  # the run's own exit closed it


def _counting_engine(registries):
    """A `gated` engine whose fetches block until `gate` is set, counting how
    many run at once; returns (gate, reached, fetched, peak())."""
    import threading

    from quicksql.executors.base import EngineSession, Executor
    from quicksql.registry import executor

    gate, reached, lock = threading.Event(), threading.Event(), threading.Lock()
    fetched: list[str] = []
    active = [0, 0]  # now, peak

    class GatedSession(EngineSession):
        def execute(self, sql, *, scoped=True):
            pass

        def fetch(self, sql, *, scoped=True):
            with lock:
                fetched.append(sql)
                active[0] += 1
                active[1] = max(active)
            try:
                reached.set()
                assert gate.wait(timeout=10)
                return pl.DataFrame({"x": [1]})
            finally:
                with lock:
                    active[0] -= 1

    @executor("gated")
    class GatedExecutor(Executor):
        def open_session(self, config, ctx):
            return GatedSession()

    registries.add(GatedExecutor)
    return gate, reached, fetched, lambda: active[1]


GATED_TWO = "-- @engine: gated\n\n-- @cell a\nSELECT 1;\n\n-- @cell b\nSELECT 2;\n"


async def test_a_new_run_waits_for_the_one_it_replaces_to_stop(tmp_path, registries) -> None:
    """Two runs on one session at once share its connections across threads:
    R while a run is under way cancels it after its current cell, and the
    new run starts only once it has stopped."""
    gate, reached, fetched, peak = _counting_engine(registries)
    path = tmp_path / "base.qsql"
    path.write_text(GATED_TWO)
    app = QsqlApp(path=path, watch=False, registries=registries)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("R")
        while not reached.is_set():
            await pilot.pause(0.05)
        try:
            await pilot.press("R")  # replace the run while its cell a is under way
            await pilot.pause(0.3)  # time enough for a concurrent run to show itself
        finally:
            gate.set()
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert peak() == 1
        assert fetched == ["SELECT 1", "SELECT 1", "SELECT 2"]  # the first run stopped after a
        assert app.results["a"].ok and app.results["b"].ok
        assert app.running == set()


async def test_a_notebook_switch_stops_the_old_notebooks_run(tmp_path, registries) -> None:
    gate, reached, fetched, _ = _counting_engine(registries)
    one = tmp_path / "one.qsql"
    one.write_text(GATED_TWO)
    two = tmp_path / "two.qsql"
    two.write_text("-- @cell a\nSELECT 2 AS x;\n")
    app = QsqlApp(path=one, watch=False, registries=registries)
    async with app.run_test() as pilot:
        await pilot.pause()
        await pilot.press("R")
        while not reached.is_set():
            await pilot.pause(0.05)
        try:
            app._load_notebook(two)
        finally:
            gate.set()
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert fetched == ["SELECT 1"]  # notebook one's b never ran


async def test_old_notebook_run_finishing_after_switch_leaves_new_one_alone(
    tmp_path, monkeypatch
) -> None:
    """A run of the previous notebook still compiling at the switch must not
    adopt its project, pend its cells or store its results in the new one."""
    import threading

    from quicksql.tui import app as tui_mod

    one = tmp_path / "one.qsql"
    one.write_text("-- @cell a\nSELECT 1 AS x;\n")
    two = tmp_path / "two.qsql"
    two.write_text("-- @cell a\nSELECT 2 AS x;\n\n-- @cell c\nSELECT 3 AS y;\n")
    gate = threading.Event()
    reached = threading.Event()
    real_compile = tui_mod.compile_file

    def compile_gated(path, *args, **kwargs):
        if path == one and threading.current_thread() is not threading.main_thread():
            reached.set()  # the run worker, compiling notebook one
            assert gate.wait(timeout=10)
        return real_compile(path, *args, **kwargs)

    monkeypatch.setattr(tui_mod, "compile_file", compile_gated)
    app = QsqlApp(path=one, watch=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        await pilot.press("R")
        while not reached.is_set():
            await pilot.pause(0.05)
        try:
            app._load_notebook(two)
        finally:
            gate.set()
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert list(app.project.cells) == ["a", "c"]  # still notebook two
        assert app.results == {} and app.running == set()
        assert _row(app, "a")[3] == "·" and _row(app, "c")[3] == "·"
