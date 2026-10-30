import polars as pl
import pytest
from textual.widgets import DataTable, TabbedContent

from quicksql.compiler import compile_file
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


async def test_user_templates_offered_and_seed_the_requested_file(tmp_path, monkeypatch) -> None:
    from textual.widgets import OptionList

    home = tmp_path / "home"
    (home / ".quicksql" / "templates").mkdir(parents=True)
    (home / ".quicksql" / "templates" / "metrics.qsql").write_text(
        "-- @cell tpl\nSELECT 7 AS seven;\n"
    )
    monkeypatch.setenv("HOME", str(home))
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
        app._refresh_data()
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


async def test_catalog_tab_browses_then_enter_dives(notebook) -> None:
    # cycling onto the Catalog tab loads the root listing but stays in cells
    # mode (browse pass-through, like the Data tab); Enter is what dives.
    # Leaving the tab drops the listing; re-entry reloads the root.
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
        await pilot.press("l")  # still browsing: h/l cycles on (catalog -> sql)
        await pilot.pause()
        assert app.query_one(TabbedContent).active == "tab_sql"
        assert app.catalog_stack == []  # leaving the tab drops the dive state
        await pilot.press("h")  # back onto Catalog: the root reloads
        await app.workers.wait_for_complete()
        await pilot.pause()
        assert app.catalog_stack
        await pilot.press("enter")  # dive: keys now live in the listing
        assert app.mode == "catalog"
        assert app.sheet_stack is app.catalog_stack  # active surface routes


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
    for key in ("r", "R", "S", "y", "f", "gg", "[", "]", "-", "n", "?", "|", "w"):
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


async def test_config_only_edit_is_logged_without_rerun(notebook) -> None:
    from textual.widgets import RichLog

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        app.query_one(TabbedContent).active = "tab_log"
        await pilot.pause()
        notebook.write_text(
            notebook.read_text().replace(
                "-- @cell users\n", "-- @cell users\n-- @output: { type: duckdb }\n"
            )
        )
        assert app._on_recompiled(compile_file(notebook)) == []
        await pilot.pause()
        lines = [s.text for s in app.query_one("#log", RichLog).lines]
        assert any("config changed" in l and "users" in l for l in lines)


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
        gen = app._run_gen
        app._on_run_event(gen, RunEvent("run_started", cells=names))
        await pilot.pause()
        assert app.running == set(names)
        assert all(_row(app, n)[3] == "…" for n in names)
        done = RunResult(cell=names[0], status="ok", rows=3, elapsed=0.25)
        app._on_run_event(gen, RunEvent("cell_finished", names[0], result=done))
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
        stale = app._run_gen - 1
        app._on_run_event(stale, RunEvent("run_started", cells=("users",)))
        assert app.running == set()
        done = RunResult(cell="users", status="ok", rows=1, elapsed=0.1)
        app._on_run_event(stale, RunEvent("cell_finished", "users", result=done))
        assert "users" not in app.results
        app._apply_results(stale, [done])
        assert "users" not in app.results


async def test_running_cell_blanks_its_stale_rows_and_ms(notebook) -> None:
    """A rerunning cell must not display the previous run's rows/ms next to
    its spinner — blank them while the cell is pending."""
    from quicksql.models import RunEvent, RunResult

    app = QsqlApp(path=notebook, watch=False, auto_run=False)
    async with app.run_test(size=(100, 40)) as pilot:
        await pilot.pause()
        old = RunResult(cell="users", status="ok", rows=42, elapsed=1.234)
        app._apply_results(app._run_gen, [old])
        await pilot.pause()
        assert _row(app, "users")[4] == "42"
        app._on_run_event(app._run_gen, RunEvent("run_started", cells=("users",)))
        await pilot.pause()
        row = _row(app, "users")
        assert row[3] == "…" and row[4] == "" and row[5] == ""


async def test_run_worker_failure_never_leaves_stuck_spinners(notebook, monkeypatch) -> None:
    from quicksql import tui as tui_mod

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


async def test_mid_run_cells_retire_one_at_a_time(tmp_path) -> None:
    """End-to-end through the real worker: while the second cell is executing,
    the first already shows done — not the old wholesale flip at run end."""
    import threading

    import polars as pl

    from quicksql.executors.base import Executor, register_frame, result_view
    from quicksql.registry import executor

    gate = threading.Event()
    reached_second = threading.Event()

    @executor("gated")
    class GatedExecutor(Executor):
        def execute(self, cell, ctx):
            if cell.name == "second":
                reached_second.set()
                assert gate.wait(timeout=10)
            return register_frame(ctx, result_view(cell.name), pl.DataFrame({"x": [1]}))

    path = tmp_path / "base.qsql"
    path.write_text(
        "-- @engine: gated\n\n"
        "-- @cell first\nSELECT 1;\n\n"
        "-- @cell second\nSELECT 2;\n\n"
        "-- @cell third\nSELECT 3;\n"
    )
    app = QsqlApp(path=path, watch=False)
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
