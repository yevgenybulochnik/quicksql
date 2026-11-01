"""The quicksql TUI app: layout, key routing and the notebook it shows."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, Sequence

import polars as pl
import yaml
from rich.syntax import Syntax
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.widgets import DataTable, Footer, Header, Input, RichLog, Static, TabbedContent, TabPane

from ..bootstrap import compile_file
from ..catalog import CatalogCache, CatalogNode, project_root_node
from ..compiler import Project
from ..errors import QsqlError
from ..models import RunResult
from ..registry import Registries
from ..runner import RunSession
from ..sheet import Sheet
from ..watcher import plan_rerun
from . import previews
from .catalog_view import CatalogView
from .panes import DataPanes, SheetTable, Surface
from .previews import preview_frame
from .runs import RunController
from .screens import HelpScreen, NotebookPicker


class QsqlApp(App):
    TITLE = "quicksql"

    CSS = """
    #cells { height: 40%; max-height: 12; border: solid $primary; }
    #detail { height: 1fr; border: solid $secondary; }
    #search { dock: bottom; display: none; }
    #sql_view, #config_view { padding: 1; }
    /* too short for both sections: drop the detail tabs, let the cell list fill */
    #body.-compact #detail { display: none; }
    #body.-compact #cells { height: 1fr; max-height: 100%; }
    /* vertical split: two side-by-side sheets. #data2 shows only when split;
       borders (and the accent-highlighted focused pane) appear only then, so
       the single-pane look is unchanged. */
    #data, #data2 { width: 1fr; }
    #data2 { display: none; }
    #data_panes.-split #data2 { display: block; }
    #data_panes.-split #data, #data_panes.-split #data2 { border: solid $panel; }
    #data.-active, #data2.-active { border: solid $accent; }
    """

    BINDINGS = [
        Binding("q", "pop_or_quit", "pop/quit"),
        Binding("r", "run_cell", "run"),
        Binding("R", "run_all", "run all"),
        Binding("t", "toggle_sql", "raw/rendered"),
        Binding("a", "toggle_autorun", "autorun"),
        Binding("A", "toggle_autorun_global", "autorun*"),
        Binding("p", "toggle_armed", "watch"),
        Binding("F", "frequency", "freq"),
        Binding("I", "describe", "describe"),
        Binding("S", "catalog", "catalog"),
        Binding("V", "visidata", "vd"),
        Binding("o", "open_notebook", "open"),
        Binding("slash", "search", "search", key_display="/"),
        Binding("question_mark", "help", "help", key_display="?"),
        Binding("ctrl+r", "refetch", "refetch", show=False),
        Binding("ctrl+d", "page(1)", "page down", show=False),
        Binding("ctrl+u", "page(-1)", "page up", show=False),
    ]

    def __init__(
        self,
        path: Path | str,
        overrides: dict[str, Any] | None = None,
        watch: bool = True,
        auto_run: bool = False,
        plugins: Sequence[str] = (),
        registries: Registries | None = None,
    ) -> None:
        super().__init__()
        self.path = Path(path)
        self.overrides = overrides or {}
        # --plugins modules; each compile adds them to the notebook's own
        # registries, after its quicksqlrc.py (reloaded when edited)
        self.plugins = tuple(plugins)
        # given: every compile uses these instead (an embedding app, a test)
        self.registries = registries
        self.watch = watch
        self.auto_run = auto_run
        self.armed = False  # watch reruns stay dormant until the first run-all
        self.project: Project | None = None
        self.runs = RunController(self)  # the session, results and pending cells
        self.autorun_off: set[str] = set()
        self.autorun_global = True
        self.show_rendered = True
        self.mode = "cells"  # or "data" / "catalog": whose keys these are
        self.data = DataPanes(self)  # the Data tab's sheet stacks
        self.catalog = CatalogView(self)  # the Catalog tab's own dive
        self._catalog_dive_pending = False  # S pressed while the root loads
        self._catalog_root_loading = False  # a root load is in flight
        self._catalog_loading: str | None = None  # the title of a load in flight
        self._row_names: list[str] = []
        self._pending_g = False
        self._last_search = ""
        self._input_mode = "search"  # what the bottom input edits: search | filter
        self._filtering: Surface | None = None  # the surface being live-filtered
        self._catalog_cache = CatalogCache()  # cleared on run/recompile/switch

    @property
    def session(self) -> RunSession:
        return self.runs.session

    @property
    def results(self) -> dict[str, RunResult]:
        return self.runs.results

    @property
    def running(self) -> set[str]:
        return self.runs.running

    # ---------- surfaces ----------

    SHEET_MODES = ("data", "catalog")  # modes whose keys live in a sheet
    MAX_DATA_ROWS = SheetTable.MAX_ROWS
    MAX_DATA_COLS = SheetTable.MAX_COLS
    PREVIEW_ROWS = previews.PREVIEW_ROWS

    @property
    def surface(self) -> Surface:
        """The active drilling surface: the catalog's in catalog mode, else
        the Data tab's. Routing by mode is what lets the sheet verbs
        (filter/yank/search/freq/drill/pop) work unchanged on either."""
        return self.catalog if self.mode == "catalog" else self.data

    @property
    def sheet_stack(self) -> list[Sheet]:
        """The active surface's stack (on the Data tab, the focused pane's).
        Reads and in-place mutations operate on it; assigning replaces it."""
        return self.surface.stack

    @sheet_stack.setter
    def sheet_stack(self, value: list[Sheet]) -> None:
        self.surface.stack = value

    @property
    def panes(self) -> list[list[Sheet]]:
        return self.data.panes

    @property
    def active_pane(self) -> int:
        return self.data.active

    @property
    def split(self) -> bool:
        return self.data.split

    @property
    def catalog_stack(self) -> list[Sheet]:
        return self.catalog.stack

    def _toggle_split(self) -> None:
        if self.mode == "data":
            self.data.toggle_split()

    # ---------- layout ----------

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="body"):
            yield DataTable(id="cells", cursor_type="row")
            with TabbedContent(id="detail"):
                with TabPane("SQL", id="tab_sql"):
                    yield Static(id="sql_view")
                with TabPane("Data", id="tab_data"):
                    with Horizontal(id="data_panes"):
                        yield DataTable(id="data", cursor_type="cell")
                        yield DataTable(id="data2", cursor_type="cell")
                with TabPane("Config", id="tab_config"):
                    yield Static(id="config_view")
                with TabPane("Log", id="tab_log"):
                    yield RichLog(id="log", markup=False, wrap=True)
                with TabPane("Catalog", id="tab_catalog"):
                    yield DataTable(id="catalog", cursor_type="cell")
        yield Input(placeholder="search...", id="search")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#cells", DataTable)
        table.add_columns("cell", "engine → sink", "auto", "status", "rows", "ms")
        self.query_one("#data", DataTable).can_focus = False
        self.query_one("#data2", DataTable).can_focus = False
        self.query_one("#catalog", DataTable).can_focus = False
        table.focus()
        if self.path.exists():
            self._load_notebook(self.path)
        else:
            self._show_picker(startup=True)

    def _compile(self, path: Path) -> Project:
        """Compile with the notebook's own registries: the builtins, the
        quicksqlrc.py next to it (reloaded when edited) and --plugins."""
        return compile_file(path, self.overrides, self.plugins, registries=self.registries)

    def _load_notebook(self, path: Path | str) -> None:
        """Open a notebook (fresh state), arming and watching per-notebook."""
        self.path = Path(path)
        # the old session's temps/statuses describe the old notebook, and an
        # in-flight run of it must not paint this one
        self.runs.reset()
        self.data.reset()
        self.catalog.stack = []
        self._catalog_dive_pending = False
        self._catalog_root_loading = False
        self._catalog_cache.invalidate()
        self.catalog.refresh()  # the old listing must not show under this notebook
        self.mode = "cells"
        self.armed = False
        self._refresh_armed_indicator()
        self.project = None
        try:
            self.project = self._compile(self.path)
        except QsqlError as exc:
            self.log_line(f"compile error: {exc}")
            return
        self.session.see(self.project)  # the baseline a save is compared against
        self._refresh_cells()
        self._refresh_detail()
        if self.query_one(TabbedContent).active == "tab_catalog":
            self._load_catalog_root()  # no activation fires: browse the new root
        if self.auto_run:
            self.action_run_all()
        else:
            self.log_line("autorun paused — R runs all & arms; p arms without running")
        if self.watch:
            self._watch_worker()  # exclusive group: replaces any previous watcher

    def _show_picker(self, startup: bool) -> None:
        from ..scaffold import template_names

        directory = self.path.parent if str(self.path.parent) else Path(".")
        # the target is only the *default* for the picker's name prompt, so a
        # taken default no longer hides the template
        if not self.path.exists():
            # the requested file is missing: any template may seed it
            creates = [(self.path.name, name) for name in template_names()]
        else:  # switching: suggest each template's own filename
            creates = [
                ("base.qsql" if name == "base" else f"{name}.qsql", name)
                for name in template_names()
            ]

        def chosen(result: tuple[str, Any] | None) -> None:
            if result is None:
                if startup:
                    self.exit()
                return
            kind, value = result
            if kind == "template":
                from ..scaffold import write_scaffold

                target, name = value
                self._load_notebook(write_scaffold(directory / target, template=name))
            else:
                self._load_notebook(Path(value))

        self.push_screen(NotebookPicker(directory, creates), chosen)

    def action_open_notebook(self) -> None:
        self._show_picker(startup=False)

    def action_help(self) -> None:
        if not isinstance(self.screen, HelpScreen):
            self.push_screen(HelpScreen())

    # rows of terminal below which the detail tabs are dropped and the cell
    # list fills the screen (header + a usable detail pane don't both fit)
    COMPACT_HEIGHT = 16

    def on_resize(self, event: events.Resize) -> None:
        try:
            body = self.query_one("#body", Vertical)
        except NoMatches:
            return  # resize before the body mounted; on_mount lays out fresh
        body.set_class(event.size.height < self.COMPACT_HEIGHT, "-compact")

    def on_unmount(self) -> None:
        self.runs.close()

    # ---------- state helpers ----------

    @property
    def current_cell(self) -> str | None:
        table = self.query_one("#cells", DataTable)
        if not self._row_names or table.cursor_row is None:
            return None
        return self._row_names[min(table.cursor_row, len(self._row_names) - 1)]

    def cell_autorun(self, name: str) -> bool:
        if not self.project:
            return False
        from ..watcher import should_rerun

        configured = should_rerun(self.project.cells[name], self.project.registries.plugins)
        return self.autorun_global and configured and name not in self.autorun_off

    def log_line(self, text: str) -> None:
        self.query_one("#log", RichLog).write(text)

    def _status(self, name: str) -> str:
        if name in self.running:
            return "…"
        result = self.results.get(name)
        if result is None:
            return "·"
        return {"ok": "✔", "skipped": "⊘"}.get(result.status, "✗")

    def _refresh_cells(self) -> None:
        table = self.query_one("#cells", DataTable)
        previous = table.cursor_row or 0
        table.clear()
        self._row_names = list(self.project.order) if self.project else []
        for name in self._row_names:
            cell = self.project.cells[name]
            # a pending cell must not show the previous run's rows/ms next to
            # its spinner; the stored result stays (display-only blanking)
            result = None if name in self.running else self.results.get(name)
            table.add_row(
                name,
                f"{cell.engine} → {cell.sink_type}",
                "☑" if self.cell_autorun(name) else "☐",
                self._status(name),
                "" if result is None or result.rows is None else str(result.rows),
                "" if result is None else f"{result.elapsed * 1000:.0f}",
            )
        if self._row_names:
            table.move_cursor(row=min(previous, len(self._row_names) - 1))

    def _refresh_detail(self) -> None:
        name = self.current_cell
        if not name or not self.project:
            return
        cell = self.project.cells[name]
        sql = cell.sql if self.show_rendered else (cell.source or cell.sql_raw)
        self.query_one("#sql_view", Static).update(
            Syntax(sql, "sql", line_numbers=True, word_wrap=True)
        )
        self.query_one("#config_view", Static).update(
            yaml.safe_dump(cell.config.model_dump(mode="json"), sort_keys=True)
        )
        self.refresh_subtitle()

    def refresh_subtitle(self) -> None:
        """The one subtitle, from whatever has the keys: a catalog load in
        flight, the catalog's top sheet in a catalog dive, the Data sheet when
        dived or browsed on its tab, else the selected cell. Runs and
        recompiles repaint through here, so they never overwrite a sheet's."""
        if self._catalog_loading is not None:
            self.sub_title = f"loading {self._catalog_loading}…"
        elif self.mode == "catalog" and self.catalog.stack:
            self.sub_title = self.catalog.subtitle
        elif self.data.stack and (
            self.mode == "data" or self.query_one(TabbedContent).active == "tab_data"
        ):
            self.sub_title = self.data.subtitle
        else:
            self.sub_title = self._cell_subtitle()

    def _cell_subtitle(self) -> str:
        name = self.current_cell
        if not name or not self.project:
            return ""
        label = "rendered" if self.show_rendered else "source"
        stack = f" · sheets:{len(self.sheet_stack)}" if self.sheet_stack else ""
        return f"{name} · sql:{label}{stack}"



    def _mutate_sheet(self, fn) -> None:
        if self.mode in self.SHEET_MODES:
            self.surface.mutate(fn)

    def _push_sheet(self, sheet: Sheet) -> None:
        """Push onto the active surface: derived sheets (freq/describe) stay
        on the catalog in catalog mode; otherwise dive into the Data tab."""
        self.sheet_stack.append(sheet)
        if self.mode == "catalog":
            self.catalog.refresh()
            return
        self._dive_into_data()

    def _dive_into_data(self) -> None:
        self.mode = "data"
        self.query_one(TabbedContent).active = "tab_data"
        self.data.refresh()

    def _preview_sheet(self, name: str) -> Sheet:
        """A cell's preview as a sheet. A full one may be just the head of its
        output, and says so, as does every sheet sorted, filtered or counted
        from it."""
        frame = self._preview_frame(name)
        sample = frame.height if frame.height >= self.PREVIEW_ROWS else None
        return Sheet(frame, title=name, sample=sample)

    def _preview_frame(self, name: str) -> pl.DataFrame:
        return preview_frame(self.project, self.results.get(name), name, self._catalog_connect)

    # ---------- actions (footer bindings) ----------

    def action_pop_or_quit(self) -> None:
        if self.mode == "catalog" and self.catalog_stack:
            self.catalog_stack.pop()
            self.catalog.refresh()  # an emptied stack clears the table
            if self.catalog_stack:
                return
            self.mode = "cells"  # popped the root: back out to the cell list
            self.query_one(TabbedContent).active = "tab_sql"
            self._refresh_detail()
            return
        if self.mode == "data" and self.sheet_stack:
            self.sheet_stack.pop()
            if self.sheet_stack:
                self.data.refresh()
                return
            if self.split:  # focused pane emptied: keep the other, full width
                self.data.collapse(1 - self.active_pane)
                self.data.refresh()
                return
            self.mode = "cells"
            self.query_one(TabbedContent).active = "tab_sql"
            self._refresh_detail()
            return
        self.exit()

    def action_toggle_sql(self) -> None:
        self.show_rendered = not self.show_rendered
        self._refresh_detail()

    def action_toggle_autorun(self) -> None:
        name = self.current_cell
        if name:
            self.autorun_off.symmetric_difference_update({name})
            self._refresh_cells()

    def action_toggle_autorun_global(self) -> None:
        self.autorun_global = not self.autorun_global
        self._refresh_cells()

    def action_run_cell(self) -> None:
        if self.current_cell:
            self.runs.start([self.current_cell], closure=True, path=self.path)

    def action_run_all(self) -> None:
        self.armed = True
        self._refresh_armed_indicator()
        self.runs.start(None, closure=True, path=self.path)

    def action_toggle_armed(self) -> None:
        """Arm/disarm watch reruns *without* running anything — the light-weight
        counterpart to run-all (R), which arms only as a side effect of a full run."""
        self.armed = not self.armed
        self._refresh_armed_indicator()
        self.log_line(
            "autorun on — saves rerun changed cells (no full run)"
            if self.armed
            else "autorun off — watch reruns paused"
        )

    def _refresh_armed_indicator(self) -> None:
        state = "▶ live" if self.armed else "⏸ paused"
        self.title = f"quicksql · {self.path.name} · {state}"

    def action_frequency(self) -> None:
        if self.mode in self.SHEET_MODES and self.sheet_stack:
            self._push_sheet(self.sheet_stack[-1].freq())

    def action_describe(self) -> None:
        if self.mode in self.SHEET_MODES and self.sheet_stack:
            self._push_sheet(self.sheet_stack[-1].describe())

    def action_catalog(self) -> None:
        """S / 5: jump to the Catalog tab and dive — back into the kept dive at
        its top sheet, or straight into the root listing."""
        if not self.project:
            return
        tabs = self.query_one(TabbedContent)
        if tabs.active != "tab_catalog":
            tabs.active = "tab_catalog"
        if self.catalog_stack:
            self.mode = "catalog"
            self.catalog.refresh()
        else:  # root loads on a threaded worker: dive when it lands
            self._catalog_dive_pending = True
            # started here, not left to the activation handler: when the tab
            # is already active, no activation fires
            self._load_catalog_root()

    def _load_catalog_root(self) -> None:
        """Load the root listing unless there's a stack or a load in flight —
        S/5, the tab activation and a notebook switch can all ask at once."""
        if self.catalog_stack or self._catalog_root_loading or not self.project:
            return
        self._catalog_root_loading = True
        self._catalog_worker(project_root_node(self.project, connect=self._catalog_connect))

    def action_refetch(self) -> None:
        """ctrl+r: drop the current catalog sheet's cached frame and reload it."""
        if self.mode != "catalog" or not self.catalog_stack:
            return
        node: CatalogNode | None = self.catalog_stack[-1].drill
        if node is None:
            return
        self._catalog_cache.invalidate(node)
        self._catalog_worker(node, replace=True)

    def _catalog_connect(self):
        """DuckDB handle for catalog reads: a cursor of the live conduit (it
        may already hold sink ATTACHes, and cursors are per-thread-safe), or
        a throwaway connection before the first run."""
        if self.session.conn is not None:
            return self.session.conn.cursor()
        import duckdb

        return duckdb.connect()

    def _drill_current(self) -> None:
        sheet = self.sheet_stack[-1]
        node: CatalogNode = sheet.drill
        if node.child is None or sheet.frame.height == 0:
            return  # a leaf (field-path sheet): nowhere further down
        child = node.child(sheet.frame.row(sheet.cursor[0], named=True))
        if child is not None:
            self._catalog_worker(child)

    def action_search(self) -> None:
        self._input_mode = "search"
        self._show_input("search...")

    def _show_input(self, placeholder: str) -> None:
        search = self.query_one("#search", Input)
        search.value = ""  # a cancelled filter/search must not leave stale text
        search.placeholder = placeholder
        search.styles.display = "block"
        search.focus()

    def _show_filter(self) -> None:
        if self.mode not in self.SHEET_MODES or not self.surface.begin_filter():
            return
        self._input_mode = "filter"
        self._filtering = self.surface
        self._show_input("filter rows by regex...")

    def _cancel_filter(self) -> None:
        if self._filtering is not None:
            self._filtering.cancel_filter()
        self._filtering = None
        self._input_mode = "search"

    def _yank(self) -> None:
        if self.mode not in self.SHEET_MODES:
            return
        got = self.surface.yank()
        if got is None:
            return
        text, count, col = got
        self.copy_to_clipboard(text)
        self.notify(f"copied {count} value(s) from {col!r}")

    def action_page(self, direction: int) -> None:
        if self.mode in self.SHEET_MODES:
            self._mutate_sheet(lambda s: self.surface.reconcile_to_viewport(s).move(direction * 20, 0))
        else:
            self.query_one("#cells", DataTable).move_cursor(
                row=(self.query_one("#cells", DataTable).cursor_row or 0) + direction * 10
            )

    def action_visidata(self) -> None:
        name = self.current_cell
        result = self.results.get(name) if name else None
        target = result.target if result and result.ok else None
        vd = shutil.which("vd")
        if not vd:
            self.notify("visidata (vd) is not installed", severity="warning")
            return
        if not target or not Path(target).exists():
            self.notify(f"no landed file for {name!r} — run it first", severity="warning")
            return
        with self.suspend():
            subprocess.call([vd, target])

    # ---------- raw keys (vim/VisiData movement + sheet ops) ----------

    def on_key(self, event: events.Key) -> None:
        if len(self.screen_stack) > 1:  # a modal (the picker) owns the keys
            return
        if self.query_one("#search", Input).has_focus:
            if event.key == "escape":
                if self._input_mode == "filter":
                    self._cancel_filter()
                self._hide_search()
            return
        if event.key == "enter" and not self.query_one("#cells", DataTable).has_focus:
            # with #cells focused the DataTable consumes Enter and emits
            # RowSelected; once focus wanders (Tab, a click on the tab bar)
            # the key bubbles here instead — same behavior either way
            self._activate_current_row()
            return
        ch = event.character
        if self._pending_g:
            self._pending_g = False
            if ch == "g":
                self._move(top=True)
            elif ch == "s":
                self._mutate_sheet(lambda s: s.select_all())
            return
        if ch == "g":
            self._pending_g = True
        elif ch == "G":
            self._move(bottom=True)
        elif ch == "j":
            self._move(1, 0)
        elif ch == "k":
            self._move(-1, 0)
        elif ch == "h":
            self._move(0, -1)
        elif ch == "l":
            self._move(0, 1)
        elif ch == "[":
            self._mutate_sheet(lambda s: s.sort(desc=False))
        elif ch == "]":
            self._mutate_sheet(lambda s: s.sort(desc=True))
        elif ch == "-":
            self._mutate_sheet(lambda s: s.hide_current())
        elif ch == "s":
            self._mutate_sheet(lambda s: s.toggle_select())
        elif ch == "f":
            self._show_filter()
        elif ch == "y":
            self._yank()
        elif ch == "n":
            self._repeat_search(reverse=False)
        elif ch == "N":
            self._repeat_search(reverse=True)
        elif ch == "|":
            self._toggle_split()
        elif ch == "w":
            self.data.switch()
        elif ch in self.TAB_KEYS:
            self._jump_to_tab(self.TAB_KEYS[ch])
        # vim-style ctrl keys, the same from any mode: h/l step through the
        # tabs, k climbs to the cell list (Esc), j dives from it (Enter)
        elif event.key in ("ctrl+h", "ctrl+l"):
            self._jump_to_tab(self._tab_after(1 if event.key == "ctrl+l" else -1))
        elif event.key in ("escape", "ctrl+k"):
            self._leave_dive()
        elif event.key == "ctrl+j" and self.mode == "cells":
            self._activate_current_row()  # not in a sheet, where Enter drills

    TAB_ORDER = ["tab_sql", "tab_data", "tab_config", "tab_log", "tab_catalog"]
    TAB_KEYS = dict(zip("12345", TAB_ORDER))

    def _tab_after(self, delta: int) -> str:
        tabs = self.query_one(TabbedContent)
        idx = self.TAB_ORDER.index(tabs.active) if tabs.active in self.TAB_ORDER else 0
        return self.TAB_ORDER[(idx + delta) % len(self.TAB_ORDER)]

    def _cycle_tab(self, delta: int) -> None:
        self.query_one(TabbedContent).active = self._tab_after(delta)

    def _jump_to_tab(self, tab: str) -> None:
        """1-5 and ctrl+h/l, from any mode. The Catalog tab is S; the others
        land with keys back on the cell list — handed back here, since 2 on
        the Data tab activates nothing — and the activation handler drops a
        Data dive left behind."""
        if tab == "tab_catalog":
            self.action_catalog()
            return
        self._leave_dive()
        self.query_one(TabbedContent).active = tab

    def _leave_dive(self) -> None:
        """Hand keys back to the cell list without popping, staying on the
        tab: a catalog dive is kept for Enter/S/5 to resume; a Data sheet
        stays on screen until the selection moves and the Data tab follows."""
        if self.mode not in self.SHEET_MODES:
            return
        if self.split:  # the split is a data-mode concern; drop it on the way out
            self.data.collapse(self.active_pane)
            self.data.refresh()
        self.mode = "cells"
        self._refresh_detail()

    def _move(self, d_row: int = 0, d_col: int = 0, top: bool = False, bottom: bool = False) -> None:
        if self.mode in self.SHEET_MODES:
            self.surface.move(d_row, d_col, top=top, bottom=bottom)
            return
        if d_col:
            self._cycle_tab(d_col)
            return
        table = self.query_one("#cells", DataTable)
        if top:
            table.move_cursor(row=0)
        elif bottom:
            table.move_cursor(row=max(table.row_count - 1, 0))
        elif d_row:
            table.move_cursor(row=(table.cursor_row or 0) + d_row)
        self._refresh_detail()

    def _hide_search(self) -> None:
        search = self.query_one("#search", Input)
        search.styles.display = "none"
        self.query_one("#cells", DataTable).focus()

    def _repeat_search(self, reverse: bool) -> None:
        if self._last_search:
            self._mutate_sheet(lambda s: s.search(self._last_search, reverse=reverse))

    def on_input_changed(self, event: Input.Changed) -> None:
        if self._input_mode == "filter" and self._filtering is not None:
            self._filtering.live_filter(event.value.strip())  # live, per keystroke

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._input_mode == "filter":
            event.input.value = ""
            self._hide_search()
            filtering, self._filtering = self._filtering, None
            self._input_mode = "search"
            if filtering is not None:
                filtering.commit_filter()
            return
        needle = event.value.strip()
        self._last_search = needle
        event.input.value = ""
        self._hide_search()
        if not needle:
            return
        if self.mode in self.SHEET_MODES:
            self._mutate_sheet(lambda s: s.search(needle))
        else:  # jump to the next cell whose name matches
            names = self._row_names
            table = self.query_one("#cells", DataTable)
            start = (table.cursor_row or 0) + 1
            for offset in range(len(names)):
                idx = (start + offset) % len(names)
                if needle.lower() in names[idx].lower():
                    table.move_cursor(row=idx)
                    break
            self._refresh_detail()

    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        """Browsing onto a tab (click or h/l) shows content but stays in cells
        mode, so j/k keep moving the cell selection; Enter is what dives.
        Leaving the Data or Catalog tab only hands keys back to the cell list
        and keeps its stack, so coming back (click, h/l, 2, S) shows the same
        sheet at the same row — on the Data tab, while the same cell is
        selected. Either way a direct jump never leaves keys routed to an
        invisible stack."""
        active = self.query_one(TabbedContent).active
        if active == "tab_data":
            # must not touch a "data" mode here: _push_sheet dives (sets the
            # mode) *before* activating the tab, and this handler fires after
            self._leave_catalog_dive()
            if not self.data.stack or self.data.cell != self.current_cell:
                self._show_current_data()
            else:
                self.data.refresh()  # the kept dive, as it was left
        elif active == "tab_catalog":
            self._leave_data_dive()
            self._load_catalog_root()
        else:
            self._leave_data_dive()
            self._leave_catalog_dive()
            self._refresh_detail()

    def _leave_data_dive(self) -> None:
        # the stack stays, like the catalog's: back on the Data tab with the
        # same cell selected it shows again, and Enter resumes it
        if self.mode == "data":
            self.mode = "cells"
        if self.split:  # the split is a data-mode concern; drop it on the way out
            self.data.collapse(self.active_pane)

    def _leave_catalog_dive(self) -> None:
        # the stack stays: its Sheets keep cursor/sort/filter/hidden columns
        # for S / Enter to resume. A notebook switch or q past the root resets it.
        if self.mode == "catalog":
            self.mode = "cells"

    def _show_current_data(self) -> None:
        name = self.current_cell
        if name:
            self.data.show(name, self._preview_sheet(name))
            self.data.refresh()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if event.data_table.id != "cells":
            return
        self._activate_current_row()

    def _activate_current_row(self) -> None:
        if (
            self.mode in self.SHEET_MODES
            and self.sheet_stack
            and self.sheet_stack[-1].drill is not None
        ):
            self._drill_current()  # Enter on a catalog sheet goes deeper
            return
        if self.query_one(TabbedContent).active == "tab_catalog":
            # browsing the Catalog tab: dive into the listing (the catalog
            # mirror of the preview dive below) — never reset to a cell preview
            if self.catalog_stack:
                self.mode = "catalog"
                self.catalog.refresh()
            return
        name = self.current_cell
        if not name:
            return
        if self.mode == "cells" and self.data.stack and self.data.cell == name:
            self._dive_into_data()  # resume the kept dive
            return
        self.data.show(name, self._preview_sheet(name))
        self._dive_into_data()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id != "cells" or self.mode != "cells":
            return
        self._refresh_detail()
        if self.query_one(TabbedContent).active == "tab_data":
            self._show_current_data()  # the Data sheet follows the selection

    # ---------- workers ----------

    @work(thread=True, exclusive=True, group="catalog")
    def _catalog_worker(self, node: CatalogNode, replace: bool = False) -> None:
        self.call_from_thread(self._catalog_load_started, node.title)
        try:
            frame, _ = self._catalog_cache.load(node)
        except Exception as exc:
            self.call_from_thread(self.log_line, f"catalog {node.title}: {exc}")
            self.call_from_thread(self.notify, f"catalog: {exc}", severity="error")
            self.call_from_thread(setattr, self, "_catalog_dive_pending", False)
            self.call_from_thread(setattr, self, "_catalog_root_loading", False)
            self.call_from_thread(self._catalog_load_ended)  # drop the loading… note
            return
        sheet = Sheet(frame, title=node.title, drill=node)
        if replace:  # refetch: swap the current sheet, keep the stack shape
            self.call_from_thread(self._replace_top_sheet, sheet)
        else:
            self.call_from_thread(self._push_catalog_sheet, sheet)

    def _catalog_load_started(self, title: str) -> None:
        self._catalog_loading = title
        self.refresh_subtitle()

    def _catalog_load_ended(self) -> None:
        self._catalog_loading = None
        self.refresh_subtitle()

    def _push_catalog_sheet(self, sheet: Sheet) -> None:
        self._catalog_root_loading = False  # a root push lands on an empty stack
        self.catalog_stack.append(sheet)
        if self._catalog_dive_pending:  # S while the root was loading
            self._catalog_dive_pending = False
            self.mode = "catalog"
        self.catalog.refresh()
        self._catalog_load_ended()

    def _replace_top_sheet(self, sheet: Sheet) -> None:
        if self.catalog_stack:
            self.catalog_stack[-1] = sheet
            self.catalog.refresh()
            self._catalog_load_ended()
        else:
            self._push_catalog_sheet(sheet)

    # ---------- run hooks (RunController, on the UI thread) ----------

    def run_adopted(self, project: Project) -> None:
        """The current run compiled the notebook afresh."""
        self.project = project
        self._refresh_cells()

    def run_progressed(self, finished: str | None = None) -> None:
        """The current run started, or its cell ``finished`` did."""
        self._refresh_cells()
        if finished is not None and finished == self.data.cell and self.data.stack:
            self.data.renew(self._preview_sheet(finished))  # never a stale sheet

    def run_landed(self) -> None:
        """A run ended, the current one or a replaced one: outputs changed."""
        self._catalog_cache.invalidate()

    def run_finished(self) -> None:
        """The current run ended."""
        self._refresh_cells()
        self._refresh_detail()

    @work(exclusive=True, group="watch")
    async def _watch_worker(self) -> None:
        import watchfiles

        from ..watcher import touches

        # watch the directory: atomic editor saves (nvim) replace the inode,
        # which silently kills a watch on the file path itself
        async for changes in watchfiles.awatch(self.path.parent):
            if not touches(changes, self.path):
                continue
            try:
                project = self._compile(self.path)
            except QsqlError as exc:
                self.log_line(f"compile error: {exc}")
                continue
            self._on_recompiled(project)

    def _on_recompiled(self, project: Project) -> list[str]:
        self.log_line(f"recompiled {self.path.name}: {len(project.cells)} cell(s)")
        planned = plan_rerun(project, self.session)
        # adopt the new project before consulting autorun overlays: a freshly
        # added cell only exists in the new one (KeyError otherwise)
        self.project = project
        self._catalog_cache.invalidate()  # cells/contexts may have changed
        self._refresh_cells()
        self._refresh_detail()
        if not self.armed:
            # one closing line per save, whatever happened
            self.log_line(
                f"changed: {', '.join(planned)} (paused — R runs all, or p to arm)"
                if planned else "nothing to rerun"
            )
            return []
        to_run = [n for n in planned if self.cell_autorun(n)]
        if to_run:
            self.log_line(f"changed -> rerunning: {', '.join(to_run)}")
            self.runs.start(to_run, closure=False, path=self.path, project=project)
        else:
            self.log_line("nothing to rerun")
        return to_run
