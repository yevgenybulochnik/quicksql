"""VisiData-style Textual TUI: master-detail over a quicksql project, reflect-only.

Top: the cell list (engine -> sink, autorun, status, rows). Below: tabs for
SQL (t toggles raw/rendered), Data (a stack of Polars Sheets with vim keys),
Config, Log, and Catalog (the project-wide schema browser, on its own
drilling stack). Nothing runs on startup: the first R (run all) arms autorun
(or p arms it without running), after which a background watcher recompiles on
save and reruns autorun cells. The file is edited in your own editor; the TUI
never writes it.

Keys: j/k cell rows . h/l cycle detail tabs . gg/G top/bottom . Enter dive into
the Data sheet (then j/k/h/l move its cursor; q climbs back out) . [ ] sort .
- hide col . s/gs select . F frequency . I describe . / search, n/N next/prev .
f filter rows by regex (live; Enter commits, Esc cancels) . y yank the cell
(or selected rows' column, ",\n"-joined) to the clipboard .
Catalog tab (S jumps in; browse via h/l, Enter dives, then drills
context/dataset/table down to field paths, q pops; levels are cached —
ctrl+r refetches the current one) .
| split the data pane into two side-by-side sheets, w switch the focused one .
t raw/rendered . a/A cell/global autorun . r/R run cell/all . p arm watch
(autorun on/off without a full run) .
V real VisiData . o open/switch notebook (auto-opens as a picker when the
file doesn't exist; creating from a template prompts for the new file's
name) . ? help overlay (all keys) . q pop/quit
"""

from __future__ import annotations

import shutil
import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import polars as pl
import yaml
from rich.syntax import Syntax
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Header, Input, OptionList, RichLog, Static, TabbedContent, TabPane
from textual.widgets.option_list import Option

from .catalog import CatalogCache, CatalogNode, project_root_node
from .compiler import Project, compile_file
from .errors import QsqlError
from .models import RunEvent, RunResult
from .runner import RunSession, run_project
from .sheet import Sheet
from .watcher import config_only_changes, hashes_of, plan_rerun


class NotebookPicker(ModalScreen):
    """Choose a notebook in the directory, or create one from a starting
    template (builtin, plus any under ~/.quicksql/templates). Picking a template
    prompts for the new file's name, prefilled with a default — Enter accepts,
    Esc steps back to the list."""

    BINDINGS = [Binding("escape", "cancel", "cancel")]

    CSS = """
    NotebookPicker { align: center middle; }
    #picker { width: 64; max-height: 20; border: solid $primary; padding: 1; }
    #picker_name { display: none; }
    #picker_status { color: $warning; }
    """

    def __init__(self, directory: Path, creates: list[tuple[str, str]]) -> None:
        super().__init__()
        self.directory = directory
        self.creates = creates  # (default target filename, template name) pairs
        self._template: str | None = None  # set while the name prompt is up

    def compose(self) -> ComposeResult:
        # notebooks only: .qsql, or .qsql.sql for editors that want SQL
        # highlighting — plain .sql files (dumps, migrations) are noise here
        notebooks = sorted(self.directory.glob("*.qsql")) + sorted(
            self.directory.glob("*.qsql.sql")
        )
        options = [Option(f"open    {f.name}", id=f"open:{f}") for f in notebooks]
        options += [
            Option(f"create  {target} — template {name!r}", id=f"template:{i}")
            for i, (target, name) in enumerate(self.creates)
        ]
        with Vertical(id="picker"):
            yield Static("select a notebook — Enter opens, Esc cancels")
            yield OptionList(*options)
            yield Input(id="picker_name", placeholder="new notebook name...")
            yield Static(id="picker_status")

    def on_mount(self) -> None:
        self.query_one(OptionList).focus()

    def on_key(self, event: events.Key) -> None:
        if self.query_one("#picker_name", Input).has_focus:
            return  # typing a name: j/k are just characters
        if event.character == "j":
            self.query_one(OptionList).action_cursor_down()
        elif event.character == "k":
            self.query_one(OptionList).action_cursor_up()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        kind, _, value = (event.option.id or "").partition(":")
        if kind == "template":
            target, self._template = self.creates[int(value)]
            name_input = self.query_one("#picker_name", Input)
            name_input.value = target  # the default name, ready to accept or edit
            name_input.styles.display = "block"
            name_input.focus()
        else:
            self.dismiss(("open", value))

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._template is None:
            return
        name = event.value.strip()
        if not name:
            return
        if not name.endswith((".qsql", ".qsql.sql")):
            name += ".qsql"
        if (self.directory / name).exists():
            self.query_one("#picker_status", Static).update(
                f"{name} already exists — pick another name"
            )
            return
        self.dismiss(("template", (name, self._template)))

    def _close_prompt(self) -> None:
        self._template = None
        self.query_one("#picker_name", Input).styles.display = "none"
        self.query_one("#picker_status", Static).update("")
        self.query_one(OptionList).focus()

    def action_cancel(self) -> None:
        if self._template is not None:  # Esc from the name prompt: back to the list
            self._close_prompt()
            return
        self.dismiss(None)


class HelpScreen(ModalScreen):
    """`?` overlay: every key, grouped. Most sheet/vim keys live only in
    on_key and show in no footer binding, so this is the single place they
    are all documented. ``?`` / Esc / q dismiss."""

    BINDINGS = [
        Binding("question_mark", "close", "close", key_display="?"),
        Binding("escape", "close", "close"),
        Binding("q", "close", "close"),
    ]

    CSS = """
    HelpScreen { align: center middle; }
    #help { width: 76; height: 80%; max-height: 34; border: round $primary; background: $surface; }
    #help_title { padding: 0 2; text-style: bold; }
    #help_body { height: 1fr; padding: 1 2; }
    """

    # (section, [(keys, what), ...]); alternatives are "/"-joined so the keys
    # flatten back to the individual bindings documented (see the TUI test)
    SECTIONS: list[tuple[str, list[tuple[str, str]]]] = [
        ("Cells", [
            ("j/k", "move the cell selection"),
            ("gg/G", "first / last cell"),
            ("h/l", "cycle detail tabs (SQL / Data / Config / Log / Catalog)"),
            ("enter", "dive into the Data sheet"),
            ("/", "search cell names"),
            ("r/R", "run cell / run all (R also arms watch reruns)"),
            ("p", "arm / pause watch reruns without running (autorun)"),
            ("a/A", "which cells autorun: this cell / global filter"),
            ("t", "SQL raw ↔ rendered"),
            ("S", "jump into the Catalog tab"),
            ("V", "open the cell's output in VisiData"),
            ("o", "open / switch notebook"),
        ]),
        ("Data sheet", [
            ("j/k/h/l", "move the cursor"),
            ("gg/G", "top / bottom row"),
            ("ctrl+u/ctrl+d", "page up / down"),
            ("[/]", "sort ascending / descending"),
            ("-", "hide the current column"),
            ("s/gs", "select row / select all"),
            ("f", "filter rows by regex (live; Enter commits)"),
            ("n/N", "next / previous search match"),
            ("F", "frequency table of the column"),
            ("I", "describe (summary stats)"),
            ("y", "yank cell / selected column to clipboard"),
            ("q", "pop the sheet (back a level)"),
            ("esc", "back to the cell list without popping"),
        ]),
        ("Catalog tab", [
            ("S/5", "jump to the Catalog tab and dive in (resumes a kept dive)"),
            ("enter", "dive, then drill: context → dataset → table → fields"),
            ("esc", "back to the cell list; the dive is kept"),
            ("ctrl+r", "refetch the current level"),
            ("q", "pop back a level, then out to the cell list"),
        ]),
        ("Split", [
            ("|", "split the data pane in two (or unsplit)"),
            ("w", "switch the focused pane (each navigates on its own)"),
        ]),
        ("General", [
            ("1/2/3/4/5", "jump to SQL / Data / Config / Log / Catalog"),
            ("?", "this help"),
            ("q", "pop / quit"),
        ]),
    ]

    def compose(self) -> ComposeResult:
        from rich.console import Group
        from rich.table import Table
        from rich.text import Text

        blocks: list[Any] = []
        for i, (title, rows) in enumerate(self.SECTIONS):
            if i:
                blocks.append(Text())
            blocks.append(Text(title, style="bold"))
            grid = Table.grid(padding=(0, 3))
            grid.add_column(justify="right", style="cyan", no_wrap=True)
            grid.add_column(overflow="fold")
            for keys, what in rows:  # Text() so "[/]" et al. aren't read as markup
                grid.add_row(Text(keys), Text(what))
            blocks.append(grid)
        with Vertical(id="help"):
            yield Static("quicksql keys — ? or Esc to close", id="help_title")
            with VerticalScroll(id="help_body"):
                yield Static(Group(*blocks))

    def action_close(self) -> None:
        self.dismiss(None)


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
    ) -> None:
        super().__init__()
        self.path = Path(path)
        self.overrides = overrides or {}
        self.watch = watch
        self.auto_run = auto_run
        self.armed = False  # watch reruns stay dormant until the first run-all
        self.project: Project | None = None
        self.session = RunSession()  # per notebook, reused across its reruns
        # runs in flight per session: a replaced session is closed by whoever
        # leaves it last — the switch if it's idle, else that run's finally.
        # Closing a DuckDB connection under another thread's query risks a crash.
        self._session_lock = threading.Lock()
        self._session_runs: dict[RunSession, int] = {}
        self.results: dict[str, RunResult] = {}
        self.running: set[str] = set()
        # bumped on the UI thread per run; events/results tagged with an older
        # generation come from a replaced (still-draining) worker and are
        # logged but never allowed to touch table state
        self._run_gen = 0
        self.autorun_off: set[str] = set()
        self.autorun_global = True
        self.show_rendered = True
        self.mode = "cells"  # or "data" / "catalog"
        # one drilling stack per pane; a vertical split appends a second stack.
        # sheet_stack (property, below) is the active surface's stack, so the
        # rest of the app is oblivious to the split — and to the catalog.
        self.panes: list[list[Sheet]] = [[]]
        self.active_pane = 0
        self.catalog_stack: list[Sheet] = []  # the Catalog tab's own dive
        self._catalog_dive_pending = False  # S pressed while the root loads
        self._catalog_root_loading = False  # a root load is in flight
        self.hashes: dict[str, str] = {}
        self._row_names: list[str] = []
        self._pending_g = False
        self._last_search = ""
        self._input_mode = "search"  # what the bottom input edits: search | filter
        self._filter_base: Sheet | None = None  # sheet being live-filtered
        self._catalog_cache = CatalogCache()  # cleared on run/recompile/switch
        # what each sheet table currently displays (slots 0/1 = panes,
        # slot 2 = the catalog table); holding the frame reference keeps
        # identity comparison sound
        self._data_shown: list[tuple[Any, tuple[str, ...], frozenset[int], int] | None] = [None] * 3

    # ---------- panes (vertical split) ----------

    SHEET_MODES = ("data", "catalog")  # modes whose keys live in a sheet

    @property
    def sheet_stack(self) -> list[Sheet]:
        """The active surface's drilling stack: the catalog's own stack in
        catalog mode, else the focused pane's. Reads and in-place mutations
        (append/pop/insert/[-1]=) operate on that stack; assigning a new list
        replaces it (see the setter). Routing by mode is what lets the sheet
        verbs (filter/yank/search/freq/drill/pop) work unchanged on either
        surface."""
        if self.mode == "catalog":
            return self.catalog_stack
        return self.panes[self.active_pane]

    @sheet_stack.setter
    def sheet_stack(self, value: list[Sheet]) -> None:
        if self.mode == "catalog":
            self.catalog_stack = value
        else:
            self.panes[self.active_pane] = value

    @property
    def split(self) -> bool:
        return len(self.panes) > 1

    PANE_TABLE_IDS = ("#data", "#data2")

    def _pane_table(self, idx: int) -> DataTable:
        return self.query_one(self.PANE_TABLE_IDS[idx], DataTable)

    def _active_table(self) -> DataTable:
        if self.mode == "catalog":
            return self.query_one("#catalog", DataTable)
        return self._pane_table(self.active_pane)

    def _sync_split_layout(self) -> None:
        self.query_one("#data_panes").set_class(self.split, "-split")
        for i in (0, 1):
            self._pane_table(i).set_class(self.split and i == self.active_pane, "-active")

    def _collapse_panes(self, keep: int) -> None:
        """Drop back to a single pane, keeping stack ``keep``."""
        self.panes = [self.panes[keep]]
        self.active_pane = 0
        self._data_shown[:2] = [None, None]  # pane slots only; catalog keeps its cache
        self._pane_table(1).clear(columns=True)
        self._sync_split_layout()

    def _toggle_split(self) -> None:
        if self.mode != "data" or not self.sheet_stack:
            return
        if self.split:
            self._collapse_panes(self.active_pane)  # unsplit: keep what's focused
        else:
            # a shallow copy is an independent stack (Sheets are frozen), so the
            # panes diverge as each is navigated; focus lands on the new pane
            self.panes.append(list(self.panes[self.active_pane]))
            self.active_pane = 1
            self._data_shown[:2] = [None, None]  # pane slots only
            self._sync_split_layout()
        self._refresh_data()

    def _switch_pane(self) -> None:
        if not self.split:
            return
        self.active_pane ^= 1
        self._sync_split_layout()
        self._refresh_data()  # moves the -active border and the active sub_title

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

    def _load_notebook(self, path: Path | str) -> None:
        """Open a notebook (fresh state), arming and watching per-notebook."""
        self.path = Path(path)
        # the old session's temps/statuses describe the old notebook, and an
        # in-flight run of it must not paint this one
        self._replace_session()
        self._run_gen += 1
        self.results = {}
        self.running = set()
        self.panes = [[]]
        self.active_pane = 0
        self.catalog_stack = []
        self._catalog_dive_pending = False
        self._catalog_root_loading = False
        self._catalog_cache.invalidate()
        self._data_shown = [None] * 3
        self._refresh_catalog()  # the old listing must not show under this notebook
        self._sync_split_layout()
        self.mode = "cells"
        self.armed = False
        self._refresh_armed_indicator()
        self.project = None
        try:
            self.project = compile_file(self.path, self.overrides)
        except QsqlError as exc:
            self.log_line(f"compile error: {exc}")
            return
        self.hashes = hashes_of(self.project)
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
        from .scaffold import template_names

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
                from .scaffold import write_scaffold

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
        self.session.close()

    def _replace_session(self) -> None:
        with self._session_lock:
            old, self.session = self.session, RunSession()
            idle = old not in self._session_runs
        if idle:
            old.close()

    @contextmanager
    def _session_in_use(self, session: RunSession):
        with self._session_lock:
            self._session_runs[session] = self._session_runs.get(session, 0) + 1
        try:
            yield
        finally:
            with self._session_lock:
                left = self._session_runs.pop(session) - 1
                if left:
                    self._session_runs[session] = left
                retired = not left and session is not self.session
            if retired:  # its notebook was switched away mid-run
                session.close()

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
        from .watcher import should_rerun

        configured = should_rerun(self.project.cells[name])
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
        label = "rendered" if self.show_rendered else "source"
        self.query_one("#sql_view", Static).update(
            Syntax(sql, "sql", line_numbers=True, word_wrap=True)
        )
        self.query_one("#config_view", Static).update(
            yaml.safe_dump(cell.config.model_dump(mode="json"), sort_keys=True)
        )
        stack = f" · sheets:{len(self.sheet_stack)}" if self.sheet_stack else ""
        self.sub_title = f"{name} · sql:{label}{stack}"

    MAX_DATA_ROWS = 200
    MAX_DATA_COLS = 40  # window rendered around the cursor; re-windows at the edges

    def _row_cap(self, sheet: Sheet) -> int | None:
        # catalog sheets list one row per column/table/dataset — never truncate
        # them, or a wide table would hide fields. data previews stay capped so
        # rebuilding a wide frame stays cheap on every cursor keypress.
        return None if sheet.drill is not None else self.MAX_DATA_ROWS

    def _refresh_data(self) -> None:
        # render every pane; the identity-cached _data_shown[idx] keeps the
        # unfocused pane from rebuilding when only the focused one changed
        for idx in (0, 1):
            table = self._pane_table(idx)
            if idx >= len(self.panes) or not self.panes[idx]:
                table.clear(columns=True)
                self._data_shown[idx] = None
                continue
            self._render_pane(idx, table)

    _SELECT_MARK = "▸"
    CATALOG_SLOT = 2  # the catalog table's _data_shown slot

    def _refresh_catalog(self) -> None:
        table = self.query_one("#catalog", DataTable)
        if not self.catalog_stack:
            table.clear(columns=True)
            self._data_shown[self.CATALOG_SLOT] = None
            return
        self._render_sheet_table(
            self.catalog_stack[-1],
            table,
            slot=self.CATALOG_SLOT,
            active=self.mode == "catalog",
        )

    def _refresh_surface(self) -> None:
        """Repaint the active drilling surface (catalog vs data panes)."""
        if self.mode == "catalog":
            self._refresh_catalog()
        else:
            self._refresh_data()

    def _render_pane(self, idx: int, table: DataTable) -> None:
        active = idx == self.active_pane
        side = ("L / " if idx == 0 else "R / ") if self.split else ""
        self._render_sheet_table(self.panes[idx][-1], table, slot=idx, active=active, side=side)

    def _render_sheet_table(
        self, sheet: Sheet, table: DataTable, slot: int, active: bool, side: str = ""
    ) -> None:
        """Render a sheet into a DataTable, identity-cached per ``slot`` so
        cursor moves reuse the built table (rebuilds only on content/window
        changes, selection flips restyle in place). Shared by the data panes
        (slots 0/1); ``side`` prefixes the sub_title when the pane is split."""
        cap = self._row_cap(sheet)
        cols = sheet.columns
        cursor_col = min(sheet.cursor[1], max(len(cols) - 1, 0))
        start = 0
        if len(cols) > self.MAX_DATA_COLS:
            start = max(0, min(cursor_col - self.MAX_DATA_COLS // 2, len(cols) - self.MAX_DATA_COLS))
        window = cols[start : start + self.MAX_DATA_COLS]
        shown = (sheet.frame, sheet.hidden, sheet.selected, start)
        prev = self._data_shown[slot]
        selection_only = (
            prev is not None
            and window
            and prev[0] is shown[0]
            and prev[1] == shown[1]
            and prev[3] == shown[3]
            and prev[2] != shown[2]
        )
        if selection_only:
            # same content and columns, only the marked rows differ: flip just
            # those cells' markers. A clear()+rebuild would set Textual's
            # _require_update_dimensions, which defers the cursor re-scroll a
            # frame and makes a scrolled viewport visibly jump on select.
            self._data_shown[slot] = shown
            self._restyle_selection(table, sheet, prev[2], window[0], cap)
        elif prev is None or prev[0] is not shown[0] or prev[1:] != shown[1:]:
            # rebuild only when content or the column window changed; wide
            # frames make rebuilds expensive and cursor moves happen per keypress
            self._data_shown[slot] = shown
            frame = sheet.frame.select(window)
            if cap is not None:
                frame = frame.head(cap)
            rows = [
                ((self._SELECT_MARK if i in sheet.selected else "") + str(row[0]), *map(str, row[1:]))
                for i, row in enumerate(frame.rows())
            ]
            table.clear(columns=True)
            for name, width in zip(window, self._column_widths(window, rows, active=active)):
                table.add_column(str(name), width=width)
            table.add_rows(rows)
        height = sheet.frame.height if cap is None else min(sheet.frame.height, cap)
        if height:
            table.move_cursor(row=min(sheet.cursor[0], height - 1), column=cursor_col - start)
        if active:
            picked = f" · {len(sheet.selected)} selected" if sheet.selected else ""
            span = ""
            if len(cols) > self.MAX_DATA_COLS:
                span = f" · cols {start + 1}-{start + len(window)}/{len(cols)}"
            self.sub_title = f"{side}{sheet.title} · {sheet.frame.height}x{len(cols)}{picked}{span}"

    def _restyle_selection(
        self, table: DataTable, sheet: Sheet, prev_selected: frozenset[int], col0: str, cap: int | None
    ) -> None:
        """Repaint just the first-column cells whose selection flipped, marker
        on or off — update_cell_at(update_width=False) touches only cell content,
        so the table keeps its dimensions and scroll and the viewport holds still."""
        limit = sheet.frame.height if cap is None else min(sheet.frame.height, cap)
        values = sheet.frame[col0]
        for i in sorted(prev_selected ^ sheet.selected):
            if 0 <= i < limit:
                mark = self._SELECT_MARK if i in sheet.selected else ""
                table.update_cell_at(Coordinate(i, 0), mark + str(values[i]), update_width=False)

    def _column_widths(
        self, window: list[str], rows: list[tuple[str, ...]], active: bool = True
    ) -> list[int]:
        """Explicit column widths for the data table. While a filter is being
        typed the shown rows are a shrinking subset — measure the captured
        base sheet instead, so widths hold still keystroke to keystroke. Only
        the focused pane is ever being filtered, so the base applies there."""
        base = self._filter_base
        if active and base is not None and all(c in base.frame.columns for c in window):
            source = base.frame.select(window)
            cap = self._row_cap(base)
            if cap is not None:
                source = source.head(cap)
            rows = [tuple(map(str, row)) for row in source.rows()]
        widths = [len(str(name)) for name in window]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(cell))
        return widths

    def _mutate_sheet(self, fn) -> None:
        if self.mode not in self.SHEET_MODES or not self.sheet_stack:
            return
        self.sheet_stack[-1] = fn(self.sheet_stack[-1])
        self._refresh_surface()

    def _push_sheet(self, sheet: Sheet) -> None:
        """Push onto the active surface: derived sheets (freq/describe) stay
        on the catalog in catalog mode; otherwise dive into the Data tab."""
        self.sheet_stack.append(sheet)
        if self.mode == "catalog":
            self._refresh_catalog()
            return
        self.mode = "data"
        self.query_one(TabbedContent).active = "tab_data"
        self._refresh_data()

    PREVIEW_ROWS = 100

    def _read_parquet_head(self, target: str) -> pl.DataFrame:
        """Preview through duckdb's parquet reader, never polars' — the polars
        reader panics on duckdb-written INTERVAL columns, and duckdb only reads
        the head row groups for a LIMIT."""
        import duckdb

        from .runner import _preview

        con = duckdb.connect()
        try:
            return _preview(con, f"read_parquet('{target}')", self.PREVIEW_ROWS)
        finally:
            con.close()

    def _preview_frame(self, name: str) -> pl.DataFrame:
        result = self.results.get(name)
        if result is not None and isinstance(result.preview, pl.DataFrame):
            return result.preview
        if result is not None and result.ok and result.target and result.target.endswith(".parquet"):
            return self._read_parquet_head(result.target)
        target = self.project.root / "data" / f"{name}.parquet" if self.project else None
        if target and target.exists():
            return self._read_parquet_head(str(target))
        return pl.DataFrame({"info": [f"no output for {name!r} yet — press r to run"]})

    # ---------- actions (footer bindings) ----------

    def action_pop_or_quit(self) -> None:
        if self.mode == "catalog" and self.catalog_stack:
            self.catalog_stack.pop()
            self._refresh_catalog()  # an emptied stack clears the table
            if self.catalog_stack:
                return
            self.mode = "cells"  # popped the root: back out to the cell list
            self.query_one(TabbedContent).active = "tab_sql"
            self._refresh_detail()
            return
        if self.mode == "data" and self.sheet_stack:
            self.sheet_stack.pop()
            if self.sheet_stack:
                self._refresh_data()
                return
            if self.split:  # focused pane emptied: keep the other, full width
                self._collapse_panes(1 - self.active_pane)
                self._refresh_data()
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
            self._run_worker([self.current_cell], closure=True)

    def action_run_all(self) -> None:
        self.armed = True
        self._refresh_armed_indicator()
        self._run_worker(None, closure=True)

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
            self._refresh_catalog()
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

    def _restore_subtitle(self) -> None:
        if self.mode == "catalog" and self.catalog_stack:
            self._refresh_catalog()
        elif self.mode == "data" and self.sheet_stack:
            self._refresh_data()
        else:
            self._refresh_detail()

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
        if self.mode not in self.SHEET_MODES or not self.sheet_stack:
            return
        self._input_mode = "filter"
        self._filter_base = self.sheet_stack[-1]
        self._show_input("filter rows by regex...")

    def _cancel_filter(self) -> None:
        if self._filter_base is not None and self.sheet_stack:
            self.sheet_stack[-1] = self._filter_base
            self._refresh_surface()
        self._filter_base = None
        self._input_mode = "search"

    def _yank(self) -> None:
        if self.mode not in self.SHEET_MODES or not self.sheet_stack:
            return
        sheet = self.sheet_stack[-1]
        if sheet.frame.height == 0:
            return
        col = sheet.current_column
        if sheet.selected:
            values = [str(sheet.frame[col][i]) for i in sorted(sheet.selected)]
            text = ",\n".join(values)  # paste-ready as a SQL select list
        else:
            values = [str(sheet.frame[col][sheet.cursor[0]])]
            text = values[0]
        self.copy_to_clipboard(text)
        self.notify(f"copied {len(values)} value(s) from {col!r}")

    def action_page(self, direction: int) -> None:
        if self.mode in self.SHEET_MODES:
            self._mutate_sheet(lambda s: self._reconcile_to_viewport(s).move(direction * 20, 0))
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
            self._switch_pane()
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
            self._collapse_panes(self.active_pane)
            self._refresh_data()
        self.mode = "cells"
        self._refresh_detail()

    def _reconcile_to_viewport(self, sheet: Sheet) -> Sheet:
        """Snap the cursor row into the data table's visible window. The mouse
        wheel scrolls the viewport but not the Sheet cursor, so without this a
        keyboard move would resume from the stale (often top) cursor and snap
        the view away from what the user is looking at. A no-op during normal
        keyboard navigation, where the cursor is always already on screen."""
        table = self._active_table()
        height = table.scrollable_content_region.height
        if not height:
            return sheet
        first = int(table.scroll_y)
        last = min(first + height - 1, sheet.frame.height - 1)
        row = min(max(sheet.cursor[0], first), last)
        return sheet if row == sheet.cursor[0] else sheet.move(row - sheet.cursor[0], 0)

    def _move(self, d_row: int = 0, d_col: int = 0, top: bool = False, bottom: bool = False) -> None:
        if self.mode in self.SHEET_MODES:
            if top:
                self._mutate_sheet(lambda s: s.top())
            elif bottom:
                self._mutate_sheet(lambda s: s.bottom())
            else:
                self._mutate_sheet(lambda s: self._reconcile_to_viewport(s).move(d_row, d_col))
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
        if self._input_mode != "filter" or self._filter_base is None or not self.sheet_stack:
            return
        # live: re-filter the captured base on every keystroke
        self.sheet_stack[-1] = self._filter_base.filtered(event.value.strip())
        self._refresh_surface()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if self._input_mode == "filter":
            event.input.value = ""
            self._hide_search()
            base, self._filter_base = self._filter_base, None
            self._input_mode = "search"
            if base is not None and self.sheet_stack and self.sheet_stack[-1] is not base:
                # commit: keep the filtered sheet on top, base beneath (q restores)
                self.sheet_stack.insert(len(self.sheet_stack) - 1, base)
                self._refresh_surface()
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
        Leaving the Data tab drops its dive; leaving the Catalog tab only hands
        keys back to the cell list and keeps its stack, so coming back (click,
        h/l, S) shows the same sheet at the same row. Either way a direct jump
        never leaves keys routed to an invisible stack."""
        active = self.query_one(TabbedContent).active
        if active == "tab_data":
            # must not touch a "data" mode here: _push_sheet dives (sets the
            # mode) *before* activating the tab, and this handler fires after
            self._leave_catalog_dive()
            if not self.sheet_stack:
                self._show_current_data()
        elif active == "tab_catalog":
            self._drop_data_dive()
            self._load_catalog_root()
        else:
            self._drop_data_dive()
            self._leave_catalog_dive()
            self._refresh_detail()

    def _drop_data_dive(self) -> None:
        if self.mode == "data":
            self.mode = "cells"
        if self.split:  # the split is a data-mode concern; drop it on the way out
            self._collapse_panes(0)
        self.panes[self.active_pane] = []

    def _leave_catalog_dive(self) -> None:
        # the stack stays: its Sheets keep cursor/sort/filter/hidden columns
        # for S / Enter to resume. A notebook switch or q past the root resets it.
        if self.mode == "catalog":
            self.mode = "cells"

    def _show_current_data(self) -> None:
        name = self.current_cell
        if name:
            self.sheet_stack = [Sheet(self._preview_frame(name), title=name)]
            self._refresh_data()

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
                self._refresh_catalog()
            return
        name = self.current_cell
        if not name:
            return
        self.sheet_stack = []
        self._push_sheet(Sheet(self._preview_frame(name), title=name))

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table.id != "cells" or self.mode != "cells":
            return
        self._refresh_detail()
        if self.query_one(TabbedContent).active == "tab_data":
            self._show_current_data()  # the Data sheet follows the selection

    # ---------- workers ----------

    @work(thread=True, exclusive=True, group="catalog")
    def _catalog_worker(self, node: CatalogNode, replace: bool = False) -> None:
        self.call_from_thread(setattr, self, "sub_title", f"loading {node.title}…")
        try:
            frame, _ = self._catalog_cache.load(node)
        except Exception as exc:
            self.call_from_thread(self.log_line, f"catalog {node.title}: {exc}")
            self.call_from_thread(self.notify, f"catalog: {exc}", severity="error")
            self.call_from_thread(setattr, self, "_catalog_dive_pending", False)
            self.call_from_thread(setattr, self, "_catalog_root_loading", False)
            self.call_from_thread(self._restore_subtitle)  # drop the loading… note
            return
        sheet = Sheet(frame, title=node.title, drill=node)
        if replace:  # refetch: swap the current sheet, keep the stack shape
            self.call_from_thread(self._replace_top_sheet, sheet)
        else:
            self.call_from_thread(self._push_catalog_sheet, sheet)

    def _push_catalog_sheet(self, sheet: Sheet) -> None:
        self._catalog_root_loading = False  # a root push lands on an empty stack
        self.catalog_stack.append(sheet)
        if self._catalog_dive_pending:  # S while the root was loading
            self._catalog_dive_pending = False
            self.mode = "catalog"
        self._refresh_catalog()
        if self.mode != "catalog":  # browse push: drop the loading… subtitle
            self._restore_subtitle()

    def _replace_top_sheet(self, sheet: Sheet) -> None:
        if self.catalog_stack:
            self.catalog_stack[-1] = sheet
            self._refresh_catalog()
        else:
            self._push_catalog_sheet(sheet)

    def _run_worker(self, select: Optional[list[str]], closure: bool = True) -> None:
        """Bump the run generation (on the UI thread) and hand off to the
        thread worker. `exclusive` only *flags* the old worker cancelled — a
        blocking run_project keeps draining events, so the generation tag is
        what keeps a replaced run from repainting the table."""
        self._run_gen += 1
        # bound now: a notebook switch before the thread starts must not
        # redirect this run to the new notebook or its session
        self._run_thread(select, closure, self._run_gen, self.path, self.session)

    @work(thread=True, exclusive=True, group="run")
    def _run_thread(
        self,
        select: Optional[list[str]],
        closure: bool,
        gen: int,
        path: Path,
        session: RunSession,
    ) -> None:
        with self._session_in_use(session):
            try:
                project = compile_file(path, self.overrides)
            except QsqlError as exc:
                self.call_from_thread(self.log_line, f"compile error: {exc}")
                return
            self.call_from_thread(self._adopt_project, gen, project)
            results: list[RunResult] = []
            try:
                results = run_project(
                    project,
                    select=select,
                    closure=closure,
                    session=session,
                    on_event=lambda e: self.call_from_thread(self._on_run_event, gen, e),
                )
            except Exception:  # never leave cells stuck at "…" on a runner crash
                import traceback

                self.call_from_thread(self.log_line, f"run failed:\n{traceback.format_exc()}")
            finally:
                self.call_from_thread(self._apply_results, gen, results)

    def _adopt_project(self, gen: int, project: Project) -> None:
        if gen != self._run_gen:
            return  # a replaced run's compile may be older, or another notebook's
        self.project = project
        self.hashes = hashes_of(project)
        self._refresh_cells()

    def _on_run_event(self, gen: int, event: RunEvent) -> None:
        """Route the runner's live events: everything is logged; run_started
        seeds the pending set from the *post-expansion* cell list (closure,
        missing-temp and not-ok upstreams included — the select list undercounts those),
        and each cell_finished retires its cell so status flips one at a time."""
        self.log_line(event.line())
        result = event.result if event.kind == "cell_finished" else None
        if result is not None and result.status == "failed" and result.error:
            # the full message, once per result: the line above keeps only its
            # first line (a skipped cell's line already says it all)
            self.log_line(f"{event.cell} failed:\n{result.error}")
        if gen != self._run_gen:
            return  # a replaced run may still be draining: log it, never show it
        if event.kind == "run_started":
            self.running = set(event.cells or ())
            self._refresh_cells()
        elif event.kind == "cell_finished" and event.cell is not None:
            self.running.discard(event.cell)
            if event.result is not None:
                self.results[event.cell] = event.result
            self._refresh_cells()

    def _apply_results(self, gen: int, results: list[RunResult]) -> None:
        self._catalog_cache.invalidate()  # outputs and warehouses just changed
        if gen != self._run_gen:
            return  # a newer run owns the table; its own events paint it
        for result in results:  # normally a no-op: cell_finished stored each
            self.results[result.cell] = result
        self.running.clear()
        self._refresh_cells()
        self._refresh_detail()

    @work(exclusive=True, group="watch")
    async def _watch_worker(self) -> None:
        import watchfiles

        from .watcher import touches

        # watch the directory: atomic editor saves (nvim) replace the inode,
        # which silently kills a watch on the file path itself
        async for changes in watchfiles.awatch(self.path.parent):
            if not touches(changes, self.path):
                continue
            try:
                project = compile_file(self.path, self.overrides)
            except QsqlError as exc:
                self.log_line(f"compile error: {exc}")
                continue
            self._on_recompiled(project)

    def _on_recompiled(self, project: Project) -> list[str]:
        self.log_line(f"recompiled {self.path.name}: {len(project.cells)} cell(s)")
        planned = plan_rerun(self.hashes, project)
        cfg_only = config_only_changes(self.project, project) if self.project else []
        if cfg_only:
            self.log_line(f"config changed (no rerun): {', '.join(cfg_only)}")
        # adopt the new project before consulting autorun overlays: a freshly
        # added cell only exists in the new one (KeyError otherwise)
        self.project = project
        self.hashes = hashes_of(project)
        self._catalog_cache.invalidate()  # cells/contexts may have changed
        self._refresh_cells()
        self._refresh_detail()
        if not self.armed:
            if planned:
                self.log_line(
                    f"changed: {', '.join(planned)} (paused — R runs all, or p to arm)"
                )
            return []
        to_run = [n for n in planned if self.cell_autorun(n)]
        if to_run:
            self.log_line(f"changed -> rerunning: {', '.join(to_run)}")
            self._run_worker(to_run, closure=False)
        return to_run
