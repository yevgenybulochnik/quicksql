"""The Catalog tab: the project-wide schema browser, on its own drilling stack.

`CatalogView` loads the notebook's catalog (`catalog.project_root_node`) one
level at a time on a thread worker, into a stack of Sheets whose ``drill`` is
the CatalogNode each came from; Enter drills a row into its child node. Frames
are cached by node (`catalog.CatalogCache`) and dropped when a run, a save or
a notebook switch changes what they describe; ctrl+r refetches one level.
The stack is kept across tab switches: each Sheet holds its own
cursor/sort/filter for S / Enter to resume.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING

from textual.widgets import DataTable

from ..catalog import CatalogCache, CatalogNode, project_root_node
from ..sheet import Sheet
from .panes import SheetTable, Surface

if TYPE_CHECKING:
    from .app import QsqlApp


class CatalogView(Surface):
    def __init__(self, app: QsqlApp) -> None:
        super().__init__(app)
        self.stack: list[Sheet] = []
        self.subtitle = ""  # the top sheet's
        self.cache = CatalogCache()
        self.loading: str | None = None  # the title of a load in flight
        self.dive_pending = False  # S pressed while the root loads
        self._root_loading = False  # a root load is in flight
        self._table: SheetTable | None = None

    def focused(self) -> SheetTable:
        if self._table is None:  # bound on first use: the DataTable mounts after __init__
            self._table = SheetTable(self.app.query_one("#catalog", DataTable))
        return self._table

    def refresh(self, deferred: bool = False) -> None:
        table = self.focused()
        if not self.stack:
            table.clear()
            self.subtitle = ""
            return
        active = self.app.mode == "catalog"
        if active and not table.table.size and not deferred:
            # S/5/ctrl+h/l just showed the tab (or a key came in the same frame)
            # and it isn't laid out yet: a cursor placed now scrolls into a 0x0
            # viewport, jumping the scroll, and a sizeless widget marks nothing
            # to repaint, so the terminal keeps the old lines. Render once it
            # has its size — once only, as a compact terminal never gives it one.
            self.app.call_after_refresh(self.refresh, deferred=True)
            return
        self.subtitle = table.render(self.stack[-1], self.filter_base if active else None)
        self.app.refresh_subtitle()

    def reset(self) -> None:
        """A notebook switch: no stack, nothing cached or pending."""
        self.stack = []
        self.dive_pending = False
        self._root_loading = False
        self.cache.invalidate()
        self.refresh()  # the old listing must not show under the new notebook

    def dive(self) -> None:
        """S / 5, once the app shows the tab: back into the kept dive at its
        top sheet, or straight into the root listing when it lands."""
        if self.stack:
            self.app.mode = "catalog"
            self.refresh()
        else:
            self.dive_pending = True
            # started here, not left to the tab's activation: when the tab is
            # already active, no activation fires
            self.load_root()

    def load_root(self) -> None:
        """Load the root listing unless there's a stack or a load in flight —
        S/5, the tab activation and a notebook switch can all ask at once."""
        project = self.app.project
        if self.stack or self._root_loading or not project:
            return
        self._root_loading = True
        self._load(project_root_node(project, connect=self.app.runs.connect))

    def drill(self) -> None:
        """Enter on a row: load the level beneath it (a leaf has none)."""
        sheet = self.stack[-1]
        node: CatalogNode = sheet.drill
        if node.child is None or sheet.frame.height == 0:
            return  # a leaf (field-path sheet): nowhere further down
        child = node.child(sheet.frame.row(sheet.cursor[0], named=True))
        if child is not None:
            self._load(child)

    def refetch(self) -> None:
        """ctrl+r: drop the top sheet's cached frame and reload it in place."""
        node: CatalogNode | None = self.stack[-1].drill if self.stack else None
        if node is None:
            return
        self.cache.invalidate(node)
        self._load(node, replace=True)

    def _load(self, node: CatalogNode, replace: bool = False) -> None:
        work = partial(self._fetch, node, replace)
        self.app.run_worker(work, thread=True, exclusive=True, group="catalog")

    def _fetch(self, node: CatalogNode, replace: bool) -> None:
        """On the worker thread: load ``node``'s frame, then hand the sheet to
        the UI thread — pushed, or swapped in for the top one on a refetch."""
        call = self.app.call_from_thread
        call(self._started, node.title)
        try:
            frame, _ = self.cache.load(node)
        except Exception as exc:
            call(self._failed, node.title, exc)
            return
        sheet = Sheet(frame, title=node.title, drill=node)
        call(self._replace_top if replace else self._push, sheet)

    def _started(self, title: str) -> None:
        self.loading = title
        self.app.refresh_subtitle()

    def _ended(self) -> None:
        self.loading = None
        self.app.refresh_subtitle()

    def _failed(self, title: str, exc: Exception) -> None:
        self.app.log_line(f"catalog {title}: {exc}")
        self.app.notify(f"catalog: {exc}", severity="error")
        self.dive_pending = False
        self._root_loading = False
        self._ended()  # drop the loading… note

    def _push(self, sheet: Sheet) -> None:
        self._root_loading = False  # a root push lands on an empty stack
        self.stack.append(sheet)
        if self.dive_pending:  # S while the root was loading
            self.dive_pending = False
            self.app.mode = "catalog"
        self.refresh()
        self._ended()

    def _replace_top(self, sheet: Sheet) -> None:
        if self.stack:
            self.stack[-1] = sheet
            self.refresh()
            self._ended()
        else:
            self._push(sheet)
