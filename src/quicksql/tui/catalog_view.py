"""The Catalog tab: the project-wide schema browser, on its own drilling stack."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.widgets import DataTable

from ..sheet import Sheet
from .panes import SheetTable, Surface

if TYPE_CHECKING:
    from .app import QsqlApp


class CatalogView(Surface):
    """The catalog's drilling stack, kept across tab switches: each Sheet
    holds its own cursor/sort/filter for S / Enter to resume."""

    def __init__(self, app: QsqlApp) -> None:
        super().__init__(app)
        self.stack: list[Sheet] = []
        self.subtitle = ""  # the top sheet's
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
