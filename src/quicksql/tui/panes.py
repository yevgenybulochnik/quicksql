"""The TUI's sheet surfaces: drilling stacks of Sheets, shown in DataTables.

`SheetTable` renders one Sheet into one DataTable, rebuilding only when what
it shows changed. A `Surface` is a drilling stack plus the verbs on its top
sheet (move, mutate, filter, search, yank); the app routes a sheet mode's keys
to its surface. `DataPanes` is the Data tab's: one stack per pane, and a
vertical split shows two side by side. The Catalog tab's is
`catalog_view.CatalogView`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from textual.coordinate import Coordinate
from textual.widgets import DataTable

from ..sheet import Sheet

if TYPE_CHECKING:
    from .app import QsqlApp


class SheetTable:
    """One DataTable showing one Sheet. Rendering is identity-cached on what
    the table shows, so cursor moves reuse the built table: it rebuilds only
    when the content or the column window changed, and a selection flip
    restyles in place."""

    MAX_ROWS = 200
    MAX_COLS = 40  # window rendered around the cursor; re-windows at the edges
    SELECT_MARK = "▸"

    def __init__(self, table: DataTable) -> None:
        self.table = table
        # (frame, hidden, selected, window start) on screen; holding the frame
        # reference keeps the identity comparison sound
        self.shown: tuple[Any, tuple[str, ...], frozenset[int], int] | None = None

    def clear(self) -> None:
        self.table.clear(columns=True)
        self.shown = None

    @classmethod
    def row_cap(cls, sheet: Sheet) -> int | None:
        # catalog sheets list one row per column/table/dataset — never truncate
        # them, or a wide table would hide fields. data previews stay capped so
        # rebuilding a wide frame stays cheap on every cursor keypress.
        return None if sheet.drill is not None else cls.MAX_ROWS

    def render(self, sheet: Sheet, widths_from: Sheet | None = None) -> str:
        """Show ``sheet`` and return its subtitle. ``widths_from`` is the sheet
        being live-filtered, measured instead of the shrinking rows shown."""
        table = self.table
        cap = self.row_cap(sheet)
        cols = sheet.columns
        cursor_col = min(sheet.cursor[1], max(len(cols) - 1, 0))
        start = 0
        if len(cols) > self.MAX_COLS:
            start = max(0, min(cursor_col - self.MAX_COLS // 2, len(cols) - self.MAX_COLS))
        window = cols[start : start + self.MAX_COLS]
        shown = (sheet.frame, sheet.hidden, sheet.selected, start)
        prev = self.shown
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
            self.shown = shown
            self._restyle_selection(sheet, prev[2], window[0], cap)
        elif prev is None or prev[0] is not shown[0] or prev[1:] != shown[1:]:
            # rebuild only when content or the column window changed; wide
            # frames make rebuilds expensive and cursor moves happen per keypress
            self.shown = shown
            frame = sheet.frame.select(window)
            if cap is not None:
                frame = frame.head(cap)
            rows = [
                ((self.SELECT_MARK if i in sheet.selected else "") + str(row[0]), *map(str, row[1:]))
                for i, row in enumerate(frame.rows())
            ]
            table.clear(columns=True)
            for name, width in zip(window, self._column_widths(window, rows, widths_from)):
                table.add_column(str(name), width=width)
            table.add_rows(rows)
        height = sheet.frame.height if cap is None else min(sheet.frame.height, cap)
        if height:
            table.move_cursor(row=min(sheet.cursor[0], height - 1), column=cursor_col - start)
        picked = f" · {len(sheet.selected)} selected" if sheet.selected else ""
        span = ""
        if len(cols) > self.MAX_COLS:
            span = f" · cols {start + 1}-{start + len(window)}/{len(cols)}"
        return f"{sheet.title} · {sheet.frame.height}x{len(cols)}{picked}{span}"

    def _restyle_selection(
        self, sheet: Sheet, prev_selected: frozenset[int], col0: str, cap: int | None
    ) -> None:
        """Repaint just the first-column cells whose selection flipped, marker
        on or off — update_cell_at(update_width=False) touches only cell content,
        so the table keeps its dimensions and scroll and the viewport holds still."""
        limit = sheet.frame.height if cap is None else min(sheet.frame.height, cap)
        values = sheet.frame[col0]
        for i in sorted(prev_selected ^ sheet.selected):
            if 0 <= i < limit:
                mark = self.SELECT_MARK if i in sheet.selected else ""
                self.table.update_cell_at(Coordinate(i, 0), mark + str(values[i]), update_width=False)

    def _column_widths(
        self, window: list[str], rows: list[tuple[str, ...]], base: Sheet | None
    ) -> list[int]:
        """Explicit column widths. While a filter is being typed the shown rows
        are a shrinking subset — measure the captured base sheet instead, so
        widths hold still keystroke to keystroke."""
        if base is not None and all(c in base.frame.columns for c in window):
            source = base.frame.select(window)
            cap = self.row_cap(base)
            if cap is not None:
                source = source.head(cap)
            rows = [tuple(map(str, row)) for row in source.rows()]
        widths = [len(str(name)) for name in window]
        for row in rows:
            for i, cell in enumerate(row):
                widths[i] = max(widths[i], len(cell))
        return widths


class Surface:
    """A drilling stack shown in a SheetTable, and the verbs on its top sheet.
    Subclasses provide ``stack``, ``focused()`` and ``refresh()``."""

    stack: list[Sheet]

    def __init__(self, app: QsqlApp) -> None:
        self.app = app
        self.filter_base: Sheet | None = None  # the sheet being live-filtered

    def focused(self) -> SheetTable:
        raise NotImplementedError

    def refresh(self) -> None:
        raise NotImplementedError

    def mutate(self, fn: Callable[[Sheet], Sheet]) -> None:
        if not self.stack:
            return
        self.stack[-1] = fn(self.stack[-1])
        self.refresh()

    def move(self, d_row: int = 0, d_col: int = 0, top: bool = False, bottom: bool = False) -> None:
        if top:
            self.mutate(lambda s: s.top())
        elif bottom:
            self.mutate(lambda s: s.bottom())
        else:
            self.mutate(lambda s: self.reconcile_to_viewport(s).move(d_row, d_col))

    def reconcile_to_viewport(self, sheet: Sheet) -> Sheet:
        """Snap the cursor row into the table's visible window. The mouse
        wheel scrolls the viewport but not the Sheet cursor, so without this a
        keyboard move would resume from the stale (often top) cursor and snap
        the view away from what the user is looking at. A no-op during normal
        keyboard navigation, where the cursor is always already on screen."""
        table = self.focused().table
        height = table.scrollable_content_region.height
        if not height:
            return sheet
        first = int(table.scroll_y)
        last = min(first + height - 1, sheet.frame.height - 1)
        row = min(max(sheet.cursor[0], first), last)
        return sheet if row == sheet.cursor[0] else sheet.move(row - sheet.cursor[0], 0)

    def begin_filter(self) -> bool:
        """Capture the top sheet to live-filter; False with nothing to filter."""
        if not self.stack:
            return False
        self.filter_base = self.stack[-1]
        return True

    def live_filter(self, pattern: str) -> None:
        """Re-filter the captured sheet, on every keystroke."""
        if self.filter_base is None or not self.stack:
            return
        self.stack[-1] = self.filter_base.filtered(pattern)
        self.refresh()

    def commit_filter(self) -> None:
        base, self.filter_base = self.filter_base, None
        if base is not None and self.stack and self.stack[-1] is not base:
            # keep the filtered sheet on top, the base beneath (q restores it)
            self.stack.insert(len(self.stack) - 1, base)
            self.refresh()

    def cancel_filter(self) -> None:
        if self.filter_base is not None and self.stack:
            self.stack[-1] = self.filter_base
            self.refresh()
        self.filter_base = None

    def yank(self) -> tuple[str, int, str] | None:
        """What y copies from the top sheet — the cursor's value, or the
        selected rows' values ",\\n"-joined — as (text, how many, column)."""
        if not self.stack or self.stack[-1].frame.height == 0:
            return None
        sheet = self.stack[-1]
        col = sheet.current_column
        if sheet.selected:
            values = [str(sheet.frame[col][i]) for i in sorted(sheet.selected)]
            return ",\n".join(values), len(values), col  # paste-ready as a SQL select list
        return str(sheet.frame[col][sheet.cursor[0]]), 1, col


class DataPanes(Surface):
    """The Data tab: one drilling stack per pane. A vertical split appends a
    second stack, shown side by side and navigated on its own; ``stack`` is
    the focused pane's, so the verbs are oblivious to the split."""

    TABLE_IDS = ("#data", "#data2")

    def __init__(self, app: QsqlApp) -> None:
        super().__init__(app)
        self.panes: list[list[Sheet]] = [[]]
        self.active = 0
        self.subtitle = ""  # the focused pane's top sheet's
        self._tables: list[SheetTable] = []

    @property
    def tables(self) -> list[SheetTable]:
        if not self._tables:  # bound on first use: the DataTables mount after __init__
            self._tables = [SheetTable(self.app.query_one(i, DataTable)) for i in self.TABLE_IDS]
        return self._tables

    @property
    def stack(self) -> list[Sheet]:  # type: ignore[override]
        return self.panes[self.active]

    @stack.setter
    def stack(self, value: list[Sheet]) -> None:
        self.panes[self.active] = value

    @property
    def split(self) -> bool:
        return len(self.panes) > 1

    def focused(self) -> SheetTable:
        return self.tables[self.active]

    def reset(self) -> None:
        """One empty pane, for a fresh notebook."""
        self.panes = [[]]
        self.active = 0
        for table in self.tables:
            table.shown = None
        self.sync_layout()

    def sync_layout(self) -> None:
        self.app.query_one("#data_panes").set_class(self.split, "-split")
        for i, table in enumerate(self.tables):
            table.table.set_class(self.split and i == self.active, "-active")

    def collapse(self, keep: int) -> None:
        """Drop back to a single pane, keeping stack ``keep``."""
        self.panes = [self.panes[keep]]
        self.active = 0
        self.tables[0].shown = None
        self.tables[1].clear()
        self.sync_layout()

    def toggle_split(self) -> None:
        if not self.stack:
            return
        if self.split:
            self.collapse(self.active)  # unsplit: keep what's focused
        else:
            # a shallow copy is an independent stack (Sheets are frozen), so the
            # panes diverge as each is navigated; focus lands on the new pane
            self.panes.append(list(self.stack))
            self.active = 1
            for table in self.tables:
                table.shown = None
            self.sync_layout()
        self.refresh()

    def switch(self) -> None:
        if not self.split:
            return
        self.active ^= 1
        self.sync_layout()
        self.refresh()  # moves the -active border and the subtitle

    def refresh(self) -> None:
        # render every pane; each SheetTable's cache keeps the unfocused pane
        # from rebuilding when only the focused one changed
        self.subtitle = ""
        for idx, table in enumerate(self.tables):
            if idx >= len(self.panes) or not self.panes[idx]:
                table.clear()
                continue
            active = idx == self.active
            subtitle = table.render(self.panes[idx][-1], self.filter_base if active else None)
            if active:
                side = ("L / " if idx == 0 else "R / ") if self.split else ""
                self.subtitle = side + subtitle
        self.app.refresh_subtitle()
