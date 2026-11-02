"""VisiData-style sheet operations: pure functions over a Polars frame.

The TUI's Data pane is a stack of these; every operation returns a new Sheet,
so they unit-test without any terminal.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any, Callable

import polars as pl


@dataclass(frozen=True)
class Sheet:
    frame: pl.DataFrame
    title: str = "data"
    cursor: tuple[int, int] = (0, 0)
    hidden: tuple[str, ...] = ()
    selected: frozenset[int] = field(default_factory=frozenset)
    # opaque catalog payload (never invoked here): replace()-based ops carry
    # it, derived sheets (freq/describe) drop it
    drill: Any = None
    # the frame is only the first `sample` rows of a larger output (the TUI's
    # preview); every sheet computed from it carries that on
    sample: int | None = None
    # the last sort, (column, descending): a reload of the frame re-applies it
    order: tuple[str, bool] | None = None

    @property
    def columns(self) -> list[str]:
        return [c for c in self.frame.columns if c not in self.hidden]

    def visible(self) -> pl.DataFrame:
        return self.frame.select(self.columns)

    @property
    def current_column(self) -> str:
        return self.columns[self.cursor[1]]

    def _clamp(self, row: int, col: int) -> tuple[int, int]:
        max_row = max(self.frame.height - 1, 0)
        max_col = max(len(self.columns) - 1, 0)
        return (min(max(row, 0), max_row), min(max(col, 0), max_col))

    def move(self, d_row: int, d_col: int) -> Sheet:
        return replace(self, cursor=self._clamp(self.cursor[0] + d_row, self.cursor[1] + d_col))

    def top(self) -> Sheet:
        return replace(self, cursor=(0, self.cursor[1]))

    def bottom(self) -> Sheet:
        return replace(self, cursor=self._clamp(self.frame.height - 1, self.cursor[1]))

    def sort(self, desc: bool = False) -> Sheet:
        col = self.current_column
        return replace(self, frame=self.frame.sort(col, descending=desc), order=(col, desc))

    def reloaded(self, frame: pl.DataFrame, key: Callable[[dict[str, Any]], Any] | None = None) -> Sheet:
        """This sheet over ``frame``, a fresh load of what it shows: its sort
        re-applied, the hidden columns still there kept hidden, the cursor on
        the same column and on the row ``key`` names as it did (else the same
        position). The selection named rows by position, so it goes."""
        order = self.order if self.order and self.order[0] in frame.columns else None
        if order:
            frame = frame.sort(order[0], descending=order[1])
        hidden = tuple(c for c in self.hidden if c in frame.columns)
        if len(hidden) == frame.width:
            hidden = ()
        row = self.cursor[0]
        if key is not None and self.frame.height:
            name = key(self.frame.row(row, named=True))
            row = next((i for i, r in enumerate(frame.iter_rows(named=True)) if key(r) == name), row)
        out = replace(self, frame=frame, hidden=hidden, selected=frozenset(), order=order)
        was = self.current_column if self.columns else None
        col = out.columns.index(was) if was in out.columns else self.cursor[1]
        return replace(out, cursor=out._clamp(row, col))

    def hide_current(self) -> Sheet:
        if len(self.columns) <= 1:
            return self
        hidden = (*self.hidden, self.current_column)
        out = replace(self, hidden=hidden)
        return replace(out, cursor=out._clamp(*out.cursor))

    def toggle_select(self) -> Sheet:
        row = self.cursor[0]
        selected = set(self.selected)
        selected.symmetric_difference_update({row})
        return replace(self, selected=frozenset(selected))

    def select_all(self) -> Sheet:
        if len(self.selected) == self.frame.height:
            return replace(self, selected=frozenset())
        return replace(self, selected=frozenset(range(self.frame.height)))

    def freq(self) -> Sheet:
        col = self.current_column
        frame = (
            self.frame.group_by(col)
            .len(name="count")
            .with_columns((pl.col("count") / self.frame.height).alias("share"))
            .sort("count", descending=True)
        )
        return Sheet(frame=frame, title=f"freq({col})", sample=self.sample)

    def describe(self) -> Sheet:
        return Sheet(frame=self.frame.describe(), title=f"describe({self.title})", sample=self.sample)

    def filtered(self, pattern: str) -> Sheet:
        if not pattern:
            return self
        # nested columns (struct/list) don't cast to strings; match the rest
        cols = [c for c in self.columns if not self.frame.schema[c].is_nested()]
        if not cols:
            return self
        matcher = pl.any_horizontal(
            [pl.col(c).cast(pl.Utf8).fill_null("").str.contains(f"(?i){pattern}") for c in cols]
        )
        try:
            frame = self.frame.filter(matcher)
        except pl.exceptions.ComputeError:  # incomplete regex while typing
            return self
        return replace(
            self,
            frame=frame,
            title=f"filter({pattern})",
            cursor=(0, self.cursor[1]),
            selected=frozenset(),
        )

    def search(self, needle: str, reverse: bool = False) -> Sheet:
        if not needle or self.frame.height == 0:
            return self
        needle = needle.lower()
        rows = self.visible().rows()
        n = len(rows)
        step = -1 if reverse else 1
        for offset in range(1, n + 1):
            idx = (self.cursor[0] + step * offset) % n
            if any(needle in str(value).lower() for value in rows[idx]):
                return replace(self, cursor=(idx, self.cursor[1]))
        return self
