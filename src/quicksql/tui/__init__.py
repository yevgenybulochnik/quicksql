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

from .app import QsqlApp
from .screens import HelpScreen, NotebookPicker

__all__ = ["HelpScreen", "NotebookPicker", "QsqlApp"]
