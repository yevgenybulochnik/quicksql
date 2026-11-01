"""The TUI's modal screens: the notebook picker and the help overlay."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from textual import events
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Input, OptionList, Static
from textual.widgets.option_list import Option


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
            ("ctrl+h/ctrl+l", "previous / next tab, from any mode"),
            ("ctrl+k", "up to the cell list without popping (esc)"),
            ("ctrl+j", "down into the sheet (enter on the cell list)"),
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
