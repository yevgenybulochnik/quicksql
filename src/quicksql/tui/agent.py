"""The TUI's side of the live session: what the live host asks of the app
(``live.host.Driver``) — start or cancel a run, read through the session,
repaint the controls, log, focus a cell, show a toast, run something on the
app's loop from a worker thread."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, ContextManager, Optional

if TYPE_CHECKING:
    from .app import QsqlApp


class AppDriver:
    def __init__(self, app: QsqlApp) -> None:
        self.app = app

    def start_run(self, select: Optional[list[str]], closure: bool) -> int:
        return self.app.runs.start(select, closure=closure, path=self.app.path)

    def cancel_run(self) -> None:
        self.app.runs.cancel()

    def reading(self) -> ContextManager[Any]:
        return self.app.runs.reading()

    def connect(self) -> Any:
        return self.app.runs.connect()

    def controls_changed(self) -> None:
        self.app.controls_changed()

    def log_line(self, text: str) -> None:
        self.app.log_line(text)

    def focus_cell(self, cell: str, tab: Optional[str]) -> None:
        self.app.focus_cell(cell, tab)

    def toast(self, message: str, severity: str) -> None:
        self.app.notify(message, title="agent", severity=severity)  # type: ignore[arg-type]

    def post(self, fn: Callable[[], None]) -> None:
        try:
            self.app.call_from_thread(fn)
        except RuntimeError:
            pass  # the app has stopped: nobody is left to tell
