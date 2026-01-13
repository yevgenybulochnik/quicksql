"""File watcher for QuickSQL files."""

from pathlib import Path
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from .file import QsqlFile
from .executor import ExecutorBuilder, ExecutorComponent


class QsqlFileEventHandler(FileSystemEventHandler):
    """Handles file modification events for QSQL files."""

    def __init__(
        self,
        file_path: Path,
        enable_logging: bool = True,
        enable_output: bool = True,
    ):
        self.file_path = file_path
        self._file = QsqlFile(file_path)
        self._enable_logging = enable_logging
        self._enable_output = enable_output
        self._executor: ExecutorComponent = self._build_executor()
        self.last_modified: dict[Path, float] = {}
        self.previous_cells: dict[str, str] = {}
        self._init_cell_state()

    def _build_executor(self) -> ExecutorComponent:
        """Build executor with configured decorators."""
        builder = ExecutorBuilder(self._file)
        if self._enable_logging:
            builder = builder.with_logging()
        if self._enable_output:
            builder = builder.with_output()
        return builder.build()

    def _init_cell_state(self) -> None:
        """Initialize cell state tracking."""
        for cell in self._file.parsed_cells:
            self.previous_cells[cell["cell_name"]] = cell["text"]

    def _refresh_executor(self) -> None:
        """Refresh executor with new file content, preserving connections."""
        self._file = QsqlFile(self.file_path)
        self._executor.refresh(self._file)

    def _detect_changed_cells(self) -> list[str]:
        """Detect which cells have changed."""
        changed_cells = []

        current_cells = {
            cell["cell_name"]: cell["text"] for cell in self._file.parsed_cells
        }

        for cell_name, cell_text in current_cells.items():
            previous_text = self.previous_cells.get(cell_name)

            if previous_text is None:
                changed_cells.append(cell_name)
                print(f"New cell detected: {cell_name}")
            elif previous_text != cell_text:
                changed_cells.append(cell_name)
                print(f"Cell changed: {cell_name}")

        for cell_name in self.previous_cells:
            if cell_name not in current_cells:
                print(f"Cell removed: {cell_name}")

        self.previous_cells = current_cells

        return changed_cells

    def on_modified(self, event) -> None:
        """Handle file modification events."""
        if event.src_path != str(self.file_path):
            return

        current_time = time.time()

        # debounce
        if self.file_path in self.last_modified:
            if current_time - self.last_modified[self.file_path] < 1:
                return

        self.last_modified[self.file_path] = current_time

        print(f"Detected modification in {self.file_path}")

        # Refresh executor to pick up file changes (preserves connections)
        self._refresh_executor()

        changed_cells = self._detect_changed_cells()
        print("changed_cells:", changed_cells)

        for cell_name in changed_cells:
            try:
                df = self._executor.execute_cell(cell_name)
                print(df)
            except Exception as e:
                print(f"Error executing cell {cell_name}: {e}")
