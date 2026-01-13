"""Example watcher implementation using the new executor pattern."""

from __future__ import annotations

from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time

from .executor import ExecutorBuilder
from .file import QsqlFile


class QsqlFileHandler(FileSystemEventHandler):
    """Handles file modification events for QSQL files."""

    def __init__(self, qsql_file: Path):
        self.qsql_file = qsql_file.resolve()
        self.last_modified: dict[Path, float] = {}

        # Store previous state of each cell
        self.previous_cells: dict[str, str] = {}
        self._initialize_cell_state()

    def _initialize_cell_state(self) -> None:
        """Store initial state of all cells."""
        qsql_file = QsqlFile(self.qsql_file)
        for block in qsql_file.cell_blocks:
            self.previous_cells[block["cell_name"]] = block["text"]

    def _detect_changed_cells(self) -> list[str]:
        """Detect which cells have changed."""
        changed_cells = []

        try:
            qsql_file = QsqlFile(self.qsql_file)
            current_cells = {
                block["cell_name"]: block["text"] for block in qsql_file.cell_blocks
            }

            # Check for modified cells
            for cell_name, current_text in current_cells.items():
                previous_text = self.previous_cells.get(cell_name)

                if previous_text is None:
                    # New cell added
                    changed_cells.append(cell_name)
                    print(f"  + New cell: {cell_name}")
                elif previous_text != current_text:
                    # Cell modified
                    changed_cells.append(cell_name)
                    print(f"  * Modified: {cell_name}")

            # Check for deleted cells
            for cell_name in self.previous_cells:
                if cell_name not in current_cells:
                    print(f"  - Deleted: {cell_name}")

            # Update previous state
            self.previous_cells = current_cells

        except Exception as e:
            print(f"  Warning: Error detecting changes: {e}")
            return []

        return changed_cells

    def on_modified(self, event) -> None:
        """Handle file modification events."""
        if event.src_path != str(self.qsql_file):
            return

        current_time = time.time()

        # Debounce: ignore if modified less than 1 second ago
        if self.qsql_file in self.last_modified:
            if current_time - self.last_modified[self.qsql_file] < 1.0:
                return

        self.last_modified[self.qsql_file] = current_time

        print(f"\nDetected change in {self.qsql_file.name}")

        try:
            # Detect which cells changed
            changed_cells = self._detect_changed_cells()

            if not changed_cells:
                print("  No cell changes detected (possibly header/comment changes)")
                return

            # Re-execute only changed cells using new executor
            qsql_file = QsqlFile(self.qsql_file)
            executor = ExecutorBuilder(qsql_file).build()

            for cell_name in changed_cells:
                try:
                    executor.execute_cell(cell_name)
                    print(f"  Executed: {cell_name}")
                except Exception as e:
                    print(f"  Error executing {cell_name}: {e}")

            if hasattr(executor, "close"):
                executor.close()  # type: ignore[union-attr]

        except Exception as e:
            print(f"Error processing changes: {e}")


class QsqlWatcher:
    """Watches a QSQL file for changes and re-executes only modified queries."""

    def __init__(self, qsql_file: Path):
        self.qsql_file = Path(qsql_file).resolve()
        self._observer: Observer | None = None

    def start(self, execute_all_initially: bool = True) -> None:
        """Start watching the file.

        Args:
            execute_all_initially: If True, execute all queries once before watching.
                                  If False, only execute when changes are detected.
        """
        print(f"Starting QSQL watcher for {self.qsql_file.name}")

        # Optionally execute all queries initially
        if execute_all_initially:
            print("  Executing all queries initially...")
            qsql_file = QsqlFile(self.qsql_file)
            executor = ExecutorBuilder(qsql_file).build()

            cell_names = [cell["cell_name"] for cell in qsql_file.parsed_cells]
            executor.execute_many(cell_names)

            if hasattr(executor, "close"):
                executor.close()  # type: ignore[union-attr]
            print()

        # Set up file watcher
        event_handler = QsqlFileHandler(self.qsql_file)
        self._observer = Observer()
        self._observer.schedule(
            event_handler, str(self.qsql_file.parent), recursive=False
        )
        self._observer.start()

        print(f"Watching for changes... (Press Ctrl+C to stop)")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self) -> None:
        """Stop watching the file."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            print("\nStopped watching")
