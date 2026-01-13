"""Custom exceptions for QuickSQL configuration validation."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CellValidationError:
    """Validation error for a single field in a cell."""

    cell_name: str
    cell_start_line: int
    field_path: str
    message: str
    invalid_value: Any = None


@dataclass
class QsqlConfigError(Exception):
    """
    Aggregated validation errors across multiple cells.

    Collects all errors and presents them in a user-friendly format
    with file path and line number context.
    """

    file_path: Path
    errors: list[CellValidationError] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Required for Exception to work properly with dataclass
        super().__init__(str(self))

    def __str__(self) -> str:
        if not self.errors:
            return f"Configuration error in {self.file_path}"

        lines = [f"Configuration errors in {self.file_path}:\n"]

        # Group errors by cell
        errors_by_cell: dict[str, list[CellValidationError]] = {}
        for error in self.errors:
            if error.cell_name not in errors_by_cell:
                errors_by_cell[error.cell_name] = []
            errors_by_cell[error.cell_name].append(error)

        for cell_name, cell_errors in errors_by_cell.items():
            first_error = cell_errors[0]
            lines.append(
                f"  Cell '{cell_name}' (line {first_error.cell_start_line + 1}):"
            )
            for error in cell_errors:
                value_str = ""
                if error.invalid_value is not None:
                    value_str = f" [{error.invalid_value!r}]"
                lines.append(f"    - {error.field_path}: {error.message}{value_str}")
            lines.append("")

        return "\n".join(lines)

    def add_error(self, error: CellValidationError) -> None:
        """Add an error to the collection."""
        self.errors.append(error)

    def has_errors(self) -> bool:
        """Check if any errors were collected."""
        return len(self.errors) > 0
