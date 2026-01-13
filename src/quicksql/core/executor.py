"""Decorator-based executor system for QuickSQL."""

from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable, Callable
import logging
import time

from pydantic import ValidationError

from .file import QsqlFile
from .manager import Backend, BackendRegistry
from .models import CellConfig
from .errors import QsqlConfigError, CellValidationError


logger = logging.getLogger(__name__)


def validate_cells(
    qsql_file: QsqlFile, skip_validation: bool = False
) -> list[dict[str, Any]]:
    """
    Validate all cell configs, collecting all errors.

    Args:
        qsql_file: The QsqlFile to validate.
        skip_validation: If True, skip raising errors on invalid configs.

    Returns:
        List of cells with validated_config added to each.

    Raises:
        QsqlConfigError: If any cell has invalid configuration and skip_validation is False.
    """
    error_collector = QsqlConfigError(file_path=qsql_file.file_path)
    validated_cells: list[dict[str, Any]] = []

    for cell in qsql_file.parsed_cells:
        cell_name = cell["cell_name"]
        cell_start = cell["cell_start"]
        config = cell.get("config", {})

        try:
            validated_config = CellConfig.model_validate(config)
            validated_cells.append(
                {
                    **cell,
                    "validated_config": validated_config,
                }
            )
        except ValidationError as e:
            for err in e.errors():
                field_path = ".".join(str(loc) for loc in err["loc"])
                error_collector.add_error(
                    CellValidationError(
                        cell_name=cell_name,
                        cell_start_line=cell_start,
                        field_path=field_path,
                        message=err["msg"],
                        invalid_value=err.get("input"),
                    )
                )
            validated_cells.append(
                {
                    **cell,
                    "validated_config": None,
                }
            )
        except ValueError as e:
            # Catch ConnectionResolver errors
            error_collector.add_error(
                CellValidationError(
                    cell_name=cell_name,
                    cell_start_line=cell_start,
                    field_path="input",
                    message=str(e),
                    invalid_value=config.get("input"),
                )
            )
            validated_cells.append(
                {
                    **cell,
                    "validated_config": None,
                }
            )

    if not skip_validation and error_collector.has_errors():
        raise error_collector

    return validated_cells


@runtime_checkable
class ExecutorComponent(Protocol):
    """Protocol for all executor components (base + decorators)."""

    def execute_cell(self, cell_name: str) -> Any:
        """Execute a single cell by name."""
        ...

    def execute_many(self, cell_names: list[str]) -> dict[str, Any]:
        """Execute multiple cells, returning results by name."""
        ...

    def get_config_fields(self) -> dict[str, type]:
        """Return config fields this component requires."""
        ...

    def get_cell_config(self, cell_name: str) -> CellConfig | None:
        """Get the validated config for a cell."""
        ...

    def refresh(self, qsql_file: QsqlFile) -> None:
        """Refresh cells from a new QsqlFile, preserving connections."""
        ...

    def close(self) -> None:
        """Close all backend connections."""
        ...


class BaseExecutor:
    """
    Core executor that handles backend connections and cell execution.

    This is the innermost component in the decorator chain.
    """

    def __init__(
        self,
        qsql_file: QsqlFile,
        validated_cells: list[dict[str, Any]],
        skip_validation: bool = False,
    ):
        self._file = qsql_file
        self._validated_cells = validated_cells
        self._skip_validation = skip_validation
        self._backend_conns: dict[str, Backend] = {}
        self._cells_by_name: dict[str, dict[str, Any]] = {
            cell["cell_name"]: cell for cell in validated_cells
        }

    def _get_or_create_backend(self, backend_name: str, conn_string: str) -> Backend:
        """Get existing backend connection or create a new one."""
        cache_key = f"{backend_name}:{conn_string}"
        if cache_key not in self._backend_conns:
            backend_class = BackendRegistry.get_backend(backend_name)
            backend_instance = backend_class()
            backend_instance.connect(conn_string)
            self._backend_conns[cache_key] = backend_instance
        return self._backend_conns[cache_key]

    def execute_cell(self, cell_name: str) -> Any:
        """Execute a single cell by name."""
        cell = self._cells_by_name.get(cell_name)
        if cell is None:
            raise ValueError(f"Cell '{cell_name}' not found")

        validated_config: CellConfig = cell["validated_config"]
        if validated_config.input is None:
            raise ValueError(f"Cell '{cell_name}' has no input configuration")

        backend = self._get_or_create_backend(
            validated_config.input.backend_name,
            validated_config.input.connection_string,
        )

        return backend.execute(cell["text"])

    def execute_many(self, cell_names: list[str]) -> dict[str, Any]:
        """Execute multiple cells, returning results by name."""
        return {name: self.execute_cell(name) for name in cell_names}

    def get_config_fields(self) -> dict[str, type]:
        """Base executor has no additional config requirements."""
        return {}

    def get_cell_config(self, cell_name: str) -> CellConfig | None:
        """Get the validated config for a cell."""
        cell = self._cells_by_name.get(cell_name)
        if cell is None:
            return None
        return cell.get("validated_config")

    def refresh(self, qsql_file: QsqlFile) -> None:
        """
        Refresh cells from a new QsqlFile, preserving backend connections.

        Re-parses and validates the file, updating internal cell state
        while keeping existing database connections cached.

        Args:
            qsql_file: The new QsqlFile to use.

        Raises:
            QsqlConfigError: If any cell has invalid configuration.
        """
        self._file = qsql_file
        self._validated_cells = validate_cells(qsql_file, self._skip_validation)
        self._cells_by_name = {
            cell["cell_name"]: cell for cell in self._validated_cells
        }

    def close(self) -> None:
        """Close all backend connections."""
        for backend in self._backend_conns.values():
            backend.close()
        self._backend_conns.clear()


class ExecutorDecorator(ABC):
    """
    Abstract base class for executor decorators (structural decorator pattern).

    Subclass this to add behavior before/after cell execution.
    """

    def __init__(self, wrapped: ExecutorComponent):
        self._wrapped = wrapped

    def execute_cell(self, cell_name: str) -> Any:
        """Default: delegate to wrapped component."""
        return self._wrapped.execute_cell(cell_name)

    def execute_many(self, cell_names: list[str]) -> dict[str, Any]:
        """Execute multiple cells, using this decorator's execute_cell."""
        # Call our own execute_cell to ensure decorator behavior is applied
        return {name: self.execute_cell(name) for name in cell_names}

    @abstractmethod
    def get_config_fields(self) -> dict[str, type]:
        """Return config fields this decorator requires."""
        ...

    def get_cell_config(self, cell_name: str) -> CellConfig | None:
        """Get the validated config for a cell, delegating to wrapped component."""
        return self._wrapped.get_cell_config(cell_name)

    def refresh(self, qsql_file: QsqlFile) -> None:
        """Refresh cells, delegating to wrapped component."""
        self._wrapped.refresh(qsql_file)

    def close(self) -> None:
        """Close resources, delegating to wrapped component."""
        self._wrapped.close()


class LoggingDecorator(ExecutorDecorator):
    """Decorator that logs cell execution timing."""

    def __init__(
        self, wrapped: ExecutorComponent, log: logging.Logger | None = None
    ) -> None:
        super().__init__(wrapped)
        self._log = log or logger

    def execute_cell(self, cell_name: str) -> Any:
        self._log.info(f"Executing cell '{cell_name}'")
        start = time.perf_counter()
        try:
            result = self._wrapped.execute_cell(cell_name)
            elapsed = time.perf_counter() - start
            self._log.info(f"Cell '{cell_name}' completed in {elapsed:.3f}s")
            return result
        except Exception as e:
            elapsed = time.perf_counter() - start
            self._log.error(f"Cell '{cell_name}' failed after {elapsed:.3f}s: {e}")
            raise

    def get_config_fields(self) -> dict[str, type]:
        return {}


class OutputDecorator(ExecutorDecorator):
    """
    Decorator that writes query results to parquet files.

    Reads the 'output' config from each cell to determine the output directory.
    Files are written as {output_dir}/{cell_name}.parquet.
    """

    def __init__(self, wrapped: ExecutorComponent) -> None:
        super().__init__(wrapped)
        self._log = logger

    def execute_cell(self, cell_name: str) -> Any:
        """Execute cell and write result to parquet if output is configured."""
        result = self._wrapped.execute_cell(cell_name)

        # Get output config for this cell
        config = self.get_cell_config(cell_name)
        if config is not None and config.output is not None:
            self._write_parquet(cell_name, config.output, result)

        return result

    def _write_parquet(self, cell_name: str, output_dir: str, result: Any) -> None:
        """Write result to parquet file."""
        from pathlib import Path

        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Write parquet file
        file_path = output_path / f"{cell_name}.parquet"

        try:
            # Result should be a Polars DataFrame
            result.write_parquet(file_path)
            self._log.info(f"Wrote output to {file_path}")
        except AttributeError:
            # Result doesn't have write_parquet method
            self._log.warning(
                f"Cannot write output for cell '{cell_name}': "
                f"result type {type(result).__name__} does not support write_parquet"
            )
        except Exception as e:
            self._log.error(f"Failed to write output for cell '{cell_name}': {e}")
            raise

    def get_config_fields(self) -> dict[str, type]:
        return {"output": str}


class ExecutorBuilder:
    """
    Fluent builder for constructing decorated executors.

    Validates all cell configs eagerly at build time, collecting
    all errors before raising.
    """

    def __init__(self, qsql_file: QsqlFile):
        self._file = qsql_file
        self._decorator_factories: list[
            Callable[[ExecutorComponent], ExecutorComponent]
        ] = []
        self._skip_validation = False

    def with_logging(self, log: logging.Logger | None = None) -> "ExecutorBuilder":
        """Add logging decorator."""
        self._decorator_factories.append(lambda wrapped: LoggingDecorator(wrapped, log))
        return self

    def with_output(self) -> "ExecutorBuilder":
        """Add output decorator to write results to parquet files."""
        self._decorator_factories.append(lambda wrapped: OutputDecorator(wrapped))
        return self

    def with_decorator(
        self, decorator_factory: Callable[[ExecutorComponent], ExecutorComponent]
    ) -> "ExecutorBuilder":
        """
        Add a custom decorator.

        Args:
            decorator_factory: Callable that takes wrapped ExecutorComponent
                              and returns a decorated ExecutorComponent.
        """
        self._decorator_factories.append(decorator_factory)
        return self

    def without_validation(self) -> "ExecutorBuilder":
        """Skip config validation (not recommended for production)."""
        self._skip_validation = True
        return self

    def build(self) -> ExecutorComponent:
        """
        Build the executor chain.

        Validates all cell configs eagerly, collecting all errors
        before raising QsqlConfigError.

        Returns:
            Fully constructed executor with all decorators applied.

        Raises:
            QsqlConfigError: If any cell has invalid configuration.
        """
        validated_cells = validate_cells(self._file, self._skip_validation)

        # Build the decorator chain (innermost to outermost)
        executor: ExecutorComponent = BaseExecutor(
            self._file, validated_cells, self._skip_validation
        )
        for factory in self._decorator_factories:
            executor = factory(executor)

        return executor
