"""Core module for QuickSQL."""

from .file import QsqlFile
from .manager import Backend, BackendRegistry, DuckDBBackend, BigQueryBackend
from .executor import (
    ExecutorComponent,
    BaseExecutor,
    ExecutorDecorator,
    LoggingDecorator,
    OutputDecorator,
    ExecutorBuilder,
)
from .models import (
    ConnectionResolver,
    ConfigSchemaRegistry,
    CellConfig,
    InputConfig,
    DuckDBConfig,
    BigQueryConfig,
)
from .errors import QsqlConfigError, CellValidationError

__all__ = [
    # File
    "QsqlFile",
    # Backends
    "Backend",
    "BackendRegistry",
    "DuckDBBackend",
    "BigQueryBackend",
    # Executor
    "ExecutorComponent",
    "BaseExecutor",
    "ExecutorDecorator",
    "LoggingDecorator",
    "OutputDecorator",
    "ExecutorBuilder",
    # Models
    "ConnectionResolver",
    "ConfigSchemaRegistry",
    "CellConfig",
    "InputConfig",
    "DuckDBConfig",
    "BigQueryConfig",
    # Errors
    "QsqlConfigError",
    "CellValidationError",
]
