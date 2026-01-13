"""QuickSQL - SQL file management and execution tool."""

from .core import (
    # File
    QsqlFile,
    # Backends
    Backend,
    BackendRegistry,
    DuckDBBackend,
    BigQueryBackend,
    # Executor
    ExecutorComponent,
    BaseExecutor,
    ExecutorDecorator,
    LoggingDecorator,
    OutputDecorator,
    ExecutorBuilder,
    # Models
    ConnectionResolver,
    ConfigSchemaRegistry,
    CellConfig,
    InputConfig,
    DuckDBConfig,
    BigQueryConfig,
    # Errors
    QsqlConfigError,
    CellValidationError,
)
from .parsers import Parser, ParserRegistry, KeyValueParser, DictLikeParser

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
    # Parsers
    "Parser",
    "ParserRegistry",
    "KeyValueParser",
    "DictLikeParser",
]
