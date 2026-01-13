"""Pydantic models for configuration validation and schema management."""

from typing import Any, Callable

from pydantic import BaseModel, model_validator


class ConnectionResolver:
    """
    Resolves connection strings to backend types.

    Uses registered matcher functions to infer backend from connection string.
    """

    _resolvers: list[tuple[Callable[[str], bool], str]] = []

    @classmethod
    def register(cls, backend_name: str, matcher: Callable[[str], bool]) -> None:
        """Register a matcher function for a backend."""
        cls._resolvers.append((matcher, backend_name))

    @classmethod
    def resolve(cls, connection_string: str) -> str:
        """
        Resolve a connection string to a backend name.

        Returns:
            Backend name (e.g., "duckdb")

        Raises:
            ValueError: If no backend matches the connection string.
        """
        for matcher, backend_name in cls._resolvers:
            if matcher(connection_string):
                return backend_name

        supported = cls._get_supported_formats()
        raise ValueError(
            f"Cannot infer backend from connection string: '{connection_string}'\n"
            f"Supported formats:\n{supported}"
        )

    @classmethod
    def _get_supported_formats(cls) -> str:
        """Get human-readable list of supported formats."""
        return (
            "  - DuckDB: .ddb, .duckdb, :memory:\n"
            "  - BigQuery: bigquery://project_id or bigquery://project_id/location"
        )

    @classmethod
    def clear(cls) -> None:
        """Clear all resolvers (for testing)."""
        cls._resolvers.clear()


def _is_duckdb(conn: str) -> bool:
    """Match .ddb, .duckdb files, or :memory:"""
    path = conn.lower().strip()
    return path.endswith(".ddb") or path.endswith(".duckdb") or path == ":memory:"


# Register built-in resolver for DuckDB
ConnectionResolver.register("duckdb", _is_duckdb)


def _is_bigquery(conn: str) -> bool:
    """Match BigQuery connection strings (bigquery:// prefix)."""
    return conn.strip().lower().startswith("bigquery://")


# Register built-in resolver for BigQuery
ConnectionResolver.register("bigquery", _is_bigquery)


class ConfigSchemaRegistry:
    """
    Registry for backend config schemas.

    Keeps schemas separate from backend implementations.
    """

    _schemas: dict[str, type[BaseModel]] = {}

    @classmethod
    def register(cls, name: str, schema: type[BaseModel]) -> None:
        """Register a config schema for a backend."""
        cls._schemas[name] = schema

    @classmethod
    def get_schema(cls, name: str) -> type[BaseModel]:
        """Get schema by backend name."""
        if name not in cls._schemas:
            raise ValueError(f"No config schema registered for '{name}'")
        return cls._schemas[name]

    @classmethod
    def get_all_schemas(cls) -> dict[str, type[BaseModel]]:
        """Get all registered schemas."""
        return cls._schemas.copy()

    @classmethod
    def clear(cls) -> None:
        """Clear registry (for testing)."""
        cls._schemas.clear()


class DuckDBConfig(BaseModel):
    """Configuration schema for DuckDB backend."""

    duckdb: str


# Register built-in schema
ConfigSchemaRegistry.register("duckdb", DuckDBConfig)


class BigQueryConfig(BaseModel):
    """Configuration schema for BigQuery backend.

    The connection string should be: project_id or project_id/location
    """

    bigquery: str


# Register BigQuery schema
ConfigSchemaRegistry.register("bigquery", BigQueryConfig)


class InputConfig(BaseModel):
    """
    Validated input configuration for a cell.

    Stores the resolved backend name and connection string.
    """

    backend_name: str
    connection_string: str


class CellConfig(BaseModel):
    """
    Full configuration for a cell.

    Supports two input formats:
    1. Simple string (inferred): input: ./test.ddb
    2. Explicit dict: input: {duckdb: ./data.db}

    Output is a simple directory path string:
        output: ./results
    """

    input: InputConfig | None = None
    output: str | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_input(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Parse and normalize the input field."""
        if "input" not in data or data["input"] is None:
            return data

        raw_input = data["input"]

        # Already parsed (InputConfig dict with backend_name)
        if isinstance(raw_input, dict) and "backend_name" in raw_input:
            return data

        # Already an InputConfig instance
        if isinstance(raw_input, InputConfig):
            return data

        # Format 1: Simple string - infer backend
        if isinstance(raw_input, str):
            backend_name = ConnectionResolver.resolve(raw_input)
            data["input"] = InputConfig(
                backend_name=backend_name,
                connection_string=raw_input,
            )
            return data

        # Format 2: Explicit dict - {backend_name: connection_string}
        if isinstance(raw_input, dict):
            if len(raw_input) != 1:
                raise ValueError(
                    f"Explicit input config must have exactly one backend, "
                    f"got {len(raw_input)}: {list(raw_input.keys())}"
                )
            backend_name, connection_string = next(iter(raw_input.items()))
            data["input"] = InputConfig(
                backend_name=backend_name,
                connection_string=str(connection_string),
            )
            return data

        raise ValueError(
            f"Invalid input format: expected string or dict, got {type(raw_input).__name__}"
        )
