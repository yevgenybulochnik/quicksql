"""Backend registry and implementations for QuickSQL."""

from abc import ABC, abstractmethod
from typing import Any

import duckdb
import polars as pl
from google.cloud import bigquery


class BackendRegistry:
    """Registry for backend classes."""

    _backends: dict[str, type["Backend"]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a backend class with a given name."""

        def decorator(backend_class: type["Backend"]) -> type["Backend"]:
            cls._backends[name] = backend_class
            return backend_class

        return decorator

    @classmethod
    def get_backend(cls, name: str) -> type["Backend"]:
        """Get a registered backend class by name."""
        if name not in cls._backends:
            raise ValueError(f"Backend '{name}' is not registered.")
        return cls._backends[name]

    @classmethod
    def clear(cls) -> None:
        """Clear all registered backends (useful for testing)."""
        cls._backends.clear()


class Backend(ABC):
    """Abstract base class for backends."""

    @abstractmethod
    def connect(self, connection_string: str) -> None:
        """Connect to the backend using the given connection string."""
        pass

    @abstractmethod
    def execute(self, query: str) -> Any:
        """Execute a query and return the result."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection to the backend."""
        pass


@BackendRegistry.register("duckdb")
class DuckDBBackend(Backend):
    """Backend for DuckDB."""

    def __init__(self) -> None:
        self.conn: duckdb.DuckDBPyConnection | None = None

    def connect(self, connection_string: str) -> None:
        """Connect to DuckDB database."""
        self.conn = duckdb.connect(connection_string)

    def execute(self, query: str) -> Any:
        """Execute query string and return a polars dataframe."""
        if self.conn is None:
            raise RuntimeError("Backend not connected. Call connect() first.")
        return self.conn.execute(query).pl()

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None


@BackendRegistry.register("bigquery")
class BigQueryBackend(Backend):
    """Backend for Google BigQuery.

    Connection string format: project_id or project_id/location
    Examples:
        - my-project
        - my-project/us-east1

    Authentication uses Application Default Credentials (ADC).
    Set up via: gcloud auth application-default login
    """

    def __init__(self) -> None:
        self.client: bigquery.Client | None = None
        self.project: str | None = None
        self.location: str | None = None

    def connect(self, connection_string: str) -> None:
        """Connect to BigQuery using project ID and optional location.

        Args:
            connection_string: Project ID, optionally with location.
                Formats:
                    - bigquery://project_id
                    - bigquery://project_id/location
                    - project_id
                    - project_id/location
        """
        conn = connection_string.strip()

        # Strip bigquery:// prefix if present
        if conn.lower().startswith("bigquery://"):
            conn = conn[len("bigquery://") :]

        parts = conn.split("/")
        self.project = parts[0]
        self.location = parts[1] if len(parts) > 1 else None

        self.client = bigquery.Client(
            project=self.project,
            location=self.location,
        )

    def execute(self, query: str) -> Any:
        """Execute query and return a Polars DataFrame.

        Args:
            query: SQL query to execute

        Returns:
            Query results as a Polars DataFrame
        """
        if self.client is None:
            raise RuntimeError("Backend not connected. Call connect() first.")

        query_job = self.client.query(query)
        # Convert to Arrow first for efficient conversion to Polars
        arrow_table = query_job.to_arrow()
        result = pl.from_arrow(arrow_table)
        # to_arrow() returns a Table, so from_arrow will always return a DataFrame
        assert isinstance(result, pl.DataFrame)
        return result

    def close(self) -> None:
        """Close the BigQuery client."""
        if self.client:
            self.client.close()
            self.client = None
            self.project = None
            self.location = None
