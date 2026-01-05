from abc import ABC, abstractmethod
from typing import Any
from pprint import pprint

import duckdb


from .file import QsqlFile


class BackendRegistry:
    """Registry for backend classes."""

    _backends = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a backend class with a given name."""

        def decorator(backend_class):
            cls._backends[name] = backend_class
            return backend_class

        return decorator

    @classmethod
    def get_backend(cls, name: str):
        """Get all registered backend classes."""
        if name not in cls._backends:
            raise ValueError(f"Backend '{name}' is not registered.")
        return cls._backends.get(name)

    @classmethod
    def clear(cls):
        """Clear all registered backends (useful for testing)."""
        cls._backends.clear()


class Backend(ABC):
    """Abstract base class for backends."""

    @abstractmethod
    def connect(self, connection_string) -> None:
        pass

    @abstractmethod
    def execute(self, query: str) -> Any:
        pass

    @abstractmethod
    def close(self) -> None:
        pass


@BackendRegistry.register("duckdb")
class DuckDBBackend(Backend):
    """Backend for Duckdb"""

    def __init__(self) -> None:
        self.conn = None

    def connect(self, connection_string: str) -> None:
        self.conn = duckdb.connect(connection_string)

    def execute(self, query: str) -> Any:
        """Exeute query string and retrun a polars dataframe"""
        return self.conn.execute(query).pl()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None


class QsqlExecutor:
    def __init__(self, qsql_file: QsqlFile):
        self._file = qsql_file
        self._backend_conns = {}
        self._init_backends()

    def _init_backends(self):
        for cell in self._file.parsed_cells:
            config = cell.get("config", {})
            input_config = config.get("input", {})

            for backend_name, conn_string in input_config.items():
                backend_class = BackendRegistry.get_backend(backend_name)

                if conn_string not in self._backend_conns:
                    backend_instance = backend_class()
                    backend_instance.connect(conn_string)
                    self._backend_conns[conn_string] = backend_instance

    def execute_cell(self, cell_name: str) -> Any:
        cell = next(
            (c for c in self._file.parsed_cells if c["cell_name"] == cell_name), None
        )

        input_config = cell.get("config").get("input")

        _, conn_string = next(iter(input_config.items()))

        backend_instance = self._backend_conns.get(conn_string)

        try:
            result = backend_instance.execute(cell["text"])
            return result
        except Exception as e:
            pprint(cell)
            raise e

    def execute_many(self, cell_names: list[str]) -> dict[str, Any]:
        results = {}

        for cell_name in cell_names:
            result = self.execute_cell(cell_name)
            results[cell_name] = result

        return results
