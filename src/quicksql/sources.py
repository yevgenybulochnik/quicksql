"""File source readers: map a source spec to a DuckDB reader expression.

Reader type comes from the spec's ``type`` or is inferred from the file
extension; readers declare which DuckDB extensions they need.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import PurePath
from typing import Any, ClassVar

from .errors import ConfigError
from .registry import Registry, source_reader

EXTENSION_TYPES = {
    ".csv": "csv",
    ".tsv": "csv",
    ".parquet": "parquet",
    ".json": "json",
    ".ndjson": "json",
    ".xlsx": "excel",
    ".xls": "excel",
}


def sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _args(path: str, opts: dict[str, Any]) -> str:
    parts = [sql_literal(path)]
    parts += [f"{k}={sql_literal(v)}" for k, v in opts.items()]
    return ", ".join(parts)


class SourceReader(ABC):
    name: ClassVar[str] = ""
    requires: ClassVar[list[str]] = []

    @abstractmethod
    def expr(self, path: str, **opts: Any) -> str:
        """DuckDB reader expression for the file at path."""


@source_reader("csv")
class CsvReader(SourceReader):
    def expr(self, path: str, **opts: Any) -> str:
        return f"read_csv_auto({_args(path, opts)})"


@source_reader("parquet")
class ParquetReader(SourceReader):
    def expr(self, path: str, **opts: Any) -> str:
        return f"read_parquet({_args(path, opts)})"


@source_reader("json")
class JsonReader(SourceReader):
    def expr(self, path: str, **opts: Any) -> str:
        return f"read_json_auto({_args(path, opts)})"


@source_reader("excel")
class ExcelReader(SourceReader):
    requires = ["excel"]

    def expr(self, path: str, **opts: Any) -> str:
        return f"read_xlsx({_args(path, opts)})"


def reader_for(path: str, type_: str | None, readers: Registry) -> SourceReader:
    """Resolve one of the notebook's readers explicitly by type or by the
    path's extension."""
    if type_ is not None:
        return readers.get(type_)
    ext = PurePath(path).suffix.lower()
    name = EXTENSION_TYPES.get(ext)
    if name is None:
        raise ConfigError(
            f"cannot infer source type for {path!r} (extension {ext!r}); set type explicitly"
        )
    return readers.get(name)
