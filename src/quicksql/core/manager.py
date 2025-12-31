from pathlib import Path
from typing import Any, Sequence

from .file import QsqlFile
from ..parsers.base import ParserRegistry, Parser


class QsqlManager:
    """Manages QSQL files and applies parsers to extract configuration."""

    def __init__(
        self,
        qsql_file: Path | QsqlFile,
        parsers: Sequence[Parser] | None = None,
    ) -> None:
        if isinstance(qsql_file, Path):
            self._file = QsqlFile(qsql_file)
        else:
            self._file = qsql_file

        # Use provided parsers or auto-discover from registry
        if parsers is None:
            self.parsers = [
                parser_class() for parser_class in ParserRegistry.get_parsers()
            ]
        else:
            self.parsers = list(parsers)

        self._header = self._parse_header()

    def _parse_header(self) -> dict[str, Any]:
        """Parse the header using the configured parsers."""
        result = {}
        for parser in self.parsers:
            parsed = parser.parse(self._file.header)
            result.update(parsed)
        return result

    @property
    def header(self) -> dict[str, Any]:
        """Get the parsed header configuration."""
        return self._header
