import re
from pathlib import Path
from typing import Any, Sequence

from .parsers import ParserRegistry, Parser


class QsqlFile:
    def __init__(self, file_path: Path) -> None:
        self.file_path: Path = file_path

        if not self.file_path.exists():
            raise FileNotFoundError(f"File {self.file_path} does not exist.")

    @property
    def content(self) -> str:
        return self.file_path.read_text()

    @property
    def lines(self) -> tuple[tuple[int, str], ...]:
        """Return a tuple with line number and line content."""
        return tuple(enumerate(self.content.splitlines()))

    @property
    def cell_blocks(self) -> list[dict[str, Any]]:
        lines = self.lines
        last_line_number = len(lines)

        cell_blocks = []

        pattern = re.compile(r"--\s*?name:\s*?(\S+)\s*?")

        for line_number, line_text in reversed(lines):
            match = pattern.search(line_text)

            if match:
                cell_dict = {
                    "cell_name": match.group(1),
                    "cell_start": line_number,
                    "cell_end": last_line_number - 1,
                    "text": "\n".join(
                        line_text[1]
                        for line_text in lines[line_number:last_line_number]
                    ),
                }
                cell_blocks.append(cell_dict)
                last_line_number = line_number

        cell_blocks.reverse()

        return cell_blocks

    @property
    def header(self) -> str:
        cell_blocks = self.cell_blocks

        if not cell_blocks:
            return self.content

        first_block = cell_blocks[0]

        return "\n".join(
            line_text[1] for line_text in self.lines[: first_block["cell_start"]]
        )


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
