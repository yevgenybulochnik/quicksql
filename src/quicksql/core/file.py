import re
from pathlib import Path
from typing import Any, Sequence

from ..parsers import ParserRegistry, Parser


class QsqlFile:
    def __init__(
        self,
        file_path: Path,
        parsers: Sequence[Parser] | None = None,
    ) -> None:
        self.file_path: Path = file_path

        if not self.file_path.exists():
            raise FileNotFoundError(f"File {self.file_path} does not exist.")

        if parsers is None:
            self.parsers = [
                parser_class() for parser_class in ParserRegistry.get_parsers()
            ]
        else:
            self.parsers = list(parsers)

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

    @property
    def parsed_header(self) -> dict[str, Any]:
        header = self.header
        header_config: dict[str, Any] = {}

        for parser in self.parsers:
            parsed = parser.parse(header)
            header_config.update(parsed)

        return header_config

    @property
    def parsed_cells(self) -> list[dict[str, Any]]:
        cell_blocks = self.cell_blocks
        parsed_cells = []

        for cell_block in cell_blocks:
            parsed_config = {}
            for parser in self.parsers:
                parsed = parser.parse(cell_block["text"])
                parsed_config.update(parsed)
            parsed_cells.append(
                {
                    **cell_block,
                    "config": {
                        **self.parsed_header,
                        **parsed_config,
                    },
                }
            )

        return parsed_cells
