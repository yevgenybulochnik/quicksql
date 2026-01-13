import re
import time
from pathlib import Path
from typing import Any, Sequence

from ..parsers import ParserRegistry, Parser


class QsqlFile:
    def __init__(
        self,
        file_path: Path,
        parsers: Sequence[Parser] | None = None,
        validate_on_init: bool = True,
    ) -> None:
        self.file_path: Path = file_path

        if validate_on_init:
            self._validate_file_exists()

        if parsers is None:
            self.parsers = [
                parser_class() for parser_class in ParserRegistry.get_parsers()
            ]
        else:
            self.parsers = list(parsers)

    def _validate_file_exists(
        self, max_retries: int = 2, base_delay: float = 0.01
    ) -> None:
        """
        Validate file exists with retry logic for initialization.
        This handles cases where the file is temporarily unavailable during object creation.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds between retries

        Raises:
            FileNotFoundError: If file doesn't exist after all retries
        """
        for attempt in range(max_retries + 1):
            if self.file_path.exists():
                return
            elif attempt < max_retries:
                time.sleep(base_delay * (2**attempt))
            else:
                raise FileNotFoundError(
                    f"File {self.file_path} does not exist after {max_retries + 1} attempts. "
                    f"This may be due to neovim's atomic save operation or the file being deleted."
                )

    @property
    def content(self) -> str:
        return self._read_with_retry()

    def _read_with_retry(self, max_retries: int = 3, base_delay: float = 0.01) -> str:
        """
        Read file content with retry logic to handle transient file availability issues.
        This is particularly useful during neovim's atomic save operations.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds between retries (exponential backoff)

        Returns:
            File content as string

        Raises:
            FileNotFoundError: If file doesn't exist after all retries
            PermissionError: If file can't be read due to permissions
            Exception: For other I/O errors
        """
        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return self.file_path.read_text()
            except FileNotFoundError as e:
                last_exception = e
                if attempt < max_retries:
                    delay = base_delay * (2**attempt)  # Exponential backoff
                    time.sleep(delay)
                    continue
                else:
                    raise FileNotFoundError(
                        f"File {self.file_path} not found after {max_retries + 1} attempts. "
                        f"This may be due to neovim's atomic save operation or file being deleted."
                    ) from e
            except (PermissionError, OSError) as e:
                # Don't retry for permission or other I/O errors
                raise e

        # This should never be reached, but just in case
        raise last_exception if last_exception else Exception("Unknown error occurred")

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
