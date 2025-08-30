import re
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, ConfigDict
import yaml


class CellConfig(BaseModel):
    name: str
    auto_run: bool = Field(default=True)
    vars: dict[str, Any] | None = Field(default=None)

    model_config = ConfigDict(extra="forbid")


@dataclass
class CellParser:

    def _parse_config(self, text: str) -> dict[str, Any]:
        key_value_pattern = re.compile(
            r"--\s*?(\S+):\s*?(\S+)\s*?",
        )

        dict_key_pattern = re.compile(r"--\s*?(\w+):\s*/\*(.*?)\*/", re.DOTALL)

        matches = [
            *key_value_pattern.findall(text),
            *dict_key_pattern.findall(text),
        ]

        yaml_string = ""

        for match in matches:
            key, value = match

            if value == "/*":
                continue
            else:
                yaml_string += f"{key}: {value}\n"

        config_dict = yaml.safe_load(yaml_string)
        return config_dict

    def _parse_sql(self, text: str) -> str:
        return text

    def parse(self, text: str) -> dict[str, Any]:
        config_dict = self._parse_config(text)
        sql_template = self._parse_sql(text)

        cell_dict = {"config": config_dict, "sql_template": sql_template}

        return cell_dict


class FileParser:
    def __init__(self, content: str):
        self._content = content

    @classmethod
    def from_string(cls, string: str):
        return cls(string)

    @classmethod
    def from_file(cls, file_path: Path):
        content = Path(file_path).read_text()
        return cls(content)

    @property
    def content(self):
        return self._content

    @property
    def lines(self) -> list[tuple[int, str]]:
        return [
            (line_number, line_text)
            for line_number, line_text in enumerate(self.content.splitlines())
        ]

    @property
    def cell_blocks(self) -> list[dict[str, Any]]:
        lines = self.lines
        last_line_number = lines[-1][0] + 1

        cell_blocks = []

        pattern = re.compile(r"--\s*?name:\s*?(\S+)\s*?")

        for line_number, line_text in reversed(lines):
            match = pattern.search(line_text)

            if match:
                cell_dict = {
                    "cell_block_name": match[1],
                    "cell_start": line_number,
                    "cell_end": last_line_number,
                    "text": line_text,
                }
                cell_blocks.append(cell_dict)
                last_line_number = line_number

        cell_blocks.reverse()

        return cell_blocks


@dataclass
class QSqlRunner:
    file_path: str
    cell_model: Any
    file: Any
    config: Any
    cell_blocks: Any
    queue: Any
