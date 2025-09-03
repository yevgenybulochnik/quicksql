import re
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field, ConfigDict
import yaml
from jinja2 import Environment


class CellConfig(BaseModel):
    name: str
    auto_run: bool = Field(default=True)
    vars: dict[str, Any] | None = Field(default=None)

    model_config = ConfigDict(extra="forbid")


class Cell(BaseModel):
    name: str
    config: CellConfig
    sql_template: str
    sql: str | None = Field(default=None)

    def render_sql(self, jinja_env) -> str:
        template = jinja_env.from_string(self.sql_template)
        rendered_sql = template.render(self.config.vars or {})
        self.sql = rendered_sql
        return self.sql


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
    file_parser: Any
    cell_parser: Any
    cell_model: Any
    config: Any
    queue: Any
    cells: Any = field(default_factory=list)
    env = Environment()

    def create_cells(self) -> None:

        for cell_block in self.file_parser.cell_blocks:
            cell_start = cell_block["cell_start"]
            cell_end = cell_block["cell_end"]
            cell_dict = self.cell_parser.parse(
                "\n".join(
                    line for _, line in self.file_parser.lines[cell_start:cell_end]
                )
            )

            cell = Cell(
                name=cell_block["cell_block_name"],
                config=CellConfig(**cell_dict["config"]),
                sql_template=cell_dict["sql_template"],
            )
            cell.render_sql(self.env)
            self.cells.append(cell)
