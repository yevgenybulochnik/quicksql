import re
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

    def _parse_config(self, text):
        key_value_pattern = re.compile(
            rf"--\s*?(\S+):\s*?(\S+)\s*?",
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

    def _parse_sql(self, text):
        return text

    def parse(self, text):
        config_dict = self._parse_config(text)
        sql_template = self._parse_sql(text)

        cell_dict = {"config": config_dict, "sql_template": sql_template}

        return cell_dict


@dataclass
class FileParser:

    def _parse_lines(self, text):
        lines = [
            (line_number, line_text)
            for line_number, line_text in enumerate(text.splitlines())
        ]

        return lines

    def _parse_cell_blocks(self, text):
        lines = self._parse_lines(text)

        last_line_number = lines[-1][0] + 1

        cell_blocks = []

        pattern = re.compile(rf"--\s*?name:\s*?(\S+)\s*?")

        for line_number, line_text in reversed(lines):
            match = pattern.search(line_text)

            if match:
                cell_end = last_line_number
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
