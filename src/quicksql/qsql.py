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
    model: CellConfig

    def parse_config(self, text):
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
        print("code", config_dict)

        return self.model(**config_dict)

    def parse_sql(self, text):
        pass
