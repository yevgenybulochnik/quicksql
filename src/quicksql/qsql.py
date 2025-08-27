import re
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field
import yaml


class CellConfig(BaseModel):
    name: str
    auto_run: bool = Field(default=True)
    vars: dict[str, Any] | None = Field(default=None)


@dataclass
class CellParser:
    model: CellConfig

    def parse_config(self, text):
        config_dict = {}

        for field_name, field in self.model.model_fields.items():

            if field.annotation == dict[str, Any] | None:
                full_key_pattern = re.compile(rf"\s*?vars:\s*?/\*(.*)\*/", re.DOTALL)
                matches = full_key_pattern.findall(text)

                if matches:
                    yaml_dict = yaml.safe_load(matches[0])
                    print(yaml_dict)
                    config_dict[field_name] = yaml_dict
            else:
                pattern = re.compile(
                    rf"\s*?{field_name}:\s*?(\S+)\s?",
                )
                matches = pattern.findall(text)

                assert len(matches) <= 1
                if matches:
                    config_dict[field_name] = matches[0]
        return self.model(**config_dict)

    def parse_sql(self, text):
        pass
