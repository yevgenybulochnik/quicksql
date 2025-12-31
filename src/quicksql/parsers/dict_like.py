import re
from typing import Any, Pattern
import yaml

from .base import Parser, ParserRegistry


@ParserRegistry.register("dict_like")
class DictLikeParser(Parser):
    """Parser for extracting YAML-like content from /* */ comment blocks."""

    @property
    def pattern(self) -> Pattern[str]:
        return re.compile(r"/\*(.*?)\*/", re.DOTALL)

    def parse(self, data: str) -> dict[str, Any]:
        matches = self.pattern.findall(data)
        result = {}
        for match in matches:
            try:
                parsed = yaml.safe_load(match.strip())
                if isinstance(parsed, dict):
                    result.update(parsed)
            except yaml.YAMLError:
                continue
        return result
