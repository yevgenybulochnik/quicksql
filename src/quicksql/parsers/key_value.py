import re
from typing import Any, Pattern

from .base import Parser, ParserRegistry


@ParserRegistry.register("key_value")
class KeyValueParser(Parser):
    """Parser for extracting key-value pairs from comment lines like '-- key: value'."""

    @property
    def pattern(self) -> Pattern[str]:
        return re.compile(r"--\s*?(\S+):\s*?(\S+)\s*?")

    def parse(self, data: str) -> dict[str, Any]:
        matches = self.pattern.findall(data)
        result = {}
        for key, value in matches:
            result[key] = value
        return result
