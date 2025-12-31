from abc import ABC, abstractmethod
from typing import Any, Pattern
import re
import yaml


class Parser(ABC):
    """Abstract base class for parsing config out of cell blocks or the global file header."""

    @property
    @abstractmethod
    def pattern(self) -> Pattern[str]:
        pass

    @abstractmethod
    def parse(self, data: str) -> dict[str, Any]:
        pass


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
