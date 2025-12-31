from abc import ABC, abstractmethod
from typing import Any, Pattern
import re
import yaml


class ParserRegistry:
    """Registry for parser classes using decorator pattern."""

    _parsers = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a parser class with a given name."""

        def decorator(parser_class):
            cls._parsers[name] = parser_class
            return parser_class

        return decorator

    @classmethod
    def get_parsers(cls):
        """Get all registered parser classes."""
        return cls._parsers.values()

    @classmethod
    def clear(cls):
        """Clear all registered parsers (useful for testing)."""
        cls._parsers.clear()


class Parser(ABC):
    """Abstract base class for parsing config out of cell blocks or the global file header."""

    @property
    @abstractmethod
    def pattern(self) -> Pattern[str]:
        pass

    @abstractmethod
    def parse(self, data: str) -> dict[str, Any]:
        pass


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
