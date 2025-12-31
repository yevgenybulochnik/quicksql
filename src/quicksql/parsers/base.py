from abc import ABC, abstractmethod
from typing import Any, Pattern


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
