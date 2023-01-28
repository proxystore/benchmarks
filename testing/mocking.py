from __future__ import annotations

from typing import Any


class MockStrictRedis:
    """Mock StrictRedis."""

    def __init__(self, *args, **kwargs):
        """Init MockStrictRedis."""
        self.data = {}

    def delete(self, key: str) -> None:
        """Delete key."""
        if key in self.data:
            del self.data[key]

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data

    def get(self, key: str) -> Any:
        """Get value with key."""
        if key in self.data:
            return self.data[key]
        return None  # pragma: no cover

    def set(  # noqa: A003
        self,
        key: str,
        value: str | bytes | int | float,
    ) -> None:
        """Set value with key."""
        self.data[key] = value
