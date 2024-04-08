from __future__ import annotations

import contextlib
from typing import Any
from typing import Generator
from unittest import mock


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

    def set(
        self,
        key: str,
        value: str | bytes | int | float,
    ) -> None:
        """Set value with key."""
        self.data[key] = value


@contextlib.contextmanager
def disable_logging(path: str) -> Generator[None, None, None]:
    with mock.patch(f'{path}.runner'), mock.patch(
        f'{path}.init_logging',
    ), mock.patch(f'{path}.CSVResultLogger'):
        yield
