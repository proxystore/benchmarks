from __future__ import annotations

import random
import sys
from types import TracebackType
from typing import Any
from typing import NamedTuple

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class MockRunConfig(BaseModel):
    param: int


class MockRunResult(NamedTuple):
    value: int
    time: float


class MockBenchmark:
    name = 'Mock Benchmark'
    config_type = MockRunConfig
    result_type = MockRunResult

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        pass

    def config(self) -> dict[str, Any]:
        return {}

    def run(self, config: MockRunConfig) -> MockRunResult:
        return MockRunResult(value=config.param, time=random.random())
