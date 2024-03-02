from __future__ import annotations

import sys
from types import TracebackType
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel

RunConfigT = TypeVar('RunConfigT', bound=BaseModel)
RunResultT = TypeVar('RunResultT', bound=BaseModel)


@runtime_checkable
class Benchmark(Protocol[RunConfigT, RunResultT]):
    name: str
    config_type: type[RunConfigT]
    result_type: type[RunResultT]

    def __enter__(self) -> Self:
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        ...

    def config(self) -> dict[str, Any]:
        ...

    def run(self, config: RunConfigT) -> RunResultT:
        ...
