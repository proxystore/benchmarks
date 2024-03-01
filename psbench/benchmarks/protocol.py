from __future__ import annotations

import sys
from types import TracebackType
from typing import Any
from typing import NamedTuple
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel

ConfigT = TypeVar('ConfigT', bound=BaseModel)
ResultT = TypeVar('ResultT', bound=NamedTuple)


@runtime_checkable
class Benchmark(Protocol[ConfigT, ResultT]):
    name: str
    config_type: type[ConfigT]
    result_type: type[ResultT]

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

    def run(self, config: ConfigT) -> ResultT:
        ...
