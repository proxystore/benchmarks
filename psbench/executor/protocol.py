from __future__ import annotations

import sys
from concurrent.futures import Future
from types import TracebackType
from typing import Callable
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


P = ParamSpec('P')
T = TypeVar('T')


@runtime_checkable
class Executor(Protocol):
    """Protocol for task execution engine."""

    max_workers: int | None

    def __enter__(self) -> Self:
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        ...

    def start(self) -> None:
        ...

    def close(self) -> None:
        ...

    def submit(
        self,
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        ...
