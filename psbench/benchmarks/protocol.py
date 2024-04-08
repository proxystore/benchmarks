from __future__ import annotations

import contextlib
import sys
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import Sequence
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

    def config(self) -> dict[str, Any]: ...

    def run(self, config: RunConfigT) -> RunResultT | Sequence[RunResultT]: ...


class ContextManagerAddIn:
    def __init__(
        self,
        managers: Sequence[AbstractContextManager[Any] | None] | None = None,
    ) -> None:
        self._managers = [] if managers is None else managers

    def __enter__(self) -> Self:
        # https://stackoverflow.com/a/39172487
        with contextlib.ExitStack() as stack:
            for manager in self._managers:
                if manager is not None:
                    stack.enter_context(manager)
            self._stack = stack.pop_all()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()
        self._stack.__exit__(exc_type, exc_value, exc_traceback)

    def close(self) -> None: ...
