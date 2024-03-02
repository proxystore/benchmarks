from __future__ import annotations

import sys
from concurrent.futures import Future
from types import TracebackType
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from globus_compute_sdk import Executor

P = ParamSpec('P')
T = TypeVar('T')


class GlobusComputeExecutor:
    """Globus Compute task execution engine."""

    def __init__(
        self,
        executor: Executor,
        max_workers: int | None = None,
    ) -> None:
        self._executor = executor
        # Worker count is not used internally for anything, but is here
        # to satisfy the Executor protocol because some external code may
        # want to know how many workers are in the executor.
        self.max_workers = max_workers

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def start(self) -> None:
        pass

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)

    def submit(
        self,
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        future = self._executor.submit(function, *args, **kwargs)
        return future
