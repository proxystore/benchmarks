from __future__ import annotations

import sys
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
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

P = ParamSpec('P')
T = TypeVar('T')


class ThreadPoolExecutor:
    def __init__(self, max_workers: int | None = None) -> None:
        self._executor = _ThreadPoolExecutor(max_workers)

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
        self._executor.shutdown()

    def submit(
        self,
        function: Callable[P, T],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        future = self._executor.submit(function, *args, **kwargs)
        return future
