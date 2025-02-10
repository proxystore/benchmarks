from __future__ import annotations

import sys
from collections.abc import Generator
from collections.abc import Iterable
from collections.abc import Iterator
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

from dask.distributed import Client

P = ParamSpec('P')
T = TypeVar('T')


class DaskExecutor(Executor):
    """Dask task execution engine."""

    def __init__(self, client: Client) -> None:
        self.client = client

    def submit(
        self,
        function: Callable[P, T],
        /,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Future[T]:
        return self.client.submit(function, *args, **kwargs)

    def map(
        self,
        function: Callable[..., T],
        *iterables: Iterable[Any],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        # Based on the Parsl implementation.
        # https://github.com/Parsl/parsl/blob/7fba7d634ccade76618ee397d3c951c5cbf2cd49/parsl/concurrent/__init__.py#L58
        futures = self.client.map(
            function,
            # Dask's Client.map is annotated as Collection[Any] but the
            # Executor.map is annotated as Iterable[Any].
            *iterables,  # type: ignore[arg-type]
            batch_size=chunksize,
        )

        def _result_iterator() -> Generator[T, None, None]:
            futures.reverse()
            while futures:
                yield futures.pop().result(timeout)

        return _result_iterator()

    def shutdown(
        self,
        wait: bool = True,
        *,
        cancel_futures: bool = False,
    ) -> None:
        # Note: wait and cancel_futures are not implemented.
        self.client.close()
