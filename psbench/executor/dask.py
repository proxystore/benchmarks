from __future__ import annotations

import functools
import sys
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Callable
from typing import cast
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

from dask.distributed import Client
from proxystore.proxy import _proxy_trampoline
from proxystore.proxy import Proxy
from proxystore.store.factory import StoreFactory
from proxystore_ex.plugins.distributed import Future as _CustomDaskFuture

P = ParamSpec('P')
T = TypeVar('T')


def proxy_task_wrapper(function: Callable[P, T]) -> Callable[P, T]:
    # See: https://github.com/proxystore/extensions/blob/v0.1.2/proxystore_ex/plugins/distributed.py#L504

    @functools.wraps(function)
    def _proxy_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        new_args = tuple(
            _proxy_trampoline(v) if isinstance(v, StoreFactory) else v
            for v in args
        )
        new_kwargs = {
            k: _proxy_trampoline(v) if isinstance(v, StoreFactory) else v
            for k, v in kwargs.items()
        }

        new_args = cast(P.args, new_args)
        new_kwargs = cast(P.kwargs, new_kwargs)

        result = function(*new_args, **new_kwargs)

        # This import is required so cloudpickle doesn't try to capture
        # Proxy from the module scope
        from proxystore.proxy import Proxy

        if isinstance(result, Proxy):
            return result.__factory__
        return result

    return _proxy_wrapper


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
        # Dask can't serialize an unresolved proxy so we replace the proxies
        # with their factories which can be serialized and wrap the task
        # with a decorator that casts the factories back to proxies.
        function = proxy_task_wrapper(function)
        new_args = tuple(
            v.__factory__ if isinstance(v, Proxy) else v for v in args
        )
        new_kwargs = {
            k: v.__factory__ if isinstance(v, Proxy) else v
            for k, v in kwargs.items()
        }

        base_future = self.client.submit(function, *new_args, **new_kwargs)

        # This custom future will cast any factory results back to proxies.
        return _CustomDaskFuture(
            key=base_future.key,
            client=base_future.client,
            inform=base_future._inform,
            state=base_future._state,
        )

    def map(
        self,
        function: Callable[P, T],
        *iterables: Iterable[P.args],
        timeout: float | None = None,
        chunksize: int = 1,
    ) -> Iterator[T]:
        # Based on the Parsl implementation.
        # https://github.com/Parsl/parsl/blob/7fba7d634ccade76618ee397d3c951c5cbf2cd49/parsl/concurrent/__init__.py#L58
        futures = [self.submit(function, *args) for args in zip(*iterables)]

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
