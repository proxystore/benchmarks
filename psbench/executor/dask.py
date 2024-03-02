from __future__ import annotations

import functools
import sys
from concurrent.futures import Future
from types import TracebackType
from typing import Callable
from typing import cast
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

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


class DaskExecutor:
    """Dask task execution engine."""

    def __init__(self, client: Client) -> None:
        self._client = client
        # The number of workers in Dask can change over time, so this assumes
        # they are all active at the start.
        self.max_workers: int | None = len(client.scheduler_info()['workers'])

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
        self._client.close()

    def submit(
        self,
        function: Callable[P, T],
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

        base_future = self._client.submit(function, *new_args, **new_kwargs)

        # This custom future will cast any factory results back to proxies.
        return _CustomDaskFuture(
            key=base_future.key,
            client=base_future.client,
            inform=base_future._inform,
            state=base_future._state,
        )
