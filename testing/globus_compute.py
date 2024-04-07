from __future__ import annotations

import contextlib
import uuid
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar
from unittest import mock

import globus_compute_sdk

RT = TypeVar('RT')


class MockClient(globus_compute_sdk.Client):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._funcs: dict[str, Callable[..., Any]] = {}

    def register_function(
        self,
        func: Callable[..., RT],
        function_id: str | None = None,
        *kwargs: Any,
    ) -> str:
        function_id = str(uuid.uuid4()) if function_id is None else function_id
        self._funcs[function_id] = func
        return function_id


class MockExecutor(globus_compute_sdk.Executor):
    """Mock Executor."""

    def __init__(
        self,
        endpoint_id: Any = None,
        container_id: Any = None,
        client: globus_compute_sdk.Client | None = None,
        funcx_client: globus_compute_sdk.Client | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Init MockExecutor."""
        self.label = str(uuid.uuid4()) if endpoint_id is None else endpoint_id
        client = client or funcx_client
        self._client = client if client is not None else MockClient()

    def submit(
        self,
        func: Callable[..., RT],
        *args: Any,
        **kwargs: Any,
    ) -> Future[RT]:
        """Mock submit to run function locally and return future."""
        fut: Future[Any] = Future()
        fut.set_result(func(*args, **kwargs))
        return fut

    def submit_to_registered_function(
        self,
        function_id: str,
        args: Any = None,
        kwargs: Any = None,
    ) -> Future[RT]:
        args = args if args is not None else ()
        kwargs = kwargs if kwargs is not None else {}
        return self.submit(self._client._funcs[function_id], *args, **kwargs)

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        """Mock executor shutdown."""
        pass


@contextlib.contextmanager
def mock_globus_compute() -> Generator[None, None, None]:
    """Context manager that mocks Globus Compute Executor."""
    with mock.patch(
        'globus_compute_sdk.Client',
        MockClient,
    ), mock.patch('globus_compute_sdk.Executor', MockExecutor):
        yield


def mock_executor() -> globus_compute_sdk.Executor:
    """Create a mock Executor."""
    with mock_globus_compute():
        return globus_compute_sdk.Executor()
