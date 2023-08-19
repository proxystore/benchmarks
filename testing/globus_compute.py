from __future__ import annotations

import contextlib
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar
from unittest import mock

import globus_compute_sdk

RT = TypeVar('RT')


class MockExecutor(globus_compute_sdk.Executor):
    """Mock Executor."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Init MockExecutor."""
        pass

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

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        """Mock executor shutdown."""
        pass


@contextlib.contextmanager
def mock_globus_compute() -> Generator[None, None, None]:
    """Context manager that mocks Globus Compute Executor."""
    with mock.patch(
        'globus_compute_sdk.Client',
    ), mock.patch('globus_compute_sdk.Executor', MockExecutor):
        yield


def mock_executor() -> globus_compute_sdk.Executor:
    """Create a mock Exectutor."""
    with mock_globus_compute():
        return globus_compute_sdk.Executor()
