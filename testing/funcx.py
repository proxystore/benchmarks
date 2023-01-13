from __future__ import annotations

import contextlib
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Generator
from typing import TypeVar
from unittest import mock

import funcx

RT = TypeVar('RT')


class MockFuncXExecutor(funcx.FuncXExecutor):
    """Mock FuncXExecutor."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Init MockFuncXExecutor."""
        pass

    def submit(
        self,
        func: Callable[..., RT],
        *args: Any,
        endpoint_id: str | None = None,
        container_uuid: str | None = None,
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
def mock_funcx() -> Generator[None, None, None]:
    """Context manager that mocks FuncXClient and FuncXExecutor."""
    with mock.patch('funcx.FuncXClient'):
        with mock.patch(
            'funcx.FuncXExecutor',
            MockFuncXExecutor,
        ):
            yield


def mock_executor() -> funcx.FuncXExecutor:
    """Create a mock FuncXExectutor."""
    with mock_funcx():
        return funcx.FuncXExecutor(funcx.FuncXClient)
