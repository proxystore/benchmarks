from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from typing import Callable

import funcx
from colmena.models import Result
from colmena.queue.base import ColmenaQueues
from colmena.task_server.base import FutureBasedTaskServer
from colmena.task_server.base import run_and_record_timing

from testing.funcx import MockFuncXExecutor


class MockFuncXTaskServer(FutureBasedTaskServer):
    """Mock FuncXTaskServer."""

    def __init__(
        self,
        methods: dict[Callable[[Any], Any], str],
        funcx_client: funcx.FuncXClient,
        queues: ColmenaQueues,
        timeout: int | None = None,
        batch_size: int = 128,
    ) -> None:
        """Init MockFuncXTaskServer."""
        super().__init__(queues, timeout)
        self._methods = {f.__name__: f for f in methods}

    def _submit(self, task: Result, topic: str) -> Future[Any]:
        func = self._methods[task.method]
        task.mark_start_task_submission()

        fut: Future[Any] = Future()
        result = run_and_record_timing(func, task)
        fut.set_result(result)
        return fut

    def _setup(self) -> None:
        self.fx_exec = MockFuncXExecutor()

    def _cleanup(self) -> None:
        pass
