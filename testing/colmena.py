from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from typing import Callable

import globus_compute_sdk
from colmena.models import Result
from colmena.queue.base import ColmenaQueues
from colmena.task_server.base import FutureBasedTaskServer
from colmena.task_server.base import run_and_record_timing

from testing.globus_compute import MockExecutor


class MockGlobusComputeTaskServer(FutureBasedTaskServer):
    """Mock GlobusComputeTaskServer."""

    def __init__(
        self,
        methods: dict[Callable[[Any], Any], str],
        client: globus_compute_sdk.Client,
        queues: ColmenaQueues,
        timeout: int | None = None,
        batch_size: int = 128,
    ) -> None:
        """Init MockGlobusComputeTaskServer."""
        self._methods = {f.__name__: f for f in methods}
        super().__init__(queues, self._methods.keys(), timeout)

    def _submit(self, task: Result, topic: str) -> Future[Any]:
        func = self._methods[task.method]
        task.mark_start_task_submission()

        fut: Future[Any] = Future()
        result = run_and_record_timing(func, task)
        fut.set_result(result)
        return fut

    def _setup(self) -> None:
        self.fx_exec = MockExecutor()

    def _cleanup(self) -> None:
        pass
