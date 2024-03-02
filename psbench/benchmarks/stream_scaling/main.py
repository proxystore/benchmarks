from __future__ import annotations

import contextlib
import sys
from types import TracebackType
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.store.base import Store

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.config import RunResult
from psbench.config import StreamConfig
from psbench.executor.protocol import Executor


class Benchmark:
    name = 'Stream Scaling'
    config_type = RunConfig
    result_type = RunResult

    def __init__(
        self,
        executor: Executor,
        store: Store[Any],
        stream_config: StreamConfig,
    ) -> None:
        self.executor = executor
        self.store = store
        self.stream_config = stream_config

    def __enter__(self) -> Self:
        # https://stackoverflow.com/a/39172487
        with contextlib.ExitStack() as stack:
            stack.enter_context(self.executor)
            stack.enter_context(self.store)
            self._stack = stack.pop_all()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self._stack.__exit__(exc_type, exc_value, exc_traceback)

    def config(self) -> dict[str, Any]:
        return {
            'executor': self.executor.__class__.__name__,
            'connector': self.store.connector.__class__.__name__,
            'stream-broker': self.stream_config.kind,
        }

    def run(self, config: RunConfig) -> RunResult:
        assert self.stream_config.kind is not None
        return RunResult(
            executor=self.executor.__class__.__name__,
            connector=self.store.connector.__class__.__name__,
            stream=self.stream_config.kind,
            data_size_bytes=config.data_size_bytes,
            producer_sleep=config.producer_sleep,
            task_count=config.task_count,
            task_sleep=config.task_sleep,
            workers=config.task_sleep,
            task_submitted_timestamp=0,
            task_received_timestamp=1,
        )
