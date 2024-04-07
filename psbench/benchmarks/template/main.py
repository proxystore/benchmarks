from __future__ import annotations

import contextlib
import logging
import random
import sys
from types import TracebackType
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from concurrent.futures import Executor

from proxystore.store.base import Store

from psbench.benchmarks.template.config import RunConfig
from psbench.benchmarks.template.config import RunResult

logger = logging.getLogger('template')


class Benchmark:
    name = 'Template'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self, executor: Executor, store: Store[Any] | None) -> None:
        self.executor = executor
        self.store = store

    def __enter__(self) -> Self:
        # https://stackoverflow.com/a/39172487
        with contextlib.ExitStack() as stack:
            stack.enter_context(self.executor)
            if self.store is not None:
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
        connector = (
            self.store.connector.__class__.__name__
            if self.store is not None
            else 'None'
        )
        return {
            'executor': self.executor.__class__.__name__,
            'connector': connector,
        }

    def run(self, config: RunConfig) -> RunResult:
        return RunResult(
            name=config.name,
            result=random.randint(0, 100),
        )
