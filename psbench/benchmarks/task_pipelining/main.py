"""Task pipelining with futures benchmark."""

from __future__ import annotations

import contextlib
import logging
import sys
import time
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.future import Future as ProxyFuture

from psbench.benchmarks.task_pipelining.config import RunConfig
from psbench.benchmarks.task_pipelining.config import RunResult
from psbench.executor.protocol import Executor
from psbench.utils import randbytes

logger = logging.getLogger('task-pipelining')


def sequential_task(
    data: Proxy[bytes],
    overhead_fraction: float,
    sleep: float,
) -> Proxy[bytes]:
    import time

    from proxystore.proxy import resolve
    from proxystore.store import get_store

    from psbench.utils import randbytes

    time.sleep(overhead_fraction * sleep)

    resolve(data)
    assert isinstance(data, bytes)

    time.sleep((1 - overhead_fraction) * sleep)

    store = get_store(data)
    assert store is not None
    result = randbytes(len(data))
    proxy = store.proxy(result, evict=True)
    # Pre-populate proxy target to prevent a double resolve after eviction.
    # This is a quick hack but is fixed in later ProxyStore versions.
    proxy.__wrapped__ = result
    return proxy


def pipelined_task(
    data: Proxy[bytes],
    future: ProxyFuture[bytes],
    overhead_fraction: float,
    sleep: float,
) -> None:
    import time

    from proxystore.proxy import resolve

    from psbench.utils import randbytes

    time.sleep(overhead_fraction * sleep)

    resolve(data)
    assert isinstance(data, bytes)

    time.sleep((1 - overhead_fraction) * sleep)

    result = randbytes(len(data))
    future.set_result(result)


def run_sequential_workflow(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: int,
    task_overhead_fraction: float,
    task_sleep: float,
) -> RunResult:
    start = time.perf_counter_ns()

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    proxy = store.proxy(data, evict=True)
    # Pre-populate proxy target to prevent a double resolve after eviction.
    # This is a quick hack but is fixed in later ProxyStore versions.
    proxy.__wrapped__ = data

    for _ in range(task_chain_length):
        future = executor.submit(
            sequential_task,
            proxy,
            overhead_fraction=task_overhead_fraction,
            sleep=task_sleep,
        )
        proxy = future.result()

    # Resolve the final resulting data
    assert isinstance(proxy, bytes)

    end = time.perf_counter_ns()

    return RunResult(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__,
        submission_method='sequential',
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
        workflow_makespan_ms=(end - start) / 1e6,
    )


def run_pipelined_workflow(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: int,
    task_overhead_fraction: float,
    task_sleep: float,
) -> RunResult:
    start = time.perf_counter_ns()

    task_futures: list[Future[None]] = []

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    proxy = store.proxy(data, evict=True)
    # Pre-populate proxy target to prevent a double resolve after eviction.
    # This is a quick hack but is fixed in later ProxyStore versions.
    proxy.__wrapped__ = data

    for i in range(task_chain_length):
        if i > 1:
            old_future = task_futures.pop(0)
            old_future.result()
        data_future: ProxyFuture[bytes] = store.future(
            evict=True,
            polling_interval=0.001,
        )
        task_future = executor.submit(
            pipelined_task,
            proxy,
            data_future,
            overhead_fraction=task_overhead_fraction,
            sleep=task_sleep,
        )
        task_futures.append(task_future)
        time.sleep((1 - task_overhead_fraction) * task_sleep)
        proxy = data_future.proxy()

    for future in task_futures:
        future.result()

    # Resolve the final resulting data
    assert isinstance(proxy, bytes)

    end = time.perf_counter_ns()

    return RunResult(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__,
        submission_method='pipelined',
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
        workflow_makespan_ms=(end - start) / 1e6,
    )


class Benchmark:
    name = 'Task Pipelining'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self, executor: Executor, store: Store[Any]) -> None:
        self.executor = executor
        self.store = store

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
        }

    def run(self, config: RunConfig) -> RunResult:
        run_workflow: Callable[..., RunResult]
        if config.submission_method == 'sequential':
            run_workflow = run_sequential_workflow
        else:
            run_workflow = run_pipelined_workflow

        result = run_workflow(
            executor=self.executor,
            store=self.store,
            task_chain_length=config.task_chain_length,
            task_data_bytes=config.task_data_bytes,
            task_overhead_fraction=config.task_overhead_fraction,
            task_sleep=config.task_sleep,
        )

        return result
