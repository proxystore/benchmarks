"""Task pipelining with futures benchmark."""

from __future__ import annotations

import logging
import queue
import threading
import time
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import NamedTuple

from parsl.concurrent import ParslPoolExecutor
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.future import Future as ProxyFuture

from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.benchmarks.task_pipelining.config import RunConfig
from psbench.benchmarks.task_pipelining.config import RunResult
from psbench.benchmarks.task_pipelining.config import SubmissionMethod
from psbench.executor.dask import DaskExecutor
from psbench.utils import randbytes

logger = logging.getLogger('task-pipelining')


class TaskTimes(NamedTuple):
    start_timestamp: float
    start_resolve_timestamp: float
    end_resolve_timestamp: float
    start_generate_timestamp: float
    end_generate_timestamp: float

    @staticmethod
    def serialize(times: list[list[float]]) -> str:
        return ':'.join('-'.join(str(v) for v in t) for t in times)

    def collate(
        self,
        submitted_timestamp: float,
        received_timestamp: float,
    ) -> list[float]:
        timestamps = list(self)
        return [submitted_timestamp, *timestamps, received_timestamp]


def sequential_no_proxy_task(
    data: bytes,
    overhead_fraction: float,
    sleep: float,
) -> tuple[bytes, TaskTimes]:
    import time

    start_timestamp = time.time()

    from psbench.utils import randbytes

    time.sleep(overhead_fraction * sleep)

    start_resolve_timestamp = time.time()
    assert isinstance(data, bytes)
    end_resolve_timestamp = time.time()

    resolve_time = end_resolve_timestamp - start_resolve_timestamp
    compute_sleep = (1 - overhead_fraction) * sleep
    time.sleep(max(compute_sleep - resolve_time, 0))

    start_generate_timestamp = time.time()
    result = randbytes(len(data))
    end_generate_timestamp = time.time()

    times = TaskTimes(
        start_timestamp=start_timestamp,
        start_resolve_timestamp=start_resolve_timestamp,
        end_resolve_timestamp=end_resolve_timestamp,
        start_generate_timestamp=start_generate_timestamp,
        end_generate_timestamp=end_generate_timestamp,
    )

    return result, times


def sequential_proxy_task(
    data: Proxy[bytes],
    overhead_fraction: float,
    sleep: float,
    prepopulate: bool = False,
) -> Proxy[tuple[Proxy[bytes], TaskTimes]]:
    import time

    start_timestamp = time.time()

    from proxystore.proxy import resolve
    from proxystore.store import get_store

    from psbench.utils import randbytes

    time.sleep(overhead_fraction * sleep)

    start_resolve_timestamp = time.time()
    resolve(data)
    assert isinstance(data, bytes)
    end_resolve_timestamp = time.time()

    resolve_time = end_resolve_timestamp - start_resolve_timestamp
    compute_sleep = (1 - overhead_fraction) * sleep
    time.sleep(max(compute_sleep - resolve_time, 0))

    start_generate_timestamp = time.time()
    store = get_store(data)
    assert store is not None
    result = randbytes(len(data))
    proxy = store.proxy(result, evict=True, populate_target=prepopulate)
    end_generate_timestamp = time.time()

    times = TaskTimes(
        start_timestamp=start_timestamp,
        start_resolve_timestamp=start_resolve_timestamp,
        end_resolve_timestamp=end_resolve_timestamp,
        start_generate_timestamp=start_generate_timestamp,
        end_generate_timestamp=end_generate_timestamp,
    )

    return store.proxy((proxy, times), evict=True, populate_target=prepopulate)


def pipelined_task(
    data: Proxy[bytes],
    future: ProxyFuture[bytes],
    overhead_fraction: float,
    sleep: float,
    prepopulate: bool = False,
) -> TaskTimes:
    import time

    start_timestamp = time.time()

    from proxystore.proxy import resolve

    from psbench.utils import randbytes

    time.sleep(overhead_fraction * sleep)

    start_resolve_timestamp = time.time()
    resolve(data)
    assert isinstance(data, bytes)
    end_resolve_timestamp = time.time()

    resolve_time = end_resolve_timestamp - start_resolve_timestamp
    compute_sleep = (1 - overhead_fraction) * sleep
    time.sleep(max(compute_sleep - resolve_time, 0))

    start_generate_timestamp = time.time()
    result = randbytes(len(data))
    future.set_result(result)
    end_generate_timestamp = time.time()

    return TaskTimes(
        start_timestamp=start_timestamp,
        start_resolve_timestamp=start_resolve_timestamp,
        end_resolve_timestamp=end_resolve_timestamp,
        start_generate_timestamp=start_generate_timestamp,
        end_generate_timestamp=end_generate_timestamp,
    )


def run_sequential_workflow(
    executor: Executor,
    store: Store[Any] | None,
    task_chain_length: int,
    task_data_bytes: int,
    task_overhead_fraction: float,
    task_sleep: float,
) -> RunResult:
    start = time.perf_counter_ns()

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    if store is not None:
        data = store.proxy(data, evict=True, populate_target=True)

    sequential_task: Callable[..., Any]
    if store is None:
        sequential_task = sequential_no_proxy_task
    else:
        sequential_task = sequential_proxy_task

    task_timestamps: list[list[float]] = []

    for _ in range(task_chain_length):
        task_submitted = time.time()
        future: Future[Any] = executor.submit(
            sequential_task,  # type: ignore[arg-type]
            data,
            overhead_fraction=task_overhead_fraction,
            sleep=task_sleep,
        )
        data, task_time = future.result()
        task_received = time.time()
        task_timestamps.append(
            task_time.collate(task_submitted, task_received),
        )

    # Resolve the final resulting data
    assert isinstance(data, bytes)

    end = time.perf_counter_ns()

    method = 'sequential-no-proxy' if store is None else 'sequential-proxy'
    return RunResult(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__
        if store is not None
        else 'None',
        submission_method=method,
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
        task_timestamps=TaskTimes.serialize(task_timestamps),
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

    task_futures: queue.Queue[Future[TaskTimes]] = queue.Queue()
    task_submitted: list[float] = []
    task_times: list[TaskTimes] = []
    task_received: list[float] = []

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    proxy = store.proxy(data, evict=True, populate_target=True)

    proxies: queue.Queue[Proxy[bytes]] = queue.Queue()
    proxies.put(proxy)

    def submitter() -> None:
        for _ in range(task_chain_length):
            data_future: ProxyFuture[bytes] = store.future(
                evict=True,
                polling_interval=0.001,
            )
            task_future = executor.submit(
                pipelined_task,
                proxies.get(),
                data_future,
                overhead_fraction=task_overhead_fraction,
                sleep=task_sleep,
                prepopulate=isinstance(
                    executor,
                    (DaskExecutor, ParslPoolExecutor),
                ),
            )
            task_submitted.append(time.time())
            task_futures.put(task_future)

            time.sleep((1 - task_overhead_fraction) * task_sleep)

            proxies.put(data_future.proxy())

    def receiver() -> None:
        time.sleep(task_overhead_fraction * task_sleep)
        for _ in range(task_chain_length):
            time.sleep((1 - task_overhead_fraction) * task_sleep)
            task_future = task_futures.get()
            task_time = task_future.result()
            task_times.append(task_time)
            task_received.append(time.time())

    submitter_thread = threading.Thread(target=submitter)
    receiver_thread = threading.Thread(target=receiver)

    submitter_thread.start()
    receiver_thread.start()

    submitter_thread.join()
    receiver_thread.join()

    # Resolve the final resulting data
    assert isinstance(proxy, bytes)

    end = time.perf_counter_ns()

    task_timestamps = [
        mid.collate(start, end)
        for start, mid, end in zip(task_submitted, task_times, task_received)
    ]

    return RunResult(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__,
        submission_method='pipelined-proxy-future',
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
        task_timestamps=TaskTimes.serialize(task_timestamps),
        workflow_makespan_ms=(end - start) / 1e6,
    )


class Benchmark(ContextManagerAddIn):
    name = 'Task Pipelining'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self, executor: Executor, store: Store[Any]) -> None:
        self.executor = executor
        self.store = store
        super().__init__(managers=[self.executor, self.store])

    def config(self) -> dict[str, Any]:
        return {
            'executor': self.executor.__class__.__name__,
            'connector': self.store.connector.__class__.__name__,
        }

    def run(self, config: RunConfig) -> RunResult:
        method = config.submission_method
        if method == SubmissionMethod.SEQUENTIAL_NO_PROXY:
            result = run_sequential_workflow(
                executor=self.executor,
                store=None,
                task_chain_length=config.task_chain_length,
                task_data_bytes=config.task_data_bytes,
                task_overhead_fraction=config.task_overhead_fraction,
                task_sleep=config.task_sleep,
            )
        elif method == SubmissionMethod.SEQUENTIAL_PROXY:
            result = run_sequential_workflow(
                executor=self.executor,
                store=self.store,
                task_chain_length=config.task_chain_length,
                task_data_bytes=config.task_data_bytes,
                task_overhead_fraction=config.task_overhead_fraction,
                task_sleep=config.task_sleep,
            )
        elif method == SubmissionMethod.PIPELINED_PROXY_FUTURE:
            result = run_pipelined_workflow(
                executor=self.executor,
                store=self.store,
                task_chain_length=config.task_chain_length,
                task_data_bytes=config.task_data_bytes,
                task_overhead_fraction=config.task_overhead_fraction,
                task_sleep=config.task_sleep,
            )
        else:
            raise AssertionError('Unreachable.')

        return result
