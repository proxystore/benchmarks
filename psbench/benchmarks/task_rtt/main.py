from __future__ import annotations

import contextlib
import logging
import os
import shutil
import sys
import time
import uuid
from concurrent.futures import Executor
from types import TracebackType
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import proxystore
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.utils import get_key

from psbench import ipfs
from psbench.benchmarks.task_rtt.config import RunConfig
from psbench.benchmarks.task_rtt.config import RunResult
from psbench.benchmarks.task_rtt.tasks import pong
from psbench.benchmarks.task_rtt.tasks import pong_ipfs
from psbench.benchmarks.task_rtt.tasks import pong_proxy
from psbench.logging import BENCH_LOG_LEVEL
from psbench.utils import randbytes

logger = logging.getLogger('task-rtt')


def time_task(
    *,
    executor: Executor,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> RunResult:
    """Execute and time a single task.

    Args:
        executor (Executor): Executor to submit task through.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        RunResult
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()
    fut = executor.submit(
        pong,
        data,
        result_size=output_size,
        sleep=task_sleep,
    )
    result = fut.result()

    end = time.perf_counter_ns()
    assert isinstance(result, bytes)

    return RunResult(
        proxystore_backend='',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_ipfs(
    *,
    executor: Executor,
    ipfs_local_dir: str,
    ipfs_remote_dir: str,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> RunResult:
    """Execute and time a single task with IPFS for transfer.

    Args:
        executor (Executor): Executor to submit task through.
        ipfs_local_dir (str): Local IPFS directory to write files to.
        ipfs_remote_dir (str): Remote IPFS directory to write files to.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        RunResult
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()

    os.makedirs(ipfs_local_dir, exist_ok=True)
    filepath = os.path.join(ipfs_local_dir, str(uuid.uuid4()))
    cid = ipfs.add_data(data, filepath)

    fut = executor.submit(
        pong_ipfs,
        cid,
        ipfs_remote_dir,
        result_size=output_size,
        sleep=task_sleep,
    )
    result = fut.result()

    if result is not None:
        data = ipfs.get_data(result)

    end = time.perf_counter_ns()
    assert isinstance(data, bytes)

    return RunResult(
        proxystore_backend='IPFS',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_proxy(
    *,
    executor: Executor,
    store: Store[Any],
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> RunResult:
    """Execute and time a single task with proxied inputs.

    Args:
        executor (Executor): Executor to submit task through.
        store (Store): ProxyStore Store to use for proxying input/outputs.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        RunResult
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()

    proxy: Proxy[bytes] = store.proxy(data, evict=True)
    fut = executor.submit(
        pong_proxy,
        proxy,
        evict_result=False,
        result_size=output_size,
        sleep=task_sleep,
    )
    (result, task_proxy_stats) = fut.result()

    proxystore.proxy.resolve(result)
    key = get_key(result)
    assert key is not None
    store.evict(key)
    end = time.perf_counter_ns()
    assert isinstance(result, bytes)
    assert isinstance(result, Proxy)

    assert store.metrics is not None
    input_metrics = store.metrics.get_metrics(proxy)
    output_metrics = store.metrics.get_metrics(result)
    assert input_metrics is not None
    assert output_metrics is not None
    assert task_proxy_stats is not None

    return RunResult(
        proxystore_backend=store.connector.__class__.__name__,
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
        input_get_ms=task_proxy_stats.input_get_ms,
        input_put_ms=input_metrics.times['store.put'].avg_time_ms,
        input_proxy_ms=input_metrics.times['store.proxy'].avg_time_ms,
        input_resolve_ms=task_proxy_stats.input_resolve_ms,
        output_get_ms=output_metrics.times['store.get'].avg_time_ms,
        output_put_ms=task_proxy_stats.output_put_ms,
        output_proxy_ms=task_proxy_stats.output_proxy_ms,
        output_resolve_ms=output_metrics.times['factory.resolve'].avg_time_ms,
    )


class Benchmark:
    name = 'Task RTT'
    config_type = RunConfig
    result_type = RunResult

    def __init__(
        self,
        executor: Executor,
        store: Store[Any] | None = None,
        use_ipfs: bool = False,
        ipfs_local_dir: str | None = None,
        ipfs_remote_dir: str | None = None,
    ) -> None:
        if store is not None and use_ipfs:
            raise ValueError(
                'IPFS and ProxyStore cannot be used at the same time.',
            )

        self.executor = executor
        self.store = store
        self.use_ipfs = use_ipfs
        self.ipfs_local_dir = ipfs_local_dir
        self.ipfs_remote_dir = ipfs_remote_dir

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
        if self.use_ipfs:
            # Clean up local and remote IPFS files
            assert self.ipfs_local_dir is not None
            shutil.rmtree(self.ipfs_local_dir)

            def _remote_cleanup() -> None:
                import shutil

                assert self.ipfs_remote_dir is not None
                shutil.rmtree(self.ipfs_remote_dir)

            fut = self.executor.submit(_remote_cleanup)
            fut.result()
            logger.log(BENCH_LOG_LEVEL, 'Cleaned up IPFS directories')

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
            'use_ipfs': self.use_ipfs,
            'ipfs_local_dir': self.ipfs_local_dir,
            'ipfs_remote_dir': self.ipfs_remote_dir,
        }

    def run(self, config: RunConfig) -> RunResult:
        if self.store is not None:
            result = time_task_proxy(
                executor=self.executor,
                store=self.store,
                input_size=config.input_size_bytes,
                output_size=config.output_size_bytes,
                task_sleep=config.sleep,
            )
        elif self.use_ipfs:
            assert self.ipfs_local_dir is not None
            assert self.ipfs_remote_dir is not None
            result = time_task_ipfs(
                executor=self.executor,
                ipfs_local_dir=self.ipfs_local_dir,
                ipfs_remote_dir=self.ipfs_remote_dir,
                input_size=config.input_size_bytes,
                output_size=config.output_size_bytes,
                task_sleep=config.sleep,
            )
        else:
            result = time_task(
                executor=self.executor,
                input_size=config.input_size_bytes,
                output_size=config.output_size_bytes,
                task_sleep=config.sleep,
            )
        return result
