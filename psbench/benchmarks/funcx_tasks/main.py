"""FuncX + ProxyStore Simple Test.

Tests round trip function execution times to a FuncX endpoint with
configurable function payload transfer methods, sizes, etc.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import sys
import time
from typing import Sequence

import funcx
import proxystore
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.utils import get_key

from psbench.argparse import add_funcx_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.csv import CSVLogger
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.tasks.pong import pong
from psbench.tasks.pong import pong_proxy
from psbench.utils import randbytes

logger = logging.getLogger('funcx-test')


@dataclasses.dataclass
class TaskStats:
    """Stats for individual task. Represents a row in the output CSV."""

    proxystore_backend: str
    task_name: str
    input_size_bytes: int
    output_size_bytes: int
    task_sleep_seconds: float
    total_time_ms: float
    input_get_ms: float | None = None
    input_set_ms: float | None = None
    input_proxy_ms: float | None = None
    input_resolve_ms: float | None = None
    output_get_ms: float | None = None
    output_set_ms: float | None = None
    output_proxy_ms: float | None = None
    output_resolve_ms: float | None = None


def time_task(
    *,
    fx: funcx.FuncXExecutor,
    endpoint: str,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single FuncX task.

    Args:
        fx (FuncXExecutor): FuncX Executor to submit task through.
        endpoint (str): Endpoint ID to submit task to.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()
    fut = fx.submit(
        pong,
        data,
        result_size=output_size,
        sleep=task_sleep,
        endpoint_id=endpoint,
    )
    result = fut.result()

    end = time.perf_counter_ns()
    assert isinstance(result, bytes)

    return TaskStats(
        proxystore_backend='',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_proxy(
    *,
    fx: funcx.FuncXExecutor,
    endpoint: str,
    store: Store,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single FuncX task with proxied inputs.

    Args:
        fx (FuncXExecutor): FuncX Executor to submit task through.
        endpoint (str): Endpoint ID to submit task to.
        store (Store): ProxyStore Store to use for proxying input/outputs.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    data: Proxy[bytes] = store.proxy(randbytes(input_size), evict=True)
    start = time.perf_counter_ns()
    fut = fx.submit(
        pong_proxy,
        data,
        evict_result=False,
        result_size=output_size,
        sleep=task_sleep,
        endpoint_id=endpoint,
    )
    (result, task_proxy_stats) = fut.result()

    proxystore.proxy.resolve(result)
    key = get_key(result)
    assert key is not None
    store.evict(key)
    end = time.perf_counter_ns()
    assert isinstance(result, bytes)
    assert isinstance(result, Proxy)

    return TaskStats(
        proxystore_backend=store.__class__.__name__,
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
        input_get_ms=task_proxy_stats.input_get_ms,
        input_set_ms=store.stats(data)['set'].avg_time_ms,
        input_proxy_ms=store.stats(data)['proxy'].avg_time_ms,
        input_resolve_ms=task_proxy_stats.input_resolve_ms,
        output_get_ms=store.stats(result)['get'].avg_time_ms,
        output_set_ms=task_proxy_stats.output_set_ms,
        output_proxy_ms=task_proxy_stats.output_proxy_ms,
        output_resolve_ms=store.stats(result)['resolve'].avg_time_ms,
    )


def runner(
    *,
    funcx_endpoint: str,
    store: Store | None,
    input_sizes: list[int],
    output_sizes: list[int],
    task_repeat: int,
    task_sleep: float,
    csv_file: str | None,
) -> None:
    """Run all task configurations and log results."""
    store_class_name = None if store is None else store.__class__.__name__
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - FuncX Endpoint: {funcx_endpoint}\n'
        f' - ProxyStore backend: {store_class_name}\n'
        f' - Task type: ping-pong\n'
        f' - Task repeat: {task_repeat}\n'
        f' - Task input sizes: {input_sizes} bytes\n'
        f' - Task output sizes: {output_sizes} bytes\n'
        f' - Task sleep time: {task_sleep} s',
    )

    runner_start = time.perf_counter_ns()
    fx = funcx.FuncXExecutor(funcx.FuncXClient())

    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, TaskStats)

    for input_size in input_sizes:
        for output_size in output_sizes:
            for _ in range(task_repeat):
                if store is None:
                    stats = time_task(
                        fx=fx,
                        endpoint=funcx_endpoint,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )
                else:
                    stats = time_task_proxy(
                        fx=fx,
                        endpoint=funcx_endpoint,
                        store=store,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )

                logger.log(
                    TESTING_LOG_LEVEL,
                    f'Task completed in {stats.total_time_ms:.3f} ms\n{stats}',
                )

                if csv_file is not None:
                    csv_logger.log(stats)

    if csv_file is not None:
        csv_logger.close()

    runner_end = time.perf_counter_ns()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start) / 1e9:.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Simple FuncX Task Benchmark with ProxyStore."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Simple FuncX task benchmark with ProxyStore.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--task-repeat',
        type=int,
        default=1,
        help='Repeat each unique task configuration',
    )
    parser.add_argument(
        '--task-sleep',
        type=float,
        default=0,
        help='Sleep time for tasks',
    )
    parser.add_argument(
        '--input-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task input size in bytes',
    )
    parser.add_argument(
        '--output-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task output size in bytes',
    )
    add_funcx_options(parser, required=True)
    add_logging_options(parser)
    add_proxystore_options(parser, required=False)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    store = init_store_from_args(args, stats=True)

    runner(
        funcx_endpoint=args.funcx_endpoint,
        store=store,
        input_sizes=args.input_sizes,
        output_sizes=args.output_sizes,
        task_repeat=args.task_repeat,
        task_sleep=args.task_sleep,
        csv_file=args.csv_file,
    )

    if store is not None:
        store.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
