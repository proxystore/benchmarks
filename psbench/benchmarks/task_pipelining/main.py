"""Task pipelining with futures benchmark."""

from __future__ import annotations

import argparse
import itertools
import logging
import sys
import time
from concurrent.futures import Future
from typing import Any
from typing import Callable
from typing import Literal
from typing import NamedTuple
from typing import Sequence

from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.future import Future as ProxyFuture

from psbench.argparse import add_executor_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.executor.factory import init_executor_from_args
from psbench.executor.protocol import Executor
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.results import CSVResultLogger
from psbench.utils import randbytes

logger = logging.getLogger('task-pipelining')


class WorkflowStats(NamedTuple):
    executor: str
    connector: str
    submission_method: Literal['sequential', 'pipelined']
    task_chain_length: int
    task_data_bytes: int
    task_overhead_fraction: float
    task_sleep: float
    workflow_makespan_ms: float


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
) -> WorkflowStats:
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

    return WorkflowStats(
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
) -> WorkflowStats:
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

    return WorkflowStats(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__,
        submission_method='pipelined',
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
        workflow_makespan_ms=(end - start) / 1e6,
    )


def runner(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: Sequence[int],
    task_overhead_fractions: Sequence[float],
    task_sleep: float,
    repeat: int,
    csv_file: str | None,
) -> None:
    runner_start = time.perf_counter()
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - Executor: {executor.__class__.__name__}\n'
        f' - ProxyStore Connector: {store.connector.__class__.__name__}\n'
        f' - Task chain length: {task_chain_length}\n'
        f' - Task data size (bytes): {task_data_bytes}\n'
        f' - Task overhead fractions (s): {task_overhead_fractions}\n'
        f' - Task sleep (s): {task_sleep}\n'
        f' - Workflow repeat: {repeat}',
    )

    csv_logger = (
        CSVResultLogger(csv_file, WorkflowStats)
        if csv_file is not None
        else None
    )

    logger.log(
        TESTING_LOG_LEVEL,
        'Submitted pre-task to alleviate cold-start penalty',
    )
    future = executor.submit(sum, [1, 2, 3])
    assert future.result() == 6

    for submission_method, data_size, overhead_frac in itertools.product(
        ('sequential', 'pipelined'),
        task_data_bytes,
        task_overhead_fractions,
    ):
        for _ in range(repeat):
            run: Callable[..., WorkflowStats]
            if submission_method == 'sequential':
                run = run_sequential_workflow
            else:
                run = run_pipelined_workflow

            stats = run(
                executor=executor,
                store=store,
                task_chain_length=task_chain_length,
                task_data_bytes=data_size,
                task_overhead_fraction=overhead_frac,
                task_sleep=task_sleep,
            )

            logger.log(
                TESTING_LOG_LEVEL,
                f'Task completed in {stats.workflow_makespan_ms:.3f} ms\n'
                f'{stats}',
            )

            if csv_logger is not None:
                csv_logger.log(stats)

    if csv_logger is not None:
        csv_logger.close()

    runner_end = time.perf_counter()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start):.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Task pipelining with futures benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--task-chain-length',
        metavar='N',
        required=True,
        type=int,
        help='Number of tasks in sequential workflow',
    )
    parser.add_argument(
        '--task-data-bytes',
        metavar='BYTES',
        nargs='+',
        required=True,
        type=int,
        help='Intermediate task data size in bytes',
    )
    parser.add_argument(
        '--task-overhead-fractions',
        metavar='FLOAT',
        nargs='+',
        required=True,
        type=float,
        help='Fractions of task sleep time considered initial overhead',
    )
    parser.add_argument(
        '--task-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Task sleep time (does not include data resolve)',
    )
    parser.add_argument(
        '--repeat',
        default=1,
        metavar='RUNS',
        type=int,
        help='Number of runs to repeat each configuration for',
    )

    add_executor_options(parser)
    add_proxystore_options(parser, required=True)
    add_logging_options(parser)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    executor = init_executor_from_args(args)
    store = init_store_from_args(args, metrics=True)
    assert store is not None

    with executor, store:
        runner(
            executor=executor,
            store=store,
            task_chain_length=args.task_chain_length,
            task_data_bytes=args.task_data_bytes,
            task_overhead_fractions=args.task_overhead_fractions,
            task_sleep=args.task_sleep,
            repeat=args.repeat,
            csv_file=args.csv_file,
        )

    return 0
