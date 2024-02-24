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

from psbench.argparse import add_executor_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.csv import CSVLogger
from psbench.executor.factory import init_executor_from_args
from psbench.executor.protocol import Executor
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.utils import randbytes

logger = logging.getLogger('task-pipelining')


class WorkflowStats(NamedTuple):
    executor: str
    connector: str
    submission_method: Literal['sequential', 'pipelined']
    task_chain_length: int
    task_data_bytes: int
    task_overhead_sleep: float
    task_compute_sleep: float
    task_submit_sleep: float
    workflow_makespan_ms: float


def sequential_task(
    data: Proxy[bytes],
    overhead_sleep: float,
    compute_sleep: float,
) -> Proxy[bytes]:
    import time

    from proxystore.proxy import resolve
    from proxystore.store import get_store

    from psbench.utils import randbytes

    time.sleep(overhead_sleep)

    resolve(data)
    assert isinstance(data, bytes)

    time.sleep(compute_sleep)

    store = get_store(data)
    data = randbytes(len(data))
    return store.proxy(data, evict=True)


def pipelined_task(
    data: Proxy[bytes],
    future: Future[bytes],
    overhead_sleep: float,
    compute_sleep: float,
) -> None:
    import time

    from proxystore.proxy import resolve

    from psbench.utils import randbytes

    time.sleep(overhead_sleep)

    resolve(data)
    assert isinstance(data, bytes)

    time.sleep(compute_sleep)

    data = randbytes(len(data))
    future.set_result(data)


def run_sequential_workflow(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: int,
    task_overhead_sleep: float,
    task_compute_sleep: float,
) -> WorkflowStats:
    start = time.perf_counter_ns()

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    proxy = store.proxy(data, evict=True)

    for _ in range(task_chain_length):
        future = executor.submit(
            sequential_task,
            proxy,
            overhead_sleep=task_overhead_sleep,
            compute_sleep=task_compute_sleep,
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
        task_overhead_sleep=task_overhead_sleep,
        task_compute_sleep=task_compute_sleep,
        task_submit_sleep=0,
        workflow_makespan_ms=(end - start) / 1e6,
    )


def run_pipelined_workflow(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: int,
    task_overhead_sleep: float,
    task_compute_sleep: float,
    task_submit_sleep: float,
) -> WorkflowStats:
    start = time.perf_counter_ns()

    task_futures: list[Future[None]] = []

    # Create the initial data for the first task
    data = randbytes(task_data_bytes)
    proxy = store.proxy(data, evict=True)

    for i in range(task_chain_length):
        if i > 1:
            task_futures[i - 1].result()
            time.sleep(task_submit_sleep)
        data_future = store.future(evict=True, polling_interval=0.001)
        task_future = executor.submit(
            pipelined_task,
            proxy,
            data_future,
            overhead_sleep=task_overhead_sleep,
            compute_sleep=task_compute_sleep,
        )
        task_futures.append(task_future)
        time.sleep(task_submit_sleep)
        proxy = data_future.proxy()

    task_futures[-1].result()

    # Resolve the final resulting data
    assert isinstance(proxy, bytes)

    end = time.perf_counter_ns()

    return WorkflowStats(
        executor=executor.__class__.__name__,
        connector=store.connector.__class__.__name__,
        submission_method='pipelined',
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_sleep=task_overhead_sleep,
        task_compute_sleep=task_compute_sleep,
        task_submit_sleep=task_submit_sleep,
        workflow_makespan_ms=(end - start) / 1e6,
    )


def runner(
    executor: Executor,
    store: Store[Any],
    task_chain_length: int,
    task_data_bytes: Sequence[int],
    task_overhead_sleep: float,
    task_compute_sleep: float,
    task_submit_sleep: float,
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
        f' - Task overhead sleep (s): {task_overhead_sleep}\n'
        f' - Task compute sleep (s): {task_compute_sleep}\n'
        f' - Task submit sleep (s): {task_submit_sleep}\n'
        f' - Workflow repeat: {repeat}',
    )

    csv_logger = (
        CSVLogger(csv_file, WorkflowStats) if csv_file is not None else None
    )

    for submission_method, data_size in itertools.product(
        ('sequential', 'pipelined'),
        task_data_bytes,
    ):
        for _ in range(repeat):
            kwargs: dict[str, Any] = {
                'executor': executor,
                'store': store,
                'task_chain_length': task_chain_length,
                'task_data_bytes': data_size,
                'task_overhead_sleep': task_overhead_sleep,
                'task_compute_sleep': task_compute_sleep,
            }

            run: Callable[..., WorkflowStats]
            if submission_method == 'sequential':
                run = run_sequential_workflow
            else:
                run = run_pipelined_workflow
                kwargs['task_submit_sleep'] = task_submit_sleep

            stats = run(**kwargs)

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
        '--task-overhead-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Simulate initial task overhead',
    )
    parser.add_argument(
        '--task-compute-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Simulate task computation',
    )
    parser.add_argument(
        '--task-submit-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Time between submitting tasks with pipelining',
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
            task_overhead_sleep=args.task_overhead_sleep,
            task_compute_sleep=args.task_compute_sleep,
            task_submit_sleep=args.task_submit_sleep,
            repeat=args.repeat,
            csv_file=args.csv_file,
        )

    return 0
