"""Workflow simulation."""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any
from typing import NamedTuple
from typing import Sequence

from proxystore.store.base import Store

from psbench.argparse import add_executor_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.csv import CSVLogger
from psbench.executor.factory import init_executor_from_args
from psbench.executor.protocol import Executor
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.memory import SystemMemoryProfiler
from psbench.proxystore import init_store_from_args

logger = logging.getLogger('workflow')


class WorkflowStats(NamedTuple):
    executor: str
    connector: str | None
    workflow_pattern: str
    task_count: int
    task_input_size_bytes: int
    task_sleep: float
    workflow_start_timestamp: float
    workflow_end_timestamp: float
    workflow_makespan_s: float


def run_mapreduce_workflow(
    executor: Executor,
    store: Store[Any] | None,
    task_count: int,
    input_size_bytes: int,
    sleep: float,
) -> WorkflowStats:
    start_timestamp = time.time()

    # Create input data
    # launch mappers
    # wait on mappers
    # launch reducer

    end_timestamp = time.time()
    return WorkflowStats(
        executor=executor.__class__.__name__,
        connector=(
            'None' if store is None else store.connector.__class__.__name__
        ),
        workflow_pattern='mapreduce',
        task_count=task_count,
        task_input_size_bytes=input_size_bytes,
        task_sleep=sleep,
        workflow_start_timestamp=start_timestamp,
        workflow_end_timestamp=end_timestamp,
        workflow_makespan_s=(end_timestamp - start_timestamp),
    )


def runner(
    executor: Executor,
    store: Store[Any] | None,
    workflow: str,
    task_count: int,
    input_sizes: Sequence[int],
    sleep: float,
    repeat: int,
    csv_file: str,
    memory_profile_interval: float,
) -> None:
    runner_start = time.perf_counter()

    connector_name = (
        'None' if store is None else store.connector.__class__.__name__
    )
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - Executor: {executor.__class__.__name__}\n'
        f' - ProxyStore Connector: {connector_name}\n'
        f' - Workflow type: {workflow}\n'
        f' - Task count: {task_count}\n'
        f' - Input data sizes (bytes): {input_sizes}\n'
        f' - Task sleep (s): {sleep}\n'
        f' - Workflow repeat: {repeat}\n'
        f' - Memory profile interval (s): {memory_profile_interval}',
    )

    if workflow == 'mapreduce':
        run = run_mapreduce_workflow
    else:
        raise AssertionError(f'Unknown workflow type "{workflow}".')

    if not csv_file.endswith('.csv'):
        raise ValueError('CSV log file should end with ".csv"')

    workflow_logger = CSVLogger(csv_file, WorkflowStats)
    memory_file = csv_file.replace('.csv', '-memory.csv')
    memory_profiler = SystemMemoryProfiler(
        memory_profile_interval,
        memory_file,
    )
    memory_profiler.start()

    logger.log(
        TESTING_LOG_LEVEL,
        'Submitted pre-task to alleviate cold-start penalty',
    )
    future = executor.submit(sum, [1, 2, 3])
    assert future.result() == 6

    for input_size in input_sizes:
        for _ in range(repeat):
            stats = run(
                executor,
                store,
                task_count=task_count,
                input_size_bytes=input_size,
                sleep=sleep,
            )

            logger.log(
                TESTING_LOG_LEVEL,
                f'Workflow completed in {stats.workflow_makespan_s:.3f} s\n'
                f'{stats}',
            )

            workflow_logger.log(stats)

    workflow_logger.close()
    logger.log(TESTING_LOG_LEVEL, f'Workflow run data saved: {csv_file}')
    memory_profiler.stop()
    memory_profiler.join(timeout=5.0)
    logger.log(TESTING_LOG_LEVEL, f'Memory profile data saved: {memory_file}')

    runner_end = time.perf_counter()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start):.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Workflow simulation benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--workflow',
        choices=['mapreduce'],
        required=True,
        help='Workflow type to simulate',
    )
    parser.add_argument(
        '--task-count',
        type=int,
        metavar='INT',
        help='Number of tasks in the workflow',
    )
    parser.add_argument(
        '--input-sizes',
        type=int,
        metavar='BYTES',
        nargs='+',
        required=True,
        help='Task input size in bytes',
    )
    parser.add_argument(
        '--sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Simulate task computation',
    )
    parser.add_argument(
        '--repeat',
        default=1,
        metavar='RUNS',
        type=int,
        help='Number of runs to repeat each configuration for',
    )
    parser.add_argument(
        '--memory-profile-interval',
        default=0.01,
        metavar='SECONDS',
        type=float,
        help='Seconds between logging system memory utilization',
    )

    add_executor_options(parser)
    add_proxystore_options(parser)
    add_logging_options(parser, require_csv=True)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    executor = init_executor_from_args(args)
    store = init_store_from_args(args, metrics=True)

    with executor:
        runner(
            executor=executor,
            store=store,
            workflow=args.workflow,
            task_count=args.task_count,
            input_sizes=args.input_sizes,
            sleep=args.sleep,
            repeat=args.repeat,
            csv_file=args.csv_file,
            memory_profile_interval=args.memory_profile_interval,
        )

    if store is not None:
        store.close()

    return 0
