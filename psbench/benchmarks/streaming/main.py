"""Streaming benchmark."""

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
from psbench.argparse import add_stream_options
from psbench.csv import CSVLogger
from psbench.executor.factory import init_executor_from_args
from psbench.executor.protocol import Executor
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.stream import stream_config_from_args

logger = logging.getLogger('streaming')


class TaskStats(NamedTuple):
    executor: str
    connector: str
    stream: str
    data_size_bytes: int
    producer_sleep: float
    task_count: int
    task_sleep: float
    workers: int
    task_submitted_timestamp: float
    task_received_timestamp: float


def runner(
    executor: Executor,
    store: Store[Any] | None,
    stream_config: dict[str, Any],
    data_size_bytes: Sequence[int],
    producer_sleep: float,
    task_count: int,
    task_sleep: float,
    workers: int,
    csv_file: str,
) -> None:
    runner_start = time.perf_counter()
    connector_name = (
        store.connector.__class__.__name__ if store is not None else 'None'
    )
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - Executor: {executor.__class__.__name__}\n'
        f' - ProxyStore Connector: {connector_name}\n'
        f' - Stream Broker: {stream_config["stream"]}\n'
        f' - Data size (bytes): {data_size_bytes}\n'
        f' - Producer sleep (s): {producer_sleep}\n'
        f' - Task count: {task_count}\n'
        f' - Task sleep (s): {task_sleep}\n'
        f' - Workers: {workers}',
    )

    csv_logger = CSVLogger(csv_file, TaskStats)

    logger.log(
        TESTING_LOG_LEVEL,
        'Submitted pre-task to alleviate cold-start penalty',
    )
    future = executor.submit(sum, [1, 2, 3])
    assert future.result() == 6

    for data_size in data_size_bytes:
        # TODO
        stats = run(...)

        logger.log(
            TESTING_LOG_LEVEL,
            f'Task completed in {stats.workflow_makespan_ms:.3f} ms\n'
            f'{stats}',
        )

        if csv_logger is not None:
            csv_logger.log(stats)

    csv_logger.close()

    runner_end = time.perf_counter()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start):.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Scalable stream processing benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--data-size-bytes',
        metavar='BYTES',
        nargs='+',
        required=True,
        type=int,
        help='Size of stream data objects in bytes',
    )
    parser.add_argument(
        '--producer-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Sleep time between producing new stream items',
    )
    parser.add_argument(
        '--task-count',
        metavar='INT',
        required=True,
        type=float,
        help='Total number of stream items to process',
    )
    parser.add_argument(
        '--task-sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Stream processing task sleep time',
    )
    parser.add_argument(
        '--workers',
        metavar='INT',
        required=True,
        type=float,
        help='Number of workers (should match the --executor config)',
    )

    add_executor_options(parser)
    add_proxystore_options(parser)
    add_stream_options(parser, required=True)
    add_logging_options(parser, require_csv=True)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    executor = init_executor_from_args(args)
    store = init_store_from_args(args, metrics=True)
    stream_config = stream_config_from_args(args)
    assert stream_config is not None

    with executor:
        runner(
            executor=executor,
            store=store,
            stream_config=stream_config,
            data_size_bytes=args.data_size_bytes,
            producer_sleep=args.producer_sleep,
            task_count=args.task_count,
            task_sleep=args.task_sleep,
            workers=args.workers,
            csv_file=args.csv_file,
        )

    if store is not None:
        store.close()

    return 0
