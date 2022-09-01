"""Endpoint Queries-per-Second Test."""
from __future__ import annotations

import argparse
import datetime
import functools
import logging
import multiprocessing
import sys
import time
from typing import Callable
from typing import NamedTuple
from typing import Sequence

if sys.version_info >= (3, 8):  # pragma: >3.7 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

from proxystore.store.endpoint import EndpointStore

from psbench.argparse import add_logging_options
from psbench.benchmarks.endpoint_qps import routes
from psbench.csv import CSVLogger
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL

PROCESS_STARTUP_BUFFER_SECONDS = 5
ROUTE_TYPE = Literal['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT']

logger = logging.getLogger('endpoint-qps')


class RunStats(NamedTuple):
    """Stats for the run."""

    route: ROUTE_TYPE
    payload_size_bytes: int
    queries_per_worker: int
    sleep_seconds: float
    workers: int
    min_worker_elapsed_time_ms: float
    max_worker_elapsed_time_ms: float
    avg_worker_elapsed_time_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    qps: float


def runner(
    endpoint: str,
    route: ROUTE_TYPE,
    *,
    payload_size: int = 0,
    queries: int = 100,
    sleep: float = 0,
    workers: int = 1,
) -> RunStats:
    """Run test workers and gather results.

    Args:
        endpoint (str): endpoint uuid.
        route (str): endpoint route to query.
        payload_size (int): bytes to send/receive for GET/SET routes.
        queries (int): number of queries to perform per worker.
        sleep (float): sleep (seconds) between queries.
        workers (int): number of worker processes to use.

    Returns:
        RunStats with summary of test run.
    """
    store = EndpointStore('store', endpoints=[endpoint], cache_size=0)

    logger.log(
        TESTING_LOG_LEVEL,
        f'starting QPS for /{route} with endpoint {endpoint}...',
    )

    func: Callable[[float], routes.Stats]
    if route == 'ENDPOINT':
        func = functools.partial(routes.endpoint_test, store, sleep, queries)
    elif route == 'EVICT':
        func = functools.partial(routes.evict_test, store, sleep, queries)
    elif route == 'EXISTS':
        func = functools.partial(routes.exists_test, store, sleep, queries)
    elif route == 'GET':
        func = functools.partial(
            routes.get_test,
            store,
            sleep,
            queries,
            payload_size,
        )
    elif route == 'SET':
        func = functools.partial(
            routes.set_test,
            store,
            sleep,
            queries,
            payload_size,
        )
    else:
        raise AssertionError('Unsupported route')

    # Tell test functions to start a few seconds from now to ensure all
    # process start at the same time
    start_time = time.time() + PROCESS_STARTUP_BUFFER_SECONDS
    readable_start_time = datetime.datetime.fromtimestamp(start_time).strftime(
        '%H:%M:%S',
    )
    logger.log(TESTING_LOG_LEVEL, f'starting test at {readable_start_time}')

    with multiprocessing.Pool(workers) as pool:
        logger.log(
            TESTING_LOG_LEVEL,
            f'initialized {workers} worker processes',
        )
        results: list[multiprocessing.pool.AsyncResult[routes.Stats]] = [
            pool.apply_async(func, [], {'start_time': start_time})
            for _ in range(workers)
        ]

        stats = [result.get() for result in results]

    min_elapsed_ms = min(s.total_elapsed_ms for s in stats)
    max_elapsed_ms = max(s.total_elapsed_ms for s in stats)
    avg_elapsed_ms = sum(s.total_elapsed_ms for s in stats) / len(stats)
    min_latency_ms = min(s.min_latency_ms for s in stats)
    max_latency_ms = max(s.max_latency_ms for s in stats)
    avg_latency_ms = sum(s.avg_latency_ms for s in stats) / len(stats)
    queries = sum(s.queries for s in stats)

    run_stats = RunStats(
        route=route,
        payload_size_bytes=payload_size,
        queries_per_worker=queries,
        sleep_seconds=sleep,
        workers=workers,
        min_worker_elapsed_time_ms=min_elapsed_ms,
        max_worker_elapsed_time_ms=max_elapsed_ms,
        avg_worker_elapsed_time_ms=avg_elapsed_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        avg_latency_ms=avg_latency_ms,
        qps=queries / (max_elapsed_ms / 1000),
    )

    logger.log(
        TESTING_LOG_LEVEL,
        f'complete {queries} queries across {workers} workers:\n'
        f'slowest worker elapsed time: {max_elapsed_ms / 1000:.3f} seconds\n'
        f'fastest worker elapsed time: {min_elapsed_ms / 1000:.3f} seconds\n'
        f'minimum request latency: {min_latency_ms:.3f} ms\n'
        f'maximum request latency: {max_latency_ms:.3f} ms\n'
        f'average request latency: {avg_latency_ms:.3f} ms\n'
        f'total QPS: {run_stats.qps:.3f}',
    )

    return run_stats


def main(argv: Sequence[str] | None = None) -> int:
    """Endpoint QPS test entrypoint."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='ProxyStore Endpoint QPS Test.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'endpoint',
        help='ProxyStore Endpoint UUID',
    )
    parser.add_argument(
        '--route',
        choices=['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'],
        required=True,
        help='Endpoint route to query',
    )
    parser.add_argument(
        '--payload-size',
        type=int,
        default=0,
        help='Payload sizes for GET/SET queries',
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=1,
        help='Number of workers (processes) making queries',
    )
    parser.add_argument(
        '--sleep',
        type=float,
        default=0,
        help='Sleep (seconds) between queries',
    )
    parser.add_argument(
        '--queries',
        type=int,
        default=100,
        help='Number of queries per worker to make',
    )
    add_logging_options(parser)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    run_stats = runner(
        args.endpoint,
        args.route,
        payload_size=args.payload_size,
        queries=args.queries,
        sleep=args.sleep,
        workers=args.workers,
    )

    if args.csv_file is not None:
        csv_logger = CSVLogger(args.csv_file, RunStats)
        csv_logger.log(run_stats)
        csv_logger.close()

        logger.log(TESTING_LOG_LEVEL, f'results logged to {args.csv_file}')

    return 0
