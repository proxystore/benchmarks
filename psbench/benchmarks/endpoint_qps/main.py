"""Endpoint Queries-per-Second Test."""

from __future__ import annotations

import argparse
import datetime
import functools
import logging
import math
import multiprocessing
import sys
import time
from statistics import stdev
from typing import Callable
from typing import Literal
from typing import NamedTuple
from typing import Sequence

from proxystore.connectors.endpoint import EndpointConnector

from psbench.argparse import add_logging_options
from psbench.benchmarks.endpoint_qps import routes
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.results import CSVLogger

PROCESS_STARTUP_BUFFER_SECONDS = 5
ROUTE_TYPE = Literal['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT']

logger = logging.getLogger('endpoint-qps')


class RunStats(NamedTuple):
    """Stats for the run."""

    route: ROUTE_TYPE
    payload_size_bytes: int
    total_queries: int
    sleep_seconds: float
    workers: int
    min_worker_elapsed_time_ms: float
    max_worker_elapsed_time_ms: float
    avg_worker_elapsed_time_ms: float
    stdev_worker_elapsed_time_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    stdev_latency_ms: float
    qps: float


def run(
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
    connector = EndpointConnector([endpoint])

    logger.log(
        TESTING_LOG_LEVEL,
        f'starting QPS for /{route} with endpoint {endpoint}...',
    )

    func: Callable[[float], routes.Stats]
    if route == 'ENDPOINT':
        func = functools.partial(
            routes.endpoint_test,
            connector,
            sleep,
            queries,
        )
    elif route == 'EVICT':
        func = functools.partial(routes.evict_test, connector, sleep, queries)
    elif route == 'EXISTS':
        func = functools.partial(routes.exists_test, connector, sleep, queries)
    elif route == 'GET':
        func = functools.partial(
            routes.get_test,
            connector,
            sleep,
            queries,
            payload_size,
        )
    elif route == 'SET':
        func = functools.partial(
            routes.set_test,
            connector,
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
    stdev_elapsed_ms = (
        stdev([s.total_elapsed_ms for s in stats]) if len(stats) > 1 else 0
    )
    min_latency_ms = min(s.min_latency_ms for s in stats)
    max_latency_ms = max(s.max_latency_ms for s in stats)
    avg_latency_ms = sum(s.avg_latency_ms for s in stats) / len(stats)
    # Avg standard deviation among k groups with equal samples in each group:
    #   sqrt( (s_1^2 + s_2^2 + ...) / k)
    stdev_latency_ms = math.sqrt(
        sum(s.stdev_latency_ms**2 for s in stats) / len(stats),
    )
    queries = sum(s.queries for s in stats)

    run_stats = RunStats(
        route=route,
        payload_size_bytes=payload_size,
        total_queries=queries,
        sleep_seconds=sleep,
        workers=workers,
        min_worker_elapsed_time_ms=min_elapsed_ms,
        max_worker_elapsed_time_ms=max_elapsed_ms,
        avg_worker_elapsed_time_ms=avg_elapsed_ms,
        stdev_worker_elapsed_time_ms=stdev_elapsed_ms,
        min_latency_ms=min_latency_ms,
        max_latency_ms=max_latency_ms,
        avg_latency_ms=avg_latency_ms,
        stdev_latency_ms=stdev_latency_ms,
        qps=queries / (max_elapsed_ms / 1000),
    )

    logger.log(
        TESTING_LOG_LEVEL,
        f'complete {queries} queries across {workers} workers:\n'
        f'slowest worker elapsed time: {max_elapsed_ms / 1000:.3f} seconds\n'
        f'fastest worker elapsed time: {min_elapsed_ms / 1000:.3f} seconds\n'
        f'minimum request latency: {min_latency_ms:.3f} ms\n'
        f'maximum request latency: {max_latency_ms:.3f} ms\n'
        'average request latency: '
        f'{avg_latency_ms:.3f} ± {stdev_latency_ms:.3f} ms\n'
        f'total QPS: {run_stats.qps:.3f}',
    )

    connector.close()

    return run_stats


def runner(
    endpoint: str,
    routes: list[ROUTE_TYPE],
    *,
    payload_sizes: list[int],
    queries: int,
    sleeps: list[float],
    workers: list[int],
    csv_file: str | None = None,
) -> None:
    """Run matrix of test test configurations.

    Args:
        endpoint (str): endpoint uuid.
        routes (str): endpoint routes to query.
        payload_sizes (int): bytes to send/receive for GET/SET routes.
        queries (int): number of queries to perform per worker.
        sleeps (float): sleep (seconds) between queries.
        workers (int): number of worker processes to use.
        csv_file (str): optional csv filepath to log results to.
    """
    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, RunStats)

    for route in routes:
        for payload_size in payload_sizes:
            for sleep in sleeps:
                for workers_ in workers:
                    run_stats = run(
                        endpoint,
                        route,
                        payload_size=payload_size,
                        queries=queries,
                        sleep=sleep,
                        workers=workers_,
                    )

                    if csv_file is not None:
                        csv_logger.log(run_stats)

    if csv_file is not None:
        csv_logger.close()
        logger.log(TESTING_LOG_LEVEL, f'results logged to {csv_file}')


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
        '--routes',
        choices=['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'],
        nargs='+',
        required=True,
        help='Endpoint routes to query',
    )
    parser.add_argument(
        '--payload-sizes',
        type=int,
        nargs='+',
        default=[0],
        help='Payload sizes for GET/SET queries',
    )
    parser.add_argument(
        '--workers',
        type=int,
        nargs='+',
        default=[1],
        help='Number of workers (processes) making queries',
    )
    parser.add_argument(
        '--sleep',
        type=float,
        nargs='+',
        default=[0],
        help='Sleeps (seconds) between queries',
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

    runner(
        args.endpoint,
        args.routes,
        payload_sizes=args.payload_sizes,
        queries=args.queries,
        sleeps=args.sleep,
        workers=args.workers,
        csv_file=args.csv_file,
    )

    return 0
