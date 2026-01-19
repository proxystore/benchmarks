"""Endpoint Queries-per-Second Test."""

from __future__ import annotations

import datetime
import functools
import logging
import math
import multiprocessing
import sys
import time
from collections.abc import Callable
from statistics import stdev
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

from proxystore.connectors.endpoint import EndpointConnector

from psbench.benchmarks.endpoint_qps import routes
from psbench.benchmarks.endpoint_qps.config import ROUTE_TYPE
from psbench.benchmarks.endpoint_qps.config import RunConfig
from psbench.benchmarks.endpoint_qps.config import RunResult
from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.logging import TEST_LOG_LEVEL

PROCESS_STARTUP_BUFFER_SECONDS = 5

logger = logging.getLogger('endpoint-qps')


def run(
    endpoint: str,
    route: ROUTE_TYPE,
    *,
    payload_size: int = 0,
    queries: int = 100,
    sleep: float = 0,
    workers: int = 1,
) -> RunResult:
    """Run test workers and gather results.

    Args:
        endpoint (str): endpoint uuid.
        route (str): endpoint route to query.
        payload_size (int): bytes to send/receive for GET/SET routes.
        queries (int): number of queries to perform per worker.
        sleep (float): sleep (seconds) between queries.
        workers (int): number of worker processes to use.

    Returns:
        RunResult with summary of test run.
    """
    connector = EndpointConnector([endpoint])

    logger.log(
        TEST_LOG_LEVEL,
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
    logger.log(TEST_LOG_LEVEL, f'starting test at {readable_start_time}')

    with multiprocessing.Pool(workers) as pool:
        logger.log(
            TEST_LOG_LEVEL,
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

    run_stats = RunResult(
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
        TEST_LOG_LEVEL,
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


class Benchmark(ContextManagerAddIn):
    name = 'Endpoint QPS'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self) -> None:
        super().__init__()

    def config(self) -> dict[str, Any]:
        return {}

    def run(self, config: RunConfig) -> RunResult:
        return run(
            config.endpoint,
            config.route,
            payload_size=config.payload_size_bytes,
            queries=config.total_queries,
            sleep=config.sleep_seconds,
            workers=config.workers,
        )
