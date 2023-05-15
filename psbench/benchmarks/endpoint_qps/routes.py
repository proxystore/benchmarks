from __future__ import annotations

import time
import uuid
from statistics import stdev
from typing import NamedTuple

import requests
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.endpoint import EndpointKey

from psbench.utils import randbytes
from psbench.utils import wait_until


class Stats(NamedTuple):
    """Results of test."""

    queries: int
    total_elapsed_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    stdev_latency_ms: float


def endpoint_test(
    connector: EndpointConnector,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /endpoint route test.

    Note:
        EndpointConnector does not have an interface for querying /endpoint
        so we do so manually with the request library.

    Args:
        connector (EndpointConnector): Connector to use for querying endpoint.
        sleep (float): sleep (seconds) between queries.
        queries (int): number of queries to make.
        start_time (float): UNIX timestamp to sleep until for starting test.
            Useful for synchronizing workers.

    Returns:
        Stats object with results of test.
    """
    if start_time is not None:
        wait_until(start_time)

    latencies: list[float] = []

    for _ in range(queries):
        start = time.perf_counter_ns()
        response = requests.get(
            f'http://{connector.endpoint_host}:{connector.endpoint_port}/endpoint',  # noqa: E501
        )
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        assert response.status_code == 200
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
        stdev_latency_ms=stdev(latencies) if len(latencies) > 1 else 0,
    )


def evict_test(
    connector: EndpointConnector,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /evict route test.

    Args:
        connector (EndpointConnector): Connector to use for querying endpoint.
        sleep (float): sleep (seconds) between queries.
        queries (int): number of queries to make.
        start_time (float): UNIX timestamp to sleep until for starting test.
            Useful for synchronizing workers.

    Returns:
        Stats object with results of test.
    """
    if start_time is not None:
        wait_until(start_time)

    latencies: list[float] = []

    fake_key = EndpointKey(str(uuid.uuid4()), None)
    for _ in range(queries):
        start = time.perf_counter_ns()
        connector.evict(fake_key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
        stdev_latency_ms=stdev(latencies) if len(latencies) > 1 else 0,
    )


def exists_test(
    connector: EndpointConnector,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /exists route test.

    Args:
        connector (EndpointConnector): Connector to use for querying endpoint.
        sleep (float): sleep (seconds) between queries.
        queries (int): number of queries to make.
        start_time (float): UNIX timestamp to sleep until for starting test.
            Useful for synchronizing workers.

    Returns:
        Stats object with results of test.
    """
    if start_time is not None:
        wait_until(start_time)

    latencies: list[float] = []

    fake_key = EndpointKey(str(uuid.uuid4()), None)
    for _ in range(queries):
        start = time.perf_counter_ns()
        connector.exists(fake_key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
        stdev_latency_ms=stdev(latencies) if len(latencies) > 1 else 0,
    )


def get_test(
    connector: EndpointConnector,
    sleep: float,
    queries: int,
    payload_size: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /get route test.

    Args:
        connector (EndpointConnector): Connector to use for querying endpoint.
        sleep (float): sleep (seconds) between queries.
        queries (int): number of queries to make.
        payload_size (int): payload size in bytes.
        start_time (float): UNIX timestamp to sleep until for starting test.
            Useful for synchronizing workers.

    Returns:
        Stats object with results of test.
    """
    if start_time is not None:
        wait_until(start_time)

    latencies: list[float] = []

    key = connector.put(randbytes(payload_size))

    for _ in range(queries):
        start = time.perf_counter_ns()
        res = connector.get(key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        assert res is not None
        time.sleep(sleep)

    connector.evict(key)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
        stdev_latency_ms=stdev(latencies) if len(latencies) > 1 else 0,
    )


def set_test(
    connector: EndpointConnector,
    sleep: float,
    queries: int,
    payload_size: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /set route test.

    Note:
        To keep the size of the endpoint's storage from exploding, this test
        executes SET followed by EVICT operations, but only the SET ops
        are included in the timing.

    Args:
        connector (EndpointConnector): Connector to use for querying endpoint.
        sleep (float): sleep (seconds) between queries.
        queries (int): number of queries to make.
        payload_size (int): payload size in bytes.
        start_time (float): UNIX timestamp to sleep until for starting test.
            Useful for synchronizing workers.

    Returns:
        Stats object with results of test.
    """
    if start_time is not None:
        wait_until(start_time)

    latencies: list[float] = []

    data = randbytes(payload_size)

    for _ in range(queries):
        # Note we do put/evict here to keep connector memory in check
        start = time.perf_counter_ns()
        key = connector.put(data)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        connector.evict(key)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
        stdev_latency_ms=stdev(latencies) if len(latencies) > 1 else 0,
    )
