from __future__ import annotations

import time
import uuid
from typing import NamedTuple

import requests
from proxystore.store.endpoint import EndpointStore

from psbench.utils import randbytes
from psbench.utils import wait_until


class Stats(NamedTuple):
    """Results of test."""

    queries: int
    total_elapsed_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float


def endpoint_test(
    store: EndpointStore,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /endpoint route test.

    Note:
        EndpointStore does not have an interface for querying /endpoint
        so we do so manually with the request library.

    Args:
        store (EndpointStore): Store interface to use for querying endpoint.
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
            f'http://{store.endpoint_host}:{store.endpoint_port}/endpoint',
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
    )


def evict_test(
    store: EndpointStore,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /evict route test.

    Args:
        store (EndpointStore): Store interface to use for querying endpoint.
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

    fake_key = str(uuid.uuid4())
    for _ in range(queries):
        start = time.perf_counter_ns()
        store.evict(fake_key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
    )


def exists_test(
    store: EndpointStore,
    sleep: float,
    queries: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /exists route test.

    Args:
        store (EndpointStore): Store interface to use for querying endpoint.
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

    fake_key = str(uuid.uuid4())
    for _ in range(queries):
        start = time.perf_counter_ns()
        store.exists(fake_key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
    )


def get_test(
    store: EndpointStore,
    sleep: float,
    queries: int,
    payload_size: int,
    start_time: float | None = None,
) -> Stats:
    """Endpoint /get route test.

    Args:
        store (EndpointStore): Store interface to use for querying endpoint.
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

    key = store.set(randbytes(payload_size))

    for _ in range(queries):
        start = time.perf_counter_ns()
        res = store.get(key)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        assert res is not None
        time.sleep(sleep)

    store.evict(key)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
    )


def set_test(
    store: EndpointStore,
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
        store (EndpointStore): Store interface to use for querying endpoint.
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
        # Note we do set/evict here to keep store memory in check
        start = time.perf_counter_ns()
        key = store.set(data)
        end = time.perf_counter_ns()
        latencies.append((end - start) / 1e6)
        store.evict(key)
        time.sleep(sleep)

    return Stats(
        queries=queries,
        total_elapsed_ms=sum(latencies),
        min_latency_ms=min(latencies),
        max_latency_ms=max(latencies),
        avg_latency_ms=sum(latencies) / len(latencies),
    )
