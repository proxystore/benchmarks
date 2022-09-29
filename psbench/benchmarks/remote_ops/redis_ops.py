from __future__ import annotations

import time
from typing import Any

import redis

from psbench.utils import randbytes


def test_evict(
    client: redis.StrictRedis[Any],
    repeat: int = 1,
) -> list[float]:
    """Test Redis eviction.

    Args:
        client (StrictRedis): client connection to remote Redis server.
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    for _ in range(repeat):
        start = time.perf_counter_ns()
        client.delete('missing-key')
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    return times_ms


def test_exists(
    client: redis.StrictRedis[Any],
    repeat: int = 1,
) -> list[float]:
    """Test Redis key exists.

    Args:
        client (StrictRedis): client connection to remote Redis server.
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    for _ in range(repeat):
        start = time.perf_counter_ns()
        client.exists('missing-key')
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    return times_ms


def test_get(
    client: redis.StrictRedis[Any],
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    """Test Redis get data.

    Args:
        client (StrictRedis): client connection to remote Redis server.
        payload_size_bytes (int): size of payload to request from target
            endpoint.
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    data = randbytes(payload_size_bytes)
    client.set('key', data)

    for _ in range(repeat):
        start = time.perf_counter_ns()
        res = client.get('key')
        assert isinstance(res, bytes)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    client.delete('key')

    return times_ms


def test_set(
    client: redis.StrictRedis[Any],
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    """Test Redis set data.

    Args:
        client (StrictRedis): client connection to remote Redis server.
        payload_size_bytes (int): size of payload to request from target
            endpoint.
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    data = randbytes(payload_size_bytes)

    for i in range(repeat):
        key = f'key-{i}'
        start = time.perf_counter_ns()
        client.set(key, data)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

        # Evict key immediately to keep memory usage low
        client.delete(key)

    return times_ms
