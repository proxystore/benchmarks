from __future__ import annotations

import time
import uuid

from proxystore.endpoint.endpoint import Endpoint

from psbench.utils import randbytes


async def test_evict(
    endpoint: Endpoint,
    target_endpoint: uuid.UUID | None,
    repeat: int = 1,
) -> list[float]:
    """Test endpoint eviction.

    Args:
        endpoint (Endpoint): local endpoint.
        target_endpoint (UUID): optional UUID of target endpoint to perform
            operation on (local endpoint forwards op to target).
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    for _ in range(repeat):
        start = time.perf_counter_ns()
        await endpoint.evict('missing-key', target_endpoint)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    return times_ms


async def test_exists(
    endpoint: Endpoint,
    target_endpoint: uuid.UUID | None,
    repeat: int = 1,
) -> list[float]:
    """Test endpoint key exists.

    Args:
        endpoint (Endpoint): local endpoint.
        target_endpoint (UUID): optional UUID of target endpoint to perform
            operation on (local endpoint forwards op to target).
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    for _ in range(repeat):
        start = time.perf_counter_ns()
        await endpoint.exists('missing-key', target_endpoint)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    return times_ms


async def test_get(
    endpoint: Endpoint,
    target_endpoint: uuid.UUID | None,
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    """Test endpoint get data.

    Args:
        endpoint (Endpoint): local endpoint.
        target_endpoint (UUID): optional UUID of target endpoint to perform
            operation on (local endpoint forwards op to target).
        payload_size_bytes (int): size of payload to request from target
            endpoint.
        repeat (int): repeat the operation this many times (default: 1).

    Returns:
        list of times for each operation to complete.
    """
    times_ms: list[float] = []

    data = randbytes(payload_size_bytes)
    await endpoint.set('key', data, target_endpoint)

    for _ in range(repeat):
        start = time.perf_counter_ns()
        res = await endpoint.get('key', target_endpoint)
        assert isinstance(res, bytes)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

    await endpoint.evict('key')

    return times_ms


async def test_set(
    endpoint: Endpoint,
    target_endpoint: uuid.UUID | None,
    payload_size_bytes: int,
    repeat: int = 1,
) -> list[float]:
    """Test endpoint set data.

    Args:
        endpoint (Endpoint): local endpoint.
        target_endpoint (UUID): optional UUID of target endpoint to perform
            operation on (local endpoint forwards op to target).
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
        await endpoint.set(key, data, target_endpoint)
        end = time.perf_counter_ns()
        times_ms.append((end - start) / 1e6)

        # Evict key immediately to keep memory usage low
        await endpoint.evict(key)

    return times_ms
