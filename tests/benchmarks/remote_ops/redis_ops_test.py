from __future__ import annotations

from collections.abc import Generator
from typing import Any

import pytest
import redis

import psbench.benchmarks.remote_ops.redis_ops as ops
from testing.mocking import MockStrictRedis


@pytest.fixture
def client() -> Generator[redis.StrictRedis[Any], None, None]:
    return MockStrictRedis()  # type: ignore


def test_evict(client: redis.StrictRedis[Any]) -> None:
    times = ops.test_evict(client, 2)
    assert len(times) == 2


def test_exists(client: redis.StrictRedis[Any]) -> None:
    times = ops.test_exists(client, 2)
    assert len(times) == 2


def test_get(client: redis.StrictRedis[Any]) -> None:
    times = ops.test_get(client, 100, 2)
    assert len(times) == 2


def test_set(client: redis.StrictRedis[Any]) -> None:
    times = ops.test_set(client, 100, 2)
    assert len(times) == 2
