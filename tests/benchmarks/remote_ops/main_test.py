from __future__ import annotations

from unittest import mock

import pytest

from psbench.benchmarks.remote_ops.config import RunConfig
from psbench.benchmarks.remote_ops.main import Benchmark
from psbench.benchmarks.remote_ops.main import runner_endpoint
from psbench.benchmarks.remote_ops.main import runner_redis
from testing.mocking import MockStrictRedis


@pytest.mark.asyncio()
async def test_runner_endpoint() -> None:
    await runner_endpoint(
        None,
        ['get', 'set', 'evict', 'exists'],
        payload_sizes=[100, 1000],
        repeat=1,
        relay_server=None,
    )


def test_runner_redis() -> None:
    with mock.patch('redis.StrictRedis', side_effect=MockStrictRedis):
        runner_redis(
            'localhost',
            1234,
            ['get', 'set', 'evict', 'exists'],
            payload_sizes=[100, 1000],
            repeat=1,
        )


def test_benchmark_endpoint() -> None:
    config = RunConfig(
        backend='endpoint',
        ops=['get'],
        payload_sizes=[100, 1000],
        repeat=3,
    )
    with Benchmark() as benchmark:
        benchmark.config()
        results = benchmark.run(config)
        assert len(results) == 2


def test_benchmark_redis() -> None:
    config = RunConfig(
        backend='redis',
        ops=['get'],
        payload_sizes=[100, 1000],
        repeat=3,
    )
    with mock.patch('redis.StrictRedis', side_effect=MockStrictRedis):
        with Benchmark(redis_host='localhost', redis_port=0) as benchmark:
            benchmark.config()
            results = benchmark.run(config)
            assert len(results) == 2
