from __future__ import annotations

from typing import Literal
from unittest import mock

import pytest

from psbench.benchmarks.endpoint_qps.config import RunConfig
from psbench.benchmarks.endpoint_qps.main import Benchmark
from psbench.benchmarks.endpoint_qps.main import run
from psbench.benchmarks.endpoint_qps.routes import Stats


def call_directly(func, *args, **kwargs):
    result = mock.MagicMock()
    result.get = mock.MagicMock(
        return_value=Stats(2, 0.1, 0.1, 0.1, 0.1, 0.1),
    )
    return result


@pytest.mark.parametrize(
    'route',
    ('GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'),
)
def test_run(
    route: Literal['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'],
) -> None:
    with (
        mock.patch(
            'psbench.benchmarks.endpoint_qps.main.EndpointConnector',
        ),
        mock.patch('multiprocessing.pool.Pool.apply_async', new=call_directly),
    ):
        run('UUID', route, payload_size=1, queries=2, sleep=0, workers=2)


def test_benchmark() -> None:
    config = RunConfig(
        endpoint='UUID',
        route='ENDPOINT',
        payload_size_bytes=0,
        total_queries=1,
        sleep_seconds=0,
        workers=1,
    )
    with (
        mock.patch(
            'psbench.benchmarks.endpoint_qps.main.EndpointConnector',
        ),
        mock.patch('multiprocessing.pool.Pool.apply_async', new=call_directly),
    ):
        with Benchmark() as benchmark:
            benchmark.config()
            result = benchmark.run(config)
            assert result.workers == config.workers
