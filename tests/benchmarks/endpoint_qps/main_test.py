from __future__ import annotations

import sys
import tempfile
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >3.7 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

import pytest

from psbench.benchmarks.endpoint_qps.main import main
from psbench.benchmarks.endpoint_qps.main import runner
from psbench.benchmarks.endpoint_qps.main import RunStats
from psbench.benchmarks.endpoint_qps.routes import Stats


@pytest.mark.parametrize(
    'route',
    ('GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'),
)
def test_runner(
    route: Literal['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'],
) -> None:
    def call_directly(func, *args, **kwargs):
        result = mock.MagicMock()
        result.get = mock.MagicMock(
            return_value=Stats(2, 0.1, 0.1, 0.1, 0.1, 0.1),
        )
        return result

    with mock.patch(
        'psbench.benchmarks.endpoint_qps.main.EndpointStore',
    ), mock.patch('multiprocessing.pool.Pool.apply_async', new=call_directly):
        runner('UUID', route, payload_size=1, queries=2, sleep=0, workers=2)


@mock.patch('psbench.benchmarks.endpoint_qps.main.runner')
def test_main(mock_runner) -> None:
    assert main(['UUID', '--route', 'GET']) == 0


@mock.patch('psbench.benchmarks.endpoint_qps.main.runner')
def test_csv_logging(mock_runner) -> None:
    mock_runner.return_value = RunStats(
        route='GET',
        payload_size_bytes=0,
        total_queries=1,
        sleep_seconds=0.0,
        workers=1,
        min_worker_elapsed_time_ms=1,
        max_worker_elapsed_time_ms=1,
        avg_worker_elapsed_time_ms=1,
        stdev_worker_elapsed_time_ms=1,
        min_latency_ms=1,
        max_latency_ms=1,
        avg_latency_ms=1,
        stdev_latency_ms=1,
        qps=1,
    )

    with tempfile.NamedTemporaryFile() as f:
        assert len(f.readlines()) == 0
        assert main(['UUID', '--route', 'GET', '--csv-file', f.name]) == 0
        assert len(f.readlines()) == 2
