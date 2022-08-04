from __future__ import annotations

import sys
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >3.7 cover
    from typing import Literal
else:  # pragma: <3.8 cover
    from typing_extensions import Literal

import pytest

from psbench.benchmarks.endpoint_qps.main import main
from psbench.benchmarks.endpoint_qps.main import runner
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
        result.get = mock.MagicMock(return_value=Stats(2, 0.1, 0.1, 0.1, 0.1))
        return result

    with mock.patch(
        'psbench.benchmarks.endpoint_qps.main.EndpointStore',
    ), mock.patch('multiprocessing.pool.Pool.apply_async', new=call_directly):
        runner('UUID', route, payload_size=1, queries=2, sleep=0, workers=2)


@mock.patch('psbench.benchmarks.endpoint_qps.main.runner')
def test_main(mock_runner) -> None:
    assert main(['UUID', '--route', 'GET']) == 0
