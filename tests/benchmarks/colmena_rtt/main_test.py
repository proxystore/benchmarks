from __future__ import annotations

import pathlib
from typing import Generator
from unittest import mock

import pytest
import redis

from psbench.benchmarks.colmena_rtt.main import main
from testing.colmena import MockFuncXTaskServer
from testing.funcx import mock_funcx


REDIS_HOST = 'localhost'
REDIS_PORT = 6379
try:
    redis_client = redis.StrictRedis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )
    redis_client.ping()
    redis_available = True  # pragma: no cover
except redis.exceptions.ConnectionError:  # pragma: no cover
    redis_available = False


@pytest.fixture
def default_args(tmp_path) -> Generator[list[str], None, None]:
    yield [
        '--input-sizes',
        '100',
        '1000',
        '--output-sizes',
        '100',
        '1000',
        '--task-repeat',
        '2',
    ]


@pytest.mark.parametrize(
    'use_proxystore,use_csv,args',
    (
        (True, True, []),
        (False, False, ['--reuse-inputs']),
    ),
)
def test_parsl_e2e(
    use_proxystore: bool,
    use_csv: bool,
    args: list[str],
    tmp_path: pathlib.Path,
    default_args: list[str],
) -> None:
    run_dir = tmp_path / 'runs'
    run_dir.mkdir()

    args = ['--parsl', '--output-dir', str(run_dir)] + args + default_args
    if use_proxystore:
        args += ['--ps-backend', 'FILE', '--ps-file-dir', str(run_dir / 'ps')]
    if use_csv:
        args += ['--csv-file', str(run_dir / 'log.csv')]

    assert main(args) == 0


def test_funcx_e2e(tmp_path: pathlib.Path, default_args: list[str]) -> None:
    run_dir = tmp_path / 'runs'
    run_dir.mkdir()

    args = ['--funcx', '--endpoint', 'ENDPOINT', '--output-dir', str(run_dir)]
    args += default_args

    with mock.patch(
        'psbench.benchmarks.colmena_rtt.main.FuncXTaskServer',
        MockFuncXTaskServer,
    ):
        with mock_funcx():
            assert main(args) == 0


@pytest.mark.skipif(
    not redis_available,
    reason='Unable to connect to Redis server at localhost:6379',
)
def test_redis_queues(
    tmp_path: pathlib.Path,
    default_args: list[str],
) -> None:  # pragma: no cover
    run_dir = tmp_path / 'runs'
    run_dir.mkdir()

    args = ['--parsl', '--output-dir', str(run_dir)]
    args += ['--redis-host', REDIS_HOST, '--redis-port', str(REDIS_PORT)]
    args += default_args

    assert main(args) == 0
