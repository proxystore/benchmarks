from __future__ import annotations

import pathlib
import uuid
from unittest import mock

import pytest
import redis
from proxystore.connectors.file import FileConnector
from proxystore.store import Store

from psbench.benchmarks.colmena_rtt.config import RunConfig
from psbench.benchmarks.colmena_rtt.main import Benchmark
from psbench.config.executor import GlobusComputeConfig
from psbench.config.executor import ParslConfig
from testing.globus_compute import mock_globus_compute
from testing.globus_compute import MockExecutor

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


@pytest.mark.parametrize('reuse_inputs', (True, False))
def test_benchmark(reuse_inputs: bool, tmp_path: pathlib.Path) -> None:
    parsl_config = ParslConfig(
        executor='thread',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    run_config = RunConfig(
        input_sizes=[10, 100],
        output_sizes=[10, 100],
        task_sleep=0,
        reuse_inputs=reuse_inputs,
    )

    with Benchmark(parsl_config, store=None) as benchmark:
        benchmark.config()
        results = benchmark.run(run_config)

    expected = len(run_config.input_sizes) * len(run_config.output_sizes)
    assert len(results) == expected


def test_benchmark_with_proxystore(
    file_store: Store[FileConnector],
    tmp_path: pathlib.Path,
) -> None:
    parsl_config = ParslConfig(
        executor='thread',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    run_config = RunConfig(
        input_sizes=[10, 100],
        output_sizes=[10, 100],
        task_sleep=0,
        reuse_inputs=False,
    )

    with Benchmark(parsl_config, store=file_store) as benchmark:
        results = benchmark.run(run_config)

    expected = len(run_config.input_sizes) * len(run_config.output_sizes)
    assert len(results) == expected


def test_benchmark_with_globus_compute(tmp_path: pathlib.Path) -> None:
    gc_config = GlobusComputeConfig(endpoint=str(uuid.uuid4()))
    run_config = RunConfig(
        input_sizes=[10, 100],
        output_sizes=[10, 100],
        task_sleep=0,
        reuse_inputs=False,
    )

    with (
        mock_globus_compute(),
        mock.patch(
            'colmena.task_server.globus.Executor',
            MockExecutor,
        ),
    ):
        with Benchmark(gc_config, store=None) as benchmark:
            results = benchmark.run(run_config)

    expected = len(run_config.input_sizes) * len(run_config.output_sizes)
    assert len(results) == expected


@pytest.mark.skipif(
    not redis_available,
    reason='Unable to connect to Redis server at localhost:6379',
)
def test_benchmark_with_redis_queue(
    tmp_path: pathlib.Path,
) -> None:  # pragma: no cover
    parsl_config = ParslConfig(
        executor='thread',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    run_config = RunConfig(
        input_sizes=[10, 100],
        output_sizes=[10, 100],
        task_sleep=0,
        reuse_inputs=False,
    )

    with Benchmark(
        parsl_config,
        store=None,
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
    ) as benchmark:
        results = benchmark.run(run_config)

    expected = len(run_config.input_sizes) * len(run_config.output_sizes)
    assert len(results) == expected
