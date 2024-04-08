from __future__ import annotations

import pathlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.store import Store

from psbench.benchmarks.task_rtt.config import RunConfig
from psbench.benchmarks.task_rtt.main import Benchmark
from psbench.benchmarks.task_rtt.main import time_task
from psbench.benchmarks.task_rtt.main import time_task_ipfs
from psbench.benchmarks.task_rtt.main import time_task_proxy
from testing.globus_compute import mock_executor
from testing.ipfs import mock_ipfs


def test_time_task() -> None:
    gce = mock_executor()

    stats = time_task(
        executor=gce,
        input_size=100,
        output_size=50,
        task_sleep=0.01,
    )

    assert stats.input_size_bytes == 100
    assert stats.output_size_bytes == 50
    assert stats.task_sleep_seconds == 0.01
    assert stats.total_time_ms >= 10


def test_time_task_ipfs(tmp_path: pathlib.Path) -> None:
    with mock_ipfs():
        gce = mock_executor()

        stats = time_task_ipfs(
            executor=gce,
            ipfs_local_dir=str(tmp_path / 'local'),
            ipfs_remote_dir=str(tmp_path / 'remote'),
            input_size=100,
            output_size=50,
            task_sleep=0.01,
        )

        assert stats.input_size_bytes == 100
        assert stats.output_size_bytes == 50
        assert stats.task_sleep_seconds == 0.01
        assert stats.total_time_ms >= 10

        stats = time_task_ipfs(
            executor=gce,
            ipfs_local_dir=str(tmp_path / 'local'),
            ipfs_remote_dir=str(tmp_path / 'remote'),
            input_size=100,
            output_size=0,
            task_sleep=0.0,
        )

        assert stats.input_size_bytes == 100
        assert stats.output_size_bytes == 0
        assert stats.task_sleep_seconds == 0.0
        assert stats.total_time_ms >= 0.0


def test_time_task_proxy(local_store: Store[LocalConnector]) -> None:
    gce = mock_executor()

    stats = time_task_proxy(
        executor=gce,
        store=local_store,
        input_size=100,
        output_size=50,
        task_sleep=0.01,
    )

    assert stats.proxystore_backend == 'LocalConnector'
    assert stats.input_size_bytes == 100
    assert stats.output_size_bytes == 50
    assert stats.task_sleep_seconds == 0.01
    assert stats.total_time_ms >= 10
    assert stats.input_get_ms is not None
    assert stats.input_get_ms > 0
    assert stats.input_put_ms is not None
    assert stats.input_put_ms > 0
    assert stats.input_proxy_ms is not None
    assert stats.input_proxy_ms > 0
    assert stats.input_resolve_ms is not None
    assert stats.input_resolve_ms > 0
    assert stats.output_get_ms is not None
    assert stats.output_get_ms > 0
    assert stats.output_put_ms is not None
    assert stats.output_put_ms > 0
    assert stats.output_proxy_ms is not None
    assert stats.output_proxy_ms > 0
    assert stats.output_resolve_ms is not None
    assert stats.output_resolve_ms > 0


def test_benchmark_store_and_ipfs(
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    with pytest.raises(ValueError, match='IPFS and ProxyStore cannot'):
        Benchmark(
            thread_executor,
            store=file_store,
            use_ipfs=True,
        )


def test_benchmark(thread_executor: ThreadPoolExecutor) -> None:
    config = RunConfig(sleep=0, input_size_bytes=10, output_size_bytes=10)

    with Benchmark(thread_executor) as benchmark:
        benchmark.config()
        result = benchmark.run(config)

    assert result.task_sleep_seconds == config.sleep
    assert result.input_size_bytes == config.input_size_bytes
    assert result.output_size_bytes == config.output_size_bytes


def test_benchmark_proxystore(
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    config = RunConfig(sleep=0, input_size_bytes=10, output_size_bytes=10)

    with Benchmark(thread_executor, store=file_store) as benchmark:
        benchmark.config()
        result = benchmark.run(config)

    assert result.task_sleep_seconds == config.sleep
    assert result.input_size_bytes == config.input_size_bytes
    assert result.output_size_bytes == config.output_size_bytes


def test_benchmark_ipfs(
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    config = RunConfig(sleep=0, input_size_bytes=10, output_size_bytes=10)

    ipfs_local_dir = tmp_path / 'ipfs-local'
    ipfs_remote_dir = tmp_path / 'ipfs-remote'
    ipfs_local_dir.mkdir()
    ipfs_remote_dir.mkdir()

    with Benchmark(
        thread_executor,
        use_ipfs=True,
        ipfs_local_dir=str(ipfs_local_dir),
        ipfs_remote_dir=str(ipfs_remote_dir),
    ) as benchmark:
        benchmark.config()
        with mock_ipfs():
            result = benchmark.run(config)

    assert result.task_sleep_seconds == config.sleep
    assert result.input_size_bytes == config.input_size_bytes
    assert result.output_size_bytes == config.output_size_bytes

    assert not ipfs_local_dir.exists()
    assert not ipfs_remote_dir.exists()
