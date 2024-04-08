from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

from psbench.benchmarks.template.config import RunConfig
from psbench.benchmarks.template.main import Benchmark


def test_benchmark(thread_executor: ThreadPoolExecutor) -> None:
    config = RunConfig(name='test')

    with Benchmark(thread_executor, None) as benchmark:
        benchmark.config()
        result = benchmark.run(config)
        assert result.name == config.name


def test_benchmark_with_proxystore(
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    config = RunConfig(name='test')

    with Benchmark(thread_executor, file_store) as benchmark:
        benchmark.config()
        result = benchmark.run(config)
        assert result.name == config.name
