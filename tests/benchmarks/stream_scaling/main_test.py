from __future__ import annotations

from proxystore.connectors.local import LocalConnector
from proxystore.store.base import Store

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.main import Benchmark
from psbench.config import StreamConfig
from testing.executor import ThreadPoolExecutor


def test_benchmark(
    local_store: Store[LocalConnector],
    thread_executor: ThreadPoolExecutor,
) -> None:
    stream_config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost'],
    )
    run_config = RunConfig(
        data_size_bytes=1,
        producer_sleep=1,
        task_count=1,
        task_sleep=1,
    )

    with Benchmark(thread_executor, local_store, stream_config) as benchmark:
        benchmark.config()

        result = benchmark.run(run_config)

        assert result.data_size_bytes == run_config.data_size_bytes
