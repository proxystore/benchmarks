from __future__ import annotations

import contextlib
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.main import Benchmark
from psbench.config import StreamConfig
from testing.executor import ThreadPoolExecutor
from testing.stream import create_stream_pair


@pytest.mark.parametrize('use_proxies', (True, False))
def test_benchmark(
    use_proxies: bool,
    file_store: Store[FileConnector],
    thread_executor: ThreadPoolExecutor,
) -> None:
    stream_config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost'],
    )
    run_config = RunConfig(
        data_size_bytes=100,
        task_count=8,
        task_sleep=0.001,
        use_proxies=use_proxies,
    )

    with contextlib.ExitStack() as stack:
        producer, consumer = stack.enter_context(
            create_stream_pair(file_store, stream_config.topic),
        )

        stack.enter_context(
            mock.patch(
                'psbench.config.stream.StreamConfig.get_publisher',
                return_value=producer._publisher,
            ),
        )

        benchmark = stack.enter_context(
            Benchmark(consumer, thread_executor, file_store, stream_config),
        )

        benchmark.config()

        result = benchmark.run(run_config)

    assert result.data_size_bytes == run_config.data_size_bytes
    assert result.completed_tasks == run_config.task_count
