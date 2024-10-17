from __future__ import annotations

import contextlib
import pathlib
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.main import Benchmark
from psbench.config import StreamConfig
from testing.stream import create_stream_pair


@pytest.mark.parametrize('method', ('default', 'proxy', 'adios'))
def test_benchmark(
    method: str,
    file_store: Store[FileConnector],
    thread_executor: ThreadPoolExecutor,
    tmp_path: pathlib.Path,
) -> None:
    stream_config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost:1234'],
    )
    run_config = RunConfig(
        data_size_bytes=100,
        max_workers=thread_executor._max_workers,
        task_count=8,
        task_sleep=0.001,
        method=method,
        adios_file=str(tmp_path / 'adios-stream'),
    )

    with contextlib.ExitStack() as stack:
        producer, consumer = stack.enter_context(
            create_stream_pair(file_store, stream_config.topic),
        )

        stack.enter_context(
            mock.patch(
                'psbench.config.stream.StreamConfig.get_publisher',
                return_value=producer.publisher,
            ),
        )
        stack.enter_context(
            mock.patch(
                'psbench.config.stream.StreamConfig.get_subscriber',
                return_value=consumer.subscriber,
            ),
        )

        benchmark = stack.enter_context(
            Benchmark(thread_executor, file_store, stream_config),
        )

        benchmark.config()

        result = benchmark.run(run_config)

    assert result.data_size_bytes == run_config.data_size_bytes
    assert result.completed_tasks == run_config.task_count
