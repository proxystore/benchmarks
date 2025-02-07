from __future__ import annotations

import pathlib
import time
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store
from proxystore.store.future import Future
from proxystore.stream.protocols import MessagePublisher

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.generator import generate_data
from psbench.benchmarks.stream_scaling.generator import generator_task
from psbench.benchmarks.stream_scaling.shims import ConsumerShim
from psbench.benchmarks.stream_scaling.shims import ProducerShim
from psbench.config.stream import StreamConfig
from testing.stream import create_stream_pair


@pytest.mark.parametrize(
    ('pregenerate', 'direct'),
    ((True, False), (False, True)),
)
def test_generator_max_items(
    pregenerate: bool,
    direct: bool,
    file_store: Store[FileConnector],
) -> None:
    stop_generator: Future[bool] = file_store.future()
    item_size_bytes = 100
    max_items = 5
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        producer_shim = ProducerShim(producer, direct_to_publisher=direct)
        consumer_shim = ConsumerShim(consumer, direct_from_subscriber=direct)

        generate_data(
            producer_shim,
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            pregenerate=pregenerate,
            interval=0,
            topic=topic,
        )

        producer_shim.close_topic(topic)

        items = list(consumer_shim)

        assert len(items) == max_items
        assert all(len(item) == item_size_bytes for item in items)


def test_generator_interval(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    interval = 0.01
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        start = time.perf_counter()
        producer_shim = ProducerShim(producer)
        generate_data(
            producer_shim,
            stop_generator,
            item_size_bytes=1,
            max_items=1,
            interval=interval,
            topic=topic,
        )
        producer_shim.close_topic(topic)
        end = time.perf_counter()
        assert (end - start) > interval


def test_generator_stop(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    topic = 'topic'

    stop_generator.set_result(True)

    with create_stream_pair(file_store, topic) as (producer, consumer):
        producer_shim = ProducerShim(producer)
        generate_data(
            producer_shim,
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            interval=0,
            topic=topic,
        )

        producer_shim.close_topic(topic)
        items = list(consumer.iter_objects())

    assert len(items) == 0


@pytest.mark.parametrize('method', ('default', 'proxy', 'adios'))
def test_generator_task(
    method: str,
    file_store: Store[FileConnector],
    tmp_path: pathlib.Path,
) -> None:
    if method == 'adios':
        try:
            import adios2  # noqa: F401
        except ImportError:  # pragma: no cover
            pytest.skip()

    run_config = RunConfig(
        data_size_bytes=100,
        max_workers=1,
        task_count=1,
        task_sleep=0,
        method=method,
        adios_file=str(tmp_path / 'adios-stream'),
    )
    stop_generator: Future[bool] = file_store.future()
    stream_config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost:1234'],
    )

    with (
        mock.patch(
            'psbench.benchmarks.stream_scaling.generator.generate_data',
        ) as mock_generate,
        mock.patch(
            'psbench.config.stream.RedisPublisher',
            spec=MessagePublisher,
        ),
    ):
        generator_task(
            run_config,
            file_store.config(),
            stream_config,
            stop_generator,
            interval=0,
        )

        assert mock_generate.call_count == 1
