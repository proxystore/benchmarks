from __future__ import annotations

import time
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store
from proxystore.store.future import Future

from psbench.benchmarks.stream_scaling.generator import generate_data
from psbench.benchmarks.stream_scaling.generator import generator_task
from psbench.benchmarks.stream_scaling.shims import ConsumerShim
from psbench.benchmarks.stream_scaling.shims import ProducerShim
from psbench.config.stream import StreamConfig
from testing.stream import create_stream_pair


@pytest.mark.parametrize(
    ('pregenerate', 'use_proxies'),
    ((True, False), (False, True)),
)
def test_generator_max_items(
    pregenerate: bool,
    use_proxies: bool,
    file_store: Store[FileConnector],
) -> None:
    stop_generator: Future[bool] = file_store.future()
    item_size_bytes = 100
    max_items = 5
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        producer_shim = ProducerShim(
            producer,
            direct_to_publisher=not use_proxies,
        )
        consumer_shim = ConsumerShim(
            consumer,
            direct_from_subscriber=not use_proxies,
        )

        generate_data(
            producer_shim,
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            pregenerate=pregenerate,
            interval=0,
            topic=topic,
        )

        items = list(consumer_shim)

        assert len(items) == max_items
        assert all(len(item) == item_size_bytes for item in items)


def test_generator_interval(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    interval = 0.01
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        start = time.perf_counter()
        generate_data(
            ProducerShim(producer),
            stop_generator,
            item_size_bytes=1,
            max_items=1,
            interval=interval,
            topic=topic,
        )
        end = time.perf_counter()
        assert (end - start) > interval


def test_generator_stop(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    topic = 'topic'

    stop_generator.set_result(True)

    with create_stream_pair(file_store, topic) as (producer, consumer):
        generate_data(
            ProducerShim(producer),
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            interval=0,
            topic=topic,
        )

        items = list(consumer.iter_objects())

    assert len(items) == 0


def test_generator_task(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    stream_config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost:1234'],
    )

    with mock.patch(
        'psbench.benchmarks.stream_scaling.generator.generate_data',
    ) as mock_generate, mock.patch('psbench.config.stream.RedisPublisher'):
        generator_task(
            file_store.config(),
            stream_config,
            stop_generator,
            item_size_bytes=100,
            max_items=1,
            interval=0,
            topic='topic',
        )

        assert mock_generate.call_count == 1
