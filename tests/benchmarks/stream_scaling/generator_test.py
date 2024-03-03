from __future__ import annotations

import contextlib
import queue
import time
from typing import Generator
from unittest import mock

from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store
from proxystore.store.future import Future
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber

from psbench.benchmarks.stream_scaling.generator import generate_data
from psbench.benchmarks.stream_scaling.generator import generator_task
from psbench.config.stream import StreamConfig


@contextlib.contextmanager
def create_stream_pair(
    store: Store[FileConnector],
    topic: str,
) -> Generator[
    tuple[StreamProducer[bytes], StreamConsumer[bytes]],
    None,
    None,
]:
    queue_ = queue.Queue[bytes]()

    publisher = QueuePublisher({topic: queue_})
    subscriber = QueueSubscriber(queue_)

    with StreamProducer[bytes](publisher, {topic: store}) as producer:
        with StreamConsumer[bytes](subscriber) as consumer:
            yield producer, consumer


def test_generator_max_items(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    item_size_bytes = 100
    max_items = 5
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        generate_data(
            producer,
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            sleep=0,
            topic=topic,
        )

        items = list(consumer.iter_objects())

    assert len(items) == max_items
    assert all(len(item) == item_size_bytes for item in items)


def test_generator_sleep(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    sleep = 0.01
    topic = 'topic'

    with create_stream_pair(file_store, topic) as (producer, consumer):
        start = time.perf_counter()
        generate_data(
            producer,
            stop_generator,
            item_size_bytes=1,
            max_items=1,
            sleep=sleep,
            topic=topic,
        )
        end = time.perf_counter()
        assert (end - start) > sleep


def test_generator_stop(file_store: Store[FileConnector]) -> None:
    stop_generator: Future[bool] = file_store.future()
    topic = 'topic'

    stop_generator.set_result(True)

    with create_stream_pair(file_store, topic) as (producer, consumer):
        generate_data(
            producer,
            stop_generator,
            item_size_bytes=100,
            max_items=5,
            sleep=0,
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
            sleep=0,
            topic='topic',
        )

        assert mock_generate.call_count == 1
