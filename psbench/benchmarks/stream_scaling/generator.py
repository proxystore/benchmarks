from __future__ import annotations

import time
from typing import Any

from proxystore.store.base import Store
from proxystore.store.config import StoreConfig
from proxystore.store.future import Future
from proxystore.stream.interface import StreamProducer
from proxystore.stream.protocols import Publisher

from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.shims import Adios2Publisher
from psbench.benchmarks.stream_scaling.shims import ProducerShim
from psbench.config.stream import StreamConfig
from psbench.utils import randbytes
from psbench.utils import wait_until


def generate_data(
    publisher: Publisher,
    stop_generator: Future[bool],
    *,
    item_size_bytes: int,
    max_items: int,
    topic: str,
    interval: float = 0,
    pregenerate: bool = False,
) -> None:
    sent_items = 0

    data: bytes | None = None
    if pregenerate:
        # Pregenerate the data when the data size is too large or the interval
        # is too small to keep up. Note that the StreamProducer will still
        # create unique events and items in the store even though the data
        # is the same each time.
        data = randbytes(item_size_bytes)

    while sent_items < max_items:
        interval_end = time.time() + interval
        if stop_generator.done():
            break

        if not pregenerate:
            data = randbytes(item_size_bytes)

        assert data is not None
        publisher.send(topic, data)
        sent_items += 1

        wait_until(interval_end)


def generator_task(
    run_config: RunConfig,
    store_config: StoreConfig,
    stream_config: StreamConfig,
    stop_generator: Future[bool],
    *,
    interval: float = 0,
    pregenerate: bool = False,
) -> None:
    publisher: Publisher
    if run_config.method in ('default', 'proxy'):
        base_publisher = stream_config.get_publisher()
        assert base_publisher is not None
        store: Store[Any] = Store.from_config(store_config)
        producer = StreamProducer[bytes](
            base_publisher,
            {stream_config.topic: store},
        )
        publisher = ProducerShim(
            producer,
            direct_to_publisher=run_config.method == 'default',
        )
    elif run_config.method == 'adios':
        publisher = Adios2Publisher(run_config.adios_file)
    else:
        raise AssertionError(f'Unknown stream method {run_config.method}.')

    generate_data(
        publisher,
        stop_generator,
        item_size_bytes=run_config.data_size_bytes,
        max_items=run_config.task_count,
        pregenerate=pregenerate,
        interval=interval,
        topic=stream_config.topic,
    )

    if run_config.method in ('default', 'proxy'):
        assert isinstance(publisher, ProducerShim)
        publisher.close_topic(stream_config.topic)
        publisher.producer.close(stores=False)
    else:
        publisher.close()
