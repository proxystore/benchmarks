from __future__ import annotations

import time
from typing import Any

from proxystore.store.base import Store
from proxystore.store.future import Future
from proxystore.store.types import StoreConfig
from proxystore.stream.interface import StreamProducer

from psbench.benchmarks.stream_scaling.shims import ProducerShim
from psbench.config.stream import StreamConfig
from psbench.utils import randbytes
from psbench.utils import wait_until


def generate_data(
    producer: ProducerShim,
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
        producer.send(topic, data)
        sent_items += 1

        wait_until(interval_end)

    producer.close_topic(topic)


def generator_task(
    store_config: StoreConfig,
    stream_config: StreamConfig,
    stop_generator: Future[bool],
    *,
    item_size_bytes: int,
    max_items: int,
    topic: str,
    interval: float = 0,
    pregenerate: bool = False,
    use_proxies: bool = True,
) -> None:
    store: Store[Any] = Store.from_config(store_config)
    publisher = stream_config.get_publisher()
    assert publisher is not None

    producer = StreamProducer[bytes](publisher, {topic: store})
    producer_shim = ProducerShim(
        producer,
        direct_to_publisher=not use_proxies,
        proxy_evict=True,
    )

    generate_data(
        producer_shim,
        stop_generator,
        item_size_bytes=item_size_bytes,
        max_items=max_items,
        pregenerate=pregenerate,
        interval=interval,
        topic=topic,
    )

    producer.close(stores=False)
