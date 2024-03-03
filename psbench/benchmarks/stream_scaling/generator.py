from __future__ import annotations

import time
from typing import Any

from proxystore.store.base import Store
from proxystore.store.future import Future
from proxystore.stream.interface import StreamProducer

from psbench.config.stream import StreamConfig
from psbench.utils import randbytes
from psbench.utils import wait_until


def generate_data(
    producer: StreamProducer[bytes],
    stop_generator: Future[bool],
    *,
    item_size_bytes: int,
    max_items: int,
    interval: float,
    topic: str,
) -> None:
    sent_items = 0

    while sent_items < max_items:
        interval_end = time.time() + interval
        if stop_generator.done():
            break

        data = randbytes(item_size_bytes)
        producer.send(topic, data, evict=True)
        sent_items += 1

        wait_until(interval_end)

    producer.close_topics(topic)


def generator_task(
    store_config: dict[str, Any],
    stream_config: StreamConfig,
    stop_generator: Future[bool],
    *,
    item_size_bytes: int,
    max_items: int,
    interval: float,
    topic: str,
) -> None:
    store: Store[Any] = Store.from_config(store_config)
    publisher = stream_config.get_publisher()

    with StreamProducer[bytes](publisher, {topic: store}) as producer:
        generate_data(
            producer,
            stop_generator,
            item_size_bytes=item_size_bytes,
            max_items=max_items,
            interval=interval,
            topic=topic,
        )
