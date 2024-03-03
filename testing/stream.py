from __future__ import annotations

import contextlib
import queue
from typing import Generator

from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer
from proxystore.stream.shims.queue import QueuePublisher
from proxystore.stream.shims.queue import QueueSubscriber


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
