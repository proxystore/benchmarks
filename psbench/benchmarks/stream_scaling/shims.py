from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer

CLOSE_SENTINAL = b'<publisher-close-topic-sentinal>'


class ConsumerShim:
    def __init__(
        self,
        consumer: StreamConsumer[bytes],
        direct_from_subscriber: bool = False,
    ) -> None:
        self.consumer = consumer
        self.direct_from_subscriber = direct_from_subscriber

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Proxy[bytes] | bytes:
        if self.direct_from_subscriber:
            data = next(self.consumer._subscriber)
            if data == CLOSE_SENTINAL:
                raise StopIteration
            return data
        else:
            return next(self.consumer)


class ProducerShim:
    def __init__(
        self,
        producer: StreamProducer[bytes],
        direct_to_publisher: bool = False,
        proxy_evict: bool = True,
    ) -> None:
        self.producer = producer
        self.direct_to_publisher = direct_to_publisher
        self.proxy_evict = proxy_evict

    def send(self, topic: str, data: bytes) -> None:
        if self.direct_to_publisher:
            self.producer._publisher.send(topic, data)
        else:
            self.producer.send(topic, data, evict=self.proxy_evict)

    def close_topic(self, topic: str) -> None:
        if self.direct_to_publisher:
            self.producer._publisher.send(topic, CLOSE_SENTINAL)
        else:
            self.producer.close_topics(topic)
