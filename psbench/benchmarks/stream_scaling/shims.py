from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import adios2
import numpy
from proxystore.proxy import Proxy
from proxystore.stream.interface import StreamConsumer
from proxystore.stream.interface import StreamProducer

CLOSE_SENTINAL = b'<publisher-close-topic-sentinal>'


class Adios2Publisher:
    def __init__(self, stream_file: str) -> None:
        self.stream = adios2.Stream(stream_file, 'w')

    def close(self) -> None:
        self.stream.close()

    def send(self, topic: str, message: bytes) -> None:
        self.stream.begin_step()
        array = numpy.frombuffer(message, dtype=numpy.int8)
        self.stream.write(
            topic,
            array,
            shape=array.shape,
            start=[0],
            count=[array.shape[0]],
        )
        self.stream.end_step()


class Adios2Subscriber:
    def __init__(
        self,
        stream_file: str,
        topic: str,
        direct: bool = True,
    ) -> None:
        self.stream = adios2.Stream(stream_file, 'r')
        self.topic = topic
        self.direct = direct

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes | int:
        # Cycle to the next step internally in self.stream.
        next(self.stream)

        if self.direct:
            array = self.stream.read(self.topic)
            message = array.tobytes()
            return message
        else:
            return self.stream.current_step()

    def close(self) -> None:
        self.stream.close()


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
            data = next(self.consumer.subscriber)
            if data == CLOSE_SENTINAL:
                raise StopIteration
            return data
        else:
            return next(self.consumer)

    def close(self) -> None:
        self.consumer.close()


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
            self.producer.publisher.send(topic, data)
        else:
            self.producer.send(topic, data, evict=self.proxy_evict)

    def close_topic(self, topic: str) -> None:
        if self.direct_to_publisher:
            self.producer.publisher.send(topic, CLOSE_SENTINAL)
        else:
            self.producer.close_topics(topic)

    def close(self) -> None:
        self.producer.close()
