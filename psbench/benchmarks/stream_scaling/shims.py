from __future__ import annotations

import sys

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import numpy
from proxystore.proxy import Proxy
from proxystore.stream import StreamConsumer
from proxystore.stream import StreamProducer
from proxystore.stream.protocols import MessagePublisher

adios_import_error: Exception | None = None
try:
    import adios2
except ImportError as e:  # pragma: no cover
    adios_import_error = e

CLOSE_SENTINAL = b'<publisher-close-topic-sentinal>'


class Adios2Publisher:
    def __init__(self, stream_file: str) -> None:
        if adios_import_error is not None:  # pragma: no cover
            raise adios_import_error

        self.stream = adios2.Stream(stream_file, 'w')

    def close(self) -> None:
        self.stream.close()

    def send_message(self, topic: str, message: bytes) -> None:
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
        if adios_import_error is not None:  # pragma: no cover
            raise adios_import_error

        self.stream = adios2.Stream(stream_file, 'r')
        self.topic = topic
        self.direct = direct

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> bytes | int:
        # Cycle to the next step internally in self.stream.
        next(self.stream)

        if self.direct:  # pragma: no cover
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
            assert isinstance(data, (Proxy, bytes))
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

    def send_message(self, topic: str, data: bytes) -> None:
        assert isinstance(self.producer.publisher, MessagePublisher)
        if self.direct_to_publisher:
            self.producer.publisher.send_message(topic, data)
        else:
            self.producer.send(topic, data, evict=self.proxy_evict)

    def close_topic(self, topic: str) -> None:
        assert isinstance(self.producer.publisher, MessagePublisher)
        if self.direct_to_publisher:
            self.producer.publisher.send_message(topic, CLOSE_SENTINAL)
        else:
            self.producer.close_topics(topic)

    def close(self) -> None:
        self.producer.close()
