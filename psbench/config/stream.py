from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import kafka
from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber
from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber
from pydantic import BaseModel


class StreamConfig(BaseModel):
    kind: str | None
    topic: str
    servers: Sequence[str]

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
        argv: Sequence[str] | None = None,
    ) -> None:
        group = parser.add_argument_group(title='Stream Broker Configuration')
        group.add_argument(
            '--stream',
            choices=('kafka', 'redis'),
            type=str.lower,
            required=required,
            help='Stream broker to use',
        )

        argv = [] if argv is None else argv
        group.add_argument(
            '--stream-topic',
            default='stream-benchmark-data',
            help='Stream topic name',
        )
        group.add_argument(
            '--stream-servers',
            metavar='ADDR',
            nargs='+',
            required=required,
            help='Stream broker server address(es)',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            kind=kwargs.get('stream', None),
            topic=kwargs.get('stream_topic'),
            servers=kwargs.get('stream_servers', ()),
        )

    def get_publisher(self) -> Publisher | None:
        if self.stream is None:
            return None

        publisher: Publisher
        if self.stream == 'kafka':
            producer = kafka.KafkaProducer(
                bootstrap_servers=self.servers,
            )
            publisher = KafkaPublisher(producer)
        elif self.redis == 'redis':
            host, port = self.servers[0].split(':')
            publisher = RedisPublisher(host, port)
        else:
            raise ValueError(f'Unknown stream broker type: {self.stream}')

        return publisher

    def get_subscriber(self) -> Subscriber | None:
        if self.stream is None:
            return None

        subscriber: Subscriber
        if self.redis == 'kafka':
            consumer = kafka.KafkaConsumer(
                self.topic,
                bootstrap_servers=self.servers,
            )
            subscriber = KafkaSubscriber(consumer)
        elif self.redis == 'redis':
            host, port = self.servers[0].split(':')
            subscriber = RedisSubscriber(host, port, topic=self.topic)
        else:
            raise ValueError(f'Unknown stream broker type: {self.stream}')

        return subscriber
