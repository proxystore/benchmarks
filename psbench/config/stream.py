from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import confluent_kafka
from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber
from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber
from pydantic import BaseModel


class StreamConfig(BaseModel):
    kind: Optional[str]  # noqa: UP007
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
            kind=kwargs.get('stream'),
            topic=kwargs['stream_topic'],
            servers=kwargs.get('stream_servers', ()),
        )

    def get_publisher(self) -> Publisher | None:
        if self.kind is None:
            return None

        publisher: Publisher
        if self.kind == 'kafka':
            producer = confluent_kafka.Producer(
                {'bootstrap_servers': ','.join(self.servers)},
            )
            publisher = KafkaPublisher(producer)
        elif self.kind == 'redis':
            host, port = self.servers[0].split(':')
            publisher = RedisPublisher(host, int(port))
        else:
            raise ValueError(f'Unknown stream broker type: {self.kind}')

        return publisher

    def get_subscriber(self) -> Subscriber | None:
        if self.kind is None:
            return None

        subscriber: Subscriber
        if self.kind == 'kafka':
            consumer = confluent_kafka.Consumer(
                {'bootstrap_servers': ','.join(self.servers)},
            )
            consumer.subscribe([self.topic])
            subscriber = KafkaSubscriber(consumer)
        elif self.kind == 'redis':
            host, port = self.servers[0].split(':')
            subscriber = RedisSubscriber(host, int(port), topic=self.topic)
        else:
            raise ValueError(f'Unknown stream broker type: {self.kind}')

        return subscriber
