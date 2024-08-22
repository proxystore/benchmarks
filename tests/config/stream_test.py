from __future__ import annotations

import argparse
from unittest import mock

import pytest
from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber

from psbench.config import StreamConfig


def test_stream_config_argparse_required() -> None:
    parser = argparse.ArgumentParser()
    StreamConfig.add_parser_group(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_stream_config_argparse_example() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--other')
    StreamConfig.add_parser_group(parser)
    args = parser.parse_args(
        ['--stream', 'kafka', '--stream-servers', 'localhost'],
    )
    config = StreamConfig.from_args(**vars(args))
    assert config.kind == 'kafka'
    assert config.servers == ['localhost']


def test_stream_config_empty_stream() -> None:
    config = StreamConfig(kind=None, topic='topic', servers=[])
    assert config.get_publisher() is None
    assert config.get_subscriber() is None


def test_stream_config_unknown_stream() -> None:
    config = StreamConfig(kind='unknown-stream', topic='topic', servers=[])

    with pytest.raises(ValueError, match='unknown-stream'):
        config.get_publisher()

    with pytest.raises(ValueError, match='unknown-stream'):
        config.get_subscriber()


def test_stream_config_kafka() -> None:
    config = StreamConfig(kind='kafka', topic='topic', servers=['localhost'])

    with mock.patch('confluent_kafka.Producer'), mock.patch(
        'confluent_kafka.Consumer',
    ):
        publisher = config.get_publisher()
        subscriber = config.get_subscriber()

    assert isinstance(publisher, KafkaPublisher)
    assert isinstance(subscriber, KafkaSubscriber)


def test_stream_config_redis() -> None:
    config = StreamConfig(
        kind='redis',
        topic='topic',
        servers=['localhost:1234'],
    )

    with mock.patch('redis.StrictRedis'):
        publisher = config.get_publisher()
        subscriber = config.get_subscriber()

    assert isinstance(publisher, RedisPublisher)
    assert isinstance(subscriber, RedisSubscriber)
