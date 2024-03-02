from __future__ import annotations

import argparse
from unittest import mock

import pytest
from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber

from psbench.stream import init_publisher_from_config
from psbench.stream import init_subscriber_from_config
from psbench.stream import stream_config_from_args


def test_no_stream() -> None:
    args = argparse.Namespace()
    args.stream = None

    assert stream_config_from_args(args) is None


def test_unknown_stream() -> None:
    args = argparse.Namespace()
    args.stream = 'unknown-stream'
    args.stream_topic = 'topic'

    with pytest.raises(ValueError, match='unknown-stream'):
        stream_config_from_args(args)

    config = {'stream': 'unknown-stream'}

    with pytest.raises(ValueError, match='unknown-stream'):
        init_publisher_from_config(config)

    with pytest.raises(ValueError, match='unknown-stream'):
        init_subscriber_from_config(config)


def test_kafka() -> None:
    args = argparse.Namespace()
    args.stream = 'kafka'
    args.stream_topic = 'topic'
    args.kafka_servers = ['localhost:1234']

    config = stream_config_from_args(args)
    assert config is not None

    with mock.patch('kafka.KafkaProducer'), mock.patch('kafka.KafkaConsumer'):
        publisher = init_publisher_from_config(config)
        subscriber = init_subscriber_from_config(config)

    assert isinstance(publisher, KafkaPublisher)
    assert isinstance(subscriber, KafkaSubscriber)


def test_redis() -> None:
    args = argparse.Namespace()
    args.stream = 'redis'
    args.stream_topic = 'topic'
    args.redis_pubsub_host = 'localhost'
    args.redis_pubsub_port = '1234'

    config = stream_config_from_args(args)
    assert config is not None

    with mock.patch('redis.StrictRedis'):
        publisher = init_publisher_from_config(config)
        subscriber = init_subscriber_from_config(config)

    assert isinstance(publisher, RedisPublisher)
    assert isinstance(subscriber, RedisSubscriber)
