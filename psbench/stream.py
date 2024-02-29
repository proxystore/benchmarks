from __future__ import annotations

import argparse
from typing import Any

import kafka
from proxystore.stream.protocols import Publisher
from proxystore.stream.protocols import Subscriber
from proxystore.stream.shims.kafka import KafkaPublisher
from proxystore.stream.shims.kafka import KafkaSubscriber
from proxystore.stream.shims.redis import RedisPublisher
from proxystore.stream.shims.redis import RedisSubscriber


def stream_config_from_args(args: argparse.Namespace) -> dict[str, Any] | None:
    """Get the stream config as a dictionary from CLI arguments."""
    if args.stream is None:
        return None

    config = {'stream': args.stream, 'topic': args.stream_topic}
    if config['stream'] == 'kafka':
        config['kafka-servers'] = args.kafka_servers
    elif config['stream'] == 'redis':
        config['redis-pubsub-host'] = args.redis_pubsub_host
        config['redis-pubsub-port'] = args.redis_pubsub_port
    else:
        raise ValueError(f'Invalid stream type: {config["stream"]}')
    return config


def init_publisher_from_config(config: dict[str, Any]) -> Publisher:
    """Initialize a publisher interface from a config.

    Args:
        config: Config created with stream_config_from_args().

    Returns:
        Publisher interface.
    """
    publisher: Publisher

    if config['stream'] == 'kafka':
        producer = kafka.KafkaProducer(
            bootstrap_servers=config['kafka-servers'],
        )
        publisher = KafkaPublisher(producer)
    elif config['stream'] == 'redis':
        publisher = RedisPublisher(
            config['redis-pubsub-host'],
            config['redis-pubsub-port'],
        )
    else:
        raise ValueError(f'Invalid stream type: {config["stream"]}')

    return publisher


def init_subscriber_from_config(config: dict[str, Any]) -> Subscriber:
    """Initialize a subscriber interface from a config.

    Args:
        config: Config created with stream_config_from_args().

    Returns:
        Subscriber interface.
    """
    subscriber: Subscriber

    if config['stream'] == 'kafka':
        consumer = kafka.KafkaConsumer(
            config['topic'],
            bootstrap_servers=config['kafka-servers'],
        )
        subscriber = KafkaSubscriber(consumer)
    elif config['stream'] == 'redis':
        subscriber = RedisSubscriber(
            config['redis-pubsub-host'],
            config['redis-pubsub-port'],
            topic=config['topic'],
        )
    else:
        raise ValueError(f'Invalid stream type: {config["stream"]}')

    return subscriber
