from __future__ import annotations

import argparse
from typing import Any

from proxystore.store import register_store
from proxystore.store.base import Store
from proxystore.store.dim.margo import MargoStore
from proxystore.store.dim.ucx import UCXStore
from proxystore.store.dim.websockets import WebsocketStore
from proxystore.store.dim.zmq import ZeroMQStore
from proxystore.store.endpoint import EndpointStore
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusEndpoints
from proxystore.store.globus import GlobusStore
from proxystore.store.redis import RedisStore


def init_store_from_args(
    args: argparse.Namespace,
    **kwargs: Any,
) -> Store | None:
    """Initialize a ProxyStore Store from CLI arguments.

    Usage:
        >>> parser = argparse.ArgumentParser(...)
        >>> add_proxystore_options(parser, required=...)
        >>> args = parser.parse_args()
        >>> store= init_store_from_args(args)

    Args:
        args (Namespace): namespace returned by argument parser.
        kwargs: additional keyword arguments to pass to store constructor.

    Returns:
        Store or None if no store was specified.
    """
    store: Store | None = None

    if args.ps_backend:
        if args.ps_backend == 'ENDPOINT':
            store = EndpointStore(
                name='endpoint-store',
                endpoints=args.ps_endpoints,
                **kwargs,
            )
        elif args.ps_backend == 'FILE':
            store = FileStore(
                name='file-store',
                store_dir=args.ps_file_dir,
                **kwargs,
            )
        elif args.ps_backend == 'GLOBUS':
            endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
            store = GlobusStore(
                name='globus-store',
                endpoints=endpoints,
                **kwargs,
            )
        elif args.ps_backend == 'REDIS':
            store = RedisStore(
                name='redis-store',
                hostname=args.ps_host,
                port=args.ps_port,
                **kwargs,
            )
        elif args.ps_backend == 'WEBSOCKET':
            store = WebsocketStore(
                name='websocket-store',
                interface=args.ps_host,
                port=args.ps_port,
                **kwargs,
            )
        elif args.ps_backend == 'MARGO':
            store = MargoStore(
                name='margo-store',
                interface=args.ps_host,
                port=args.ps_port,
                protocol=args.ps_margo_protocol,
                **kwargs,
            )
        elif args.ps_backend == 'UCX':
            store = UCXStore(
                name='ucx-store',
                interface=args.ps_host,
                port=args.ps_port,
                **kwargs,
            )
        elif args.ps_backend == 'ZMQ':
            store = ZeroMQStore(
                name='zmq-store',
                interface=args.ps_host,
                port=args.ps_port,
                **kwargs,
            )
        else:
            raise ValueError(f'Invalid backend: {args.ps_backend}')
        register_store(store)

    return store
