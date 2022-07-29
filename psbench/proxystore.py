from __future__ import annotations

import argparse
from typing import Any

import proxystore
from proxystore.store import init_store
from proxystore.store import STORES
from proxystore.store.base import Store
from proxystore.store.globus import GlobusEndpoints


def proxystore_version() -> tuple[int, ...]:
    """Return the ProxyStore version as a tuple of ints."""
    return tuple(int(v) for v in proxystore.__version__.split('.'))


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
    name = 'default-store'

    if (
        proxystore_version() > (0, 3, 3)
        and args.ps_backend == STORES.ENDPOINT.name
    ):  # pragma: no cover
        store = init_store(
            STORES.ENDPOINT,
            name=name,
            endpoints=args.ps_endpoints,
            **kwargs,
        )
    elif args.ps_backend == STORES.FILE.name:
        store = init_store(
            STORES.FILE,
            name=name,
            store_dir=args.ps_file_dir,
            **kwargs,
        )
    elif args.ps_backend == STORES.GLOBUS.name:
        endpoints = GlobusEndpoints.from_json(args.ps_globus_config)
        store = init_store(
            STORES.GLOBUS,
            name=name,
            endpoints=endpoints,
            **kwargs,
        )
    elif args.ps_backend == STORES.REDIS.name:
        store = init_store(
            STORES.REDIS,
            name=name,
            hostname=args.ps_redis_host,
            port=args.ps_redis_port,
            **kwargs,
        )

    return store
