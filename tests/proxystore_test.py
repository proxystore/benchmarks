from __future__ import annotations

import argparse
from typing import Any
from unittest import mock

import pytest
from proxystore.store.base import Store
from proxystore.store.endpoint import EndpointStore
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusStore
from proxystore.store.redis import RedisStore

from psbench.proxystore import init_store_from_args
from psbench.proxystore import proxystore_version


def test_proxystore_version() -> None:
    # ProxyStore 0.3.3 is minimum version required by setup.cfg
    assert proxystore_version() >= (0, 3, 3)


@pytest.mark.parametrize(
    'backend,backend_type,kwargs',
    (
        ('ENDPOINT', EndpointStore, {'ps_endpoints': ['abcd']}),
        ('FILE', FileStore, {'ps_file_dir': '/tmp/file'}),
        ('GLOBUS', GlobusStore, {'ps_globus_config': '/tmp/file'}),
        (
            'REDIS',
            RedisStore,
            {'ps_redis_host': 'localhost', 'ps_redis_port': 1234},
        ),
        (None, None, {}),
    ),
)
def test_store_from_args(
    backend: str | None,
    backend_type: type[Store] | None,
    kwargs: dict[str, Any],
) -> None:
    args = argparse.Namespace()
    args.ps_backend = backend
    for key, value in kwargs.items():
        setattr(args, key, value)

    with mock.patch('psbench.proxystore.init_store'), mock.patch(
        'psbench.proxystore.GlobusEndpoints.from_json',
    ):
        store = init_store_from_args(args)
        if backend is None:
            assert store is None
        else:
            assert store is not None
