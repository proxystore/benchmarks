from __future__ import annotations

import argparse
from typing import Any
from unittest import mock

import pytest
from proxystore.store.base import Store
from proxystore.store.dim.margo import MargoStore
from proxystore.store.dim.ucx import UCXStore
from proxystore.store.dim.websockets import WebsocketStore
from proxystore.store.dim.zmq import ZeroMQStore
from proxystore.store.endpoint import EndpointStore
from proxystore.store.file import FileStore
from proxystore.store.globus import GlobusStore
from proxystore.store.redis import RedisStore

from psbench.proxystore import init_store_from_args


@pytest.mark.parametrize(
    'backend,backend_type,kwargs',
    (
        ('ENDPOINT', EndpointStore, {'ps_endpoints': ['abcd']}),
        ('FILE', FileStore, {'ps_file_dir': '/tmp/file'}),
        ('GLOBUS', GlobusStore, {'ps_globus_config': '/tmp/file'}),
        (
            'REDIS',
            RedisStore,
            {'ps_host': 'localhost', 'ps_port': 1234},
        ),
        (
            'WEBSOCKET',
            WebsocketStore,
            {'ps_host': 'localhost', 'ps_port': 1234},
        ),
        (
            'MARGO',
            MargoStore,
            {
                'ps_host': 'localhost',
                'ps_port': 1234,
                'ps_margo_protocol': 'tcp',
            },
        ),
        ('UCX', UCXStore, {'ps_host': 'localhost', 'ps_port': 1234}),
        ('ZMQ', ZeroMQStore, {'ps_host': 'localhost', 'ps_port': 1234}),
        (None, None, {}),
        ('INVALID_BACKEND', None, {}),
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

    with mock.patch('psbench.proxystore.register_store'), mock.patch(
        'psbench.proxystore.FileStore',
    ), mock.patch('psbench.proxystore.RedisStore'), mock.patch(
        'psbench.proxystore.EndpointStore',
    ), mock.patch(
        'psbench.proxystore.GlobusStore',
    ), mock.patch(
        'psbench.proxystore.GlobusEndpoints.from_json',
    ), mock.patch(
        'psbench.proxystore.WebsocketStore',
    ), mock.patch(
        'psbench.proxystore.MargoStore',
    ), mock.patch(
        'psbench.proxystore.UCXStore',
    ), mock.patch(
        'psbench.proxystore.ZeroMQStore',
    ):
        if backend in [
            'ENDPOINT',
            'FILE',
            'GLOBUS',
            'REDIS',
            'WEBSOCKET',
            'MARGO',
            'UCX',
            'ZMQ',
            None,
        ]:
            store = init_store_from_args(args)
            if backend is None:
                assert store is None
            else:
                assert store is not None
        else:
            with pytest.raises(ValueError):
                init_store_from_args(args)
