from __future__ import annotations

import argparse
from typing import Any
from unittest import mock

import pytest
from proxystore.connectors.connector import Connector
from proxystore.connectors.dim.margo import MargoConnector
from proxystore.connectors.dim.ucx import UCXConnector
from proxystore.connectors.dim.zmq import ZeroMQConnector
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.redis import RedisConnector

from psbench.proxystore import init_store_from_args


@pytest.mark.parametrize(
    'backend,backend_type,kwargs',
    (
        ('ENDPOINT', EndpointConnector, {'ps_endpoints': ['abcd']}),
        ('FILE', FileConnector, {'ps_file_dir': '/tmp/file'}),
        ('GLOBUS', GlobusConnector, {'ps_globus_config': '/tmp/file'}),
        (
            'REDIS',
            RedisConnector,
            {'ps_host': 'localhost', 'ps_port': 1234},
        ),
        (
            'MARGO',
            MargoConnector,
            {
                'ps_host': 'localhost',
                'ps_port': 1234,
                'ps_margo_protocol': 'tcp',
            },
        ),
        ('UCX', UCXConnector, {'ps_host': 'localhost', 'ps_port': 1234}),
        ('ZMQ', ZeroMQConnector, {'ps_host': 'localhost', 'ps_port': 1234}),
        (None, None, {}),
        ('INVALID_BACKEND', None, {}),
    ),
)
def test_store_from_args(
    backend: str | None,
    backend_type: type[Connector] | None,
    kwargs: dict[str, Any],
) -> None:
    args = argparse.Namespace()
    args.ps_backend = backend
    for key, value in kwargs.items():
        setattr(args, key, value)

    with mock.patch('psbench.proxystore.register_store'), mock.patch(
        'psbench.proxystore.FileConnector',
    ), mock.patch('psbench.proxystore.RedisConnector'), mock.patch(
        'psbench.proxystore.EndpointConnector',
    ), mock.patch(
        'psbench.proxystore.GlobusConnector',
    ), mock.patch(
        'psbench.proxystore.GlobusEndpoints.from_json',
    ), mock.patch(
        'psbench.proxystore.MargoConnector',
    ), mock.patch(
        'psbench.proxystore.UCXConnector',
    ), mock.patch(
        'psbench.proxystore.ZeroMQConnector',
    ):
        if backend in [
            'ENDPOINT',
            'FILE',
            'GLOBUS',
            'REDIS',
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
