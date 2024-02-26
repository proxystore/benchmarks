from __future__ import annotations

import argparse
import sys
from typing import Any
from unittest import mock

import pytest
from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.protocols import Connector
from proxystore.connectors.redis import RedisConnector
from proxystore.ex.connectors.dim.margo import MargoConnector
from proxystore.ex.connectors.dim.ucx import UCXConnector
from proxystore.ex.connectors.dim.zmq import ZeroMQConnector

from psbench.proxystore import init_store_from_args


class _MockDAOSConnector:
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        pass


@pytest.mark.parametrize(
    ('backend', 'backend_type', 'kwargs'),
    (
        (
            'daos',
            _MockDAOSConnector,
            {
                'ps_daos_pool': 'mypool',
                'ps_daos_container': 'mycontainer',
                'ps_daos_namespace': 'mystore',
            },
        ),
        ('endpoint', EndpointConnector, {'ps_endpoints': ['abcd']}),
        ('file', FileConnector, {'ps_file_dir': '/tmp/file'}),
        ('globus', GlobusConnector, {'ps_globus_config': '/tmp/file'}),
        (
            'redis',
            RedisConnector,
            {'ps_host': 'localhost', 'ps_port': 1234},
        ),
        (
            'margo',
            MargoConnector,
            {
                'ps_port': 1234,
                'ps_address': None,
                'ps_interface': 'lo',
                'ps_margo_protocol': 'tcp',
            },
        ),
        (
            'ucx',
            UCXConnector,
            {'ps_port': 1234, 'ps_address': None, 'ps_interface': 'lo'},
        ),
        (
            'zmq',
            ZeroMQConnector,
            {'ps_port': 1234, 'ps_address': None, 'ps_interface': 'lo'},
        ),
        (None, None, {}),
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
    ), mock.patch('psbench.proxystore.RedisConnector'), mock.patch.dict(
        sys.modules,
        {
            'proxystore.ex.connectors.daos': mock.MagicMock(),
            'DAOSConnector': _MockDAOSConnector,
        },
    ), mock.patch(
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
        store = init_store_from_args(args)
        if backend is None:
            assert store is None
        else:
            assert store is not None


def test_invalid_backend():
    args = argparse.Namespace()
    args.ps_backend = 'invalid-backend'

    with pytest.raises(ValueError):
        init_store_from_args(args)
