from __future__ import annotations

import argparse
import sys
from typing import Any
from unittest import mock

import pytest

from psbench.config import StoreConfig


class _MockDAOSConnector:
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        pass


def test_store_config_argparse_empty() -> None:
    parser = argparse.ArgumentParser()
    StoreConfig.add_parser_group(parser, required=False)
    args = parser.parse_args([])
    config = StoreConfig.from_args(**vars(args))
    assert config.connector is None


def test_store_config_argparse_required() -> None:
    parser = argparse.ArgumentParser()
    StoreConfig.add_parser_group(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_store_config_argparse_example() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--other')
    StoreConfig.add_parser_group(parser)
    args = parser.parse_args(
        ['--ps-connector', 'file', '--ps-file-dir', '/tmp/x', '--other', 'X'],
    )
    config = StoreConfig.from_args(**vars(args))
    assert config.connector == 'file'
    assert config.options['file_dir'] == '/tmp/x'
    assert 'other' not in config.options


def test_store_config_daos() -> None:
    config = StoreConfig(
        connector='daos',
        options={
            'daos_pool': 'mypool',
            'daos_container': 'mycontainer',
            'daos_namespace': 'mystore',
        },
    )
    with mock.patch.dict(
        sys.modules,
        {
            'proxystore.ex.connectors.daos': mock.MagicMock(),
            'DAOSConnector': _MockDAOSConnector,
        },
    ):
        assert config.get_store(register=False) is not None


def test_store_config_endpoint() -> None:
    config = StoreConfig(
        connector='endpoint',
        options={'endpoints': ['abcd']},
    )
    with mock.patch('psbench.config.store.EndpointConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_file() -> None:
    config = StoreConfig(
        connector='file',
        options={'file_dir': '/tmp/x/'},
    )
    with mock.patch('psbench.config.store.FileConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_globus() -> None:
    config = StoreConfig(
        connector='globus',
        options={'globus_config': '/tmp/file'},
    )
    with mock.patch('psbench.config.store.GlobusConnector'), mock.patch(
        'psbench.config.store.GlobusEndpoints.from_json',
    ):
        assert config.get_store(register=False) is not None


def test_store_config_redis() -> None:
    config = StoreConfig(
        connector='redis',
        options={'host': 'localhost', 'port': 1234},
    )
    with mock.patch('psbench.config.store.RedisConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_margo() -> None:
    config = StoreConfig(
        connector='margo',
        options={
            'port': 1234,
            'address': None,
            'interface': 'lo',
            'margo_protocol': 'tcp',
        },
    )
    with mock.patch('psbench.config.store.MargoConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_ucx() -> None:
    config = StoreConfig(
        connector='ucx',
        options={'port': 1234, 'address': None, 'interface': 'lo'},
    )
    with mock.patch('psbench.config.store.UCXConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_zmq() -> None:
    config = StoreConfig(
        connector='zmq',
        options={'port': 1234, 'address': None, 'interface': 'lo'},
    )
    with mock.patch('psbench.config.store.ZeroMQConnector'):
        assert config.get_store(register=False) is not None


def test_store_config_register() -> None:
    config = StoreConfig(
        connector='redis',
        options={'host': 'localhost', 'port': 1234},
    )
    with mock.patch('psbench.config.store.RedisConnector'), mock.patch(
        'psbench.config.store.register_store',
    ) as mock_register:
        assert config.get_store(register=True) is not None
        assert mock_register.call_count == 1


def test_store_config_empty():
    config = StoreConfig(connector=None, options={})

    assert config.get_store() is None


def test_store_config_unknown():
    config = StoreConfig(connector='abcd', options={})

    with pytest.raises(ValueError, match='abcd'):
        config.get_store()
