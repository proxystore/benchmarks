from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.connectors.endpoint import EndpointConnector
from proxystore.connectors.file import FileConnector
from proxystore.connectors.globus import GlobusConnector
from proxystore.connectors.globus import GlobusEndpoints
from proxystore.connectors.protocols import Connector
from proxystore.connectors.redis import RedisConnector
from proxystore.ex.connectors.dim.margo import MargoConnector
from proxystore.ex.connectors.dim.ucx import UCXConnector
from proxystore.ex.connectors.dim.zmq import ZeroMQConnector
from proxystore.store import register_store
from proxystore.store import Store
from pydantic import BaseModel
from pydantic import Field


class StoreConfig(BaseModel):
    connector: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
        argv: Sequence[str] | None = None,
    ) -> None:
        group = parser.add_argument_group(title='ProxyStore Configuration')
        group.add_argument(
            '--ps-connector',
            choices=[
                'daos',
                'file',
                'globus',
                'redis',
                'endpoint',
                'margo',
                'ucx',
                'zmq',
            ],
            type=str.lower,
            required=required,
            help='ProxyStore connector to use',
        )

        argv = [] if argv is None else argv
        group.add_argument(
            '--ps-daos-pool',
            metavar='NAME',
            required=required and 'daos' in argv,
            help='DAOS pool name.',
        )
        group.add_argument(
            '--ps-daos-container',
            metavar='NAME',
            required=required and 'daos' in argv,
            help='DAOS container name.',
        )
        group.add_argument(
            '--ps-daos-namespace',
            metavar='NAME',
            required=required and 'daos' in argv,
            help='DAOS dictionary name within container.',
        )
        group.add_argument(
            '--ps-endpoints',
            metavar='UUID',
            nargs='+',
            required=required and 'endpoint' in argv,
            help='ProxyStore Endpoint UUIDs accessible by the program',
        )
        group.add_argument(
            '--ps-file-dir',
            metavar='DIR',
            required=required and 'file' in argv,
            help='Temp directory to store ProxyStore objects in',
        )
        group.add_argument(
            '--ps-globus-config',
            metavar='CFG',
            required=required and 'globus' in argv,
            help='Globus Endpoint config for ProxyStore',
        )
        group.add_argument(
            '--ps-host',
            metavar='HOST',
            required=required and 'redis' in argv,
            help='Hostname of server or interface to use with ProxyStore',
        )
        group.add_argument(
            '--ps-port',
            metavar='PORT',
            type=int,
            required=(
                required
                and any(c in argv for c in ('redis', 'margo', 'uxc', 'zmq'))
            ),
            help='Port of server to use with ProxyStore',
        )
        group.add_argument(
            '--ps-margo-protocol',
            default='tcp',
            metavar='PROTOCOL',
            help='Margo protocol to use with ProxyStore',
        )
        group.add_argument(
            '--ps-address',
            default=None,
            metavar='ADDRESS',
            help='Host IP address that can be used by the DIMs',
        )
        group.add_argument(
            '--ps-interface',
            default=None,
            metavar='INTERFACE',
            help='Interface name to be used by the DIMs',
        )

    @classmethod
    def from_dict(cls, **kwargs: Any) -> Self:
        connector = kwargs.get('ps-connector', None)
        options = {k: v for k, v in kwargs.items() if k.startswith('ps_')}
        return cls(connector=connector, options=options)

    def get_store(
        self,
        register: bool = True,
        **kwargs: Any,
    ) -> Store[Any] | None:
        if self.connector is None:
            return None

        connector: Connector

        if self.connector == 'daos':
            # This import will fail is pydaos is not installed so we defer the
            # import to here.
            from proxystore.ex.connectors.daos import DAOSConnector

            connector = DAOSConnector(
                pool=self.options['ps_daos_pool'],
                container=self.options['ps_daos_container'],
                namespace=self.options['ps_daos_namespace'],
            )
        elif self.connector == 'endpoint':
            connector = EndpointConnector(self.options['ps_endpoints'])
        elif self.connector == 'file':
            connector = FileConnector(self.options['ps_file_dir'])
        elif self.connector == 'globus':
            endpoints = GlobusEndpoints.from_json(
                self.options['ps_globus_config'],
            )
            connector = GlobusConnector(endpoints)
        elif self.connector == 'redis':
            connector = RedisConnector(
                self.options['ps_host'],
                self.options['ps_port'],
            )
        elif self.connector == 'margo':
            connector = MargoConnector(
                port=self.options['ps_port'],
                protocol=self.options['ps_margo_protocol'],
                address=self.options['ps_address'],
                interface=self.options['ps_interface'],
            )
        elif self.connector == 'ucx':
            connector = UCXConnector(
                port=self.options['ps_port'],
                interface=self.options['ps_interface'],
                address=self.options['ps_address'],
            )
        elif self.connector == 'zmq':
            connector = ZeroMQConnector(
                port=self.options['ps_port'],
                interface=self.options['ps_interface'],
                address=self.options['ps_address'],
            )
        else:
            raise ValueError(f'Invalid backend: {self.connector}')

        store = Store(f'{self.connector}-store', connector, **kwargs)

        if register:
            register_store(store)

        return store
