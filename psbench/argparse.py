from __future__ import annotations

import argparse
import re
import sys

from proxystore.store import STORES


def add_funcx_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for FuncX.

    Args:
        parser (ArgumentParser): parser object to FuncX endpoint argument to.
        required (bool): require the FuncX endpoint to be specified
            (default: False).
    """
    group = parser.add_argument_group(
        title='FuncX',
        description='FuncX Endpoint configuration',
    )
    group.add_argument(
        '--funcx-endpoint',
        metavar='UUID',
        required=required,
        help='FuncX endpoint for task execution',
    )


def add_logging_options(parser: argparse.ArgumentParser) -> None:
    """Add CLI arguments for logging options."""
    group = parser.add_argument_group(
        title='Logging',
        description='Logging configurations',
    )

    group.add_argument(
        '--log-level',
        choices=['ERROR', 'WARNING', 'TESTING', 'INFO', 'DEBUG'],
        default='TESTING',
        help='Set minimum logging level',
    )
    group.add_argument(
        '--log-file',
        help='Optionally write log to file',
    )
    group.add_argument(
        '--csv-file',
        help='Optionally log data to CSV file',
    )


def add_proxystore_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for ProxyStore backends to a parser.

    Warning:
        Backend specific config options will be dynamically set as required
        dependending on which backend is specified as is found in sys.argv.
        As a side effect, if parse_args() is called with a custom list of
        arguments, sys.argv may be empty and therefore required flags will
        not be correctly set.

    Args:
        parser (ArgumentParser): parser object to add ProxyStore backend
            argument group to.
        required (bool): require a ProxyStore backend to be specified
            (default: False).
    """
    group = parser.add_argument_group(
        title='ProxyStore',
        description='ProxyStore backend options',
    )
    group.add_argument(
        '--ps-backend',
        choices=[name for name, _ in STORES.__members__.items()],
        required=required,
        help='ProxyStore backend to use',
    )

    args_str = ' '.join(sys.argv)
    group.add_argument(
        '--ps-endpoints',
        metavar='UUID',
        nargs='+',
        required=bool(re.search('--ps-backend( |=)ENDPOINT', args_str)),
        help='ProxyStore Endpoint UUIDs accessible by the program',
    )
    group.add_argument(
        '--ps-file-dir',
        metavar='DIR',
        required=bool(re.search('--ps-backend( |=)FILE', args_str)),
        help='Temp directory to store ProxyStore objects in',
    )
    group.add_argument(
        '--ps-globus-config',
        metavar='CFG',
        required=bool(re.search('--ps-backend( |=)GLOBUS', args_str)),
        help='Globus Endpoint config for ProxyStore',
    )
    group.add_argument(
        '--ps-redis-host',
        metavar='HOST',
        required=bool(re.search('--ps-backend( |=)REDIS', args_str)),
        help='Hostname of Redis server to use with ProxyStore',
    )
    group.add_argument(
        '--ps-redis-port',
        metavar='PORT',
        type=int,
        required=bool(re.search('--ps-backend( |=)REDIS', args_str)),
        help='Port of Redis server to use with ProxyStore',
    )
    group.add_argument(
        '--ps-intrasite-interface',
        metavar='IFACE',
        required=bool(re.search('--ps-backend( |=)INTRASITE', args_str)),
        help='Network interface to use with ProxyStore'
    )
    group.add_argument(
        '--ps-intrasite-port',
        metavar='PORT',
        type=int,
        required=bool(re.search('--ps-backend( |=)INTRASITE', args_str)),
        help='Port to use with ProxyStore',
    )
