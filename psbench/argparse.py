from __future__ import annotations

import argparse
import re
import sys


def add_funcx_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for FuncX.

    Args:
        parser (ArgumentParser): parser object to add FuncX arguments to.
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


def add_ipfs_options(parser: argparse.ArgumentParser) -> None:
    """Add CLI arguments for IPFS.

    Args:
        parser (ArgumentParser): parser object to add IPFS arguments to.
    """
    args_str = ' '.join(sys.argv)
    parser.add_argument(
        '--ipfs',
        action='store_true',
        default=False,
        help='Use IPFS for data transfer.',
    )
    parser.add_argument(
        '--ipfs-local-dir',
        required=bool(re.search(r'--ipfs($|\s)', args_str)),
        help='Local directory to write IPFS files to.',
    )
    parser.add_argument(
        '--ipfs-remote-dir',
        required=bool(re.search(r'--ipfs($|\s)', args_str)),
        help='Local directory to write IPFS files to.',
    )


def add_dspaces_options(parser: argparse.ArgumentParser) -> None:
    """Add CLI arguments for DataSpaces.

    Args:
        parser (ArgumentParser): parser object to add DataSpaces arguments to.
    """
    args_str = ' '.join(sys.argv)
    parser.add_argument(
        '--dspaces',
        action='store_true',
        default=False,
        help='Use DataSpaces for data transfer.',
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
        choices=[
            'FILE',
            'GLOBUS',
            'REDIS',
            'ENDPOINT',
            'WEBSOCKET',
            'MARGO',
            'UCX',
            'ZMQ',
        ],
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
        '--ps-host',
        metavar='HOST',
        required=bool(
            re.search(
                '--ps-backend( |=)(REDIS|WEBSOCKET|MARGO|UCX|ZMQ)',
                args_str,
            ),
        ),
        help='Hostname of server or network interface to use with ProxyStore',
    )
    group.add_argument(
        '--ps-port',
        metavar='PORT',
        type=int,
        required=bool(
            re.search(
                '--ps-backend( |=)(REDIS|WEBSOCKET|MARGO|UCX|ZMQ)',
                args_str,
            ),
        ),
        help='Port of server to use with ProxyStore',
    )
    group.add_argument(
        '--ps-margo-protocol',
        metavar='PROTOCOL',
        help='Optionally specify the Margo protocol to use with ProxyStore',
        default='tcp',
    )
