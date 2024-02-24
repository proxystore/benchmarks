from __future__ import annotations

import argparse
import re
import sys


def add_dask_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for Dask Distributed.

    Args:
        parser (ArgumentParser): parser object to add Dask arguments to.
        required (bool): require the non-default Dask options to be specified
            (default: False).
    """
    group = parser.add_argument_group(
        title='Dask Distributed',
        description='Dask Distributed configuration',
    )
    group.add_argument(
        '--dask-scheduler',
        default=None,
        metavar='ADDR',
        help='Dask scheduler address (default uses LocalCluster)',
    )
    group.add_argument(
        '--dask-workers',
        default=None,
        metavar='WORKERS',
        type=int,
        help='Number of workers to start if using LocalCluster',
    )
    group.add_argument(
        '--dask-use-threads',
        action='store_true',
        help='Use threads instead of processes for LocalCluster workers',
    )
    group.add_argument(
        '--dask-dashboard-address',
        default=None,
        metavar='ADDR',
        help='Optional Dask dashboard address for LocalCluster',
    )


def add_globus_compute_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for Globus Compute.

    Args:
        parser (ArgumentParser): parser object to add Globus Compute arguments
            to.
        required (bool): require the Globus Compute endpoint to be specified
            (default: False).
    """
    group = parser.add_argument_group(
        title='Globus Compute',
        description='Globus Compute Endpoint configuration',
    )
    group.add_argument(
        '--globus-compute-endpoint',
        metavar='UUID',
        required=required,
        help='Globus Compute endpoint for task execution',
    )


def add_parsl_options(
    parser: argparse.ArgumentParser,
    required: bool = False,
) -> None:
    """Add CLI arguments for Parsl.

    Args:
        parser (ArgumentParser): parser object to add Parsl arguments to.
        required (bool): require the non-default Parsl options to be specified
            (default: False).
    """
    group = parser.add_argument_group(
        title='Parsl Configuration',
        description='Parsl configuration',
    )
    mutex_group = group.add_mutually_exclusive_group(required=required)
    mutex_group.add_argument(
        '--parsl-thread-executor',
        action='store_true',
        help='Use a Parsl ThreadPoolExecutor',
    )
    mutex_group.add_argument(
        '--parsl-local-htex',
        action='store_true',
        help='Use a Parsl HighThroughputExecutor with a LocalProvider',
    )
    group.add_argument(
        '--parsl-run-dir',
        default='runinfo',
        metavar='DIR',
        help='Parsl run directory',
    )
    group.add_argument(
        '--parsl-workers',
        default=None,
        metavar='WORKERS',
        type=int,
        help='Number of Parsl workers to configure',
    )


def add_executor_options(parser: argparse.ArgumentParser) -> None:
    """Add task executor arguments.

    Args:
        parser (ArgumentParser): parser object to add IPFS arguments to.
    """
    args_str = ' '.join(sys.argv)
    parser.add_argument(
        '--executor',
        choices=['dask', 'globus', 'parsl'],
        help=(
            'Task executor to use. Each executor type may have additional '
            'required options'
        ),
    )
    add_dask_options(
        parser,
        required=bool(re.search('--executor([ \t]+|=)dask', args_str)),
    )
    add_globus_compute_options(
        parser,
        required=bool(re.search('--executor([ \t]+|=)globus', args_str)),
    )
    add_parsl_options(
        parser,
        required=bool(re.search('--executor([ \t]+|=)parsl', args_str)),
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
    ' '.join(sys.argv)
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
        depending on which backend is specified as is found in sys.argv.
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
                '--ps-backend( |=)(REDIS)',
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
                '--ps-backend( |=)(REDIS|MARGO|UCX|ZMQ)',
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
    group.add_argument(
        '--ps-address',
        metavar='ADDRESS',
        default=None,
        help='Optionally specify host IP address that can be used by the DIMs',
    )
    group.add_argument(
        '--ps-interface',
        metavar='INTERFACE',
        default=None,
        help='Optionally provide interface name to be used by the DIMs',
    )
