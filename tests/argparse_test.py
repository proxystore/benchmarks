from __future__ import annotations

import argparse
from unittest import mock

import pytest

from psbench.argparse import add_dask_options
from psbench.argparse import add_globus_compute_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_parsl_options
from psbench.argparse import add_proxystore_options


def test_add_dask_options() -> None:
    parser = argparse.ArgumentParser()
    add_dask_options(parser)
    parser.parse_args(['--dask-scheduler', 'localhost'])


def test_add_globus_compute_options(capsys) -> None:
    parser = argparse.ArgumentParser()
    add_globus_compute_options(parser)
    parser.parse_args([])
    args = parser.parse_args(['--globus-compute-endpoint', 'ABCD'])
    assert args.globus_compute_endpoint == 'ABCD'

    parser = argparse.ArgumentParser()
    add_globus_compute_options(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_add_parsl_options(capsys) -> None:
    parser = argparse.ArgumentParser()
    add_parsl_options(parser)
    parser.parse_args([])
    args = parser.parse_args(['--parsl-local-htex'])
    assert not args.parsl_thread_executor
    assert args.parsl_local_htex

    parser = argparse.ArgumentParser()
    add_parsl_options(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_add_logging_options() -> None:
    parser = argparse.ArgumentParser()
    add_logging_options(parser)
    parser.parse_args(['--log-level', 'INFO'])


def test_add_proxystore_options() -> None:
    parser = argparse.ArgumentParser()
    add_proxystore_options(parser)
    parser.parse_args([])

    parser = argparse.ArgumentParser()
    add_proxystore_options(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])

    parser = argparse.ArgumentParser()
    add_proxystore_options(parser)

    parser.parse_args(['--ps-backend', 'ENDPOINT', '--ps-endpoints', 'ABCD'])
    parser.parse_args(['--ps-backend', 'FILE', '--ps-file-dir', '/tmp/x'])
    parser.parse_args(['--ps-backend', 'GLOBUS', '--ps-globus-config', 'cfg'])
    parser.parse_args(
        [
            '--ps-backend',
            'REDIS',
            '--ps-host',
            'localhost',
            '--ps-port',
            '1234',
        ],
    )
    parser.parse_args(
        [
            '--ps-backend',
            'MARGO',
            '--ps-host',
            'localhost',
            '--ps-port',
            '1234',
            '--ps-margo-protocol',
            'tcp',
        ],
    )
    parser.parse_args(
        [
            '--ps-backend',
            'UCX',
            '--ps-host',
            'localhost',
            '--ps-port',
            '1234',
        ],
    )
    parser.parse_args(
        [
            '--ps-backend',
            'ZMQ',
            '--ps-host',
            'localhost',
            '--ps-port',
            '1234',
        ],
    )
