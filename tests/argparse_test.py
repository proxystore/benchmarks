from __future__ import annotations

import argparse

import pytest

from psbench.argparse import add_funcx_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options


def test_add_funcx_options() -> None:
    parser = argparse.ArgumentParser()
    add_funcx_options(parser)
    parser.parse_args([])
    args = parser.parse_args(['--funcx-endpoint', 'ABCD'])
    assert args.funcx_endpoint == 'ABCD'

    parser = argparse.ArgumentParser()
    add_funcx_options(parser, required=True)
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
            '--ps-redis-host',
            'localhost',
            '--ps-redis-port',
            '1234',
        ],
    )
