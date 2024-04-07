from __future__ import annotations

import argparse

from psbench.config.ipfs import IPFSConfig


def test_ipfs_argparse() -> None:
    parser = argparse.ArgumentParser()
    IPFSConfig.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--ipfs',
            '--ipfs-local-dir',
            'local',
            '--ipfs-remote-dir',
            'remote',
        ],
    )

    config = IPFSConfig.from_args(**vars(args))
    assert config.use_ipfs
    assert config.local_dir == 'local'
    assert config.remote_dir == 'remote'


def test_ipfs_defaults() -> None:
    config = IPFSConfig.from_args()
    assert not config.use_ipfs
    assert config.local_dir is None
    assert config.remote_dir is None
