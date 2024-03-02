from __future__ import annotations

import argparse

from psbench.config.general import GeneralConfig


def test_general_argparse() -> None:
    parser = argparse.ArgumentParser()
    GeneralConfig.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--csv-file',
            'test.csv',
            '--log-level',
            'ERROR',
            '--log-file',
            'test.log',
            '--repeat',
            '2',
            '--run-dir',
            'test/',
        ],
    )

    config = GeneralConfig.from_args(**vars(args))
    assert config.csv_file == 'test.csv'
    assert config.log_level == 'ERROR'
    assert config.log_file == 'test.log'
    assert config.repeat == 2
    assert config.run_dir == 'test/'


def test_general_defaults() -> None:
    GeneralConfig.from_args()
