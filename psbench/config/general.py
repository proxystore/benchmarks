from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel

from psbench.logging import TEST_LOG_LEVEL


class GeneralConfig(BaseModel):
    csv_file: str = 'results.csv'
    log_file: str = 'log.txt'
    log_level: Union[int, str] = TEST_LOG_LEVEL  # noqa: UP007
    repeat: int = 1
    run_dir: str = 'runs/'

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='General Configuration')

        group.add_argument(
            '--csv-file',
            default='results.csv',
            help='Name of results CSV file inside --run-dir',
        )
        group.add_argument(
            '--log-level',
            choices=['ERROR', 'WARNING', 'BENCH', 'TEST', 'INFO', 'DEBUG'],
            default='BENCH',
            help='Minimum logging level',
        )
        group.add_argument(
            '--log-file',
            default='log.txt',
            help='Name of log file inside --run-dir',
        )
        group.add_argument(
            '--repeat',
            default=1,
            metavar='INT',
            type=int,
            help='Repeat each benchmark configuration',
        )
        group.add_argument(
            '--run-dir',
            default='runs/',
            metavar='PATH',
            help=(
                'Run directory for logs and results. A subdirectory with '
                'the benchmark name and timestamp will be created for each '
                'invocation of the script.'
            ),
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        options: dict[str, Any] = {}

        if 'csv_file' in kwargs:
            options['csv_file'] = kwargs['csv_file']
        if 'log_file' in kwargs:
            options['log_file'] = kwargs['log_file']
        if 'log_level' in kwargs:
            options['log_level'] = kwargs['log_level']
        if 'repeat' in kwargs:
            options['repeat'] = kwargs['repeat']
        if 'run_dir' in kwargs:
            options['run_dir'] = kwargs['run_dir']

        return cls(**options)
