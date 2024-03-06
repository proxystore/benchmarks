from __future__ import annotations

import argparse
import itertools
import sys
from typing import Any
from typing import List
from typing import Literal

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class RunConfig(BaseModel):
    submission_method: Literal['sequential', 'pipelined']
    task_chain_length: int
    task_data_bytes: int
    task_overhead_fraction: float
    task_sleep: float


class RunResult(BaseModel):
    executor: str
    connector: str
    submission_method: Literal['sequential', 'pipelined']
    task_chain_length: int
    task_data_bytes: int
    task_overhead_fraction: float
    task_sleep: float
    workflow_makespan_ms: float


class BenchmarkMatrix(BaseModel):
    submission_method: List[Literal['sequential', 'pipelined']]  # noqa: UP006
    task_chain_length: int
    task_data_bytes: List[int]  # noqa: UP006
    task_overhead_fractions: List[float]  # noqa: UP006
    task_sleep: float

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--submission-method',
            choices=['sequential', 'pipelined'],
            default=['sequential', 'pipelined'],
            nargs='+',
            help='Task submission method',
        )
        group.add_argument(
            '--task-chain-length',
            metavar='N',
            required=True,
            type=int,
            help='Number of tasks in sequential workflow',
        )
        group.add_argument(
            '--task-data-bytes',
            metavar='BYTES',
            nargs='+',
            required=True,
            type=int,
            help='Intermediate task data size in bytes',
        )
        group.add_argument(
            '--task-overhead-fractions',
            metavar='FLOAT',
            nargs='+',
            required=True,
            type=float,
            help='Fractions of task sleep time considered initial overhead',
        )
        group.add_argument(
            '--task-sleep',
            metavar='SECONDS',
            required=True,
            type=float,
            help='Task sleep time (does not include data resolve)',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            submission_method=kwargs['submission_method'],
            task_chain_length=kwargs['task_chain_length'],
            task_data_bytes=kwargs['task_data_bytes'],
            task_overhead_fractions=kwargs['task_overhead_fractions'],
            task_sleep=kwargs['task_sleep'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                submission_method=submission_method,
                task_chain_length=self.task_chain_length,
                task_data_bytes=task_data_bytes,
                task_overhead_fraction=task_overhead_fraction,
                task_sleep=self.task_sleep,
            )
            for (
                task_data_bytes,
                task_overhead_fraction,
                submission_method,
            ) in itertools.product(
                self.task_data_bytes,
                self.task_overhead_fractions,
                self.submission_method,
            )
        )
