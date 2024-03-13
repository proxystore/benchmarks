from __future__ import annotations

import argparse
import enum
import itertools
import sys
from typing import Any
from typing import List

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class SubmissionMethod(enum.Enum):
    SEQUENTIAL_NO_PROXY = 'sequential-no-proxy'
    SEQUENTIAL_PROXY = 'sequential-proxy'
    PIPELINED_PROXY_FUTURE = 'pipelined-proxy-future'


class RunConfig(BaseModel):
    submission_method: SubmissionMethod
    task_chain_length: int
    task_data_bytes: int
    task_overhead_fraction: float
    task_sleep: float


class RunResult(BaseModel):
    executor: str
    connector: str
    submission_method: str
    task_chain_length: int
    task_data_bytes: int
    task_overhead_fraction: float
    task_sleep: float
    task_timestamps: str
    workflow_makespan_ms: float


class BenchmarkMatrix(BaseModel):
    submission_method: List[SubmissionMethod]  # noqa: UP006
    task_chain_length: int
    task_data_bytes: List[int]  # noqa: UP006
    task_overhead_fractions: List[float]  # noqa: UP006
    task_sleep: float

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--submission-method',
            choices=[e.value for e in SubmissionMethod],
            default=['sequential-proxy', 'pipelined-proxy-future'],
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
                submission_method=SubmissionMethod(submission_method),
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
