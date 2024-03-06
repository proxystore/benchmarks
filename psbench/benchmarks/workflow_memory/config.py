from __future__ import annotations

import argparse
import enum
import itertools
import sys
from typing import Any
from typing import List
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class DataManagement(enum.Enum):
    NONE = 'none'
    DEFAULT_PROXY = 'default-proxy'
    OWNED_PROXY = 'owned-proxy'


class RunConfig(BaseModel):
    data_management: DataManagement
    stage_sizes: List[int]  # noqa: UP006
    data_size_bytes: int
    task_sleep: float


class RunResult(BaseModel):
    executor: str
    connector: Optional[str]  # noqa: UP007
    data_management: str
    stage_sizes: str
    data_size_bytes: int
    task_sleep: float
    workflow_start_timestamp: float
    workflow_end_timestamp: float
    workflow_makespan_s: float


class BenchmarkMatrix(BaseModel):
    data_management: List[DataManagement]  # noqa: UP006
    stage_sizes: List[int]  # noqa: UP006
    data_sizes_bytes: List[int]  # noqa: UP006
    task_sleep: float
    memory_profile_interval: float

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--data-management',
            choices=['none', 'default-proxy', 'owned-proxy'],
            default=['none', 'default-proxy', 'owned-proxy'],
            nargs='+',
            help=(
                'Data management method. Default will repeat with all options'
            ),
        )
        group.add_argument(
            '--stage-sizes',
            type=int,
            metavar='COUNT',
            nargs='+',
            required=True,
            help='List of stages sizes of the simulated workflow',
        )
        group.add_argument(
            '--data-sizes-bytes',
            type=int,
            metavar='BYTES',
            nargs='+',
            required=True,
            help='Task input/output sizes in bytes',
        )
        group.add_argument(
            '--task-sleep',
            metavar='SECONDS',
            required=True,
            type=float,
            help='Simulate task computation',
        )
        group.add_argument(
            '--memory-profile-interval',
            default=0.01,
            metavar='SECONDS',
            type=float,
            help='Seconds between logging system memory utilization',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            data_management=[
                DataManagement(d) for d in kwargs['data_management']
            ],
            stage_sizes=kwargs['stage_sizes'],
            data_sizes_bytes=kwargs['data_sizes_bytes'],
            task_sleep=kwargs['task_sleep'],
            memory_profile_interval=kwargs['memory_profile_interval'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                data_management=data_management,
                stage_sizes=self.stage_sizes,
                data_size_bytes=data_size_bytes,
                task_sleep=self.task_sleep,
            )
            for data_size_bytes, data_management in itertools.product(
                self.data_sizes_bytes,
                self.data_management,
            )
        )
