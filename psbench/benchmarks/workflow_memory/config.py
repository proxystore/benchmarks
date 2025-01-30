from __future__ import annotations

import argparse
import enum
import sys
from typing import Any
from typing import List  # noqa: UP035
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.utils.data import readable_to_bytes
from pydantic import BaseModel


class DataManagement(enum.Enum):
    NONE = 'none'
    DEFAULT_PROXY = 'default-proxy'
    MANUAL_PROXY = 'manual-proxy'
    OWNED_PROXY = 'owned-proxy'


class RunConfig(BaseModel):
    data_management: DataManagement
    stage_task_counts: List[int]  # noqa: UP006
    stage_bytes_sizes: List[int]  # noqa: UP006
    stage_repeat: int
    task_sleep: float


class RunResult(BaseModel):
    executor: str
    connector: Optional[str]  # noqa: UP007
    data_management: str
    stage_task_counts: str
    stage_bytes_sizes: str
    stage_repeat: int
    task_sleep: float
    workflow_start_timestamp: float
    workflow_end_timestamp: float
    workflow_makespan_s: float


class BenchmarkMatrix(BaseModel):
    data_management: List[DataManagement]  # noqa: UP006
    stage_task_counts: List[int]  # noqa: UP006
    stage_bytes_sizes: List[int]  # noqa: UP006
    stage_repeat: int
    task_sleep: float
    memory_profile_interval: float

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--data-management',
            choices=['none', 'default-proxy', 'manual-proxy', 'owned-proxy'],
            default=['default-proxy', 'manual-proxy', 'owned-proxy'],
            nargs='+',
            help=(
                'Data management method. Default will repeat with all options'
            ),
        )
        group.add_argument(
            '--stage-task-counts',
            type=int,
            metavar='COUNT',
            nargs='+',
            required=True,
            help='Number of tasks in each workflow stage.',
        )
        group.add_argument(
            '--stage-bytes-sizes',
            metavar='BYTES',
            nargs='+',
            required=True,
            help=(
                'Intermediate data sizes for tasks in each stage. '
                'Should have length of stage-task-counts + 1.'
            ),
        )
        group.add_argument(
            '--stage-repeat',
            metavar='SECONDS',
            default=1,
            type=int,
            help='Number of times to repeat the stages',
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
        stage_bytes_sizes = [
            readable_to_bytes(s) for s in kwargs['stage_bytes_sizes']
        ]
        return cls(
            data_management=[
                DataManagement(d) for d in kwargs['data_management']
            ],
            stage_task_counts=kwargs['stage_task_counts'],
            stage_bytes_sizes=stage_bytes_sizes,
            stage_repeat=kwargs['stage_repeat'],
            task_sleep=kwargs['task_sleep'],
            memory_profile_interval=kwargs['memory_profile_interval'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                data_management=data_management,
                stage_task_counts=self.stage_task_counts,
                stage_bytes_sizes=self.stage_bytes_sizes,
                stage_repeat=self.stage_repeat,
                task_sleep=self.task_sleep,
            )
            for data_management in self.data_management
        )
