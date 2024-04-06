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
    data_size_bytes: int
    max_workers: int
    task_count: int
    task_sleep: float
    use_proxies: bool


class RunResult(BaseModel):
    executor: str
    connector: str
    stream: str
    data_size_bytes: int
    task_count: int
    task_sleep: float
    use_proxies: bool
    workers: int
    completed_tasks: int
    start_submit_tasks_timestamp: float
    end_tasks_done_timestamp: float


class BenchmarkMatrix(BaseModel):
    data_size_bytes: List[int]  # noqa: UP006
    max_workers: int
    stream_method: List[Literal['default', 'proxy']]  # noqa: UP006
    task_count: int
    task_sleep: int

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--data-size-bytes',
            metavar='BYTES',
            nargs='+',
            required=True,
            type=int,
            help='Size of stream data objects in bytes',
        )
        group.add_argument(
            '--max-workers',
            metavar='INT',
            required=True,
            type=int,
            help='Max workers that will be available in the executor',
        )
        group.add_argument(
            '--stream-method',
            choices=['default', 'proxy'],
            nargs='+',
            required=True,
            help='Stream method',
        )
        group.add_argument(
            '--task-count',
            metavar='INT',
            required=True,
            type=float,
            help='Total number of stream items to process',
        )
        group.add_argument(
            '--task-sleep',
            metavar='SECONDS',
            required=True,
            type=float,
            help='Stream processing task sleep time',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            data_size_bytes=kwargs['data_size_bytes'],
            max_workers=kwargs['max_workers'],
            stream_method=kwargs['stream_method'],
            task_count=kwargs['task_count'],
            task_sleep=kwargs['task_sleep'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                data_size_bytes=size,
                max_workers=self.max_workers,
                task_count=self.task_count,
                task_sleep=self.task_sleep,
                use_proxies=method == 'proxy',
            )
            for size, method in itertools.product(
                self.data_size_bytes,
                self.stream_method,
            )
        )
