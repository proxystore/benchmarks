from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import List

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel


class RunConfig(BaseModel):
    data_size_bytes: int
    producer_sleep: float
    task_count: int
    task_sleep: int


class RunResult(BaseModel):
    executor: str
    connector: str
    stream: str
    data_size_bytes: int
    producer_sleep: float
    task_count: int
    task_sleep: float
    workers: int
    task_submitted_timestamp: float
    task_received_timestamp: float


class BenchmarkMatrix(BaseModel):
    data_size_bytes: List[int]  # noqa: UP006
    producer_sleep: float
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
            '--producer-sleep',
            metavar='SECONDS',
            required=True,
            type=float,
            help='Sleep time between producing new stream items',
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
            producer_sleep=kwargs['producer_sleep'],
            task_count=kwargs['task_count'],
            task_sleep=kwargs['task_sleep'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                data_size_bytes=size,
                producer_sleep=self.producer_sleep,
                task_count=self.task_count,
                task_sleep=self.task_sleep,
            )
            for size in self.data_size_bytes
        )
