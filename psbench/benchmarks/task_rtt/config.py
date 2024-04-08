from __future__ import annotations

import argparse
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


class RunConfig(BaseModel):
    sleep: float
    input_size_bytes: int
    output_size_bytes: int


class RunResult(BaseModel):
    proxystore_backend: str
    task_name: str
    input_size_bytes: int
    output_size_bytes: int
    task_sleep_seconds: float
    total_time_ms: float
    input_get_ms: Optional[float] = None  # noqa: UP007
    input_put_ms: Optional[float] = None  # noqa: UP007
    input_proxy_ms: Optional[float] = None  # noqa: UP007
    input_resolve_ms: Optional[float] = None  # noqa: UP007
    output_get_ms: Optional[float] = None  # noqa: UP007
    output_put_ms: Optional[float] = None  # noqa: UP007
    output_proxy_ms: Optional[float] = None  # noqa: UP007
    output_resolve_ms: Optional[float] = None  # noqa: UP007


class BenchmarkMatrix(BaseModel):
    sleep: float
    input_sizes: List[int]  # noqa: UP006
    output_sizes: List[int]  # noqa: UP006

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--task-sleep',
            type=float,
            default=0,
            help='Sleep time for tasks',
        )
        group.add_argument(
            '--input-sizes',
            type=int,
            nargs='+',
            required=True,
            help='Task input size in bytes',
        )
        group.add_argument(
            '--output-sizes',
            type=int,
            nargs='+',
            required=True,
            help='Task output size in bytes',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            sleep=kwargs['task_sleep'],
            input_sizes=kwargs['input_sizes'],
            output_sizes=kwargs['output_sizes'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                sleep=self.sleep,
                input_size_bytes=input_size,
                output_size_bytes=output_size,
            )
            for input_size, output_size in itertools.product(
                self.input_sizes,
                self.output_sizes,
            )
        )
