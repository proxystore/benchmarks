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
    name: str


class RunResult(BaseModel):
    name: str
    result: int


class BenchmarkMatrix(BaseModel):
    names: List[str]  # noqa: UP006

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            '--names',
            nargs='+',
            required=True,
            help='Task names',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(names=kwargs['names'])

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(RunConfig(name=name) for name in self.names)
