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

ROUTE_TYPE = Literal['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT']


class RunConfig(BaseModel):
    endpoint: str
    route: ROUTE_TYPE
    payload_size_bytes: int
    total_queries: int
    sleep_seconds: float
    workers: int


class RunResult(BaseModel):
    route: ROUTE_TYPE
    payload_size_bytes: int
    total_queries: int
    sleep_seconds: float
    workers: int
    min_worker_elapsed_time_ms: float
    max_worker_elapsed_time_ms: float
    avg_worker_elapsed_time_ms: float
    stdev_worker_elapsed_time_ms: float
    min_latency_ms: float
    max_latency_ms: float
    avg_latency_ms: float
    stdev_latency_ms: float
    qps: float


class BenchmarkMatrix(BaseModel):
    endpoint: str
    routes: List[ROUTE_TYPE]  # noqa: UP006
    payload_size_bytes: List[int]  # noqa: UP006
    total_queries: int
    sleep_seconds: List[float]  # noqa: UP006
    workers: List[int]  # noqa: UP006

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')
        group.add_argument(
            'endpoint',
            help='ProxyStore Endpoint UUID',
        )
        group.add_argument(
            '--routes',
            choices=['GET', 'SET', 'EXISTS', 'EVICT', 'ENDPOINT'],
            nargs='+',
            required=True,
            help='Endpoint routes to query',
        )
        group.add_argument(
            '--payload-sizes',
            type=int,
            nargs='+',
            default=[0],
            help='Payload sizes for GET/SET queries',
        )
        group.add_argument(
            '--workers',
            type=int,
            nargs='+',
            default=[1],
            help='Number of workers (processes) making queries',
        )
        group.add_argument(
            '--sleep',
            type=float,
            nargs='+',
            default=[0],
            help='Sleeps (seconds) between queries',
        )
        group.add_argument(
            '--queries',
            type=int,
            default=100,
            help='Number of queries per worker to make',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            endpoint=kwargs['endpoint'],
            routes=kwargs['routes'],
            payload_size_bytes=kwargs['payload_sizes'],
            total_queries=kwargs['queries'],
            sleep_seconds=kwargs['sleep'],
            workers=kwargs['workers'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        return tuple(
            RunConfig(
                endpoint=self.endpoint,
                route=route,
                payload_size_bytes=payload_size_bytes,
                total_queries=self.total_queries,
                sleep_seconds=sleep_seconds,
                workers=workers,
            )
            for (
                route,
                payload_size_bytes,
                sleep_seconds,
                workers,
            ) in itertools.product(
                self.routes,
                self.payload_size_bytes,
                self.sleep_seconds,
                self.workers,
            )
        )
