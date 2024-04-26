from __future__ import annotations

import argparse
import sys
from typing import Any
from typing import List
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from colmena.models.results import Result
from pydantic import BaseModel


class RunConfig(BaseModel):
    input_sizes: List[int]  # noqa: UP006
    output_sizes: List[int]  # noqa: UP006
    task_sleep: float
    reuse_inputs: bool


class RunResult(BaseModel):
    task_id: str
    method: str
    success: bool
    input_size_bytes: int
    output_size_bytes: int
    proxystore_backend: str
    # Defined in colmena.models.Timestamps
    time_created: float
    time_input_received: float
    time_compute_started: float
    time_compute_ended: float
    time_result_sent: float
    time_result_received: float
    time_start_task_submission: float
    time_task_received: float
    # Defined in colmena.models.TimeSpans
    time_running: float
    time_serialize_inputs: float
    time_deserialize_inputs: float
    time_serialize_results: float
    time_deserialize_results: float
    time_async_resolve_proxies: float

    @classmethod
    def from_result(
        cls,
        result: Result,
        input_size_bytes: int,
        output_size_bytes: int,
        proxystore_backend: str,
    ) -> RunResult:
        kwargs = {
            'task_id': result.task_id,
            'method': result.method,
            'success': result.success,
            'input_size_bytes': input_size_bytes,
            'output_size_bytes': output_size_bytes,
            'proxystore_backend': proxystore_backend,
        }
        for field in result.timestamp.__fields_set__:
            if f'time_{field}' in cls.__fields__:  # pragma: no branch
                kwargs[f'time_{field}'] = getattr(result.timestamp, field)
        for field in result.time.__fields_set__:
            if f'time_{field}' in cls.__fields__:  # pragma: no branch
                kwargs[f'time_{field}'] = getattr(result.time, field)
        return cls(**kwargs)


class BenchmarkMatrix(BaseModel):
    redis_host: Optional[str]  # noqa: UP007
    redis_port: Optional[int]  # noqa: UP007
    input_sizes: List[int]  # noqa: UP006
    output_sizes: List[int]  # noqa: UP006
    task_sleep: float
    reuse_inputs: bool

    @staticmethod
    def add_parser_group(parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')

        group.add_argument(
            '--redis-host',
            default=None,
            help='Hostname for Colmena RedisQueue',
        )
        group.add_argument(
            '--redis-port',
            default=None,
            type=int,
            help='Port for Colmena RedisQueue',
        )
        group.add_argument(
            '--input-sizes',
            type=int,
            nargs='+',
            required=True,
            help='Task input sizes [bytes]',
        )
        group.add_argument(
            '--output-sizes',
            type=int,
            nargs='+',
            required=True,
            help='Task output sizes [bytes]',
        )
        group.add_argument(
            '--task-sleep',
            type=float,
            default=0,
            help='Sleep time for tasks',
        )
        group.add_argument(
            '--reuse-inputs',
            action='store_true',
            default=False,
            help='Send the same input to each task',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            redis_host=kwargs['redis_host'],
            redis_port=kwargs['redis_port'],
            input_sizes=kwargs['input_sizes'],
            output_sizes=kwargs['output_sizes'],
            task_sleep=kwargs['task_sleep'],
            reuse_inputs=kwargs['reuse_inputs'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        config = RunConfig(
            input_sizes=self.input_sizes,
            output_sizes=self.output_sizes,
            task_sleep=self.task_sleep,
            reuse_inputs=self.reuse_inputs,
        )
        return (config,)
