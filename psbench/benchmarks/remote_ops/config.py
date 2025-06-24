from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Any
from typing import List  # noqa: UP035
from typing import Literal
from typing import Optional

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel

BACKEND_TYPE = Literal['endpoint', 'redis']
OP_TYPE = Literal['evict', 'exists', 'get', 'set']


class RunConfig(BaseModel):
    backend: BACKEND_TYPE
    ops: List[OP_TYPE]  # noqa: UP006
    payload_sizes: List[int]  # noqa: UP006
    repeat: int


class RunResult(BaseModel):
    backend: BACKEND_TYPE
    op: OP_TYPE
    payload_size_bytes: Optional[int]  # noqa: UP045
    repeat: int
    total_time_ms: float
    avg_time_ms: float
    min_time_ms: float
    max_time_ms: float
    stdev_time_ms: float
    avg_bandwidth_mbps: Optional[float]  # noqa: UP045


class BenchmarkMatrix(BaseModel):
    backend: BACKEND_TYPE
    endpoint: Optional[str]  # noqa: UP045
    redis_host: Optional[str]  # noqa: UP045
    redis_port: Optional[int]  # noqa: UP045
    ops: List[OP_TYPE]  # noqa: UP006
    payload_sizes: List[int]  # noqa: UP006
    relay_server: Optional[str]  # noqa: UP045
    repeat: int
    use_uvloop: bool

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        argv: Sequence[str] | None = None,
    ) -> None:
        group = parser.add_argument_group(title='Benchmark Parameters')

        args_str = ' '.join(argv) if argv is not None else ''
        group.add_argument(
            'backend',
            choices=['endpoint', 'redis'],
            help='Remote objects store backend to test',
        )
        group.add_argument(
            '--endpoint',
            required='endpoint' in args_str,
            help='Remote Endpoint UUID',
        )
        group.add_argument(
            '--redis-host',
            required='redis' in args_str,
            help='Redis server hostname/IP',
        )
        group.add_argument(
            '--redis-port',
            required='redis' in args_str,
            help='Redis server port',
        )
        group.add_argument(
            '--ops',
            choices=['get', 'set', 'exists', 'evict'],
            nargs='+',
            required=True,
            help='Endpoint operations to measure',
        )
        group.add_argument(
            '--payload-sizes',
            type=int,
            nargs='+',
            default=0,
            help='Payload sizes for get/set operations',
        )
        group.add_argument(
            '--relay-server',
            required='endpoint' in args_str,
            help='Relay server address for connecting to the remote endpoint',
        )
        group.add_argument(
            '--no-uvloop',
            action='store_true',
            help='Override using uvloop if available (endpoint backend only)',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(
            backend=kwargs['backend'],
            endpoint=kwargs['endpoint'],
            redis_host=kwargs['redis_host'],
            redis_port=kwargs['redis_port'],
            ops=kwargs['ops'],
            payload_sizes=kwargs['payload_sizes'],
            relay_server=kwargs['relay_server'],
            repeat=kwargs.get('repeat', 1),
            use_uvloop=not kwargs['no_uvloop'],
        )

    def configs(self) -> tuple[RunConfig, ...]:
        config = RunConfig(
            backend=self.backend,
            ops=self.ops,
            payload_sizes=self.payload_sizes,
            repeat=self.repeat,
        )
        return (config,)
