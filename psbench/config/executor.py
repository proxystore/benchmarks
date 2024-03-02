from __future__ import annotations

import argparse
import multiprocessing
import sys
import uuid
from typing import Any
from typing import Literal
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import dask
import globus_compute_sdk
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.executors.base import ParslExecutor as _ParslExecutor
from parsl.providers import LocalProvider
from pydantic import BaseModel

from psbench.executor.dask import DaskExecutor
from psbench.executor.globus import GlobusComputeExecutor
from psbench.executor.parsl import ParslExecutor
from psbench.executor.protocol import Executor


class DaskConfig(BaseModel):
    scheduler_address: str | None = None
    threaded_workers: bool = False
    workers: int | None = None

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Dask Configuration')

        group.add_argument(
            '--dask-scheduler',
            default=None,
            metavar='ADDR',
            help='Dask scheduler address (default uses LocalCluster)',
        )
        group.add_argument(
            '--dask-workers',
            default=None,
            metavar='WORKERS',
            type=int,
            help='Number of workers to start if using LocalCluster',
        )
        group.add_argument(
            '--dask-use-threads',
            action='store_true',
            help='Use threads instead of processes for LocalCluster workers',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        options: dict[str, int | str] = {}

        if 'dask_scheduler' in kwargs:
            options['scheduler_address'] = kwargs['dask_scheduler']
        if 'dask_workers' in kwargs:
            options['workers'] = kwargs['dask_workers']
        if 'dask_use_threads' in kwargs:
            options['threaded_workers'] = kwargs['dask_use_threads']

        return cls(**options)

    def get_executor(self) -> DaskExecutor:
        if self.scheduler_address is not None:
            client = dask.distributed.Client(self.scheduler_address)
        else:
            client = dask.distributed.Client(
                n_workers=self.workers,
                processes=not self.threaded_workers,
                dashboard_address=None,
            )
        return DaskExecutor(client)


class GlobusComputeConfig(BaseModel):
    endpoint: uuid.UUID | str

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Globus Compute Configuration')

        group.add_argument(
            '--globus-compute-endpoint',
            metavar='UUID',
            required=required,
            help='Globus Compute Endpoint UUID',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        return cls(endpoint=kwargs['globus_compute_endpoint'])

    def get_executor(self) -> GlobusComputeExecutor:
        return GlobusComputeExecutor(
            globus_compute_sdk.Executor(self.endpoint),
        )


class ParslConfig(BaseModel):
    executor: Literal['thread', 'htex']
    run_dir: str
    workers: int | None = None

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Parsl Configuration')

        group.add_argument(
            '--parsl-executor',
            choices=['thread', 'htex-local'],
            help='Parsl executor type',
        )
        group.add_argument(
            '--parsl-workers',
            default=None,
            metavar='WORKERS',
            type=int,
            help='Number of Parsl workers to configure',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        options = {
            'executor': kwargs['parsl_executor'],
            'run_dir': kwargs['parsl_run_dir'],
        }
        if 'parsl_workers' in kwargs:
            options['workers'] = kwargs['parsl_workers']
        return cls(**options)

    def get_executor(self) -> ParslExecutor:
        executor: _ParslExecutor

        workers = (
            self.workers
            if self.workers is not None
            else multiprocessing.cpu_count()
        )
        if self.executor == 'thread':
            executor = ThreadPoolExecutor(max_threads=workers)
        elif self.executor == 'htex-local':
            executor = HighThroughputExecutor(
                max_workers=workers,
                address=address_by_hostname(),
                cores_per_worker=1,
                provider=LocalProvider(
                    channel=LocalChannel(),
                    init_blocks=1,
                    max_blocks=1,
                ),
            )
        else:
            raise AssertionError(f'Unknown Parsl Executor "{self.executor}".')

        config = Config(executors=[executor], run_dir=self.run_dir)
        return ParslExecutor(config)


class ExecutorConfig(BaseModel):
    kind: Literal['dask', 'globus', 'parsl']
    config: DaskConfig | GlobusComputeConfig | ParslConfig

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
        argv: Sequence[str] | None = None,
    ) -> None:
        parser.add_argument(
            '--executor',
            choices=['dask', 'globus', 'parsl'],
            help='Task executor/workflow engine',
        )

        argv = [] if argv is None else argv
        DaskConfig.add_parser_group(
            parser,
            required=required and 'dask' in argv,
        )
        GlobusComputeConfig.add_parser_group(
            parser,
            required=required and 'globus' in argv,
        )
        ParslConfig.add_parser_group(
            parser,
            required=required and 'parsl' in argv,
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        kind = kwargs['executor']

        config: DaskConfig | GlobusComputeConfig | ParslConfig
        if kind == 'dask':
            config = DaskConfig.from_args(**kwargs)
        elif kind == 'globus':
            config = GlobusComputeConfig.from_args(**kwargs)
        elif kind == 'parsl':
            config = ParslConfig.from_args(**kwargs)
        else:
            raise ValueError(f'Unknown executor type "{kind}".')

        return cls(kind=kind, config=config)

    def get_executor(self) -> Executor:
        return self.config.get_executor()
