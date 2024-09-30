from __future__ import annotations

import argparse
import multiprocessing
import sys
from concurrent.futures import Executor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from typing import Literal
from typing import Optional
from typing import Sequence

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import dask
import globus_compute_sdk
from parsl.concurrent import ParslPoolExecutor
from parsl.config import Config
from pydantic import BaseModel

from psbench.config.parsl import CONFIG_FACTORY
from psbench.executor.dask import DaskExecutor


class DaskConfig(BaseModel):
    scheduler_address: Optional[str] = None  # noqa: UP007
    threaded_workers: bool = False
    workers: Optional[int] = None  # noqa: UP007

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
        options: dict[str, Any] = {}

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
    endpoint: str

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

    def get_executor(self) -> globus_compute_sdk.Executor:
        return globus_compute_sdk.Executor(self.endpoint)


class ParslConfig(BaseModel):
    executor: str
    run_dir: str
    max_workers: int

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Parsl Configuration')

        group.add_argument(
            '--parsl-executor',
            choices=list(CONFIG_FACTORY.keys()),
            required=required,
            help='Parsl executor type',
        )
        group.add_argument(
            '--parsl-max-workers',
            metavar='WORKERS',
            required=required,
            type=int,
            help=(
                'Number of Parsl workers. This configures the value for '
                'local executors or hints to the benchmark how many workers '
                'there will be when using an executor over multiple nodes.'
            ),
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        options = {
            'executor': kwargs['parsl_executor'],
            'run_dir': kwargs['parsl_run_dir'],
            'max_workers': kwargs['parsl_max_workers'],
        }
        return cls(**options)

    def get_config(self) -> Config:
        try:
            factory = CONFIG_FACTORY[self.executor]
        except KeyError as e:
            raise ValueError(
                f'Unknown Parsl Executor "{self.executor}". '
                f'Expected one of: {list(CONFIG_FACTORY.keys())}.',
            ) from e

        return factory(self.run_dir, self.max_workers)

    def get_executor(self) -> ParslPoolExecutor:
        return ParslPoolExecutor(self.get_config())


class ProcessPoolConfig(BaseModel):
    max_workers: int

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Process Pool Configuration')

        group.add_argument(
            '--process-pool-max-workers',
            metavar='WORKERS',
            type=int,
            help='Number of process in the pool. Default is number of CPUs.',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        max_workers = kwargs.get('process_pool_max_workers')
        max_workers = (
            multiprocessing.cpu_count() if max_workers is None else max_workers
        )
        return cls(max_workers=max_workers)

    def get_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(self.max_workers)


class ThreadPoolConfig(BaseModel):
    max_workers: int

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
    ) -> None:
        group = parser.add_argument_group(title='Thread Pool Configuration')

        group.add_argument(
            '--thread-pool-max-workers',
            metavar='WORKERS',
            type=int,
            help='Number of threads in the pool. Default is number of CPUs.',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        max_workers = kwargs.get('thread_pool_max_workers')
        max_workers = (
            multiprocessing.cpu_count() if max_workers is None else max_workers
        )
        return cls(max_workers=max_workers)

    def get_executor(self) -> ThreadPoolExecutor:
        return ThreadPoolExecutor(self.max_workers)


class ExecutorConfig(BaseModel):
    kind: Literal['dask', 'globus', 'parsl', 'process', 'thread']
    # It would be preferred to type this as:
    #     config: DaskConfig | GlobusComputeConfig | ParslConfig | ...
    # but Pydantic v1 handles union types in unexpected ways so type attempt
    # to be coerced incorrectly.
    config: Any

    @staticmethod
    def add_parser_group(
        parser: argparse.ArgumentParser,
        required: bool = True,
        argv: Sequence[str] | None = None,
    ) -> None:
        parser.add_argument(
            '--executor',
            choices=['dask', 'globus', 'parsl', 'process', 'thread'],
            required=required,
            help='Task executor/workflow engine',
        )

        executor_type: str | None = None
        if argv is not None and '--executor' in argv:
            executor_type = argv[argv.index('--executor') + 1]

        DaskConfig.add_parser_group(
            parser,
            required=required and executor_type == 'dask',
        )
        GlobusComputeConfig.add_parser_group(
            parser,
            required=required and executor_type == 'globus',
        )
        ParslConfig.add_parser_group(
            parser,
            required=required and executor_type == 'parsl',
        )
        ProcessPoolConfig.add_parser_group(
            parser,
            required=required and executor_type == 'process',
        )
        ThreadPoolConfig.add_parser_group(
            parser,
            required=required and executor_type == 'thread',
        )

    @classmethod
    def from_args(cls, **kwargs: Any) -> Self:
        kind = kwargs['executor']

        config: Any
        if kind == 'dask':
            config = DaskConfig.from_args(**kwargs)
        elif kind == 'globus':
            config = GlobusComputeConfig.from_args(**kwargs)
        elif kind == 'parsl':
            config = ParslConfig.from_args(**kwargs)
        elif kind == 'process':
            config = ProcessPoolConfig.from_args(**kwargs)
        elif kind == 'thread':
            config = ThreadPoolConfig.from_args(**kwargs)
        else:
            # Unreachable because of Pydantic type validation
            raise AssertionError(f'Unknown executor type "{kind}".')

        return cls(kind=kind, config=config)

    def get_executor(self) -> Executor:
        return self.config.get_executor()
