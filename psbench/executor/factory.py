from __future__ import annotations

import argparse
import multiprocessing

import dask
import globus_compute_sdk
from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.executors.base import ParslExecutor as ParslBaseExecutor
from parsl.providers import LocalProvider

from psbench.executor.dask import DaskExecutor
from psbench.executor.globus import GlobusComputeExecutor
from psbench.executor.parsl import ParslExecutor
from psbench.executor.protocol import Executor


def init_executor_from_args(args: argparse.Namespace) -> Executor:
    """Initialize an Executor from CLI arguments.

    Usage:
        >>> parser = argparse.ArgumentParser(...)
        >>> add_executor_options(parser, required=...)
        >>> args = parser.parse_args()
        >>> executor = init_executor_from_args(args)

    Args:
        args (Namespace): namespace returned by argument parser.

    Returns:
        Executor.
    """
    if args.executor == 'dask':
        if args.dask_scheduler is not None:
            client = dask.distributed.Client(args.dask_scheduler)
        else:
            client = dask.distributed.Client(
                n_workers=args.dask_workers,
                processes=not args.dask_use_threads,
                dashboard_address=args.dask_dashboard_address,
            )
        return DaskExecutor(client)
    elif args.executor == 'globus':
        return GlobusComputeExecutor(
            globus_compute_sdk.Executor(args.globus_compute_endpoint),
        )
    elif args.executor == 'parsl':
        executor: ParslBaseExecutor
        workers = (
            args.parsl_workers
            if args.parsl_workers is not None
            else multiprocessing.cpu_count()
        )
        if args.parsl_thread_executor:
            executor = ThreadPoolExecutor(max_threads=workers)
        elif args.parsl_local_htex:
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
            raise AssertionError(
                'Unknown Parsl executor type. Use --parsl-thread-executor '
                'or --parsl-local-htex.',
            )
        config = Config(executors=[executor], run_dir=args.parsl_run_dir)
        return ParslExecutor(config)
    else:
        raise AssertionError(
            f'Unknown executor type "{args.executor}". '
            'Expected "dask", "globus", or "parsl".',
        )
