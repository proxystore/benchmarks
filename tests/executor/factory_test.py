from __future__ import annotations

import argparse
import pathlib
from unittest import mock

from psbench.argparse import add_executor_options
from psbench.executor.dask import DaskExecutor
from psbench.executor.factory import init_executor_from_args
from psbench.executor.globus import GlobusComputeExecutor
from psbench.executor.parsl import ParslExecutor
from testing.globus_compute import mock_globus_compute


def test_init_dask_executor_local() -> None:
    parser = argparse.ArgumentParser()
    add_executor_options(parser)
    args = parser.parse_args(
        ['--executor', 'dask', '--dask-workers', '1', '--dask-use-threads'],
    )

    executor = init_executor_from_args(args)
    assert isinstance(executor, DaskExecutor)
    executor.close()


def test_init_dask_executor_remote_scheduler() -> None:
    parser = argparse.ArgumentParser()
    add_executor_options(parser)
    args = parser.parse_args(
        ['--executor', 'dask', '--dask-scheduler', 'localhost'],
    )

    with mock.patch('dask.distributed.Client'):
        executor = init_executor_from_args(args)

    assert isinstance(executor, DaskExecutor)
    executor.close()


def test_init_globus_compute_executor() -> None:
    parser = argparse.ArgumentParser()
    add_executor_options(parser)
    args = parser.parse_args(
        ['--executor', 'globus', '--globus-compute-endpoint', 'UUID'],
    )

    with mock_globus_compute():
        executor = init_executor_from_args(args)

    assert isinstance(executor, GlobusComputeExecutor)
    executor.close()


def test_init_parsl_thread_executor(tmp_path: pathlib.Path) -> None:
    parser = argparse.ArgumentParser()
    add_executor_options(parser)
    args = parser.parse_args(
        [
            '--executor',
            'parsl',
            '--parsl-thread-executor',
            '--parsl-run-dir',
            str(tmp_path),
        ],
    )

    executor = init_executor_from_args(args)
    assert isinstance(executor, ParslExecutor)


def test_init_parsl_htex(tmp_path: pathlib.Path) -> None:
    parser = argparse.ArgumentParser()
    add_executor_options(parser)
    args = parser.parse_args(
        [
            '--executor',
            'parsl',
            '--parsl-local-htex',
            '--parsl-run-dir',
            str(tmp_path),
        ],
    )

    executor = init_executor_from_args(args)
    assert isinstance(executor, ParslExecutor)
