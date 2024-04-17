from __future__ import annotations

import argparse
import pathlib
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from globus_compute_sdk import Executor as GlobusComputeExecutor
from parsl.concurrent import ParslPoolExecutor

from psbench.config.executor import DaskConfig
from psbench.config.executor import ExecutorConfig
from psbench.config.executor import GlobusComputeConfig
from psbench.config.executor import ParslConfig
from psbench.config.executor import ProcessPoolConfig
from psbench.config.executor import ThreadPoolConfig
from psbench.executor.dask import DaskExecutor
from testing.globus_compute import mock_globus_compute


def test_dask_argparse() -> None:
    parser = argparse.ArgumentParser()
    DaskConfig.add_parser_group(parser)
    args = parser.parse_args(['--dask-scheduler', 'localhost'])
    config = DaskConfig.from_args(**vars(args))
    assert config.scheduler_address == 'localhost'

    parser = argparse.ArgumentParser()
    DaskConfig.add_parser_group(parser)
    args = parser.parse_args(['--dask-workers', '1', '--dask-use-threads'])
    config = DaskConfig.from_args(**vars(args))
    assert config.scheduler_address is None
    assert config.threaded_workers
    assert config.workers == 1

    DaskConfig.from_args()


def test_globus_compute_argparse() -> None:
    parser = argparse.ArgumentParser()
    GlobusComputeConfig.add_parser_group(parser)
    args = parser.parse_args(['--globus-compute-endpoint', 'ABCD'])
    config = GlobusComputeConfig.from_args(**vars(args))
    assert config.endpoint == 'ABCD'

    parser = argparse.ArgumentParser()
    GlobusComputeConfig.add_parser_group(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_parsl_argparse() -> None:
    parser = argparse.ArgumentParser()
    ParslConfig.add_parser_group(parser)
    args = parser.parse_args(
        ['--parsl-executor', 'thread', '--parsl-max-workers', '1'],
    )
    config = ParslConfig.from_args(parsl_run_dir='test', **vars(args))
    assert config.executor == 'thread'
    assert config.run_dir == 'test'
    assert config.max_workers == 1

    parser = argparse.ArgumentParser()
    ParslConfig.add_parser_group(parser, required=True)
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            parser.parse_args([])


def test_process_pool_argparse() -> None:
    parser = argparse.ArgumentParser()
    ProcessPoolConfig.add_parser_group(parser)
    args = parser.parse_args(['--process-pool-max-workers', '1'])
    config = ProcessPoolConfig.from_args(**vars(args))
    assert config.max_workers == 1

    config = ProcessPoolConfig.from_args()
    assert config.max_workers > 0


def test_thread_pool_argparse() -> None:
    parser = argparse.ArgumentParser()
    ThreadPoolConfig.add_parser_group(parser)
    args = parser.parse_args(['--thread-pool-max-workers', '1'])
    config = ThreadPoolConfig.from_args(**vars(args))
    assert config.max_workers == 1

    config = ThreadPoolConfig.from_args()
    assert config.max_workers > 0


def test_executor_argparse() -> None:
    # Dask
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(parser)
    args = parser.parse_args(['--executor', 'dask'])
    config = ExecutorConfig.from_args(**vars(args))
    assert config.kind == 'dask'
    assert isinstance(config.config, DaskConfig)

    # Globus Compute
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(parser)
    args = parser.parse_args(
        ['--executor', 'globus', '--globus-compute-endpoint', 'UUID'],
    )
    config = ExecutorConfig.from_args(**vars(args))
    assert config.kind == 'globus'
    assert isinstance(config.config, GlobusComputeConfig)

    # Parsl
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--executor',
            'parsl',
            '--parsl-executor',
            'thread',
            '--parsl-max-workers',
            '1',
        ],
    )
    config = ExecutorConfig.from_args(
        parsl_run_dir='/tmp/rundir',
        **vars(args),
    )
    assert config.kind == 'parsl'
    assert isinstance(config.config, ParslConfig)

    # ProcessPool
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(parser)
    args = parser.parse_args(['--executor', 'process'])
    config = ExecutorConfig.from_args(**vars(args))
    assert config.kind == 'process'
    assert isinstance(config.config, ProcessPoolConfig)

    # ThreadPool
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(parser)
    args = parser.parse_args(['--executor', 'thread'])
    config = ExecutorConfig.from_args(**vars(args))
    assert config.kind == 'thread'
    assert isinstance(config.config, ThreadPoolConfig)

    # Missing args when required=True
    parser = argparse.ArgumentParser()
    ExecutorConfig.add_parser_group(
        parser,
        required=True,
        argv=['--executor', 'globus'],
    )
    # Suppress argparse error message
    with mock.patch('argparse.ArgumentParser._print_message'):
        with pytest.raises(SystemExit):
            # Will fail because globus config requires other options not here
            parser.parse_args(['--executor', 'globus'])


def test_dask_config_local() -> None:
    config = DaskConfig(threaded_workers=True, workers=1)
    executor = config.get_executor()
    assert isinstance(executor, DaskExecutor)
    executor.shutdown()


def test_dask_config_remote_scheduler() -> None:
    config = DaskConfig(scheduler_address='localhost')

    with mock.patch('dask.distributed.Client'):
        executor = config.get_executor()

    assert isinstance(executor, DaskExecutor)
    executor.shutdown()


def test_globus_compute_config() -> None:
    config = GlobusComputeConfig(endpoint='UUID')

    with mock_globus_compute():
        executor = config.get_executor()

    assert isinstance(executor, GlobusComputeExecutor)
    executor.shutdown()


def test_parsl_config_unknown(tmp_path: pathlib.Path) -> None:
    config = ParslConfig(
        executor='unknown',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    with pytest.raises(ValueError, match=config.executor):
        config.get_executor()


def test_parsl_config_thread_executor(tmp_path: pathlib.Path) -> None:
    config = ParslConfig(
        executor='thread',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    with config.get_executor() as executor:
        assert isinstance(executor, ParslPoolExecutor)


def test_parsl_config_htex_local(tmp_path: pathlib.Path) -> None:
    config = ParslConfig(
        executor='htex-local',
        run_dir=str(tmp_path),
        max_workers=1,
    )
    with mock.patch('psbench.config.parsl.address_by_interface'):
        config.get_config()


def test_parsl_config_htex_polaris_headless(tmp_path: pathlib.Path) -> None:
    config = ParslConfig(
        executor='htex-polaris-headless',
        run_dir=str(tmp_path),
        max_workers=256,
    )
    with mock.patch('psbench.config.parsl.address_by_interface'):
        config.get_config()


def test_executor_config() -> None:
    config = ExecutorConfig(
        kind='globus',
        config=GlobusComputeConfig(endpoint='UUID'),
    )

    with mock_globus_compute():
        executor = config.get_executor()

    assert isinstance(executor, GlobusComputeExecutor)
    executor.shutdown()


def test_process_pool_config_executor() -> None:
    config = ProcessPoolConfig(max_workers=1)
    with config.get_executor() as executor:
        assert isinstance(executor, ProcessPoolExecutor)


def test_thread_pool_config_executor() -> None:
    config = ThreadPoolConfig(max_workers=1)
    with config.get_executor() as executor:
        assert isinstance(executor, ThreadPoolExecutor)
