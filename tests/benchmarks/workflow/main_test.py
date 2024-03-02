from __future__ import annotations

import os
import pathlib
import time
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store.base import Store

from psbench.benchmarks.workflow.main import DataManagement
from psbench.benchmarks.workflow.main import main
from psbench.benchmarks.workflow.main import run_workflow
from psbench.benchmarks.workflow.main import runner
from psbench.benchmarks.workflow.main import task_no_proxy
from psbench.benchmarks.workflow.main import task_proxy
from psbench.benchmarks.workflow.main import validate_workflow
from psbench.benchmarks.workflow.main import WorkflowStats
from psbench.utils import randbytes
from testing.executor import ProcessPoolExecutor
from testing.globus_compute import mock_globus_compute


@pytest.mark.parametrize(
    ('stage_sizes', 'valid'),
    (
        ((), False),
        ((1, -1, 1), False),
        ((1, 0, 1), False),
        ((1, 3, 2), False),
        ((1, 1, 1), True),
        ((3, 3, 3), True),
        ((1, 3, 1), True),
        ((3, 1, 3), True),
    ),
)
def test_validate_workflow(stage_sizes: tuple[int], valid: bool) -> None:
    if valid:
        validate_workflow(stage_sizes)
    else:
        with pytest.raises(ValueError):
            validate_workflow(stage_sizes)


def test_task_no_proxy() -> None:
    sleep = 0.001
    size = 100
    data = randbytes(size)

    start = time.perf_counter()
    result = task_no_proxy(data, output_size_bytes=size, sleep=sleep)
    end = time.perf_counter()

    assert len(result) == size
    assert (end - start) >= sleep


def test_task_proxy(file_store: Store[FileConnector]) -> None:
    sleep = 0.001
    size = 100
    data = file_store.proxy(randbytes(size), evict=True)

    start = time.perf_counter()
    result = task_proxy(data, output_size_bytes=size, sleep=sleep)
    end = time.perf_counter()

    assert isinstance(result, Proxy)
    assert len(result) == size
    assert (end - start) >= sleep

    key = result.__factory__.key
    assert file_store.exists(key)
    file_store.evict(key)


@pytest.mark.parametrize(
    'data_management',
    (
        DataManagement.NONE,
        DataManagement.DEFAULT_PROXY,
        DataManagement.OWNED_PROXY,
    ),
)
def test_run_workflow(
    data_management: DataManagement,
    process_executor: ProcessPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    stage_sizes = [1, 1, 3, 3, 1]
    data_size_bytes = 100
    sleep = 0.001

    stats = run_workflow(
        process_executor,
        file_store,
        data_management,
        stage_sizes=stage_sizes,
        data_size_bytes=data_size_bytes,
        sleep=sleep,
    )

    assert stats.workflow_makespan_s >= len(stage_sizes) * sleep


def test_runner(
    process_executor: ProcessPoolExecutor,
    file_store: Store[FileConnector],
    tmp_path: pathlib.Path,
) -> None:
    data_management = [
        DataManagement.NONE,
        DataManagement.DEFAULT_PROXY,
        DataManagement.OWNED_PROXY,
    ]
    data_sizes = [100, 1000]
    repeat = 5

    csv_file = str(tmp_path / 'data.csv')
    stats = WorkflowStats(
        executor='threadpool',
        connector='none',
        data_management='none',
        stage_sizes='1-1-3-1',
        data_size_bytes=100,
        task_sleep=0.001,
        workflow_start_timestamp=0,
        workflow_end_timestamp=1,
        workflow_makespan_s=1,
    )

    with mock.patch(
        'psbench.benchmarks.workflow.main.run_workflow',
        return_value=stats,
    ) as mock_run_workflow:
        runner(
            process_executor,
            file_store,
            data_management=data_management,
            stage_sizes=[1, 1, 3, 1],
            data_sizes=data_sizes,
            sleep=stats.task_sleep,
            repeat=repeat,
            csv_file=csv_file,
            memory_profile_interval=0.001,
        )

    runs = len(data_management) * repeat * len(data_sizes)
    assert mock_run_workflow.call_count == runs
    assert os.path.isfile(csv_file)


def test_runner_csv_error(
    process_executor: ProcessPoolExecutor,
    file_store: Store[FileConnector],
    tmp_path: pathlib.Path,
) -> None:
    with pytest.raises(ValueError, match='CSV log file should end with'):
        runner(
            process_executor,
            file_store,
            data_management=[DataManagement.NONE],
            stage_sizes=[1],
            data_sizes=[100],
            sleep=0.001,
            repeat=1,
            csv_file=str(tmp_path / 'bad-file'),
            memory_profile_interval=0.001,
        )


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--data-management',
        'none',
        '--stage-sizes',
        '1',
        '3',
        '1',
        '--data-sizes',
        '100',
        '--sleep',
        '0.01',
        '--csv-file',
        str(tmp_path / 'data.csv'),
        '--ps-backend',
        'file',
        '--ps-file-dir',
        str(tmp_path / 'dump'),
    ]

    with mock.patch(
        'psbench.benchmarks.workflow.main.runner',
    ), mock.patch(
        'psbench.proxystore.register_store',
    ), mock_globus_compute():
        main(args)
