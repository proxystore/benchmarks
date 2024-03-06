from __future__ import annotations

import pathlib
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

from psbench.benchmarks.task_pipelining.main import main
from psbench.benchmarks.task_pipelining.main import run_pipelined_workflow
from psbench.benchmarks.task_pipelining.main import run_sequential_workflow
from psbench.benchmarks.task_pipelining.main import runner
from testing.executor import ThreadPoolExecutor
from testing.globus_compute import mock_globus_compute


def test_run_sequential_workflow(
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    task_chain_length = 5
    task_data_bytes = 100
    task_overhead_fraction = 0.1
    task_sleep = 0.01

    stats = run_sequential_workflow(
        thread_executor,
        file_store,
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
    )

    expected_time_s = task_chain_length * task_sleep
    assert stats.workflow_makespan_ms > (expected_time_s * 1000)


def test_run_pipelined_workflow(
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    task_chain_length = 5
    task_data_bytes = 100
    task_overhead_fraction = 0.1
    task_sleep = 0.01

    stats = run_pipelined_workflow(
        thread_executor,
        file_store,
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_fraction=task_overhead_fraction,
        task_sleep=task_sleep,
    )

    expected_time_s = task_chain_length * task_sleep
    assert stats.workflow_makespan_ms > (expected_time_s * 1000)


@pytest.mark.parametrize('use_csv', (True, False))
def test_runner(
    use_csv: bool,
    tmp_path: pathlib.Path,
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    task_data_bytes = [1, 10]
    repeat = 3
    csv_file = str(tmp_path / 'data.csv') if use_csv else None

    runner(
        thread_executor,
        file_store,
        task_chain_length=1,
        task_data_bytes=task_data_bytes,
        task_overhead_fractions=[0.1],
        task_sleep=1.0,
        repeat=repeat,
        csv_file=csv_file,
    )

    if csv_file is not None:
        with open(csv_file) as f:
            lines = f.readlines()
        assert len(lines) == (2 * len(task_data_bytes) * repeat) + 1


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--task-chain-length',
        '5',
        '--task-data-bytes',
        '100',
        '1000',
        '--task-overhead-fractions',
        '0.1',
        '0.2',
        '--task-sleep',
        '2',
        '--ps-backend',
        'FILE',
        '--ps-file-dir',
        str(tmp_path),
    ]

    with mock.patch(
        'psbench.benchmarks.task_pipelining.main.runner',
    ), mock.patch(
        'psbench.proxystore.register_store',
    ), mock_globus_compute():
        main(args)
