from __future__ import annotations

import pathlib
from typing import Any
from typing import Generator
from unittest import mock

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store import store_registration
from proxystore.store.base import Store

from psbench.benchmarks.task_pipelining.main import main
from psbench.benchmarks.task_pipelining.main import run_pipelined_workflow
from psbench.benchmarks.task_pipelining.main import run_sequential_workflow
from psbench.benchmarks.task_pipelining.main import runner
from psbench.executor.protocol import Executor
from testing.executor import ThreadPoolExecutor
from testing.globus_compute import mock_globus_compute


@pytest.fixture()
def executor() -> Generator[ThreadPoolExecutor, None, None]:
    with ThreadPoolExecutor(2) as executor:
        yield executor


@pytest.fixture()
def store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store(
        'task-pipelining-fixture',
        FileConnector(str(tmp_path / 'store')),
    ) as store:
        with store_registration(store):
            yield store


def test_run_sequential_workflow(
    executor: Executor,
    store: Store[Any],
) -> None:
    task_chain_length = 5
    task_data_bytes = 100
    task_overhead_sleep = 0.001
    task_compute_sleep = 0.01

    stats = run_sequential_workflow(
        executor,
        store,
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_sleep=task_overhead_sleep,
        task_compute_sleep=task_compute_sleep,
    )

    expected_time_s = (
        task_chain_length * task_overhead_sleep * task_compute_sleep
    )
    assert stats.workflow_makespan_ms > (expected_time_s * 1000)


def test_run_pipelined_workflow(
    executor: Executor,
    store: Store[Any],
) -> None:
    task_chain_length = 5
    task_data_bytes = 100
    task_overhead_sleep = 0.001
    task_compute_sleep = 0.001
    task_submit_sleep = 0.001

    stats = run_pipelined_workflow(
        executor,
        store,
        task_chain_length=task_chain_length,
        task_data_bytes=task_data_bytes,
        task_overhead_sleep=task_overhead_sleep,
        task_compute_sleep=task_compute_sleep,
        task_submit_sleep=task_submit_sleep,
    )

    expected_time_s = task_chain_length * task_compute_sleep
    assert stats.workflow_makespan_ms > (expected_time_s * 1000)


@pytest.mark.parametrize('use_csv', (True, False))
def test_runner(
    use_csv: bool,
    tmp_path: pathlib.Path,
    executor: Executor,
    store: Store[Any],
) -> None:
    task_data_bytes = [1, 10]
    repeat = 3
    csv_file = str(tmp_path / 'data.csv') if use_csv else None

    runner(
        executor,
        store,
        task_chain_length=1,
        task_data_bytes=task_data_bytes,
        task_overhead_sleep=0,
        task_compute_sleep=0,
        task_submit_sleep=0,
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
        '--task-overhead-sleep',
        '1',
        '--task-compute-sleep',
        '2',
        '--task-submit-sleep',
        '3',
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
