from __future__ import annotations

import time

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.proxy import Proxy
from proxystore.store.base import Store

from psbench.benchmarks.workflow_memory.config import DataManagement
from psbench.benchmarks.workflow_memory.config import RunConfig
from psbench.benchmarks.workflow_memory.main import Benchmark
from psbench.benchmarks.workflow_memory.main import task_no_proxy
from psbench.benchmarks.workflow_memory.main import task_proxy
from psbench.benchmarks.workflow_memory.main import validate_workflow
from psbench.utils import randbytes
from testing.executor import ProcessPoolExecutor


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
        DataManagement.MANUAL_PROXY,
        DataManagement.OWNED_PROXY,
    ),
)
def test_benchmark_run_workflow(
    data_management: DataManagement,
    process_executor: ProcessPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    config = RunConfig(
        data_management=data_management,
        stage_task_counts=[1, 1, 3, 1],
        stage_bytes_sizes=[100, 100, 100, 100, 100],
        stage_repeat=1,
        task_sleep=0.001,
    )

    with Benchmark(process_executor, file_store) as benchmark:
        benchmark.config()

        result = benchmark.run(config)

    min_makespan = config.task_sleep * len(config.stage_task_counts)
    assert result.workflow_makespan_s > min_makespan


def test_benchmark_run_workflow_mismatched_sizes(
    process_executor: ProcessPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    config = RunConfig(
        data_management=DataManagement.NONE,
        stage_task_counts=[1, 1, 3, 1],
        stage_bytes_sizes=[100, 100, 100, 100],
        stage_repeat=1,
        task_sleep=0.001,
    )

    with Benchmark(process_executor, file_store) as benchmark:
        with pytest.raises(ValueError, match='Length of'):
            benchmark.run(config)
