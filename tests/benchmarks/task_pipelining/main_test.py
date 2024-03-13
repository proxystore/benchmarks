from __future__ import annotations

import pathlib

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.store.base import Store

from psbench.benchmarks.task_pipelining.config import RunConfig
from psbench.benchmarks.task_pipelining.main import Benchmark
from psbench.benchmarks.task_pipelining.main import run_pipelined_workflow
from psbench.benchmarks.task_pipelining.main import run_sequential_workflow
from psbench.benchmarks.task_pipelining.main import SubmissionMethod
from testing.executor import ThreadPoolExecutor


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

    expected_time_s = task_chain_length * (task_overhead_fraction * task_sleep)
    assert stats.workflow_makespan_ms > (expected_time_s * 1000)


@pytest.mark.parametrize(
    'submission_method',
    (
        SubmissionMethod.SEQUENTIAL_NO_PROXY,
        SubmissionMethod.SEQUENTIAL_PROXY,
        SubmissionMethod.PIPELINED_PROXY_FUTURE,
    ),
)
def test_benchmark_run(
    submission_method: SubmissionMethod,
    tmp_path: pathlib.Path,
    thread_executor: ThreadPoolExecutor,
    file_store: Store[FileConnector],
) -> None:
    config = RunConfig(
        submission_method=submission_method,
        task_chain_length=1,
        task_data_bytes=100,
        task_overhead_fraction=0.1,
        task_sleep=0.001,
    )

    with Benchmark(thread_executor, file_store) as benchmark:
        benchmark.config()
        result = benchmark.run(config)

    assert result.submission_method == config.submission_method.value
    assert result.workflow_makespan_ms > config.task_sleep
