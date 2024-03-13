from __future__ import annotations

import argparse

from psbench.benchmarks.task_pipelining.config import BenchmarkMatrix
from psbench.benchmarks.task_pipelining.config import SubmissionMethod


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--submission-method',
            'pipelined-proxy-future',
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
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.submission_method == [
        SubmissionMethod.PIPELINED_PROXY_FUTURE,
    ]
    assert matrix.task_chain_length == 5
    assert matrix.task_data_bytes == [100, 1000]
    assert matrix.task_overhead_fractions == [0.1, 0.2]
    assert matrix.task_sleep == 2


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        submission_method=[
            SubmissionMethod.SEQUENTIAL_PROXY,
            SubmissionMethod.PIPELINED_PROXY_FUTURE,
        ],
        task_chain_length=5,
        task_data_bytes=[100, 1000, 10000],
        task_overhead_fractions=[0.1],
        task_sleep=2.0,
    )

    configs = matrix.configs()
    expected = (
        len(matrix.submission_method)
        * len(matrix.task_data_bytes)
        * len(matrix.task_overhead_fractions)
    )
    assert len(configs) == expected

    for config in configs:
        assert config.task_chain_length == matrix.task_chain_length
        assert config.task_sleep == matrix.task_sleep
