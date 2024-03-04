from __future__ import annotations

import argparse

from psbench.benchmarks.stream_scaling.config import BenchmarkMatrix


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--data-size-bytes',
            '1',
            '2',
            '3',
            '--stream-method',
            'default',
            '--task-count',
            '5',
            '--task-sleep',
            '6',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.data_size_bytes == [1, 2, 3]
    assert matrix.stream_method == ['default']
    assert matrix.task_count == 5
    assert matrix.task_sleep == 6


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        data_size_bytes=[1, 2, 3],
        stream_method=['default', 'proxy'],
        task_count=5,
        task_sleep=6,
    )

    configs = matrix.configs()
    expected = len(matrix.data_size_bytes) * len(matrix.stream_method)
    assert len(configs) == expected

    for config in configs:
        assert config.task_count == matrix.task_count
        assert config.task_sleep == matrix.task_sleep
