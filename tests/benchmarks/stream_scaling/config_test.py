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
            '--max-workers',
            '7',
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
    assert matrix.max_workers == 7
    assert matrix.stream_method == ['default']
    assert matrix.task_count == 5
    assert matrix.task_sleep == 6


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        data_size_bytes=[1, 2, 3],
        max_workers=7,
        stream_method=['default', 'proxy'],
        task_count=5,
        task_sleep=6,
        adios_file='/tmp/adios-stream',
    )

    configs = matrix.configs()
    expected = len(matrix.data_size_bytes) * len(matrix.stream_method)
    assert len(configs) == expected

    for config in configs:
        assert config.max_workers == matrix.max_workers
        assert config.task_count == matrix.task_count
        assert config.task_sleep == matrix.task_sleep
