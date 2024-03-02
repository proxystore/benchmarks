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
            '--producer-sleep',
            '4',
            '--task-count',
            '5',
            '--task-sleep',
            '6',
            '--workers',
            '7',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.data_size_bytes == [1, 2, 3]
    assert matrix.producer_sleep == 4
    assert matrix.task_count == 5
    assert matrix.task_sleep == 6
    assert matrix.workers == 7


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        data_size_bytes=[1, 2, 3],
        producer_sleep=4,
        task_count=5,
        task_sleep=6,
        workers=7,
    )

    configs = matrix.configs()
    assert len(configs) == len(matrix.data_size_bytes)

    for config in configs:
        assert config.producer_sleep == matrix.producer_sleep
        assert config.task_count == matrix.task_count
        assert config.task_sleep == matrix.task_sleep
        assert config.workers == config.workers
