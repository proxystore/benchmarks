from __future__ import annotations

import argparse

from psbench.benchmarks.task_rtt.config import BenchmarkMatrix


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--input-sizes',
            '1',
            '2',
            '--output-sizes',
            '3',
            '4',
            '5',
            '--task-sleep',
            '6',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.input_sizes == [1, 2]
    assert matrix.output_sizes == [3, 4, 5]
    assert matrix.sleep == 6


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        input_sizes=[1, 2],
        output_sizes=[2, 3, 4],
        sleep=6,
    )

    assert len(matrix.configs()) == 6
