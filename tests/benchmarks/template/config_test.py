from __future__ import annotations

import argparse

from psbench.benchmarks.template.config import BenchmarkMatrix


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(['--names', 'a', 'b', 'c'])
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.names == ['a', 'b', 'c']


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(names=['a', 'b', 'c'])

    assert len(matrix.configs()) == 3
