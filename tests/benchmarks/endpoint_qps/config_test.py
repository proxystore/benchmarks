from __future__ import annotations

import argparse

from psbench.benchmarks.endpoint_qps.config import BenchmarkMatrix


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            'UUID',
            '--routes',
            'GET',
            '--payload-sizes',
            '1',
            '2',
            '--workers',
            '3',
            '4',
            '--sleep',
            '5',
            '6',
            '--queries',
            '7',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.endpoint == 'UUID'
    assert matrix.routes == ['GET']
    assert matrix.payload_size_bytes == [1, 2]
    assert matrix.workers == [3, 4]
    assert matrix.sleep_seconds == [5, 6]
    assert matrix.total_queries == 7


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        endpoint='UUID',
        routes=['GET'],
        payload_size_bytes=[1, 2],
        workers=[3, 4],
        sleep_seconds=[5, 6],
        total_queries=7,
    )

    configs = matrix.configs()
    assert len(configs) == (1 * 2 * 2 * 2)
    for config in configs:
        assert config.endpoint == matrix.endpoint
