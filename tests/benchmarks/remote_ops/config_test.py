from __future__ import annotations

import argparse

from psbench.benchmarks.remote_ops.config import BenchmarkMatrix


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            'endpoint',
            '--endpoint',
            'UUID',
            '--ops',
            'get',
            '--relay-server',
            'ws://localhost',
            '--payload-sizes',
            '100',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.backend == 'endpoint'
    assert matrix.endpoint == 'UUID'
    assert matrix.redis_host is None
    assert matrix.redis_port is None
    assert matrix.ops == ['get']
    assert matrix.relay_server == 'ws://localhost'
    assert matrix.payload_sizes == [100]
    assert matrix.use_uvloop


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        backend='redis',
        endpoint=None,
        redis_host='localhost',
        redis_port=0,
        ops=['get', 'set'],
        payload_sizes=[1, 2],
        relay_server=None,
        repeat=3,
        use_uvloop=False,
    )

    (config,) = matrix.configs()
    assert matrix.backend == config.backend
    assert matrix.ops == config.ops
    assert matrix.payload_sizes == config.payload_sizes
    assert matrix.repeat == config.repeat
