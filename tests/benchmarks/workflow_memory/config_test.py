from __future__ import annotations

import argparse

from psbench.benchmarks.workflow_memory.config import BenchmarkMatrix
from psbench.benchmarks.workflow_memory.config import DataManagement


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--data-management',
            'none',
            'owned-proxy',
            '--stage-sizes',
            '1',
            '3',
            '1',
            '--data-sizes-bytes',
            '100',
            '--task-sleep',
            '0.01',
            '--memory-profile-interval',
            '0.001',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.data_management == [
        DataManagement.NONE,
        DataManagement.OWNED_PROXY,
    ]
    assert matrix.stage_sizes == [1, 3, 1]
    assert matrix.data_sizes_bytes == [100]
    assert matrix.task_sleep == 0.01
    assert matrix.memory_profile_interval == 0.001


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        data_management=[
            DataManagement.NONE,
            DataManagement.DEFAULT_PROXY,
            DataManagement.OWNED_PROXY,
        ],
        stage_sizes=[1, 3, 1],
        data_sizes_bytes=[100, 1000],
        task_sleep=0.01,
        memory_profile_interval=0.001,
    )

    configs = matrix.configs()
    expected = len(matrix.data_management) * len(matrix.data_sizes_bytes)
    assert len(configs) == expected

    for config in configs:
        assert config.task_sleep == matrix.task_sleep
