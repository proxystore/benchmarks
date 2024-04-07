from __future__ import annotations

import argparse

from colmena.models import Result
from colmena.models import TimeSpans
from colmena.models import Timestamps

from psbench.benchmarks.colmena_rtt.config import BenchmarkMatrix
from psbench.benchmarks.colmena_rtt.config import RunResult


def test_from_result() -> None:
    timestamps = Timestamps(
        created=0,
        input_received=0,
        compute_started=0,
        compute_ended=0,
        result_sent=0,
        result_received=0,
        start_task_submission=0,
        task_received=0,
    )
    times = TimeSpans(
        running=0,
        serialize_inputs=0,
        deserialize_inputs=0,
        serialize_results=0,
        deserialize_results=0,
        async_resolve_proxies=0,
    )
    colmena_result = Result(
        inputs=((), {}),
        value=None,
        method='method',
        success=True,
        timestamp=timestamps,
        time=times,
    )

    result = RunResult.from_result(
        colmena_result,
        input_size_bytes=1,
        output_size_bytes=2,
        proxystore_backend='test',
    )

    assert result.task_id == colmena_result.task_id
    assert result.method == colmena_result.method
    assert result.success == colmena_result.success
    assert result.input_size_bytes == 1
    assert result.output_size_bytes == 2
    assert result.proxystore_backend == 'test'
    assert result.time_created == timestamps.created
    assert result.time_running == times.running


def test_benchmark_matrix_argparse() -> None:
    parser = argparse.ArgumentParser()
    BenchmarkMatrix.add_parser_group(parser)
    args = parser.parse_args(
        [
            '--input-size',
            '1',
            '2',
            '3',
            '--output-sizes',
            '4',
            '5',
            '6',
            '--task-sleep',
            '7',
            '--reuse-inputs',
        ],
    )
    matrix = BenchmarkMatrix.from_args(**vars(args))

    assert matrix.redis_host is None
    assert matrix.redis_port is None
    assert matrix.input_sizes == [1, 2, 3]
    assert matrix.output_sizes == [4, 5, 6]
    assert matrix.task_sleep == 7
    assert matrix.reuse_inputs


def test_benchmark_matrix_configs() -> None:
    matrix = BenchmarkMatrix(
        redis_host=None,
        redis_port=None,
        input_sizes=[1, 2, 3],
        output_sizes=[4, 5, 6],
        task_sleep=7,
        reuse_inputs=False,
    )

    configs = matrix.configs()
    assert len(configs) == 1

    for config in configs:
        assert config.input_sizes == matrix.input_sizes
        assert config.output_sizes == matrix.output_sizes
        assert config.task_sleep == matrix.task_sleep
        assert config.reuse_inputs == matrix.reuse_inputs
