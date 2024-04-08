from __future__ import annotations

import logging
import statistics
import time
from typing import Sequence
from typing import TypeVar

from pydantic import BaseModel

from psbench.benchmarks.protocol import Benchmark
from psbench.logging import BENCH_LOG_LEVEL
from psbench.results import ResultLogger

RunConfigT = TypeVar('RunConfigT', bound=BaseModel)
RunResultT = TypeVar('RunResultT', bound=BaseModel)

logger = logging.getLogger(__name__)


def runner(
    benchmark: Benchmark[RunConfigT, RunResultT],
    configs: Sequence[RunConfigT],
    result_logger: ResultLogger[RunResultT],
    repeat: int = 1,
) -> None:
    logger.log(BENCH_LOG_LEVEL, f'Starting benchmark: {benchmark.name}')
    pretty_config = '\n'.join(
        f'- {k}: {v}' for k, v in benchmark.config().items()
    )
    logger.log(BENCH_LOG_LEVEL, f'Benchmark config:\n{pretty_config}')

    benchmark_start = time.perf_counter()

    for config in configs:
        logger.log(
            BENCH_LOG_LEVEL,
            f'Starting run config (repeat={repeat}): {config}',
        )

        run_times: list[float] = []
        for i in range(repeat):
            run_start = time.perf_counter()
            result = benchmark.run(config)
            run_time = time.perf_counter() - run_start
            run_times.append(run_time)

            results = [result] if not isinstance(result, Sequence) else result
            for result in results:
                result_logger.log(result)

            logger.log(
                BENCH_LOG_LEVEL,
                f'Run {i+1}/{repeat} completed in {run_time:.3f}s',
            )

        avg_run_time = sum(run_times) / len(run_times)
        std_run_time = (
            0.0 if len(run_times) <= 1 else statistics.stdev(run_times)
        )
        logger.log(
            BENCH_LOG_LEVEL,
            f'Average run time: {avg_run_time:.3f} ± {std_run_time:.3f}s',
        )

    benchmark_end = time.perf_counter()

    logger.log(
        BENCH_LOG_LEVEL,
        f'Benchmark completed: {benchmark_end - benchmark_start:.3f} s',
    )
