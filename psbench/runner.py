from __future__ import annotations

import logging
import time
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

from pydantic import BaseModel

from psbench.benchmarks.protocol import Benchmark
from psbench.logging import TESTING_LOG_LEVEL
from psbench.results import CSVLogger

RunConfigT = TypeVar('RunConfigT', bound=BaseModel)
RunResultT = TypeVar('RunResultT', bound=NamedTuple)

logger = logging.getLogger(__name__)


def runner(
    benchmark: Benchmark[RunConfigT, RunResultT],
    configs: Sequence[RunConfigT],
    result_logger: CSVLogger[RunResultT],
    repeat: int = 1,
) -> None:
    logger.log(TESTING_LOG_LEVEL, f'Starting benchmark: {benchmark.name}')
    pretty_config = '\n'.join(
        f'- {k}: {v}' for k, v in benchmark.config().items()
    )
    logger.log(TESTING_LOG_LEVEL, f'Benchmark config:\n{pretty_config}')

    benchmark_start = time.perf_counter()

    with benchmark:
        for config in configs:
            logger.log(
                TESTING_LOG_LEVEL,
                f'Starting run config (repeat={repeat}): {config}',
            )

            for i in range(repeat):
                run_start = time.perf_counter()
                result = benchmark.run(config)
                run_end = time.perf_counter()

                result_logger.log(result)
                logger.log(
                    TESTING_LOG_LEVEL,
                    f'Run {i+1}/{repeat} completed in {run_end - run_start} s',
                )

    benchmark_end = time.perf_counter()

    logger.log(
        TESTING_LOG_LEVEL,
        f'Benchmark completed: {benchmark_end - benchmark_start:.3f} s',
    )
