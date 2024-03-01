from __future__ import annotations

import logging
import os
import time
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

from pydantic import BaseModel

from psbench.benchmarks.protocol import Benchmark
from psbench.config import RunConfig
from psbench.csv import CSVLogger
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL

ConfigT = TypeVar('ConfigT', bound=BaseModel)
ResultT = TypeVar('ResultT', bound=NamedTuple)

logger = logging.getLogger(__name__)


def runner(
    benchmark: Benchmark[ConfigT, ResultT],
    configs: Sequence[ConfigT],
    run_config: RunConfig,
) -> None:
    log_file = os.path.join(run_config.run_dir, run_config.log_file)
    csv_file = os.path.join(run_config.run_dir, run_config.csv_file)

    init_logging(log_file, run_config.log_level, force=True)
    csv_logger = CSVLogger(csv_file, benchmark.result_type)

    logger.log(TESTING_LOG_LEVEL, f'Starting benchmark: {benchmark.name}')
    pretty_config = '\n  '.join(
        f'{k}: {v}' for k, v in benchmark.config().items()
    )
    logger.log(TESTING_LOG_LEVEL, f'Global benchmark config:\n{pretty_config}')

    benchmark_start = time.perf_counter()

    with benchmark:
        for config in configs:
            logger.log(
                TESTING_LOG_LEVEL,
                f'Starting benchmark config (runs={run_config.repeat}): '
                f'{config}',
            )

            for i in range(run_config.repeat):
                run_start = time.perf_counter()
                result = benchmark.run(config)
                run_end = time.perf_counter()

                csv_logger.log(result)
                logger.log(
                    TESTING_LOG_LEVEL,
                    f'Run {i}/{run_config.repeat} completed in '
                    f'{run_end - run_start} s',
                )

    benchmark_end = time.perf_counter()

    csv_logger.close()

    logger.log(
        TESTING_LOG_LEVEL,
        f'Benchmark completed: {benchmark_start - benchmark_end:.3f} s',
    )
