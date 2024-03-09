from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Sequence

from psbench.benchmarks.workflow_memory.config import BenchmarkMatrix
from psbench.benchmarks.workflow_memory.main import Benchmark
from psbench.config import ExecutorConfig
from psbench.config import GeneralConfig
from psbench.config import StoreConfig
from psbench.logging import BENCH_LOG_LEVEL
from psbench.logging import init_logging
from psbench.memory import SystemMemoryProfiler
from psbench.results import CSVResultLogger
from psbench.runner import runner

benchmark_name = Benchmark.name.lower().replace(' ', '-')
logger = logging.getLogger(f'run.{benchmark_name}')


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Workflow memory usage simulation.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    BenchmarkMatrix.add_parser_group(parser)
    ExecutorConfig.add_parser_group(parser, required=True, argv=argv)
    StoreConfig.add_parser_group(parser, required=True, argv=argv)
    GeneralConfig.add_parser_group(parser)

    args = vars(parser.parse_args(argv))

    general_config = GeneralConfig.from_args(**args)
    general_config.run_dir = os.path.join(
        general_config.run_dir,
        f'{benchmark_name}-{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}',
    )

    log_file = os.path.join(general_config.run_dir, general_config.log_file)
    init_logging(
        log_file,
        general_config.log_level,
        general_config.log_file_level,
        force=True,
    )

    matrix = BenchmarkMatrix.from_args(**args)
    executor_config = ExecutorConfig.from_args(
        parsl_run_dir=os.path.join(general_config.run_dir, 'parsl_runinfo'),
        **args,
    )
    store_config = StoreConfig.from_args(**args)
    logger.log(BENCH_LOG_LEVEL, 'All configurations loaded')

    # We'll let the Benchmark object handle entering and exit these context
    # managers.
    executor = executor_config.get_executor()
    # Caching will throw off the memory usage as it will grow across
    # distinct runs.
    store = store_config.get_store(cache_size=0)
    assert store is not None

    benchmark = Benchmark(executor, store)
    logger.log(BENCH_LOG_LEVEL, 'Benchmark initialized')

    csv_file = os.path.join(general_config.run_dir, general_config.csv_file)
    memory_file = csv_file.replace('.csv', '-memory.csv')

    memory_profiler = SystemMemoryProfiler(
        matrix.memory_profile_interval,
        memory_file,
    )
    memory_profiler.start()

    with CSVResultLogger(csv_file, benchmark.result_type) as csv_logger:
        runner(
            benchmark,
            matrix.configs(),
            csv_logger,
            repeat=general_config.repeat,
        )

    memory_profiler.stop()
    memory_profiler.join(timeout=5.0)
    logger.log(BENCH_LOG_LEVEL, f'Memory profile data saved: {memory_file}')

    logger.log(
        BENCH_LOG_LEVEL,
        f'All logs and results saved to: {general_config.run_dir}',
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
