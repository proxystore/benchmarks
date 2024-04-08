from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Sequence

from psbench.benchmarks.task_rtt.config import BenchmarkMatrix
from psbench.benchmarks.task_rtt.main import Benchmark
from psbench.config import ExecutorConfig
from psbench.config import GeneralConfig
from psbench.config import IPFSConfig
from psbench.config import StoreConfig
from psbench.logging import BENCH_LOG_LEVEL
from psbench.logging import init_logging
from psbench.results import CSVResultLogger
from psbench.runner import runner

benchmark_name = Benchmark.name.lower().replace(' ', '-')
logger = logging.getLogger(f'run.{benchmark_name}')


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Task round-trip time benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    BenchmarkMatrix.add_parser_group(parser)
    ExecutorConfig.add_parser_group(parser, required=True, argv=argv)
    StoreConfig.add_parser_group(parser, required=False, argv=argv)
    IPFSConfig.add_parser_group(parser, required=False, argv=argv)
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
    ipfs_config = IPFSConfig.from_args(**args)
    logger.log(BENCH_LOG_LEVEL, 'All configurations loaded')

    benchmark = Benchmark(
        executor=executor_config.get_executor(),
        store=store_config.get_store(),
        use_ipfs=ipfs_config.use_ipfs,
        ipfs_local_dir=ipfs_config.local_dir,
        ipfs_remote_dir=ipfs_config.remote_dir,
    )
    logger.log(BENCH_LOG_LEVEL, 'Benchmark initialized')

    csv_file = os.path.join(general_config.run_dir, general_config.csv_file)
    with CSVResultLogger(csv_file, benchmark.result_type) as csv_logger:
        runner(
            benchmark,
            matrix.configs(),
            csv_logger,
            repeat=general_config.repeat,
        )

    logger.log(
        BENCH_LOG_LEVEL,
        f'All logs and results saved to: {general_config.run_dir}',
    )

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
