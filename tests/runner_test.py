from __future__ import annotations

import pathlib

from psbench.results import CSVLogger
from psbench.runner import runner
from testing.benchmark import MockBenchmark
from testing.benchmark import MockRunConfig
from testing.benchmark import MockRunResult


def test_runner(tmp_path: pathlib.Path) -> None:
    benchmark = MockBenchmark()
    configs = [MockRunConfig(param=i) for i in range(5)]
    csv_file = str(tmp_path / 'results.csv')
    repeat = 3

    with CSVLogger(csv_file, MockRunResult) as csv_logger:
        runner(benchmark, configs, csv_logger, repeat=repeat)

    with open(csv_file) as f:
        runs = len(f.readlines()) - 1

    assert runs == repeat * len(configs)
