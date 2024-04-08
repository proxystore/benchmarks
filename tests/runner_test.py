from __future__ import annotations

from psbench.results import BasicResultLogger
from psbench.runner import runner
from testing.benchmark import MockBenchmark
from testing.benchmark import MockRunConfig
from testing.benchmark import MockRunResult


def test_runner() -> None:
    configs = [MockRunConfig(param=i) for i in range(5)]
    repeat = 3

    with BasicResultLogger(MockRunResult) as logger:
        with MockBenchmark() as benchmark:
            runner(benchmark, configs, logger, repeat=repeat)

        assert len(logger.results) == repeat * len(configs)
