from __future__ import annotations

import pathlib
import time

import pytest

from psbench.memory import SystemMemoryProfiler


def test_memory_profiler() -> None:
    profiler = SystemMemoryProfiler(0.001)
    profiler.start()

    time.sleep(0.01)

    profiler.stop()
    profiler.join(timeout=1.0)

    assert len(profiler.get_memory_log()) > 0


def test_memory_profiler_csv_logger(tmp_path: pathlib.Path) -> None:
    csv_file = tmp_path / 'memory.csv'
    profiler = SystemMemoryProfiler(0.001, csv_file)
    profiler.start()

    time.sleep(0.01)

    profiler.stop()
    profiler.join(timeout=1.0)

    with open(csv_file) as f:
        log = f.readlines()

    # Length 2 for header + at least one row
    assert len(log) >= 2


def test_memory_profiler_one_time_use() -> None:
    profiler = SystemMemoryProfiler(0.001)
    profiler.start()
    profiler.stop()
    profiler.join(timeout=1.0)

    with pytest.raises(RuntimeError, match='has already finished'):
        profiler.run()
