from __future__ import annotations

import pathlib

import pytest
from parsl.config import Config
from parsl.executors import ThreadPoolExecutor

from psbench.executor.parsl import ParslExecutor
from psbench.executor.protocol import Executor


@pytest.fixture()
def config(tmp_path: pathlib.Path) -> Config:
    return Config(
        executors=[ThreadPoolExecutor(max_threads=1)],
        run_dir=str(tmp_path / 'runs'),
        strategy='none',
    )


def test_is_executor_protocol(config: Config) -> None:
    with ParslExecutor(config) as executor:
        assert isinstance(executor, Executor)


def test_submit_function(config: Config) -> None:
    with ParslExecutor(config) as executor:
        future = executor.submit(round, 1.75, ndigits=1)
        assert future.result() == 1.8

        future = executor.submit(round, 1.75, ndigits=0)
        assert future.result() == 2
