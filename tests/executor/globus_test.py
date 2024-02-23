from __future__ import annotations

import pytest

from psbench.executor.globus import GlobusComputeExecutor
from psbench.executor.protocol import Executor
from testing.globus_compute import MockExecutor


@pytest.fixture()
def mock_executor() -> MockExecutor:
    return MockExecutor()


def test_is_executor_protocol(mock_executor: MockExecutor) -> None:
    with GlobusComputeExecutor(mock_executor) as executor:
        assert isinstance(executor, Executor)


def test_submit_function(mock_executor: MockExecutor) -> None:
    with GlobusComputeExecutor(mock_executor) as executor:
        future = executor.submit(round, 1.75, ndigits=1)
        assert future.result() == 1.8
