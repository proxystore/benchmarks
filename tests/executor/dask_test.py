from __future__ import annotations

import pytest
from dask.distributed import Client

from psbench.executor.dask import DaskExecutor
from psbench.executor.protocol import Executor


@pytest.fixture()
def local_client() -> Client:
    client = Client(
        n_workers=1,
        processes=False,
        dashboard_address=None,
    )
    return client


def test_is_executor_protocol(local_client: Client) -> None:
    with DaskExecutor(local_client) as executor:
        assert isinstance(executor, Executor)


def test_submit_function(local_client: Client) -> None:
    with DaskExecutor(local_client) as executor:
        future = executor.submit(round, 1.75, ndigits=1)
        assert future.result() == 1.8
