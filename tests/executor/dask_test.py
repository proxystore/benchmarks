from __future__ import annotations

from typing import Generator

import pytest
from dask.distributed import Client
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import store_registration
from proxystore.store.base import Store

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


@pytest.fixture()
def store() -> Generator[Store[LocalConnector], None, None]:
    with Store('dask-executor-fixture', LocalConnector()) as store:
        with store_registration(store):
            yield store


def test_is_executor_protocol(local_client: Client) -> None:
    with DaskExecutor(local_client) as executor:
        assert isinstance(executor, Executor)


def test_submit_function(local_client: Client) -> None:
    with DaskExecutor(local_client) as executor:
        future = executor.submit(round, 1.75, ndigits=1)
        assert future.result() == 1.8


def proxy_function(proxy: Proxy[int]) -> Proxy[int]:
    from proxystore.store import get_store

    result = proxy * 2

    return get_store(proxy).proxy(result)


def test_submit_proxies(
    local_client: Client,
    store: Store[LocalConnector],
) -> None:
    value = store.proxy(1)

    with DaskExecutor(local_client) as executor:
        future = executor.submit(proxy_function, value)
        assert future.result() == 2.0
