from __future__ import annotations

import pathlib
import time

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.store import unregister_store

from psbench import ipfs
from psbench.benchmarks.task_rtt.tasks import pong
from psbench.benchmarks.task_rtt.tasks import pong_ipfs
from psbench.benchmarks.task_rtt.tasks import pong_proxy
from testing.ipfs import mock_ipfs


def test_pong() -> None:
    start = time.perf_counter_ns()
    res = pong(b'abcd', result_size=10, sleep=0.01)
    end = time.perf_counter_ns()

    assert len(res) == 10
    assert isinstance(res, bytes)
    assert (end - start) / 1e9 >= 0.01


def test_pong_ipfs(tmp_path: pathlib.Path):
    with mock_ipfs():
        cid = ipfs.add_data(b'data', tmp_path / 'data')
        start = time.perf_counter_ns()
        res = pong_ipfs(cid, str(tmp_path), result_size=10, sleep=0.01)
        end = time.perf_counter_ns()
        assert res is not None
        data = ipfs.get_data(res)

        assert len(data) == 10
        assert (end - start) / 1e9 >= 0.01

        assert pong_ipfs(cid, str(tmp_path), result_size=0) is None


def test_pong_proxy() -> None:
    store = Store('pong-proxy-stats-store', LocalConnector())
    register_store(store)
    input_data: Proxy[bytes] = store.proxy(b'abcd')

    start = time.perf_counter_ns()
    result_data, stats = pong_proxy(input_data, result_size=10, sleep=0.01)
    end = time.perf_counter_ns()

    assert stats is None
    assert isinstance(result_data, Proxy)
    assert isinstance(result_data, bytes)
    assert len(result_data) == 10
    assert (end - start) / 1e9 >= 0.01
    unregister_store(store)


def test_pong_proxy_stats() -> None:
    store = Store('pong-proxy-stats-store', LocalConnector(), metrics=True)
    register_store(store)
    input_data: Proxy[bytes] = store.proxy(b'abcd')
    _, stats = pong_proxy(input_data, result_size=10)
    assert stats is not None
    assert stats.input_get_ms is not None
    assert stats.input_get_ms > 0
    assert stats.input_resolve_ms is not None
    assert stats.input_resolve_ms > 0
    assert stats.output_put_ms is not None
    assert stats.output_put_ms > 0
    assert stats.output_proxy_ms is not None
    assert stats.output_proxy_ms > 0
    unregister_store(store)


def test_pong_proxy_store_unknown() -> None:
    # TODO: change to UnknownStoreError when ProxyStore version is >0.3.3
    with pytest.raises(AssertionError):
        pong_proxy(b'abcd', result_size=1)
