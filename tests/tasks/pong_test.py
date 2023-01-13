from __future__ import annotations

import time

import pytest
from proxystore.proxy import Proxy
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.local import LocalStore

from psbench.tasks.pong import pong
from psbench.tasks.pong import pong_proxy


def test_pong() -> None:
    start = time.perf_counter_ns()
    res = pong(b'abcd', result_size=10, sleep=0.01)
    end = time.perf_counter_ns()

    assert len(res) == 10
    assert isinstance(res, bytes)
    assert (end - start) / 1e9 >= 0.01


def test_pong_proxy() -> None:
    store = LocalStore(name='pong-proxy-store')
    register_store(store)
    input_data: Proxy[bytes] = store.proxy(b'abcd')

    start = time.perf_counter_ns()
    result_data, stats = pong_proxy(input_data, result_size=10, sleep=0.01)
    end = time.perf_counter_ns()

    assert stats is None
    assert isinstance(result_data, Proxy) and isinstance(result_data, bytes)
    assert len(result_data) == 10
    assert (end - start) / 1e9 >= 0.01
    unregister_store(store)


def test_pong_proxy_stats() -> None:
    store = LocalStore(name='pong-proxy-stats-store', stats=True)
    register_store(store)
    input_data: Proxy[bytes] = store.proxy(b'abcd')
    _, stats = pong_proxy(input_data, result_size=10)
    assert stats is not None
    assert stats.input_get_ms is not None and stats.input_get_ms > 0
    assert stats.input_resolve_ms is not None and stats.input_resolve_ms > 0
    assert stats.output_set_ms is not None and stats.output_set_ms > 0
    assert stats.output_proxy_ms is not None and stats.output_proxy_ms > 0
    unregister_store(store)


def test_pong_proxy_store_unknown() -> None:
    # TODO: change to UnknownStoreError when ProxyStore version is >0.3.3
    with pytest.raises(AssertionError):
        pong_proxy(b'abcd', result_size=1)
