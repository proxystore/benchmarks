from __future__ import annotations

import time
from typing import Generator
from unittest import mock

import pytest
from proxystore.store.endpoint import EndpointStore

from psbench.benchmarks.endpoint_qps.routes import endpoint_test
from psbench.benchmarks.endpoint_qps.routes import evict_test
from psbench.benchmarks.endpoint_qps.routes import exists_test
from psbench.benchmarks.endpoint_qps.routes import get_test
from psbench.benchmarks.endpoint_qps.routes import set_test

PAYLOAD = 100
QUERIES = 10


@pytest.fixture()
def endpoint_store() -> Generator[EndpointStore, None, None]:
    with mock.patch('proxystore.store.endpoint.EndpointStore'):
        from proxystore.store import endpoint

        yield endpoint.EndpointStore('name', endpoints=['UUID'])


def test_endpoint_route(endpoint_store) -> None:
    mock_request = mock.MagicMock()
    mock_request.status_code = 200
    with mock.patch('requests.get', return_value=mock_request):
        stats = endpoint_test(endpoint_store, 0, QUERIES)
        stats = endpoint_test(endpoint_store, 0, QUERIES, time.time())

    assert stats.queries == QUERIES
    assert stats.total_elapsed_ms > 0


def test_evict_route(endpoint_store) -> None:
    stats = evict_test(endpoint_store, 0, QUERIES)
    stats = evict_test(endpoint_store, 0, QUERIES, time.time())

    assert stats.queries == QUERIES
    assert stats.total_elapsed_ms > 0


def test_exists_route(endpoint_store) -> None:
    stats = exists_test(endpoint_store, 0, QUERIES)
    stats = exists_test(endpoint_store, 0, QUERIES, time.time())

    assert stats.queries == QUERIES
    assert stats.total_elapsed_ms > 0


def test_get_route(endpoint_store) -> None:
    stats = get_test(endpoint_store, 0, QUERIES, PAYLOAD)
    stats = get_test(endpoint_store, 0, QUERIES, PAYLOAD, time.time())

    assert stats.queries == QUERIES
    assert stats.total_elapsed_ms > 0


def test_set_route(endpoint_store) -> None:
    stats = set_test(endpoint_store, 0, QUERIES, PAYLOAD)
    stats = set_test(endpoint_store, 0, QUERIES, PAYLOAD, time.time())

    assert stats.queries == QUERIES
    assert stats.total_elapsed_ms > 0
