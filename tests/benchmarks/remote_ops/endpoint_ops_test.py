from __future__ import annotations

import uuid
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from proxystore.endpoint.endpoint import Endpoint

import psbench.benchmarks.remote_ops.endpoint_ops as ops


@pytest_asyncio.fixture
async def endpoint() -> AsyncGenerator[Endpoint, None]:
    async with Endpoint('test-ep', uuid.uuid4()) as ep:
        yield ep


@pytest.mark.asyncio()
async def test_evict(endpoint: Endpoint) -> None:
    times = await ops.test_evict(endpoint, None, 2)
    assert len(times) == 2


@pytest.mark.asyncio()
async def test_exists(endpoint: Endpoint) -> None:
    times = await ops.test_exists(endpoint, None, 2)
    assert len(times) == 2


@pytest.mark.asyncio()
async def test_get(endpoint: Endpoint) -> None:
    times = await ops.test_get(endpoint, None, 100, 2)
    assert len(times) == 2


@pytest.mark.asyncio()
async def test_set(endpoint: Endpoint) -> None:
    times = await ops.test_set(endpoint, None, 100, 2)
    assert len(times) == 2
