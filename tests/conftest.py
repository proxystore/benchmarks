from __future__ import annotations

from collections.abc import Generator

import proxystore
import pytest

from testing.fixtures import file_store
from testing.fixtures import local_store
from testing.fixtures import process_executor
from testing.fixtures import thread_executor


@pytest.fixture(autouse=True)
def _verify_no_registered_stores() -> Generator[None, None, None]:
    yield

    if len(proxystore.store._stores) > 0:  # pragma: no cover
        raise RuntimeError(
            'Test left at least one store registered: '
            f'{tuple(proxystore.store._stores.keys())}.',
        )
