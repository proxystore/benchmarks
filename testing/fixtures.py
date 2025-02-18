from __future__ import annotations

import pathlib
from collections.abc import Generator
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

import pytest
from proxystore.connectors.file import FileConnector
from proxystore.connectors.local import LocalConnector
from proxystore.store import store_registration
from proxystore.store.base import Store


@pytest.fixture
def process_executor() -> Generator[ProcessPoolExecutor, None, None]:
    with ProcessPoolExecutor(4) as executor:
        yield executor


@pytest.fixture
def thread_executor() -> Generator[ThreadPoolExecutor, None, None]:
    with ThreadPoolExecutor(4) as executor:
        yield executor


@pytest.fixture
def file_store(
    tmp_path: pathlib.Path,
) -> Generator[Store[FileConnector], None, None]:
    with Store(
        'file-store-fixture',
        FileConnector(str(tmp_path / 'store')),
        metrics=True,
        populate_target=False,
    ) as store:
        with store_registration(store):
            yield store


@pytest.fixture
def local_store() -> Generator[Store[LocalConnector], None, None]:
    with Store(
        'local-store-fixture',
        LocalConnector(),
        metrics=True,
        populate_target=False,
    ) as store:
        with store_registration(store):
            yield store
