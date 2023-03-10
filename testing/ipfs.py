from __future__ import annotations

import contextlib
import pathlib
from typing import Generator
from unittest import mock


def mock_add_data(data: bytes, filepath: str | pathlib.Path) -> str:
    with open(filepath, 'wb') as f:
        f.write(data)
    return str(filepath)


def mock_get_data(cid: str) -> bytes:
    with open(cid, 'rb') as f:
        return f.read()


@contextlib.contextmanager
def mock_ipfs() -> Generator[None, None, None]:
    with mock.patch('psbench.ipfs.add_data', side_effect=mock_add_data):
        with mock.patch('psbench.ipfs.get_data', side_effect=mock_get_data):
            yield
