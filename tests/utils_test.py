from __future__ import annotations

import os
import pathlib

import pytest

from psbench.utils import make_parent_dirs
from psbench.utils import randbytes


def test_make_parent_dirs(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'parent' / 'file')
    assert not os.path.exists(filepath)
    make_parent_dirs(filepath)
    assert os.path.isdir(os.path.join(str(tmp_path), 'parent'))

    # Check idempotency
    make_parent_dirs(filepath)


@pytest.mark.parametrize('size', (0, 1, 10, 100))
def test_randbytes(size: int) -> None:
    b = randbytes(size)
    assert isinstance(b, bytes)
    assert len(b) == size
