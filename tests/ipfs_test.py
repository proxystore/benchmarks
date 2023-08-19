from __future__ import annotations

import pathlib
import uuid
from unittest import mock

from psbench.ipfs import add_data
from psbench.ipfs import get_data


def test_add_data(tmp_path: pathlib.Path) -> None:
    cid = str(uuid.uuid4())
    filepath = str(tmp_path / str(uuid.uuid4()))
    output = f'add {cid} {filepath}'.encode()

    with mock.patch('subprocess.check_output', return_value=output):
        found_cid = add_data(b'data', filepath)
        assert found_cid == cid

    with open(filepath, 'rb') as f:
        assert f.read() == b'data'


def test_get_data(tmp_path: pathlib.Path) -> None:
    def _mock_run(*args: str) -> None:
        # Filepath is last in args
        filepath = args[0][-1]
        with open(filepath, 'wb') as f:
            f.write(b'data')

    with mock.patch('subprocess.run', side_effect=_mock_run):
        assert get_data('test-cid') == b'data'
