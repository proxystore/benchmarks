from __future__ import annotations

import pathlib

from psbench.run.remote_ops import main
from testing.mocking import disable_logging


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        'endpoint',
        '--endpoint',
        'UUID',
        '--ops',
        'get',
        '--relay-server',
        'ws://localhost',
        '--payload-sizes',
        '100',
    ]

    with disable_logging('psbench.run.remote_ops'):
        main(args)
