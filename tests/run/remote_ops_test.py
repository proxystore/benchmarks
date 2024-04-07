from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.remote_ops import main


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

    with mock.patch(
        'psbench.run.remote_ops.runner',
    ), mock.patch(
        'psbench.run.remote_ops.init_logging',
    ):
        main(args)
