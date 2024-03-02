from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.stream_scaling import main


def test_stream_scaling_main(tmp_path: pathlib.Path) -> None:
    argv = [
        '--data-size-bytes',
        '1',
        '2',
        '3',
        '--producer-sleep',
        '4',
        '--task-count',
        '5',
        '--task-sleep',
        '6',
        '--executor',
        'dask',
        '--ps-connector',
        'file',
        '--ps-file-dir',
        str(tmp_path),
        '--stream',
        'redis',
        '--stream-servers',
        'localhost',
    ]

    with mock.patch('psbench.run.stream_scaling.runner'), mock.patch(
        'psbench.config.StoreConfig.get_store',
    ):
        main(argv)
