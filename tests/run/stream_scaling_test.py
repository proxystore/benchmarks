from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.stream_scaling import main
from testing.mocking import disable_logging


def test_stream_scaling_main(tmp_path: pathlib.Path) -> None:
    argv = [
        '--data-size-bytes',
        '1',
        '2',
        '3',
        '--max-workers',
        '7',
        '--stream-method',
        'proxy',
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

    with (
        disable_logging('psbench.run.stream_scaling'),
        mock.patch(
            'psbench.config.StoreConfig.get_store',
        ),
        mock.patch(
            'psbench.config.ExecutorConfig.get_executor',
        ),
        mock.patch(
            'psbench.run.stream_scaling.runner',
        ),
    ):
        main(argv)
