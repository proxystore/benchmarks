from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.task_pipelining import main
from testing.globus_compute import mock_globus_compute
from testing.mocking import disable_logging


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--task-chain-length',
        '5',
        '--task-data-bytes',
        '100',
        '1000',
        '--task-overhead-fractions',
        '0.1',
        '0.2',
        '--task-sleep',
        '2',
        '--ps-connector',
        'file',
        '--ps-file-dir',
        str(tmp_path),
    ]

    with disable_logging('psbench.run.task_pipelining'), mock.patch(
        'psbench.config.StoreConfig.get_store',
    ), mock_globus_compute():
        main(args)
