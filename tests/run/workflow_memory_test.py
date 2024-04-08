from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.workflow_memory import main
from testing.globus_compute import mock_globus_compute
from testing.mocking import disable_logging


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--data-management',
        'none',
        '--stage-task-counts',
        '1',
        '3',
        '1',
        '--stage-bytes-sizes',
        '100',
        '--task-sleep',
        '0.01',
        '--run-dir',
        str(tmp_path),
        '--ps-connector',
        'file',
        '--ps-file-dir',
        str(tmp_path / 'dump'),
    ]

    with disable_logging('psbench.run.workflow_memory'), mock.patch(
        'psbench.config.StoreConfig.get_store',
    ), mock.patch(
        'psbench.run.workflow_memory.SystemMemoryProfiler',
    ), mock_globus_compute():
        main(args)
