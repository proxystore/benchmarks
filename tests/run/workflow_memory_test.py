from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.workflow_memory import main
from testing.globus_compute import mock_globus_compute


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--data-management',
        'none',
        '--stage-sizes',
        '1',
        '3',
        '1',
        '--data-sizes-bytes',
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

    with mock.patch(
        'psbench.run.workflow_memory.runner',
    ), mock.patch(
        'psbench.config.StoreConfig.get_store',
    ), mock_globus_compute():
        main(args)

    csv_files = list(tmp_path.glob('*/*.csv'))
    assert len(csv_files) == 2
