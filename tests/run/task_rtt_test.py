from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.task_rtt import main
from testing.globus_compute import mock_globus_compute


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--input-sizes',
        '1',
        '--output-sizes',
        '2',
    ]

    with mock.patch(
        'psbench.run.task_rtt.runner',
    ), mock.patch(
        'psbench.run.task_rtt.init_logging',
    ), mock_globus_compute():
        main(args)
