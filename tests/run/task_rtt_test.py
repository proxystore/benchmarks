from __future__ import annotations

import pathlib

from psbench.run.task_rtt import main
from testing.globus_compute import mock_globus_compute
from testing.mocking import disable_logging


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

    with disable_logging('psbench.run.task_rtt'), mock_globus_compute():
        main(args)
