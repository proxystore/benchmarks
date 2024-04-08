from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.template import main
from testing.globus_compute import mock_globus_compute


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--name',
        'a',
        'b',
        'c',
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
    ]

    with mock.patch(
        'psbench.run.template.runner',
    ), mock.patch(
        'psbench.config.StoreConfig.get_store',
    ), mock.patch(
        'psbench.run.template.init_logging',
    ), mock_globus_compute():
        main(args)
