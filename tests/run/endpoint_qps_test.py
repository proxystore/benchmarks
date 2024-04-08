from __future__ import annotations

import pathlib
from unittest import mock

from psbench.run.endpoint_qps import main


def test_main(tmp_path: pathlib.Path) -> None:
    args = ['UUID', '--routes', 'GET']

    with mock.patch(
        'psbench.run.endpoint_qps.runner',
    ), mock.patch(
        'psbench.run.endpoint_qps.init_logging',
    ):
        main(args)
