from __future__ import annotations

import pathlib

from psbench.run.endpoint_qps import main
from testing.mocking import disable_logging


def test_main(tmp_path: pathlib.Path) -> None:
    args = ['UUID', '--routes', 'GET']

    with disable_logging('psbench.run.endpoint_qps'):
        main(args)
