from __future__ import annotations

import pathlib

import pytest

from psbench.run.colmena_rtt import main
from testing.globus_compute import mock_globus_compute
from testing.mocking import disable_logging


def test_main(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'globus',
        '--globus-compute-endpoint',
        'UUID',
        '--input-sizes',
        '10',
        '100',
        '--output-sizes',
        '10',
        '100',
    ]

    with disable_logging('psbench.run.colmena_rtt'), mock_globus_compute():
        main(args)


def test_main_bad_executor(tmp_path: pathlib.Path) -> None:
    args = [
        '--executor',
        'dask',
        '--input-sizes',
        '10',
        '100',
        '--output-sizes',
        '10',
        '100',
    ]

    with disable_logging('psbench.run.colmena_rtt'):
        with pytest.raises(
            ValueError,
            match=(
                'This benchmark only supports the Globus Compute and Parsl '
                'executors.'
            ),
        ):
            main(args)
