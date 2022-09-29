from __future__ import annotations

import sys
import tempfile
import uuid
from unittest import mock

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from unittest.mock import AsyncMock
else:  # pragma: <3.8 cover
    from asynctest import CoroutineMock as AsyncMock

import pytest

from psbench.benchmarks.remote_ops.main import main
from psbench.benchmarks.remote_ops.main import runner


@pytest.mark.asyncio
async def test_runner() -> None:
    await runner(
        None,
        ['GET', 'SET', 'EVICT', 'EXISTS'],
        payload_sizes=[100, 1000],
        repeat=1,
        server=None,
    )


@pytest.mark.asyncio
async def test_csv_logging() -> None:
    with tempfile.NamedTemporaryFile() as f:
        assert len(f.readlines()) == 0
        await runner(
            remote_endpoint=None,
            ops=['EXISTS', 'EVICT'],
            payload_sizes=[1, 2, 3],
            repeat=3,
            server=None,
            csv_file=f.name,
        )
        assert len(f.readlines()) == 1 + (2 * 3)


def test_main() -> None:
    with mock.patch(
        'psbench.benchmarks.remote_ops.main.runner',
        AsyncMock(),
    ):
        assert (
            main(
                [
                    '--remote',
                    str(uuid.uuid4()),
                    '--ops',
                    'GET',
                    '--payload-sizes',
                    '1000',
                    '--server',
                    'wss://localhost:8765',
                ],
            )
            == 0
        )

        assert (
            main(
                [
                    '--remote',
                    str(uuid.uuid4()),
                    '--ops',
                    'GET',
                    '--payload-sizes',
                    '1000',
                    '--server',
                    'wss://localhost:8765',
                    '--no-uvloop',
                ],
            )
            == 0
        )
