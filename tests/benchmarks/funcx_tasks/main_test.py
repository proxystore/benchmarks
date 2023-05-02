from __future__ import annotations

import logging
import pathlib
import tempfile
import uuid
from unittest import mock

import pytest
from proxystore.connectors.local import LocalConnector
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.store import unregister_store

from psbench.benchmarks.funcx_tasks.main import main
from psbench.benchmarks.funcx_tasks.main import runner
from psbench.benchmarks.funcx_tasks.main import time_task
from psbench.benchmarks.funcx_tasks.main import time_task_ipfs
from psbench.benchmarks.funcx_tasks.main import time_task_proxy
from testing.funcx import mock_executor
from testing.funcx import mock_funcx
from testing.ipfs import mock_ipfs


def test_time_task() -> None:
    fx = mock_executor()

    stats = time_task(
        fx=fx,
        input_size=100,
        output_size=50,
        task_sleep=0.01,
    )

    assert stats.input_size_bytes == 100
    assert stats.output_size_bytes == 50
    assert stats.task_sleep_seconds == 0.01
    assert stats.total_time_ms >= 10


def test_time_task_ipfs(tmp_path: pathlib.Path) -> None:
    with mock_ipfs():
        fx = mock_executor()

        stats = time_task_ipfs(
            fx=fx,
            ipfs_local_dir=str(tmp_path / 'local'),
            ipfs_remote_dir=str(tmp_path / 'remote'),
            input_size=100,
            output_size=50,
            task_sleep=0.01,
        )

        assert stats.input_size_bytes == 100
        assert stats.output_size_bytes == 50
        assert stats.task_sleep_seconds == 0.01
        assert stats.total_time_ms >= 10

        stats = time_task_ipfs(
            fx=fx,
            ipfs_local_dir=str(tmp_path / 'local'),
            ipfs_remote_dir=str(tmp_path / 'remote'),
            input_size=100,
            output_size=0,
            task_sleep=0.0,
        )

        assert stats.input_size_bytes == 100
        assert stats.output_size_bytes == 0
        assert stats.task_sleep_seconds == 0.0
        assert stats.total_time_ms >= 0.0


def test_time_task_proxy() -> None:
    fx = mock_executor()
    store = Store('test-time-task-store', LocalConnector(), metrics=True)
    register_store(store)

    stats = time_task_proxy(
        fx=fx,
        store=store,
        input_size=100,
        output_size=50,
        task_sleep=0.01,
    )

    assert stats.proxystore_backend == 'LocalConnector'
    assert stats.input_size_bytes == 100
    assert stats.output_size_bytes == 50
    assert stats.task_sleep_seconds == 0.01
    assert stats.total_time_ms >= 10
    assert stats.input_get_ms is not None and stats.input_get_ms > 0
    assert stats.input_put_ms is not None and stats.input_put_ms > 0
    assert stats.input_proxy_ms is not None and stats.input_proxy_ms > 0
    assert stats.input_resolve_ms is not None and stats.input_resolve_ms > 0
    assert stats.output_get_ms is not None and stats.output_get_ms > 0
    assert stats.output_put_ms is not None and stats.output_put_ms > 0
    assert stats.output_proxy_ms is not None and stats.output_proxy_ms > 0
    assert stats.output_resolve_ms is not None and stats.output_resolve_ms > 0
    unregister_store(store)


@pytest.mark.parametrize(
    'use_ipfs,use_proxystore,log_to_csv',
    ((False, True, False), (True, False, False), (False, False, True)),
)
def test_runner(
    caplog,
    use_ipfs: bool,
    use_proxystore: bool,
    log_to_csv: bool,
    tmp_path: pathlib.Path,
) -> None:
    caplog.set_level(logging.ERROR)

    if use_proxystore:
        store = Store('test-runner-store', LocalConnector(), metrics=True)
        register_store(store)
    else:
        store = None
    csv_file: str | None = None
    if log_to_csv:
        temp_file = tempfile.NamedTemporaryFile()
        csv_file = temp_file.name

    input_sizes = [0, 10, 100]
    output_sizes = [0, 10, 100]
    task_repeat = 2

    ipfs_local_dir = tmp_path / 'ipfs-local'
    ipfs_remote_dir = tmp_path / 'ipfs-remote'
    ipfs_local_dir.mkdir()
    ipfs_remote_dir.mkdir()

    with mock_funcx():
        with mock_ipfs():
            runner(
                funcx_endpoint=str(uuid.uuid4()),
                store=store,
                use_ipfs=use_ipfs,
                ipfs_local_dir=str(ipfs_local_dir),
                ipfs_remote_dir=str(ipfs_remote_dir),
                input_sizes=input_sizes,
                output_sizes=output_sizes,
                task_repeat=task_repeat,
                task_sleep=0.001,
                csv_file=csv_file,
            )

    if log_to_csv:
        assert len(temp_file.readlines()) == (
            (len(input_sizes) * len(output_sizes) * task_repeat) + 1
        )
        temp_file.close()
    if use_proxystore:
        unregister_store(store)


def test_runner_error() -> None:
    with Store('test-runner-store', LocalConnector()) as store:
        with pytest.raises(ValueError):
            runner(
                funcx_endpoint=str(uuid.uuid4()),
                store=store,
                use_ipfs=True,
                ipfs_local_dir='/tmp/local/',
                ipfs_remote_dir='/tmp/remote/',
                input_sizes=[0],
                output_sizes=[0],
                task_repeat=1,
                task_sleep=0,
                csv_file=None,
            )


@mock.patch('psbench.benchmarks.funcx_tasks.main.runner')
def test_main(mock_runner) -> None:
    main(
        [
            '--funcx-endpoint',
            'ABCD',
            '--input-sizes',
            '0',
            '--output-sizes',
            '1',
            '2',
        ],
    )

    with mock.patch(
        'psbench.benchmarks.funcx_tasks.main.init_store_from_args',
        return_value=Store('test-main-store', LocalConnector()),
    ):
        main(
            [
                '--funcx-endpoint',
                'ABCD',
                '--input-sizes',
                '0',
                '--output-sizes',
                '1',
                '2',
            ],
        )
