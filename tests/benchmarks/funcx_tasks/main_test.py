from __future__ import annotations

import logging
import tempfile
import uuid
from unittest import mock

import pytest
from proxystore.store import register_store
from proxystore.store import unregister_store
from proxystore.store.local import LocalStore

from psbench.benchmarks.funcx_tasks.main import main
from psbench.benchmarks.funcx_tasks.main import runner
from psbench.benchmarks.funcx_tasks.main import time_task
from psbench.benchmarks.funcx_tasks.main import time_task_proxy
from testing.funcx import mock_executor
from testing.funcx import mock_funcx


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


def test_time_task_proxy() -> None:
    fx = mock_executor()
    store = LocalStore(name='test-time-task-store', stats=True)
    register_store(store)

    stats = time_task_proxy(
        fx=fx,
        store=store,
        input_size=100,
        output_size=50,
        task_sleep=0.01,
    )

    assert stats.proxystore_backend == 'LocalStore'
    assert stats.input_size_bytes == 100
    assert stats.output_size_bytes == 50
    assert stats.task_sleep_seconds == 0.01
    assert stats.total_time_ms >= 10
    assert stats.input_get_ms is not None and stats.input_get_ms > 0
    assert stats.input_set_ms is not None and stats.input_set_ms > 0
    assert stats.input_proxy_ms is not None and stats.input_proxy_ms > 0
    assert stats.input_resolve_ms is not None and stats.input_resolve_ms > 0
    assert stats.output_get_ms is not None and stats.output_get_ms > 0
    assert stats.output_set_ms is not None and stats.output_set_ms > 0
    assert stats.output_proxy_ms is not None and stats.output_proxy_ms > 0
    assert stats.output_resolve_ms is not None and stats.output_resolve_ms > 0
    unregister_store(store)


@pytest.mark.parametrize(
    'use_proxystore,log_to_csv',
    ((True, False), (False, True)),
)
def test_runner(caplog, use_proxystore: bool, log_to_csv: bool) -> None:
    caplog.set_level(logging.ERROR)

    if use_proxystore:
        store = LocalStore(name='test-runner-store', stats=True)
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

    with mock_funcx():
        runner(
            funcx_endpoint=str(uuid.uuid4()),
            store=store,
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
        return_value=LocalStore('test-main-store'),
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
