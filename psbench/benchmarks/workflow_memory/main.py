"""Workflow simulation."""

from __future__ import annotations

import gc
import logging
import sys
import time
from collections.abc import Callable
from collections.abc import Sequence
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

from proxystore.proxy import Proxy
from proxystore.proxy import resolve
from proxystore.store.base import Store
from proxystore.store.ref import borrow
from proxystore.store.ref import into_owned
from proxystore.store.scopes import submit
from proxystore.store.utils import get_key

from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.benchmarks.workflow_memory.config import DataManagement
from psbench.benchmarks.workflow_memory.config import RunConfig
from psbench.benchmarks.workflow_memory.config import RunResult
from psbench.utils import randbytes

logger = logging.getLogger('workflow-memory')

T = TypeVar('T')


def task_no_proxy(
    *data: bytes,
    output_size_bytes: int,
    sleep: float,
) -> bytes:
    import time

    assert all(isinstance(d, bytes) for d in data)
    time.sleep(sleep)
    return randbytes(output_size_bytes)


def task_proxy(
    *data: Proxy[bytes],
    output_size_bytes: int,
    sleep: float,
) -> Proxy[bytes]:
    import time

    from proxystore.proxy import is_resolved
    from proxystore.store import get_store

    # Force proxies to resolve
    for d in data:
        # isinstance is not guaranteed to resolve the proxy when
        # populate_target=True.
        resolve(d)
        assert isinstance(d, bytes)
    assert all(is_resolved(d) for d in data)
    time.sleep(sleep)

    output = randbytes(output_size_bytes)
    store = get_store(data[0])
    assert store is not None

    return store.proxy(output, evict=False, populate_target=True)


def _generate_start_data(
    data_management: DataManagement,
    data_count: int,
    data_bytes: int,
    store: Store[Any] | None,
) -> tuple[Any, ...]:
    if data_management is DataManagement.NONE:
        return tuple(randbytes(data_bytes) for _ in range(data_count))
    elif (
        data_management is DataManagement.DEFAULT_PROXY
        or data_management is DataManagement.MANUAL_PROXY
    ):
        assert store is not None
        return tuple(
            store.proxy(
                randbytes(data_bytes),
                evict=False,
                populate_target=True,
            )
            for _ in range(data_count)
        )
    elif data_management is DataManagement.OWNED_PROXY:
        assert store is not None
        return tuple(
            store.owned_proxy(randbytes(data_bytes), populate_target=True)
            for _ in range(data_count)
        )
    else:
        raise AssertionError('Unreachable.')


def validate_workflow(stage_sizes: Sequence[int]) -> None:
    # Workflow rules:
    # - Each stage is classified as single- or multi-task stage.
    # - A multi-task stage can only follow a multi-task stage of the same size.
    if len(stage_sizes) == 0:
        raise ValueError('Stage sizes cannot be empty.')
    if any(size <= 0 for size in stage_sizes):
        raise ValueError('All stage sizes must be greater than zero.')

    for stage, size in enumerate(stage_sizes):
        if stage == 0:
            continue

        if size > 1 and stage_sizes[stage - 1] not in (1, size):
            raise ValueError(
                f'Stage index {stage} has size {size} but follows '
                f'stage index {stage - 1} with size {stage_sizes[stage - 1]}. '
                'Stages following a multi-task stage must either have size of '
                'one or the same size as the previous.',
            )


def _run_workflow_stage(
    input_data: tuple[Any, ...],
    executor: Executor,
    data_management: DataManagement,
    stage_task_count: int,
    stage_output_bytes: int,
    sleep: float,
) -> tuple[Any, ...]:
    # Returns list of output data of tasks. This could be proxies or bytes.
    task: Callable[..., Any]
    if data_management is DataManagement.NONE:
        task = task_no_proxy
    elif (
        data_management is DataManagement.DEFAULT_PROXY
        or data_management is DataManagement.MANUAL_PROXY
        or data_management is DataManagement.OWNED_PROXY
    ):
        task = task_proxy
    else:
        raise AssertionError(f'Unknown data management: {data_management}.')

    futures: list[Future[Any]] = []

    # This will have length equal to stage_size
    stage_task_inputs: tuple[list[Any], ...]
    if len(input_data) == stage_task_count:
        stage_task_inputs = tuple([data] for data in input_data)
    elif len(input_data) == 1:
        stage_task_inputs = tuple(
            [input_data[0]] for _ in range(stage_task_count)
        )
    elif stage_task_count == 1:
        stage_task_inputs = (list(input_data),)
    else:
        raise AssertionError(
            'Bad input data size and stage size configuration. '
            'Is the workflow configuration valid?',
        )

    if data_management is DataManagement.OWNED_PROXY:
        stage_task_inputs = tuple(
            [borrow(task_input) for task_input in task_inputs]
            for task_inputs in stage_task_inputs
        )

    for task_input in stage_task_inputs:
        future: Future[Any] = submit(
            executor.submit,
            args=(task, *task_input),
            kwargs={'output_size_bytes': stage_output_bytes, 'sleep': sleep},
        )
        futures.append(future)

    return_data: tuple[Any, ...] = tuple(future.result() for future in futures)

    if data_management is DataManagement.OWNED_PROXY:
        return_data = tuple(map(into_owned, return_data))
    return return_data


def run_workflow(
    executor: Executor,
    store: Store[Any] | None,
    data_management: DataManagement,
    stage_task_counts: Sequence[int],
    stage_bytes_sizes: Sequence[int],
    stage_repeat: int,
    sleep: float,
) -> RunResult:
    start_timestamp = time.time()

    validate_workflow(stage_task_counts)
    if len(stage_task_counts) + 1 != len(stage_bytes_sizes):
        raise ValueError(
            'Length of data sizes must be one greater than number of stages.',
        )

    proxy_keys: list[tuple[Any, ...]] = []

    for _ in range(stage_repeat):
        current_data = _generate_start_data(
            data_management,
            data_count=stage_task_counts[0],
            data_bytes=stage_bytes_sizes[0],
            store=store,
        )

        for stage_index, stage_task_count in enumerate(stage_task_counts):
            # Keep track of what proxies were created for clean up at end
            if data_management == DataManagement.DEFAULT_PROXY:
                proxy_keys.extend(get_key(p) for p in current_data)
            new_data = _run_workflow_stage(
                current_data,
                executor=executor,
                data_management=data_management,
                stage_task_count=stage_task_count,
                stage_output_bytes=stage_bytes_sizes[stage_index + 1],
                sleep=sleep,
            )
            if data_management is DataManagement.MANUAL_PROXY:
                assert store is not None
                for proxy in current_data:
                    store.evict(get_key(proxy))
            current_data = new_data

    # Housekeeping to clean up any outstanding memory we might have
    if data_management is DataManagement.OWNED_PROXY:
        del current_data
    elif data_management is DataManagement.MANUAL_PROXY:
        assert store is not None
        for proxy in current_data:
            store.evict(get_key(proxy))

    gc.collect()

    end_timestamp = time.time()
    if store is not None:
        for proxy_key in proxy_keys:
            if data_management == DataManagement.DEFAULT_PROXY:
                store.evict(proxy_key)
            else:
                raise AssertionError('Unreachable.')

    return RunResult(
        executor=executor.__class__.__name__,
        connector=(
            'None' if store is None else store.connector.__class__.__name__
        ),
        data_management=data_management.value,
        stage_task_counts='-'.join(str(s) for s in stage_task_counts),
        stage_bytes_sizes='-'.join(str(s) for s in stage_bytes_sizes),
        stage_repeat=stage_repeat,
        task_sleep=sleep,
        workflow_start_timestamp=start_timestamp,
        workflow_end_timestamp=end_timestamp,
        workflow_makespan_s=(end_timestamp - start_timestamp),
    )


class Benchmark(ContextManagerAddIn):
    name = 'Workflow Memory'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self, executor: Executor, store: Store[Any]) -> None:
        self.executor = executor
        self.store = store
        super().__init__(managers=[self.executor, self.store])

    def config(self) -> dict[str, Any]:
        return {
            'executor': self.executor.__class__.__name__,
            'connector': self.store.connector.__class__.__name__,
        }

    def run(self, config: RunConfig) -> RunResult:
        # We are interested in memory used so let's make sure we start
        # fresh.
        gc.collect()
        result = run_workflow(
            executor=self.executor,
            store=(
                None
                if config.data_management is DataManagement.NONE
                else self.store
            ),
            data_management=config.data_management,
            stage_task_counts=config.stage_task_counts,
            stage_bytes_sizes=config.stage_bytes_sizes,
            stage_repeat=config.stage_repeat,
            sleep=config.task_sleep,
        )

        return result
