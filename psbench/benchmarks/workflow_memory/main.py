"""Workflow simulation."""

from __future__ import annotations

import contextlib
import gc
import logging
import sys
import time
from concurrent.futures import Future
from types import TracebackType
from typing import Any
from typing import Callable
from typing import Sequence
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.ref import borrow
from proxystore.store.ref import into_owned
from proxystore.store.scopes import submit

from psbench.benchmarks.workflow_memory.config import DataManagement
from psbench.benchmarks.workflow_memory.config import RunConfig
from psbench.benchmarks.workflow_memory.config import RunResult
from psbench.executor.protocol import Executor
from psbench.utils import randbytes

logger = logging.getLogger('workflow-memory')

T = TypeVar('T')


def task_no_proxy(
    *data: bytes,
    output_size_bytes: int,
    sleep: float,
) -> bytes:
    import time

    from psbench.utils import randbytes

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

    from psbench.utils import randbytes

    # Force proxies to resolve
    assert all(isinstance(d, bytes) for d in data)
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
    elif data_management is DataManagement.DEFAULT_PROXY:
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
    stage_size: int,
    data_size_bytes: int,
    sleep: float,
) -> tuple[Any, ...]:
    # Returns list of output data of tasks. This could be proxies or bytes.
    task: Callable[..., bytes]
    if data_management is DataManagement.NONE:
        task = task_no_proxy
    elif (
        data_management is DataManagement.DEFAULT_PROXY
        or data_management is DataManagement.OWNED_PROXY
    ):
        task = task_proxy
    else:
        raise AssertionError(f'Unknown data management: {data_management}.')

    futures: list[Future[bytes]] = []

    # This will have length equal to stage_size
    stage_task_inputs: tuple[list[Any], ...]
    if len(input_data) == stage_size:
        stage_task_inputs = tuple([data] for data in input_data)
    elif len(input_data) == 1:
        stage_task_inputs = tuple([input_data[0]] for _ in range(stage_size))
    elif stage_size == 1:
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
        future: Future[bytes] = submit(
            executor.submit,
            args=(task, *task_input),
            kwargs={'output_size_bytes': data_size_bytes, 'sleep': sleep},
        )
        futures.append(future)

    return_data: tuple[Any, ...] = tuple(future.result() for future in futures)
    # Resolve data if necessary
    for data in return_data:
        assert isinstance(data, bytes)
    if data_management is DataManagement.OWNED_PROXY:
        return_data = tuple(map(into_owned, return_data))
    return return_data


def run_workflow(
    executor: Executor,
    store: Store[Any] | None,
    data_management: DataManagement,
    stage_sizes: Sequence[int],
    data_size_bytes: int,
    sleep: float,
) -> RunResult:
    start_timestamp = time.time()

    validate_workflow(stage_sizes)

    proxy_keys: list[tuple[Any, ...]] = []

    current_data = _generate_start_data(
        data_management,
        data_count=stage_sizes[0],
        data_bytes=data_size_bytes,
        store=store,
    )

    for stage_size in stage_sizes:
        # Keep track of what proxies were created for clean up at end
        proxy_keys.extend(
            p.__factory__.key for p in current_data if isinstance(p, Proxy)
        )
        new_data = _run_workflow_stage(
            current_data,
            executor=executor,
            data_management=data_management,
            stage_size=stage_size,
            data_size_bytes=data_size_bytes,
            sleep=sleep,
        )
        current_data = new_data

    # Housekeeping to clean up any outstanding memory we might have
    del current_data
    if store is not None:
        for proxy_key in proxy_keys:
            if data_management == DataManagement.OWNED_PROXY:
                assert not store.exists(proxy_key)
            elif data_management == DataManagement.DEFAULT_PROXY:
                store.evict(proxy_key)
            else:
                raise AssertionError('Unreachable.')

    end_timestamp = time.time()
    return RunResult(
        executor=executor.__class__.__name__,
        connector=(
            'None' if store is None else store.connector.__class__.__name__
        ),
        data_management=data_management.value,
        stage_sizes='-'.join(str(s) for s in stage_sizes),
        data_size_bytes=data_size_bytes,
        task_sleep=sleep,
        workflow_start_timestamp=start_timestamp,
        workflow_end_timestamp=end_timestamp,
        workflow_makespan_s=(end_timestamp - start_timestamp),
    )


class Benchmark:
    name = 'Workflow Memory'
    config_type = RunConfig
    result_type = RunResult

    def __init__(self, executor: Executor, store: Store[Any]) -> None:
        self.executor = executor
        self.store = store

    def __enter__(self) -> Self:
        # https://stackoverflow.com/a/39172487
        with contextlib.ExitStack() as stack:
            stack.enter_context(self.executor)
            stack.enter_context(self.store)
            self._stack = stack.pop_all()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self._stack.__exit__(exc_type, exc_value, exc_traceback)

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
            stage_sizes=config.stage_sizes,
            data_size_bytes=config.data_size_bytes,
            sleep=config.task_sleep,
        )

        return result
