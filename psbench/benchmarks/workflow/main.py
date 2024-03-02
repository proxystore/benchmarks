"""Workflow simulation."""

from __future__ import annotations

import argparse
import enum
import itertools
import logging
import sys
import time
from concurrent.futures import Future
from typing import Any
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.ref import borrow
from proxystore.store.ref import OwnedProxy
from proxystore.store.scopes import submit

from psbench.argparse import add_executor_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.executor.factory import init_executor_from_args
from psbench.executor.protocol import Executor
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.memory import SystemMemoryProfiler
from psbench.proxystore import init_store_from_args
from psbench.results import CSVLogger
from psbench.utils import randbytes

logger = logging.getLogger('workflow')

T = TypeVar('T')


class DataManagement(enum.Enum):
    NONE = 'none'
    DEFAULT_PROXY = 'default-proxy'
    OWNED_PROXY = 'owned-proxy'


class WorkflowStats(NamedTuple):
    executor: str
    connector: str | None
    data_management: str
    stage_sizes: str
    data_size_bytes: int
    task_sleep: float
    workflow_start_timestamp: float
    workflow_end_timestamp: float
    workflow_makespan_s: float


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
            store.proxy(randbytes(data_bytes), populate_target=True)
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


def into_owned(proxy: Proxy[T]) -> OwnedProxy[T]:
    factory = proxy.__factory__
    factory.evict = False
    return OwnedProxy(factory)


def _run_workflow_stage(
    input_data: tuple[Any, ...],
    executor: Executor,
    data_management: DataManagement,
    stage_size: int,
    data_size_bytes: int,
    sleep: float,
) -> tuple[Any, ...]:
    # Returns list of output data of tasks. This could be proxies or bytes.
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
        future = submit(
            executor.submit,
            args=(task, *task_input),
            kwargs={'output_size_bytes': data_size_bytes, 'sleep': sleep},
        )
        futures.append(future)

    return_data = tuple(future.result() for future in futures)
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
) -> WorkflowStats:
    start_timestamp = time.time()

    validate_workflow(stage_sizes)

    current_data = _generate_start_data(
        data_management,
        data_count=stage_sizes[0],
        data_bytes=data_size_bytes,
        store=store,
    )

    for stage_size in stage_sizes:
        new_data = _run_workflow_stage(
            current_data,
            executor=executor,
            data_management=data_management,
            stage_size=stage_size,
            data_size_bytes=data_size_bytes,
            sleep=sleep,
        )
        current_data = new_data

    end_timestamp = time.time()
    return WorkflowStats(
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


def runner(
    executor: Executor,
    store: Store[Any] | None,
    data_management: list[DataManagement],
    stage_sizes: Sequence[int],
    data_sizes: Sequence[int],
    sleep: float,
    repeat: int,
    csv_file: str,
    memory_profile_interval: float,
) -> None:
    runner_start = time.perf_counter()

    connector_name = (
        'None' if store is None else store.connector.__class__.__name__
    )
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - Executor: {executor.__class__.__name__}\n'
        f' - ProxyStore Connector: {connector_name}\n'
        f' - Data management: {", ".join(d.value for d in data_management)}\n'
        f' - Workflow stage sizes: {"-".join(str(s) for s in stage_sizes)}\n'
        f' - Data sizes (bytes): {data_sizes}\n'
        f' - Task sleep (s): {sleep}\n'
        f' - Workflow repeat: {repeat}\n'
        f' - Memory profile interval (s): {memory_profile_interval}',
    )

    if not csv_file.endswith('.csv'):
        raise ValueError('CSV log file should end with ".csv"')

    workflow_logger = CSVLogger(csv_file, WorkflowStats)
    memory_file = csv_file.replace('.csv', '-memory.csv')
    memory_profiler = SystemMemoryProfiler(
        memory_profile_interval,
        memory_file,
    )
    memory_profiler.start()

    logger.log(
        TESTING_LOG_LEVEL,
        'Submitted pre-task to alleviate cold-start penalty',
    )
    future = executor.submit(sum, [1, 2, 3])
    assert future.result() == 6

    options = itertools.product(data_management, data_sizes)
    for data_method, data_size_bytes in options:
        for _ in range(repeat):
            stats = run_workflow(
                executor,
                store,
                data_management=data_method,
                stage_sizes=stage_sizes,
                data_size_bytes=data_size_bytes,
                sleep=sleep,
            )

            logger.log(
                TESTING_LOG_LEVEL,
                f'Workflow completed in {stats.workflow_makespan_s:.3f} s\n'
                f'{stats}',
            )

            workflow_logger.log(stats)

    workflow_logger.close()
    logger.log(TESTING_LOG_LEVEL, f'Workflow run data saved: {csv_file}')
    memory_profiler.stop()
    memory_profiler.join(timeout=5.0)
    logger.log(TESTING_LOG_LEVEL, f'Memory profile data saved: {memory_file}')

    runner_end = time.perf_counter()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start):.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Workflow simulation benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--data-management',
        choices=['none', 'default-proxy', 'owned-proxy'],
        default=['none', 'default-proxy', 'owned-proxy'],
        nargs='+',
        help='Data management method. Default will repeat with all options',
    )
    parser.add_argument(
        '--stage-sizes',
        type=int,
        metavar='COUNT',
        nargs='+',
        required=True,
        help='List of stages sizes of the simulated workflow',
    )
    parser.add_argument(
        '--data-sizes',
        type=int,
        metavar='BYTES',
        nargs='+',
        required=True,
        help='Task input/output size in bytes (runs repeated for each size)',
    )
    parser.add_argument(
        '--sleep',
        metavar='SECONDS',
        required=True,
        type=float,
        help='Simulate task computation',
    )
    parser.add_argument(
        '--repeat',
        default=1,
        metavar='RUNS',
        type=int,
        help='Number of runs to repeat each configuration for',
    )
    parser.add_argument(
        '--memory-profile-interval',
        default=0.01,
        metavar='SECONDS',
        type=float,
        help='Seconds between logging system memory utilization',
    )

    add_executor_options(parser)
    add_proxystore_options(parser, required=True)
    add_logging_options(parser, require_csv=True)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    executor = init_executor_from_args(args)
    store = init_store_from_args(args, metrics=True)
    assert store is not None
    data_management = [DataManagement(d) for d in args.data_management]

    with executor:
        runner(
            executor=executor,
            store=store,
            data_management=data_management,
            stage_sizes=args.stage_sizes,
            data_sizes=args.data_sizes,
            sleep=args.sleep,
            repeat=args.repeat,
            csv_file=args.csv_file,
            memory_profile_interval=args.memory_profile_interval,
        )

    store.close()

    return 0
