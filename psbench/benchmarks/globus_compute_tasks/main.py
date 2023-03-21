"""Globus Compute + ProxyStore Simple Test.

Tests round trip function execution times to a Globus Compute endpoint with
configurable function payload transfer methods, sizes, etc.
"""
from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import shutil
import sys
import time
import uuid
from typing import Sequence

import globus_compute_sdk
import proxystore
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.utils import get_key

from psbench import ipfs
from psbench.argparse import add_dspaces_options
from psbench.argparse import add_globus_compute_options
from psbench.argparse import add_dspaces_options
from psbench.argparse import add_ipfs_options
from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.csv import CSVLogger
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.tasks.pong import pong
from psbench.tasks.pong import pong_dspaces
from psbench.tasks.pong import pong_ipfs
from psbench.tasks.pong import pong_proxy
from psbench.utils import randbytes

logger = logging.getLogger('globus-compute-test')


@dataclasses.dataclass
class TaskStats:
    """Stats for individual task. Represents a row in the output CSV."""

    proxystore_backend: str
    task_name: str
    input_size_bytes: int
    output_size_bytes: int
    task_sleep_seconds: float
    total_time_ms: float
    input_get_ms: float | None = None
    input_put_ms: float | None = None
    input_proxy_ms: float | None = None
    input_resolve_ms: float | None = None
    output_get_ms: float | None = None
    output_put_ms: float | None = None
    output_proxy_ms: float | None = None
    output_resolve_ms: float | None = None


def time_task(
    *,
    gce: globus_compute_sdk.Executor,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single Globus Compute task.

    Args:
        gce (Executor): Globus Compute Executor to submit task through.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()
    fut = gce.submit(
        pong,
        data,
        result_size=output_size,
        sleep=task_sleep,
    )
    result = fut.result()

    end = time.perf_counter_ns()
    assert isinstance(result, bytes)

    return TaskStats(
        proxystore_backend='',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_ipfs(
    *,
    gce: globus_compute_sdk.Executor,
    ipfs_local_dir: str,
    ipfs_remote_dir: str,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single Globus Compute task with IPFS for transfer.

    Args:
        gce (Executor): Globus Compute Executor to submit task through.
        ipfs_local_dir (str): Local IPFS directory to write files to.
        ipfs_remote_dir (str): Remote IPFS directory to write files to.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()

    os.makedirs(ipfs_local_dir, exist_ok=True)
    filepath = os.path.join(ipfs_local_dir, str(uuid.uuid4()))
    cid = ipfs.add_data(data, filepath)

    fut = gce.submit(
        pong_ipfs,
        cid,
        ipfs_remote_dir,
        result_size=output_size,
        sleep=task_sleep,
    )
    result = fut.result()

    if result is not None:
        data = ipfs.get_data(result)

    end = time.perf_counter_ns()
    assert isinstance(data, bytes)

    return TaskStats(
        proxystore_backend='IPFS',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_dspaces(
    *,
    fx: funcx.FuncXExecutor,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single FuncX task with DataSpaces for data transfer.

    Args:
        fx (FuncXExecutor): FuncX Executor to submit task through.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    import dspaces as ds
    import numpy as np
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    version = 1

    client = ds.DSpaces()
    path = str(uuid.uuid4())

    data = randbytes(input_size)
    local_size = input_size / size
    start = time.perf_counter_ns()

    client.Put(np.array(bytearray(data)), path, version=version, offset=((input_size * rank),))
    fut = fx.submit(
        pong_dspaces,
        path,
        input_size,
        rank,
        size,
        version=version,
        result_size=output_size,
        sleep=task_sleep,
    )
	
    result = fut.result()
    
    if result is not None:
        out_path = result[0]
        out_size = result[1]
        data = client.Get(
            out_path,
            version,
            lb=((out_size * rank),),
            ub=((out_size * rank + out_size - 1),),
            dtype=bytes,
            timeout=-1,
        ).tobytes()

    end = time.perf_counter_ns()
    assert isinstance(data, bytes)

    return TaskStats(
        proxystore_backend='DataSpaces',
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
    )


def time_task_proxy(
    *,
    gce: globus_compute_sdk.Executor,
    store: Store,
    input_size: int,
    output_size: int,
    task_sleep: float,
) -> TaskStats:
    """Execute and time a single Globus Compute task with proxied inputs.

    Args:
        gce (Executor): Globus Compute Executor to submit task through.
        store (Store): ProxyStore Store to use for proxying input/outputs.
        input_size (int): number of bytes to send as input to task.
        output_size (int): number of bytes task should return.
        task_sleep (int): number of seconds to sleep inside task.

    Returns:
        TaskStats
    """
    data = randbytes(input_size)
    start = time.perf_counter_ns()

    proxy: Proxy[bytes] = store.proxy(data, evict=True)
    fut = gce.submit(
        pong_proxy,
        proxy,
        evict_result=False,
        result_size=output_size,
        sleep=task_sleep,
    )
    (result, task_proxy_stats) = fut.result()

    proxystore.proxy.resolve(result)
    key = get_key(result)
    assert key is not None
    store.evict(key)
    end = time.perf_counter_ns()
    assert isinstance(result, bytes)
    assert isinstance(result, Proxy)

    input_metrics = store.metrics.get_metrics(proxy)
    output_metrics = store.metrics.get_metrics(result)

    return TaskStats(
        proxystore_backend=store.connector.__class__.__name__,
        task_name='pong',
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        task_sleep_seconds=task_sleep,
        total_time_ms=(end - start) / 1e6,
        input_get_ms=task_proxy_stats.input_get_ms,
        input_put_ms=input_metrics.times['store.put'].avg_time_ms,
        input_proxy_ms=input_metrics.times['store.proxy'].avg_time_ms,
        input_resolve_ms=task_proxy_stats.input_resolve_ms,
        output_get_ms=output_metrics.times['store.get'].avg_time_ms,
        output_put_ms=task_proxy_stats.output_put_ms,
        output_proxy_ms=task_proxy_stats.output_proxy_ms,
        output_resolve_ms=output_metrics.times['factory.resolve'].avg_time_ms,
    )


def runner(
    *,
    globus_compute_endpoint: str,
    store: Store | None,
    use_dspaces: bool,
    use_ipfs: bool,
    ipfs_local_dir: str | None,
    ipfs_remote_dir: str | None,
    input_sizes: list[int],
    output_sizes: list[int],
    task_repeat: int,
    task_sleep: float,
    csv_file: str | None,
) -> None:
    """Run all task configurations and log results."""
    store_connector_name = (
        None if store is None else store.connector.__class__.__name__
    )
    logger.log(
        TESTING_LOG_LEVEL,
        'Starting test runner\n'
        f' - Globus Compute Endpoint: {globus_compute_endpoint}\n'
        f' - ProxyStore backend: {store_connector_name}\n'
        f' - DataSpaces enabled: {use_dspaces}\n'
        f' - IPFS enabled: {use_ipfs}\n'
        f' - Task type: ping-pong\n'
        f' - Task repeat: {task_repeat}\n'
        f' - Task input sizes: {input_sizes} bytes\n'
        f' - Task output sizes: {output_sizes} bytes\n'
        f' - Task sleep time: {task_sleep} s',
    )

    if store is not None and (use_ipfs or use_dspaces):
        raise ValueError(
            f'{"IPFS" if use_ipfs else "DataSpaces"} and ProxyStore cannot be used at the same time.',
        )

    runner_start = time.perf_counter_ns()
    gce = globus_compute_sdk.Executor(
        endpoint_id=globus_compute_endpoint,
        batch_size=1,
    )

    if csv_file is not None:
        csv_logger = CSVLogger(csv_file, TaskStats)

    for input_size in input_sizes:
        for output_size in output_sizes:
            for _ in range(task_repeat):
                if store is not None:
                    stats = time_task_proxy(
                        gce=gce,
                        store=store,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )
                elif use_ipfs:
                    assert ipfs_local_dir is not None
                    assert ipfs_remote_dir is not None
                    stats = time_task_ipfs(
                        gce=gce,
                        ipfs_local_dir=ipfs_local_dir,
                        ipfs_remote_dir=ipfs_remote_dir,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )
                elif use_dspaces:
                    stats = time_task_dspaces(
                        fx=fx,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )
                else:
                    stats = time_task(
                        gce=gce,
                        input_size=input_size,
                        output_size=output_size,
                        task_sleep=task_sleep,
                    )

                logger.log(
                    TESTING_LOG_LEVEL,
                    f'Task completed in {stats.total_time_ms:.3f} ms\n{stats}',
                )

                if csv_file is not None:
                    csv_logger.log(stats)

    if csv_file is not None:
        csv_logger.close()
    if use_ipfs:
        # Clean up local and remote IPFS files
        assert ipfs_local_dir is not None
        shutil.rmtree(ipfs_local_dir)

        def _remote_cleanup() -> None:
            import shutil

            assert ipfs_remote_dir is not None
            shutil.rmtree(ipfs_remote_dir)

        fut = gce.submit(_remote_cleanup)
        fut.result()

    gce.shutdown()

    runner_end = time.perf_counter_ns()
    logger.log(
        TESTING_LOG_LEVEL,
        f'Test runner complete in {(runner_end - runner_start) / 1e9:.3f} s',
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Simple Globus Compute Task Benchmark with ProxyStore."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Simple Globus Compute task benchmark with ProxyStore.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--task-repeat',
        type=int,
        default=1,
        help='Repeat each unique task configuration',
    )
    parser.add_argument(
        '--task-sleep',
        type=float,
        default=0,
        help='Sleep time for tasks',
    )
    parser.add_argument(
        '--input-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task input size in bytes',
    )
    parser.add_argument(
        '--output-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task output size in bytes',
    )
    add_globus_compute_options(parser, required=True)
    add_logging_options(parser)
    add_proxystore_options(parser, required=False)
    add_ipfs_options(parser)
    add_dspaces_options(parser)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)

    store = init_store_from_args(args, metrics=True)

    runner(
        globus_compute_endpoint=args.globus_compute_endpoint,
        store=store,
        use_dspaces=args.dspaces,
        use_ipfs=args.ipfs,
        ipfs_local_dir=args.ipfs_local_dir,
        ipfs_remote_dir=args.ipfs_remote_dir,
        input_sizes=args.input_sizes,
        output_sizes=args.output_sizes,
        task_repeat=args.task_repeat,
        task_sleep=args.task_sleep,
        csv_file=args.csv_file,
    )

    if store is not None:
        store.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
