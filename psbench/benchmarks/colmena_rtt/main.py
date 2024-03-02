"""Colmena round trip task time benchmark.

Tests round trip task times in Colmena with configurable backends,
ProxyStore methods, payload sizes, etc. Colmena additionally requires
Redis which is not installed when installing the psbench package.

The Parsl executor config can be modified in
    psbench/benchmarks/colmena_rtt/config.py

Note: this is a fork of
    https://github.com/exalearn/colmena/tree/master/demo_apps/synthetic-data
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from threading import Event
from typing import NamedTuple
from typing import Sequence

import globus_compute_sdk
from colmena.models import Result
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues
from colmena.queue.redis import RedisQueues
from colmena.task_server.base import BaseTaskServer
from colmena.task_server.globus import GlobusComputeTaskServer
from colmena.task_server.parsl import ParslTaskServer
from colmena.thinker import agent
from colmena.thinker import BaseThinker
from proxystore.proxy import Proxy
from proxystore.store import unregister_store
from proxystore.store.base import Store
from proxystore.store.utils import get_key

from psbench.argparse import add_logging_options
from psbench.argparse import add_proxystore_options
from psbench.benchmarks.colmena_rtt.config import get_config
from psbench.logging import init_logging
from psbench.logging import TESTING_LOG_LEVEL
from psbench.proxystore import init_store_from_args
from psbench.results import CSVLogger

logger = logging.getLogger('colmena-rtt')


class TaskStats(NamedTuple):
    """Stats from an individual task. Represents a row in the output CSV."""

    task_id: str
    method: str
    success: bool
    input_size_bytes: int
    output_size_bytes: int
    proxystore_backend: str
    # Defined in colmena.models.Timestamps
    time_created: float
    time_input_received: float
    time_compute_started: float
    time_compute_ended: float
    time_result_sent: float
    time_result_received: float
    time_start_task_submission: float
    time_task_received: float
    # Defined in colmena.models.TimeSpans
    time_running: float
    time_serialize_inputs: float
    time_deserialize_inputs: float
    time_serialize_results: float
    time_deserialize_results: float
    time_async_resolve_proxies: float

    @classmethod
    def from_result(
        cls,
        result: Result,
        input_size_bytes: int,
        output_size_bytes: int,
        proxystore_backend: str,
    ) -> TaskStats:
        """Construct a TaskStats instance from a Colmena result."""
        kwargs = {
            'task_id': result.task_id,
            'method': result.method,
            'success': result.success,
            'input_size_bytes': input_size_bytes,
            'output_size_bytes': output_size_bytes,
            'proxystore_backend': proxystore_backend,
        }
        for field in result.timestamp.__fields_set__:
            if f'time_{field}' in cls._fields:  # pragma: no branch
                kwargs[f'time_{field}'] = getattr(result.timestamp, field)
        for field in result.time.__fields_set__:
            if f'time_{field}' in cls._fields:  # pragma: no branch
                kwargs[f'time_{field}'] = getattr(result.time, field)
        return cls(**kwargs)


class Thinker(BaseThinker):
    """Benchmark Thinker.

    Executes matrix of tasks based on parameters synchronously (i.e., one
    task is executes and completes before the next one is created).
    """

    def __init__(
        self,
        queues: ColmenaQueues,
        store: Store | None,
        input_sizes_bytes: list[int],
        output_sizes_bytes: list[int],
        task_repeat: int,
        task_sleep: float,
        reuse_inputs: bool,
    ) -> None:
        """Init Thinker."""
        super().__init__(queues)
        self.store = store
        self.input_sizes_bytes = input_sizes_bytes
        self.output_sizes_bytes = output_sizes_bytes
        self.task_repeat = task_repeat
        self.task_sleep = task_sleep
        self.reuse_inputs = reuse_inputs

        self.results: list[TaskStats] = []
        self.alternator = Event()

    @agent
    def consumer(self) -> None:
        """Process and save task results."""
        expected_tasks = (
            self.task_repeat
            * len(self.input_sizes_bytes)
            * len(self.output_sizes_bytes)
        )
        for _ in range(expected_tasks):
            result = self.queues.get_result(topic='generate')
            if not result.success:  # pragma: no cover
                raise ValueError(f'Failure in task: {result}.')
            value = result.value
            if isinstance(value, Proxy) and self.store is not None:
                self.store.evict(get_key(value))

            task_stats = TaskStats.from_result(
                result,
                result.task_info['input_size'],
                result.task_info['output_size'],
                (
                    self.store.connector.__class__.__name__
                    if self.store is not None
                    else ''
                ),
            )
            logger.log(
                TESTING_LOG_LEVEL,
                f'Task {result.task_id} completed.\n{task_stats}',
            )
            self.results.append(task_stats)
            self.alternator.set()

    @agent
    def producer(self) -> None:
        """Execute tasks as ready."""
        for input_size in self.input_sizes_bytes:
            if self.reuse_inputs:
                input_data = generate_bytes(input_size)
            for output_size in self.output_sizes_bytes:
                for _ in range(self.task_repeat):
                    if self.done.is_set():  # pragma: no cover
                        break
                    if not self.reuse_inputs:
                        input_data = generate_bytes(input_size)
                    self.queues.send_inputs(
                        input_data,
                        output_size,
                        self.task_sleep,
                        method='target_function',
                        topic='generate',
                        task_info={
                            'input_size': input_size,
                            'output_size': output_size,
                        },
                    )
                    self.alternator.wait()
                    self.alternator.clear()


def generate_bytes(size: int) -> bytes:
    """Generate random bytes."""
    return os.urandom(size)


def target_function(
    data: bytes,
    output_size_bytes: int,
    sleep: float = 0,
) -> bytes:
    """Colmena target function.

    Args:
        data (bytes): input data (may be a proxy).
        output_size_bytes (int): size of data to return.
        sleep (float): sleep (seconds) to simulate work.

    Returns:
        bytes with size output_size_bytes.
    """
    import time

    from proxystore.proxy import Proxy
    from proxystore.store import get_store
    from proxystore.store.utils import get_key

    # Check that proxy acts as the wrapped np object
    assert isinstance(data, bytes)

    if isinstance(data, Proxy):  # pragma: no cover
        store = get_store(data)
        if store is not None:
            store.evict(get_key(data))

    time.sleep(sleep)  # simulate additional work

    return generate_bytes(output_size_bytes)


def main(argv: Sequence[str] | None = None) -> int:
    """Benchmark entrypoint."""
    argv = argv if argv is not None else sys.argv[1:]

    parser = argparse.ArgumentParser(
        description='Template benchmark.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    backend_group = parser.add_mutually_exclusive_group(required=True)
    backend_group.add_argument(
        '--globus-compute',
        action='store_true',
        help='Use the Globus Compute Colmena Task Server',
    )
    backend_group.add_argument(
        '--parsl',
        action='store_true',
        help='Use the Parsl Colmena Task Server',
    )

    globus_compute_group = parser.add_argument_group()
    globus_compute_group.add_argument(
        '--endpoint',
        required='--globus-compute' in sys.argv,
        help='Globus Compute endpoint for task execution',
    )

    task_group = parser.add_argument_group()
    task_group.add_argument(
        '--redis-host',
        default=None,
        help='Hostname for Colmena RedisQueue',
    )
    task_group.add_argument(
        '--redis-port',
        default=None,
        help='Port for Colmena PipeQueue',
    )
    task_group.add_argument(
        '--input-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task input sizes [bytes]',
    )
    task_group.add_argument(
        '--output-sizes',
        type=int,
        nargs='+',
        required=True,
        help='Task output sizes [bytes]',
    )
    task_group.add_argument(
        '--task-repeat',
        type=int,
        default=1,
        help='Number of time to repeat each task configuration',
    )
    task_group.add_argument(
        '--task-sleep',
        type=float,
        default=0,
        help='Sleep time for tasks',
    )
    task_group.add_argument(
        '--reuse-inputs',
        action='store_true',
        default=False,
        help='Send the same input to each task',
    )
    task_group.add_argument(
        '--output-dir',
        type=str,
        default='runs',
        help='Colmena run output directory',
    )

    add_logging_options(parser)
    add_proxystore_options(parser, required=False)
    args = parser.parse_args(argv)

    init_logging(args.log_file, args.log_level, force=True)
    store = init_store_from_args(args, metrics=True)

    output_dir = os.path.join(
        args.output_dir,
        datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'),
    )
    os.makedirs(output_dir, exist_ok=True)

    # Make the queues
    if (
        args.redis_host is not None or args.redis_port is not None
    ):  # pragma: no cover
        queues = RedisQueues(
            topics=['generate'],
            hostname=args.redis_host,
            port=args.redis_port,
            serialization_method='pickle',
            keep_inputs=False,
            proxystore_name=None if store is None else store.name,
            proxystore_threshold=0,
        )
    else:
        queues = PipeQueues(
            topics=['generate'],
            serialization_method='pickle',
            keep_inputs=False,
            proxystore_name=None if store is None else store.name,
            proxystore_threshold=0,
        )

    doer: BaseTaskServer
    if args.globus_compute:
        doer = GlobusComputeTaskServer(
            {target_function: args.endpoint},
            globus_compute_sdk.Client(),
            queues,
        )
    elif args.parsl:
        config = get_config(output_dir)
        doer = ParslTaskServer([target_function], queues, config)
    else:
        raise AssertionError(
            '--globus-compute and --parsl are part of a required mutex group.',
        )

    thinker = Thinker(
        queues=queues,
        store=store,
        input_sizes_bytes=args.input_sizes,
        output_sizes_bytes=args.output_sizes,
        task_repeat=args.task_repeat,
        task_sleep=args.task_sleep,
        reuse_inputs=args.reuse_inputs,
    )

    try:
        # Launch the servers
        doer.start()
        thinker.start()
        logging.log(TESTING_LOG_LEVEL, 'launched thinker and task servers')

        # Wait for the task generator to complete
        thinker.join()
        logging.log(TESTING_LOG_LEVEL, 'thinker completed')
    finally:
        queues.send_kill_signal()

    # Wait for the task server to complete
    doer.join()
    logging.info(TESTING_LOG_LEVEL, 'task server completed')

    if args.csv_file is not None and len(thinker.results) > 0:
        with CSVLogger(args.csv_file, TaskStats) as csv_logger:
            for result in thinker.results:
                csv_logger.log(result)

    if store is not None:
        store.close()
        unregister_store(store)
        logger.log(TESTING_LOG_LEVEL, f'cleaned up {store.name}')

    return 0
