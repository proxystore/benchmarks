"""Colmena round trip task time benchmark.

Tests round trip task times in Colmena with configurable backends,
ProxyStore methods, payload sizes, etc. Colmena additionally requires
Redis which is not installed when installing the psbench package.

Note: this is a fork of
    https://github.com/exalearn/colmena/tree/master/demo_apps/synthetic-data
"""

from __future__ import annotations

import logging
import os
import sys
from threading import Event
from typing import Any

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    pass
else:  # pragma: <3.11 cover
    pass

import globus_compute_sdk
from colmena.queue.base import ColmenaQueues
from colmena.queue.python import PipeQueues
from colmena.queue.redis import RedisQueues
from colmena.task_server.base import BaseTaskServer
from colmena.task_server.globus import GlobusComputeTaskServer
from colmena.task_server.parsl import ParslTaskServer
from colmena.thinker import agent
from colmena.thinker import BaseThinker
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.utils import get_key

from psbench.benchmarks.colmena_rtt.config import RunConfig
from psbench.benchmarks.colmena_rtt.config import RunResult
from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.config.executor import GlobusComputeConfig
from psbench.config.executor import ParslConfig
from psbench.logging import TEST_LOG_LEVEL

logger = logging.getLogger('colmena-rtt')


class Thinker(BaseThinker):
    def __init__(
        self,
        queues: ColmenaQueues,
        store: Store[Any] | None,
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

        self.results: list[RunResult] = []
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
            assert result is not None
            if not result.success:  # pragma: no cover
                raise ValueError(f'Failure in task: {result}.')
            value = result.value
            if isinstance(value, Proxy) and self.store is not None:
                self.store.evict(get_key(value))

            assert result.task_info is not None
            task_stats = RunResult.from_result(
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
                TEST_LOG_LEVEL,
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
                for i in range(self.task_repeat):
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
                    logger.log(
                        TEST_LOG_LEVEL,
                        f'Submitted task {i+1}/{self.task_repeat} '
                        f'(input_size={input_size}, '
                        f'output_size={output_size}, '
                        f'sleep={self.task_sleep}).',
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


class Benchmark(ContextManagerAddIn):
    name = 'Colmena RTT'
    config_type = RunConfig
    result_type = RunResult

    def __init__(
        self,
        executor_config: GlobusComputeConfig | ParslConfig,
        store: Store[Any] | None = None,
        redis_host: str | None = None,
        redis_port: int | None = None,
        repeat: int = 1,
    ) -> None:
        self.executor_config = executor_config
        self.store = store
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.repeat = repeat
        super().__init__(managers=[self.store])

    def config(self) -> dict[str, Any]:
        executor = (
            'Globus Compute'
            if isinstance(self.executor_config, GlobusComputeConfig)
            else 'Parsl'
        )
        connector = (
            'None'
            if self.store is None
            else self.store.connector.__class__.__name__
        )
        return {
            'executor': executor,
            'connector': connector,
            'redis_host': self.redis_host,
            'redis_port': self.redis_port,
            'repeat': self.repeat,
        }

    def run(self, config: RunConfig) -> list[RunResult]:
        # Make the queues
        queues: ColmenaQueues
        if (
            self.redis_host is not None and self.redis_port is not None
        ):  # pragma: no cover
            queues = RedisQueues(
                topics=['generate'],
                hostname=self.redis_host,
                port=self.redis_port,
                serialization_method='pickle',
                keep_inputs=False,
                proxystore_name=None
                if self.store is None
                else self.store.name,
                proxystore_threshold=0,
            )
        else:
            queues = PipeQueues(
                topics=['generate'],
                serialization_method='pickle',
                keep_inputs=False,
                proxystore_name=None
                if self.store is None
                else self.store.name,
                proxystore_threshold=0,
            )

        doer: BaseTaskServer
        if isinstance(self.executor_config, GlobusComputeConfig):
            doer = GlobusComputeTaskServer(
                {target_function: self.executor_config.endpoint},
                globus_compute_sdk.Client(),
                queues,
            )
        elif isinstance(self.executor_config, ParslConfig):
            parsl_config = self.executor_config.get_config()
            doer = ParslTaskServer([target_function], queues, parsl_config)
        else:
            raise AssertionError('Unreachable.')

        thinker = Thinker(
            queues=queues,
            store=self.store,
            input_sizes_bytes=config.input_sizes,
            output_sizes_bytes=config.output_sizes,
            task_repeat=self.repeat,
            task_sleep=config.task_sleep,
            reuse_inputs=config.reuse_inputs,
        )

        try:
            # Launch the servers
            doer.start()
            thinker.start()
            logging.log(TEST_LOG_LEVEL, 'Launched thinker and task servers')

            # Wait for the task generator to complete
            thinker.join()
            logging.log(TEST_LOG_LEVEL, 'Thinker completed')
        finally:
            queues.send_kill_signal()

        # Wait for the task server to complete
        doer.join()
        logging.log(TEST_LOG_LEVEL, 'Task server completed')

        return thinker.results
