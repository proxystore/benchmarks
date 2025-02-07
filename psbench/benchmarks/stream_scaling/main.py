from __future__ import annotations

import collections
import logging
import os
import shutil
import time
from concurrent.futures import Executor
from concurrent.futures import Future
from typing import Any

from parsl.concurrent import ParslPoolExecutor
from proxystore.proxy import Proxy
from proxystore.store.base import Store
from proxystore.store.future import Future as ProxyFuture
from proxystore.stream import StreamConsumer

from psbench.benchmarks.protocol import ContextManagerAddIn
from psbench.benchmarks.stream_scaling.config import RunConfig
from psbench.benchmarks.stream_scaling.config import RunResult
from psbench.benchmarks.stream_scaling.generator import generator_task
from psbench.benchmarks.stream_scaling.shims import Adios2Subscriber
from psbench.benchmarks.stream_scaling.shims import ConsumerShim
from psbench.config import StreamConfig
from psbench.logging import TEST_LOG_LEVEL

adios_import_error: Exception | None = None
try:
    import adios2
except ImportError as e:  # pragma: no cover
    adios_import_error = e

logger = logging.getLogger('stream-scaling')


def warmup_task() -> None:
    pass


def compute_task(data: bytes, sleep: float) -> None:
    # Resolve data if necessary
    assert isinstance(data, bytes)

    time.sleep(sleep)


def compute_task_adios(
    step: int,
    sleep: float,
    adios_file: str,
    topic: str,
    expected_size: int,
) -> None:
    with adios2.FileReader(adios_file) as reader:
        array = reader.read(topic, step_selection=[step, 1])
        data = array.tobytes()
        assert len(data) == expected_size
        assert isinstance(data, bytes)

    time.sleep(sleep)


def pregenerate(
    size: int,
    interval: float,
    example_size: int = 100_000_000,
    example_time: float = 0.250,
) -> bool:
    # Determine if the data size is too large to generate within a given
    # time interval.
    #
    # Benchmark with QueuePublisher and Store[FileConnector]:
    #   - 1000 x 1MB: 1.8s (556 items/s) (1MB in 1.8 ms)
    #   - 25 x 100MB: 5.7s (4.4 items/s) (100MB in 228 ms)
    # To err on the safe side, I've chose 100 MB in 0.25 s as the
    # example interval. Note that this was on a pretty fast workstation.
    example_rate = example_size / example_time
    expected_time = size / example_rate
    return expected_time > interval


class Benchmark(ContextManagerAddIn):
    name = 'Stream Scaling'
    config_type = RunConfig
    result_type = RunResult

    def __init__(
        self,
        executor: Executor,
        store: Store[Any],
        stream_config: StreamConfig,
    ) -> None:
        self.executor = executor
        self.store = store
        self.stream_config = stream_config
        super().__init__([self.executor, self.store])

    def config(self) -> dict[str, Any]:
        return {
            'executor': self.executor.__class__.__name__,
            'connector': self.store.connector.__class__.__name__,
            'stream-config': self.stream_config,
        }

    def run(self, config: RunConfig) -> RunResult:
        if (
            config.method == 'adios' and adios_import_error is not None
        ):  # pragma: no cover
            raise adios_import_error

        compute_workers = config.max_workers - 1
        producer_interval = config.task_sleep / compute_workers
        pregen_data = pregenerate(config.data_size_bytes, producer_interval)
        logger.log(TEST_LOG_LEVEL, f'Compute workers: {compute_workers}')
        logger.log(TEST_LOG_LEVEL, 'Generator workers: 1')
        logger.log(
            TEST_LOG_LEVEL,
            f'Generator item interval: {producer_interval}',
        )

        logger.log(TEST_LOG_LEVEL, 'Submitting warm up task')
        self.executor.submit(warmup_task).result()
        logger.log(TEST_LOG_LEVEL, 'Warmup task completed')

        stop_generator: ProxyFuture[bool] = self.store.future()
        generator_task_future = self.executor.submit(
            generator_task,
            run_config=config,
            store_config=self.store.config(),
            stream_config=self.stream_config,
            stop_generator=stop_generator,
            pregenerate=pregen_data,
            interval=producer_interval,
        )
        logger.log(
            TEST_LOG_LEVEL,
            'Submitted generator task: '
            f'item_size_bytes={config.data_size_bytes}, '
            f'max_items={config.task_count}, '
            f'interval_seconds={producer_interval}, '
            f'pregenerate={pregen_data}, '
            f'method={config.method}',
        )
        completed_tasks = 0
        running_tasks: collections.deque[Future[bytes] | Future[None]] = (
            collections.deque()
        )

        # The type of consumer is almost that of the Subscriber protocol but
        # the Adios2Subscriber is slightly different.
        consumer: Any
        if config.method in ('default', 'proxy'):
            subscriber = self.stream_config.get_subscriber()
            assert subscriber is not None
            base_consumer = StreamConsumer[bytes](subscriber)
            consumer = ConsumerShim(
                base_consumer,
                direct_from_subscriber=config.method == 'default',
            )
        elif config.method == 'adios':
            waited = 0
            while True:
                if waited > 60:  # pragma: no cover
                    raise RuntimeError('Timeout waiting for ADIOS file.')
                if os.path.exists(config.adios_file):
                    break
                time.sleep(1)
                waited += 1

            consumer = Adios2Subscriber(
                config.adios_file,
                topic=self.stream_config.topic,
                direct=False,
            )
        else:
            raise AssertionError(f'Unsupported method {config.method}.')

        start = time.time()

        try:
            for i, item in enumerate(consumer):
                if isinstance(self.executor, ParslPoolExecutor) and isinstance(
                    item,
                    Proxy,
                ):  # pragma: no cover
                    # Quick hack because Parsl will accidentally resolve
                    # proxy when it scans tasks inputs for any special
                    # files.
                    item.__proxy_wrapped__ = None
                if config.method == 'adios':
                    task_future = self.executor.submit(
                        compute_task_adios,
                        item,
                        sleep=config.task_sleep,
                        adios_file=config.adios_file,
                        topic=self.stream_config.topic,
                        expected_size=config.data_size_bytes,
                    )
                else:
                    task_future = self.executor.submit(
                        compute_task,
                        item,
                        sleep=config.task_sleep,
                    )
                logger.log(
                    TEST_LOG_LEVEL,
                    f'Submitted compute task {i + 1}/{config.task_count}',
                )
                running_tasks.append(task_future)

                # Only start waiting on old tasks once we've filled the
                # available compute workers.
                if len(running_tasks) >= compute_workers:
                    oldest_task_future = running_tasks.popleft()
                    oldest_task_future.result()
                    completed_tasks += 1
        except KeyboardInterrupt:  # pragma: no cover
            logger.warning(
                'Caught KeyboardInterrupt... sending stop signal to generator',
            )
            stop_generator.set_result(True)
        finally:
            logger.log(TEST_LOG_LEVEL, 'Waiting on generator task')
            generator_task_future.result()

        logger.log(
            TEST_LOG_LEVEL,
            'Finished submitting new compute tasks',
        )

        consumer.close()

        logger.log(TEST_LOG_LEVEL, 'Waiting on generator task')
        generator_task_future.result()

        # Wait on remaining tasks. There should be compute_workers number
        # of tasks remaining unless a KeyboardInterrupt occurred.
        logger.log(TEST_LOG_LEVEL, 'Waiting on outstanding compute tasks')
        while len(running_tasks) > 0:
            task = running_tasks.popleft()
            task.result()
            completed_tasks += 1

        logger.log(TEST_LOG_LEVEL, 'All compute tasks finished')

        if config.method == 'adios':
            shutil.rmtree(config.adios_file)

        end = time.time()

        assert self.stream_config.kind is not None
        return RunResult(
            executor=self.executor.__class__.__name__,
            connector=self.store.connector.__class__.__name__,
            stream=self.stream_config.kind,
            data_size_bytes=config.data_size_bytes,
            task_count=config.task_count,
            task_sleep=config.task_sleep,
            method=config.method,
            workers=config.max_workers,
            completed_tasks=completed_tasks,
            start_submit_tasks_timestamp=start,
            end_tasks_done_timestamp=end,
        )
