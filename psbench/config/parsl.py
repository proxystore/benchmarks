from __future__ import annotations

import multiprocessing

from parsl.addresses import address_by_hostname
from parsl.addresses import address_by_interface
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.launchers import MpiExecLauncher
from parsl.providers import LocalProvider


def get_thread_config(
    run_dir: str,
    workers: int | None = None,
) -> Config:
    workers = workers if workers is not None else multiprocessing.cpu_count()
    executor = ThreadPoolExecutor(max_threads=workers)
    return Config(executors=[executor], run_dir=run_dir)


def get_htex_local_config(
    run_dir: str,
    workers: int | None = None,
) -> Config:
    workers = workers if workers is not None else multiprocessing.cpu_count()
    executor = HighThroughputExecutor(
        label='htex-local',
        max_workers_per_node=workers,
        address=address_by_hostname(),
        cores_per_worker=1,
        provider=LocalProvider(
            channel=LocalChannel(),
            init_blocks=1,
            max_blocks=1,
        ),
    )
    return Config(executors=[executor], run_dir=run_dir)


def get_htex_polaris_headless(
    run_dir: str,
    workers: int | None = None,
) -> Config:
    # Polaris nodes have 32 physical cores so if the user passes in
    # more than 32 workers we know there are multiple nodes
    # and to set the max per node to 32.
    workers = workers if workers is not None else 32
    workers_per_node = min(workers, 32)
    executor = HighThroughputExecutor(
        label='htex-polaris-headless',
        max_workers_per_node=workers_per_node,
        address=address_by_interface('bond0'),
        cpu_affinity='block-reverse',
        prefetch_capacity=0,
        provider=LocalProvider(
            channel=LocalChannel(),
            launcher=MpiExecLauncher(
                bind_cmd='--cpu-bind',
                overrides='--depth=64 --ppn 1',
            ),
            cmd_timeout=120,
            nodes_per_block=max(1, workers // 32),
            init_blocks=1,
            max_blocks=1,
        ),
    )
    return Config(executors=[executor], run_dir=run_dir)


CONFIG_FACTORY = {
    'thread': get_thread_config,
    'htex-local': get_htex_local_config,
    'htex-polaris-headless': get_htex_polaris_headless,
}
