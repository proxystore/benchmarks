from __future__ import annotations

from parsl.addresses import address_by_hostname
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.executors import ThreadPoolExecutor
from parsl.providers import LocalProvider


def get_thread_config(
    run_dir: str,
    workers: int | None = None,
) -> Config:
    executor = ThreadPoolExecutor(max_threads=workers)
    return Config(executors=[executor], run_dir=run_dir)


def get_htex_local_config(
    run_dir: str,
    workers: int | None = None,
) -> Config:
    executor = HighThroughputExecutor(
        label='htex-local',
        max_workers=workers,
        address=address_by_hostname(),
        cores_per_worker=1,
        provider=LocalProvider(
            channel=LocalChannel(),
            init_blocks=1,
            max_blocks=1,
        ),
    )
    return Config(executors=[executor], run_dir=run_dir)
