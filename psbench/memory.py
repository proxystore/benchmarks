from __future__ import annotations

import pathlib
import threading
import time
from typing import NamedTuple

import psutil

from psbench.results import CSVLogger


class MemoryUsage(NamedTuple):
    unix_timestamp: float
    total_bytes: int
    available_bytes: int
    used_bytes: int
    free_bytes: int

    @classmethod
    def from_current_system_usage(cls) -> MemoryUsage:
        timestamp = time.time()
        usage = psutil.virtual_memory()
        return cls(
            unix_timestamp=timestamp,
            total_bytes=usage.total,
            available_bytes=usage.available,
            used_bytes=usage.used,
            free_bytes=usage.free,
        )


class SystemMemoryProfiler(threading.Thread):
    def __init__(
        self,
        polling_interval_seconds: float = 1.0,
        csv_file: str | pathlib.Path | None = None,
    ):
        self._polling_interval_seconds = polling_interval_seconds
        self._memory_log: list[MemoryUsage] = []
        self._stop_event = threading.Event()
        self._csv_logger = (
            CSVLogger(str(csv_file), MemoryUsage)
            if csv_file is not None
            else None
        )
        super().__init__()

    def get_memory_log(self) -> list[MemoryUsage]:
        return self._memory_log

    def run(self) -> None:
        if self._stop_event.is_set():
            raise RuntimeError(
                'SystemMemoryProfiler has already finished.',
            )

        while not self._stop_event.is_set():
            usage = MemoryUsage.from_current_system_usage()
            self._memory_log.append(usage)
            if self._csv_logger is not None:
                self._csv_logger.log(usage)
            time.sleep(self._polling_interval_seconds)

        if self._csv_logger is not None:
            self._csv_logger.close()

    def stop(self) -> None:
        self._stop_event.set()
