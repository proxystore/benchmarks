from __future__ import annotations

import csv
import logging
import os
import sys
import warnings
from typing import Any
from typing import Generic
from typing import NamedTuple
from typing import TypeVar

TESTING_LOG_LEVEL = 25

DTYPE = TypeVar('DTYPE', bound=NamedTuple)


class CSVLogger(Generic[DTYPE]):
    """CSV logger where rows are represented as a NamedTuple."""

    def __init__(self, filepath: str, data_type: type[DTYPE]) -> None:
        """Init CSVLogger."""
        has_headers = False
        if os.path.isfile(filepath):
            with open(filepath) as f:
                header_row = f.readline()
                headers = [h.strip() for h in header_row.split(',')]
                if set(headers) != set(data_type._fields):
                    raise ValueError(
                        f'File {filepath} already exists and its headers '
                        f'do not match {data_type._fields}.',
                    )
                has_headers = True

        make_parent_dirs(filepath)
        self.f = open(filepath, 'a', newline='')
        self.writer = csv.DictWriter(self.f, fieldnames=data_type._fields)
        if not has_headers:
            self.writer.writeheader()

    def log(self, data: DTYPE) -> None:
        """Log new row."""
        self.writer.writerow(data._asdict())

    def close(self) -> None:
        """Close file handles."""
        self.f.close()


def init_logging(
    logfile: str | None = None,
    level: int | str = logging.INFO,
    force: bool = False,
) -> None:
    """Initialize logging with custom formats.

    Adds a custom log level TESTING which is higher than INFO and lower
    than WARNING.

    Usage:
        >>> logger = init_logger(...)
        >>> logger.log(TESTING_LOG_LEVEL, 'testing log message')

    Args:
        logfile (str): option filepath to write log to (default: None).
        level (int, str): minimum logging level (default: INFO).
        force (bool): remove any existing handlers attached to the root
            handler. Only available in Python 3.8 and later. This option is
            useful to silencing the third-party package logging. Note:
            should not be set when running inside pytest (default: False).
    """
    logging.addLevelName(TESTING_LOG_LEVEL, 'TESTING')

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if logfile is not None:
        make_parent_dirs(logfile)
        handlers.append(logging.FileHandler(logfile))

    kwargs: dict[str, Any] = {}
    if force:  # pragma: no cover
        if sys.version_info >= (3, 8):
            kwargs['force'] = force
        else:
            warnings.warn(
                'The force argument is only available in Python 3.8 and later',
            )

    logging.basicConfig(
        format=(
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=level,
        handlers=handlers,
        **kwargs,
    )


def make_parent_dirs(filepath: str) -> None:
    """Make parent directories of a filepath."""
    parent_dir = os.path.dirname(filepath)
    if len(parent_dir) > 0 and not os.path.isdir(parent_dir):
        os.makedirs(parent_dir, exist_ok=True)
