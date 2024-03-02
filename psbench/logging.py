from __future__ import annotations

import logging
import sys
from typing import Any

from psbench.utils import make_parent_dirs

TESTING_LOG_LEVEL = 25


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
            handler. This option is useful to silencing the third-party
            package logging. Note: should not be set when running inside
            pytest (default: False).
    """
    logging.addLevelName(TESTING_LOG_LEVEL, 'TESTING')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)

    handlers: list[logging.Handler] = [stdout_handler]
    if logfile is not None:
        make_parent_dirs(logfile)
        handler = logging.FileHandler(logfile)
        handler.setLevel(logging.INFO)
        handlers.append(handler)

    kwargs: dict[str, Any] = {}
    if force:  # pragma: no cover
        kwargs['force'] = force

    logging.basicConfig(
        format=(
            '[%(asctime)s.%(msecs)03d] %(levelname)-5s (%(name)s) :: '
            '%(message)s'
        ),
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG,
        handlers=handlers,
        **kwargs,
    )
