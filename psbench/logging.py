from __future__ import annotations

import logging
import sys
import warnings
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
