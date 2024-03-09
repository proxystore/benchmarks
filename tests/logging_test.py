from __future__ import annotations

import logging
import pathlib

from psbench.logging import init_logging
from psbench.logging import TEST_LOG_LEVEL


def test_logging_no_file() -> None:
    init_logging()

    logger = logging.getLogger()
    logger.log(TEST_LOG_LEVEL, 'test')


def test_logging_with_file(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.txt')

    init_logging(filepath)

    logger = logging.getLogger()
    logger.info('test')
