from __future__ import annotations

import logging
import os
import pathlib
from typing import NamedTuple

import pytest

from psbench.logging import CSVLogger
from psbench.logging import init_logging
from psbench.logging import make_parent_dirs
from psbench.logging import TESTING_LOG_LEVEL


class Data(NamedTuple):
    time: float
    value: int
    result: str


def test_csv_logger_basic(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    logger = CSVLogger(filepath, Data)

    logger.log(Data(1.0, 2, '3'))
    logger.log(Data(4.0, 5, '6'))

    logger.close()

    with open(filepath) as f:
        data = f.readlines()

    assert len(data) == 3
    assert 'time' in data[0]
    assert 'value' in data[0]
    assert 'result' in data[0]
    assert '1.0' in data[1]


def test_csv_logger_append(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    logger = CSVLogger(filepath, Data)
    logger.log(Data(1.0, 2, '3'))
    logger.close()

    logger = CSVLogger(filepath, Data)
    logger.log(Data(4.0, 5, '6'))
    logger.log(Data(7.0, 8, '9'))
    logger.close()

    with open(filepath) as f:
        assert len(f.readlines()) == 4


def test_csv_logger_mismatch_headers(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    logger = CSVLogger(filepath, Data)
    logger.log(Data(1.0, 2, '3'))
    logger.close()

    class _OtherData(NamedTuple):
        x: int

    with pytest.raises(ValueError):
        CSVLogger(filepath, _OtherData)


def test_logging_no_file() -> None:
    init_logging()

    logger = logging.getLogger()
    logger.log(TESTING_LOG_LEVEL, 'test')


def test_logging_with_file(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.txt')

    init_logging(filepath)

    logger = logging.getLogger()
    logger.info('test')


def test_make_parent_dirs(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'parent' / 'file')
    assert not os.path.exists(filepath)
    make_parent_dirs(filepath)
    assert os.path.isdir(os.path.join(str(tmp_path), 'parent'))

    # Check idempotency
    make_parent_dirs(filepath)
