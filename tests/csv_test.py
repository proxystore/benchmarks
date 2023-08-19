from __future__ import annotations

import dataclasses
import pathlib
from typing import NamedTuple

import pytest

from psbench.csv import CSVLogger
from psbench.csv import field_names


@dataclasses.dataclass
class DataDC:
    time: float
    value: int
    result: str


class DataNT(NamedTuple):
    time: float
    value: int
    result: str


def test_field_names_dataclass() -> None:
    data = DataDC(1.0, 1, '')
    assert list(field_names(data)) == ['time', 'value', 'result']
    assert list(field_names(DataDC)) == ['time', 'value', 'result']


def test_field_names_namedtuple() -> None:
    data = DataNT(1.0, 1, '')
    assert list(field_names(data)) == ['time', 'value', 'result']
    assert list(field_names(DataNT)) == ['time', 'value', 'result']


def test_csv_logger_basic(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    with CSVLogger(filepath, DataNT) as logger:
        logger.log(DataNT(1.0, 2, '3'))
        logger.log(DataNT(4.0, 5, '6'))

    with open(filepath) as f:
        data = f.readlines()

    assert len(data) == 3
    assert 'time' in data[0]
    assert 'value' in data[0]
    assert 'result' in data[0]
    assert '1.0' in data[1]


def test_csv_logger_append(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    logger1 = CSVLogger(filepath, DataNT)
    logger1.log(DataNT(1.0, 2, '3'))
    logger1.close()

    logger2 = CSVLogger(filepath, DataDC)
    logger2.log(DataDC(4.0, 5, '6'))
    logger2.log(DataDC(7.0, 8, '9'))
    logger2.close()

    with open(filepath) as f:
        assert len(f.readlines()) == 4


def test_csv_logger_mismatch_headers(tmp_path: pathlib.Path) -> None:
    filepath = str(tmp_path / 'log.csv')
    logger = CSVLogger(filepath, DataNT)
    logger.log(DataNT(1.0, 2, '3'))
    logger.close()

    class _OtherData(NamedTuple):
        x: int

    with pytest.raises(ValueError):
        CSVLogger(filepath, _OtherData)
