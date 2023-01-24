from __future__ import annotations

import csv
import dataclasses
import os
import sys
from typing import Any
from typing import cast
from typing import ClassVar
from typing import Generic
from typing import NamedTuple
from typing import overload
from typing import Sequence
from typing import TypeVar
from typing import Union

if sys.version_info >= (3, 8):  # pragma: >=3.8 cover
    from typing import Protocol
    from typing import runtime_checkable
else:  # pragma: <3.8 cover
    from typing_extensions import Protocol
    from typing_extensions import runtime_checkable

from psbench.utils import make_parent_dirs


# https://github.com/python/mypy/issues/14029
# for mypy >=0.990
@runtime_checkable
class NewDataClassProtocol(Protocol):
    """Dataclass Protocol Type."""

    __dataclass_fields__: ClassVar[dict[str, Any]]


# for mypy <0.990
@runtime_checkable
class OldDataClassProtocol(Protocol):
    """Dataclass Protocol Type."""

    __dataclass_fields__: dict[str, Any]


@runtime_checkable
class NamedTupleProtocol(Protocol):
    """NamedTuple Protocol Type."""

    _fields: tuple[str, Any]

    def _asdict(self) -> dict[str, Any]:
        ...


DTYPE = TypeVar(
    'DTYPE',
    bound=Union[OldDataClassProtocol, NewDataClassProtocol, NamedTuple],
)


class CSVLogger(Generic[DTYPE]):
    """CSV logger where rows are represented as a NamedTuple."""

    def __init__(self, filepath: str, data_type: type[DTYPE]) -> None:
        """Init CSVLogger."""
        has_headers = False
        fields = field_names(data_type)
        if os.path.isfile(filepath):
            with open(filepath) as f:
                header_row = f.readline()
                headers = [h.strip() for h in header_row.split(',')]
                if header_row != '' and set(headers) != set(fields):
                    raise ValueError(
                        f'File {filepath} already exists and its headers '
                        f'do not match {fields}. Got {headers}.',
                    )
                if header_row != '':
                    has_headers = True

        make_parent_dirs(filepath)
        self.f = open(filepath, 'a', newline='')
        self.writer = csv.DictWriter(self.f, fieldnames=fields)
        if not has_headers:
            self.writer.writeheader()

    def __enter__(self) -> CSVLogger[DTYPE]:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exception_type: Any,
        exception_value: Any,
        traceback: Any,
    ) -> None:
        """Exit context manager."""
        self.close()

    def log(self, data: DTYPE) -> None:
        """Log new row."""
        if isinstance(data, (OldDataClassProtocol, NewDataClassProtocol)):
            self.writer.writerow(dataclasses.asdict(data))
        elif isinstance(data, NamedTupleProtocol):
            cast(NamedTupleProtocol, data)
            self.writer.writerow(data._asdict())
        else:
            raise AssertionError

    def close(self) -> None:
        """Close file handles."""
        self.f.close()


@overload
def field_names(data_type: type[DTYPE]) -> Sequence[str]:
    ...


@overload
def field_names(data_type: DTYPE) -> Sequence[str]:
    ...


def field_names(data_type: DTYPE | type[DTYPE]) -> Sequence[str]:
    """Extract field names from NamedTuple or Dataclass."""
    if isinstance(data_type, (OldDataClassProtocol, NewDataClassProtocol)):
        return [f.name for f in dataclasses.fields(data_type)]
    elif isinstance(data_type, NamedTupleProtocol):
        return data_type._fields
    else:
        raise AssertionError
