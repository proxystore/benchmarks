from __future__ import annotations

import psbench
import testing  # noqa: F401


def test_version() -> None:
    assert isinstance(psbench.__version__, str)
